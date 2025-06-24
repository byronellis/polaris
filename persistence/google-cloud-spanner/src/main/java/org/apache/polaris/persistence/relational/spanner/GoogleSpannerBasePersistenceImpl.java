/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.persistence.relational.spanner;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.TransactionRunner;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.BaseMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.IntegrationPersistence;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.persistence.pagination.DonePageToken;
import org.apache.polaris.core.persistence.pagination.HasPageOffset;
import org.apache.polaris.core.persistence.pagination.HasPageSize;
import org.apache.polaris.core.persistence.pagination.LimitPageToken;
import org.apache.polaris.core.persistence.pagination.OffsetLimitPageToken;
import org.apache.polaris.core.persistence.pagination.Page;
import org.apache.polaris.core.persistence.pagination.PageToken;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.persistence.relational.spanner.model.Entity;
import org.apache.polaris.persistence.relational.spanner.model.GrantRecord;
import org.apache.polaris.persistence.relational.spanner.model.PrincipalAuthenticationData;
import org.apache.polaris.persistence.relational.spanner.model.Realm;
import org.apache.polaris.persistence.relational.spanner.util.SpannerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleSpannerBasePersistenceImpl implements BasePersistence, IntegrationPersistence {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GoogleSpannerBasePersistenceImpl.class);
  private static final Statement GET_NEXT_ID_STMT =
      Statement.of("SELECT GET_NEXT_SEQUENCE_VALUE(SEQUENCE Ids) AS next_value");

  private static final String ENTITY_TABLE_NAME = "Entities";

  private final Spanner spanner;

  private final DatabaseId databaseId;

  private final PrincipalSecretsGenerator secretsGenerator;

  private final PolarisStorageIntegrationProvider storageIntegrationProvider;

  public GoogleSpannerBasePersistenceImpl(
      Spanner spanner,
      DatabaseId databaseId,
      PrincipalSecretsGenerator secretsGenerator,
      PolarisStorageIntegrationProvider storageIntegrationProvider) {
    this.spanner = spanner;
    this.databaseId = databaseId;
    this.secretsGenerator = secretsGenerator;
    this.storageIntegrationProvider = storageIntegrationProvider;
  }

  protected DatabaseClient client() {
    return spanner.getDatabaseClient(databaseId);
  }

  protected TransactionRunner readWriteTransaction() {
    return client().readWriteTransaction();
  }

  @Override
  public long generateNewId(PolarisCallContext callCtx) {
    return Optional.ofNullable(
            readWriteTransaction()
                .run(
                    txn -> {
                      ResultSet result = txn.executeQuery(GET_NEXT_ID_STMT);
                      return result.next() ? result.getLong(0) : null;
                    }))
        .orElseThrow();
  }

  protected void checkEntityVersion(
      TransactionContext txn, String realmId, PolarisEntityCore entity)
      throws RetryOnConcurrencyException {
    // No need to check for a version if the entity is null.
    if (entity == null) {
      return;
    }
    Long currentVersion =
        txn.readRow(
                Entity.TABLE_NAME, Entity.toKey(realmId, entity), ImmutableList.of("EntityVersion"))
            .getLong(0);
    if (currentVersion == null || currentVersion != entity.getEntityVersion()) {
      throw new RetryOnConcurrencyException(
          "Entity '%s' id '%s' concurrently modified; expected version %s",
          entity.getName(), entity.getId(), entity.getEntityVersion());
    }
  }

  protected <T extends PolarisEntityCore> void checkEntityVersions(
      TransactionContext txn, String realmId, List<T> toCheck) throws RetryOnConcurrencyException {
    ResultSet results =
        txn.read(
            Entity.TABLE_NAME,
            SpannerUtil.asKeySet(toCheck.stream().map(e -> Entity.toKey(realmId, e)).toList()),
            ImmutableList.of("Id", "EntityVersion"));

    HashMap<Long, Long> versions = new HashMap<>();
    while (results.next()) {
      versions.put(results.getLong(0), results.getLong(1));
    }
    for (PolarisEntityCore entity : toCheck) {
      Long currentVersion = versions.get(entity.getId());
      if (currentVersion == null || currentVersion != entity.getEntityVersion()) {
        throw new RetryOnConcurrencyException(
            "Entity '%s' id '%s' concurrently modified; expected version %s",
            entity.getName(), entity.getId(), entity.getEntityVersion());
      }
    }
  }

  @Override
  public void writeEntity(
      PolarisCallContext callCtx,
      PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      PolarisBaseEntity originalEntity) {
    if (originalEntity == null) {
      client()
          .write(
              ImmutableList.of(
                  Entity.upsert(callCtx.getRealmContext().getRealmIdentifier(), entity)));
    } else {
      readWriteTransaction()
          .run(
              txn -> {
                checkEntityVersion(
                    txn, callCtx.getRealmContext().getRealmIdentifier(), originalEntity);
                txn.buffer(Entity.upsert(callCtx.getRealmContext().getRealmIdentifier(), entity));
                return null;
              });
    }
  }

  @Override
  public void writeEntities(
      PolarisCallContext callCtx,
      List<PolarisBaseEntity> entities,
      List<PolarisBaseEntity> originalEntities) {
    Function<Integer, PolarisBaseEntity> getOriginalEntity =
        (idx) -> {
          return (originalEntities == null || originalEntities.size() <= idx)
              ? null
              : originalEntities.get(idx);
        };
    readWriteTransaction()
        .run(
            txn -> {
              checkEntityVersions(
                  txn, callCtx.getRealmContext().getRealmIdentifier(), originalEntities);
              for (PolarisBaseEntity entity : entities) {
                txn.buffer(Entity.upsert(callCtx.getRealmContext().getRealmIdentifier(), entity));
              }
              return null;
            });
  }

  @Override
  public void writeToGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {
    client()
        .write(
            ImmutableList.of(
                GrantRecord.upsert(callCtx.getRealmContext().getRealmIdentifier(), grantRec)));
  }

  @Override
  public void deleteEntity(PolarisCallContext callCtx, PolarisBaseEntity entity) {
    client()
        .write(
            ImmutableList.of(
                Entity.delete(callCtx.getRealmContext().getRealmIdentifier(), entity)));
  }

  @Override
  public void deleteFromGrantRecords(PolarisCallContext callCtx, PolarisGrantRecord grantRec) {
    client()
        .write(
            ImmutableList.of(
                Mutation.delete(
                    GrantRecord.TABLE_NAME,
                    GrantRecord.toKey(callCtx.getRealmContext().getRealmIdentifier(), grantRec))));
  }

  @Override
  public void deleteAllEntityGrantRecords(
      PolarisCallContext callCtx,
      PolarisEntityCore entity,
      List<PolarisGrantRecord> grantsOnGrantee,
      List<PolarisGrantRecord> grantsOnSecurable) {}

  @Override
  public void deleteAll(PolarisCallContext callCtx) {
    // Delete the realm entry, which will cause a cascading delete of all other tables
    // as they are interleaved.
    client().write(ImmutableList.of(Realm.delete(callCtx.getRealmContext().getRealmIdentifier())));
  }

  @Override
  public PolarisBaseEntity lookupEntity(
      PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    PolarisBaseEntity entity =
        Entity.fromStruct(
            client()
                .readOnlyTransaction()
                .readRow(
                    Entity.TABLE_NAME,
                    Entity.toKey(callCtx.getRealmContext().getRealmIdentifier(), entityId),
                    Entity.TABLE_COLUMNS));
    // Check the catalogid and typecode and return null if they don't match. However, given that
    // primary key is (realm,id) this should never actually be a failure since the id is never
    // repeated within a realm (and since ids come from a sequence, in reality, id is global)
    if (entity == null || entity.getCatalogId() != catalogId || entity.getTypeCode() != typeCode) {
      return null;
    }
    return entity;
  }

  @Override
  public PolarisBaseEntity lookupEntityByName(
      PolarisCallContext callCtx, long catalogId, long parentId, int typeCode, String name) {
    return Entity.fromStruct(
        client()
            .readOnlyTransaction()
            .readRowUsingIndex(
                Entity.TABLE_NAME,
                Entity.NAME_LOOKUP_INDEX,
                Entity.namedEntityKey(
                    callCtx.getRealmContext().getRealmIdentifier(),
                    catalogId,
                    parentId,
                    typeCode,
                    name),
                Entity.TABLE_COLUMNS));
  }

  protected KeySet entityKeySet(String realmId, Iterable<PolarisEntityId> entityIds) {
    KeySet.Builder keys = KeySet.newBuilder();
    for (PolarisEntityId id : entityIds) {
      keys = keys.addKey(Entity.toKey(realmId, id));
    }
    return keys.build();
  }

  @Override
  public List<PolarisBaseEntity> lookupEntities(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    List<PolarisBaseEntity> entities = new ArrayList<>();
    try (ResultSet results =
        client()
            .singleUse()
            .read(
                Entity.TABLE_NAME,
                entityKeySet(callCtx.getRealmContext().getRealmIdentifier(), entityIds),
                Entity.TABLE_COLUMNS)) {
      while (results.next()) {
        entities.add(Entity.fromStruct(results.getCurrentRowAsStruct()));
      }
    }
    return entities;
  }

  @Override
  public List<PolarisChangeTrackingVersions> lookupEntityVersions(
      PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    List<PolarisChangeTrackingVersions> versions = new ArrayList<>();
    try (ResultSet results =
        client()
            .singleUse()
            .read(
                Entity.TABLE_NAME,
                entityKeySet(callCtx.getRealmContext().getRealmIdentifier(), entityIds),
                ImmutableList.of(Entity.ENTITY_VERSION, Entity.GRANT_RECORDS_VERSION))) {
      while (results.next()) {
        versions.add(
            new PolarisChangeTrackingVersions((int) results.getLong(0), (int) results.getLong(1)));
      }
    }
    return versions;
  }

  @Override
  public Page<EntityNameLookupRecord> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      PageToken pageToken) {
    return listEntities(callCtx, catalogId, parentId, entityType, (e) -> true, pageToken);
  }

  @Override
  public Page<EntityNameLookupRecord> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      Predicate<PolarisBaseEntity> entityFilter,
      PageToken pageToken) {
    return listEntities(
        callCtx,
        catalogId,
        parentId,
        entityType,
        entityFilter,
        EntityNameLookupRecord::new,
        pageToken);
  }

  @Override
  public <T> Page<T> listEntities(
      PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      PolarisEntityType entityType,
      Predicate<PolarisBaseEntity> entityFilter,
      Function<PolarisBaseEntity, T> transformer,
      PageToken pageToken) {
    ArrayList<T> currentPage = new ArrayList<>();

    long rawOffset =
        (pageToken instanceof HasPageOffset) ? ((HasPageOffset) pageToken).getPageOffset() : 0;
    int pageSize =
        (pageToken instanceof HasPageSize)
            ? ((HasPageSize) pageToken).getPageSize()
            : Integer.MAX_VALUE;
    boolean reading = true;
    while (reading) {
      int rawReads = 0;
      try (ResultSet result =
          client()
              .singleUseReadOnlyTransaction()
              .executeQuery(Entity.listEntities(pageToken).build())) {
        while (result.next()) {
          rawReads++;
          PolarisBaseEntity entity = Entity.fromStruct(result.getCurrentRowAsStruct());
          if (entityFilter.test(entity)) {
            currentPage.add(transformer.apply(entity));
          }
        }
        rawOffset += rawReads;
        // If we read less than our page size or we have filled our current page we
        // can stop reading. Otherwise we should keep reading until we fill the page
        if (rawReads < pageSize || currentPage.size() == pageSize) {
          reading = false;
        }
      }
    }
    // if we have returned fewer results than requested or the page size with a limit
    // page token then we should be done. If we are an offset limit page token we should
    // adjust to our new raw offset.
    if (currentPage.size() < pageSize || pageToken instanceof LimitPageToken) {
      pageToken = new DonePageToken();
    } else if (pageToken instanceof OffsetLimitPageToken) {
      pageToken = PageToken.fromLimitWithOffset(pageSize, rawOffset);
    }
    return new Page<>(pageToken, currentPage);
  }

  @Override
  public int lookupEntityGrantRecordsVersion(
      PolarisCallContext callCtx, long catalogId, long entityId) {
    return (int)
        client()
            .singleUse()
            .readRow(
                Entity.TABLE_NAME,
                Entity.toKey(callCtx.getRealmContext().getRealmIdentifier(), entityId),
                ImmutableList.of(Entity.GRANT_RECORDS_VERSION))
            .getLong(0);
  }

  @Override
  public PolarisGrantRecord lookupGrantRecord(
      PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    return GrantRecord.fromStruct(
        client()
            .singleUse()
            .readRow(
                GrantRecord.TABLE_NAME,
                GrantRecord.toKey(
                    callCtx.getRealmContext().getRealmIdentifier(),
                    securableCatalogId,
                    securableId,
                    granteeCatalogId,
                    granteeId,
                    privilegeCode),
                GrantRecord.TABLE_COLUMNS));
  }

  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurable(
      PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    List<PolarisGrantRecord> grants = new ArrayList<>();
    try (ResultSet results =
        client()
            .singleUse()
            .read(
                GrantRecord.TABLE_NAME,
                KeySet.range(
                    GrantRecord.entityGrantKey(
                        callCtx.getRealmContext().getRealmIdentifier(),
                        securableCatalogId,
                        securableId)),
                GrantRecord.TABLE_COLUMNS)) {
      while (results.next()) {
        grants.add(GrantRecord.fromStruct(results.getCurrentRowAsStruct()));
      }
    }
    return grants;
  }

  @Override
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGrantee(
      PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    List<PolarisGrantRecord> grants = new ArrayList<>();
    try (ResultSet results =
        client()
            .singleUse()
            .readUsingIndex(
                GrantRecord.TABLE_NAME,
                GrantRecord.GRANTEE_INDEX_NAME,
                KeySet.singleKey(
                    Key.of(
                        callCtx.getRealmContext().getRealmIdentifier(),
                        granteeCatalogId,
                        granteeId)),
                GrantRecord.TABLE_COLUMNS)) {
      while (results.next()) {
        grants.add(GrantRecord.fromStruct(results.getCurrentRowAsStruct()));
      }
    }
    return grants;
  }

  @Override
  public boolean hasChildren(
      PolarisCallContext callContext,
      PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    // If we get any results here we have at least one child
    return client()
            .singleUse()
            .readRowUsingIndex(
                Entity.TABLE_NAME,
                Entity.CHILDREN_INDEX,
                Key.of(callContext.getRealmContext().getRealmIdentifier(), parentId),
                ImmutableList.of("Id"))
        != null;
  }

  @Override
  public PolarisPrincipalSecrets loadPrincipalSecrets(PolarisCallContext callCtx, String clientId) {
    try (ResultSet results =
        client()
            .singleUse()
            .read(
                PrincipalAuthenticationData.TABLE_NAME,
                KeySet.range(
                    PrincipalAuthenticationData.toKeyRange(
                        callCtx.getRealmContext().getRealmIdentifier(), clientId)),
                PrincipalAuthenticationData.TABLE_COLUMNS)) {
      if (results.next()) {
        return PrincipalAuthenticationData.fromStruct(results.getCurrentRowAsStruct());
      }
      return null;
    }
  }

  @Override
  public PolarisPrincipalSecrets generateNewPrincipalSecrets(
      PolarisCallContext callCtx, String principalName, long principalId) {
    return readWriteTransaction()
        .run(
            txn -> {
              PolarisPrincipalSecrets principalSecrets;
              boolean exists = false;
              do {
                principalSecrets = secretsGenerator.produceSecrets(principalName, principalId);
                exists =
                    txn.readRow(
                            PrincipalAuthenticationData.TABLE_NAME,
                            PrincipalAuthenticationData.toKey(
                                callCtx.getRealmContext().getRealmIdentifier(),
                                principalSecrets.getPrincipalClientId(),
                                principalSecrets.getPrincipalId()),
                            ImmutableList.of("PrincipalId"))
                        != null;
              } while (exists);
              txn.buffer(
                  PrincipalAuthenticationData.upsert(
                      callCtx.getRealmContext().getRealmIdentifier(), principalSecrets));
              return principalSecrets;
            });
  }

  @Override
  public PolarisPrincipalSecrets rotatePrincipalSecrets(
      PolarisCallContext callCtx,
      String clientId,
      long principalId,
      boolean reset,
      String oldSecretHash) {
    return readWriteTransaction()
        .run(
            txn -> {
              PolarisPrincipalSecrets secrets =
                  PrincipalAuthenticationData.fromStruct(
                      txn.readRow(
                          PrincipalAuthenticationData.TABLE_NAME,
                          PrincipalAuthenticationData.toKey(
                              callCtx.getRealmContext().getRealmIdentifier(),
                              clientId,
                              principalId),
                          PrincipalAuthenticationData.TABLE_COLUMNS));
              callCtx
                  .getDiagServices()
                  .checkNotNull(
                      secrets,
                      "cannot_find_secrets",
                      "client_id={} principalId={}",
                      clientId,
                      principalId);
              secrets.rotateSecrets(oldSecretHash);
              if (reset) {
                secrets.rotateSecrets(secrets.getMainSecretHash());
              }
              txn.buffer(
                  PrincipalAuthenticationData.upsert(
                      callCtx.getRealmContext().getRealmIdentifier(), secrets));
              return secrets;
            });
  }

  @Override
  public void deletePrincipalSecrets(
      PolarisCallContext callCtx, String clientId, long principalId) {
    client()
        .write(
            ImmutableList.of(
                PrincipalAuthenticationData.delete(
                    callCtx.getRealmContext().getRealmIdentifier(), clientId, principalId)));
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegration(
          PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return storageIntegrationProvider.getStorageIntegrationForConfig(
        polarisStorageConfigurationInfo);
  }

  @Override
  public <T extends PolarisStorageConfigurationInfo> void persistStorageIntegrationIfNeeded(
      PolarisCallContext callCtx,
      PolarisBaseEntity entity,
      PolarisStorageIntegration<T> storageIntegration) {}

  @Override
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegration(
          PolarisCallContext callContext, PolarisBaseEntity entity) {
    PolarisStorageConfigurationInfo storageConfig =
        BaseMetaStoreManager.extractStorageConfiguration(callContext, entity);
    return storageIntegrationProvider.getStorageIntegrationForConfig(storageConfig);
  }

  protected void bootstrapRealm(String realmId) {
    client().write(ImmutableList.of(Realm.upsert(realmId)));
  }
}
