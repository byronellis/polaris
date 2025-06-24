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

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.InMemoryEntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.EntityResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.persistence.relational.spanner.util.SpannerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("google-cloud-spanner")
public class GoogleCloudSpannerMetaStoreManagerFactory implements MetaStoreManagerFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GoogleCloudSpannerMetaStoreManagerFactory.class);

  final Map<String, PrincipalSecretsGenerator> secretGenerators = new HashMap<>();
  final Map<String, RealmState> realmStateMap = new HashMap<>();
  final Map<String, StorageCredentialCache> storageCredentialCacheMap = new HashMap<>();

  @Inject GoogleCloudSpannerConfiguration googleCloudSpannerConfiguration;
  @Inject PolarisStorageIntegrationProvider polarisStorageIntegrationProvider;

  @Inject PolarisDiagnostics polarisDiagnostics;

  @Inject PolarisConfigurationStore configurationStore;

  protected GoogleCloudSpannerMetaStoreManagerFactory() {}

  protected AtomicReference<Spanner> spannerReference = new AtomicReference<>(null);
  protected AtomicReference<DatabaseId> databaseIdAtomicReference = new AtomicReference<>(null);

  protected Spanner getSpanner() {

    return spannerReference.updateAndGet(
        current -> {
          if (current == null) {
            return SpannerUtil.spannerFromConfiguration(googleCloudSpannerConfiguration);
          }
          return current;
        });
  }

  protected DatabaseId getDatabaseId() {
    return databaseIdAtomicReference.updateAndGet(
        current -> {
          if (current == null) {
            return SpannerUtil.databaseFromConfiguration(googleCloudSpannerConfiguration);
          }
          return current;
        });
  }

  private RealmState getOrCreateRealmState(RealmContext realmContext) {
    return realmStateMap.computeIfAbsent(
        realmContext.getRealmIdentifier(),
        (realmId) -> {
          PolarisMetaStoreManager metaStoreManager = new AtomicOperationMetaStoreManager();
          // For the moment each realm gets its own Spanner connection... we could prob
          return new RealmState(
              metaStoreManager,
              () -> {
                return new GoogleSpannerBasePersistenceImpl(
                    getSpanner(),
                    getDatabaseId(),
                    secretGenerators.get(realmId),
                    polarisStorageIntegrationProvider);
              },
              new InMemoryEntityCache(() -> realmId, configurationStore, metaStoreManager));
        });
  }

  @Override
  public PolarisMetaStoreManager getOrCreateMetaStoreManager(RealmContext realmContext) {
    return getOrCreateRealmState(realmContext).metaStoreManager();
  }

  @Override
  public Supplier<? extends BasePersistence> getOrCreateSessionSupplier(RealmContext realmContext) {
    return getOrCreateRealmState(realmContext).sessionSupplier();
  }

  @Override
  public StorageCredentialCache getOrCreateStorageCredentialCache(RealmContext realmContext) {
    return storageCredentialCacheMap.computeIfAbsent(
        realmContext.getRealmIdentifier(),
        (realmId) -> new StorageCredentialCache(realmContext, configurationStore));
  }

  @Override
  public EntityCache getOrCreateEntityCache(RealmContext realmContext) {
    return getOrCreateRealmState(realmContext).entityCache();
  }

  @Override
  public Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    Map<String, PrincipalSecretsResult> results = new HashMap<>();
    for (String realmId : realms) {
      secretGenerators.put(
          realmId, PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet));

      RealmContext realmContext = () -> realmId;
      RealmState state = getOrCreateRealmState(realmContext);

      BasePersistence session = state.sessionSupplier().get();
      // Make sure we have a realm entry before continuing with the realm creation process
      if (session instanceof GoogleSpannerBasePersistenceImpl) {
        ((GoogleSpannerBasePersistenceImpl) session).bootstrapRealm(realmId);
      }

      PolarisCallContext callCtx =
          new PolarisCallContext(realmContext, session, polarisDiagnostics);
      CallContext restore = CallContext.getCurrentContext();
      CallContext.setCurrentContext(callCtx);

      // Check for the root principal
      EntityResult preliminaryCheck =
          state
              .metaStoreManager()
              .readEntityByName(
                  callCtx,
                  null,
                  PolarisEntityType.PRINCIPAL,
                  PolarisEntitySubType.NULL_SUBTYPE,
                  PolarisEntityConstants.getRootPrincipalName());
      if (preliminaryCheck.isSuccess()) {
        String overrideMessage =
            "It appears this metastore manager has already been bootstrapped. "
                + "To continue bootstrapping, please first purge the metastore with the `purge` command.";
        LOGGER.error("\n\n {} \n\n", overrideMessage);
        throw new IllegalArgumentException(overrideMessage);
      }

      BaseResult result = state.metaStoreManager().bootstrapPolarisService(callCtx);
      if (result.isSuccess()) {
        EntityResult rootPrincipal =
            state
                .metaStoreManager()
                .readEntityByName(
                    callCtx,
                    null,
                    PolarisEntityType.PRINCIPAL,
                    PolarisEntitySubType.NULL_SUBTYPE,
                    PolarisEntityConstants.getRootPrincipalName());
        PrincipalSecretsResult secrets =
            state
                .metaStoreManager()
                .loadPrincipalSecrets(
                    callCtx,
                    PolarisEntity.of(rootPrincipal.getEntity())
                        .getInternalPropertiesAsMap()
                        .get(PolarisEntityConstants.getClientIdPropertyName()));
        results.put(realmId, secrets);
      }
      CallContext.setCurrentContext(restore);
    }
    return results;
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> results = new HashMap<>();
    for (String realmId : realms) {
      RealmContext realmContext = () -> realmId;
      RealmState state = getOrCreateRealmState(realmContext);

      PolarisCallContext callCtx =
          new PolarisCallContext(realmContext, state.sessionSupplier().get(), polarisDiagnostics);
      CallContext restore = CallContext.getCurrentContext();
      CallContext.setCurrentContext(callCtx);
      results.put(realmId, getOrCreateMetaStoreManager(() -> realmId).purge(callCtx));
      CallContext.setCurrentContext(restore);
    }
    return results;
  }

  /**
   * In most of the metastore manager factory implementations, the act of getting/creating the
   * metastore manager, session supplier or entity cache result in the population of the maps for
   * the other two.
   *
   * <p>To make it more obvious that these three always go together put everything in a single map
   * that and create everything all at once.
   */
  protected record RealmState(
      PolarisMetaStoreManager metaStoreManager,
      Supplier<BasePersistence> sessionSupplier,
      EntityCache entityCache) {}
}
