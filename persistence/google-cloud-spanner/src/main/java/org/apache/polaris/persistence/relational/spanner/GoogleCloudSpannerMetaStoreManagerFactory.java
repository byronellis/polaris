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

import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.config.PolarisConfigurationStore;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.AtomicOperationMetaStoreManager;
import org.apache.polaris.core.persistence.BasePersistence;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PrincipalSecretsGenerator;
import org.apache.polaris.core.persistence.bootstrap.BootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableBootstrapOptions;
import org.apache.polaris.core.persistence.bootstrap.ImmutableSchemaOptions;
import org.apache.polaris.core.persistence.bootstrap.RootCredentialsSet;
import org.apache.polaris.core.persistence.bootstrap.SchemaOptions;
import org.apache.polaris.core.persistence.cache.EntityCache;
import org.apache.polaris.core.persistence.cache.InMemoryEntityCache;
import org.apache.polaris.core.persistence.dao.entity.BaseResult;
import org.apache.polaris.core.persistence.dao.entity.PrincipalSecretsResult;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.persistence.relational.spanner.model.Realm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
@Identifier("google-cloud-spanner")
public class GoogleCloudSpannerMetaStoreManagerFactory implements MetaStoreManagerFactory {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GoogleCloudSpannerMetaStoreManagerFactory.class);

  final Map<String, PolarisMetaStoreManager> metaStoreManagerMap = new HashMap<>();
  final Map<String, EntityCache> entityCacheMap = new HashMap<>();
  final Map<String, Supplier<BasePersistence>> sessionSupplierMap = new HashMap<>();

  @Inject GoogleCloudSpannerConfiguration googleCloudSpannerConfiguration;
  @Inject PolarisStorageIntegrationProvider polarisStorageIntegrationProvider;

  @Inject Clock clock;
  @Inject PolarisDiagnostics polarisDiagnostics;

  @Inject PolarisConfigurationStore configurationStore;

  protected GoogleCloudSpannerMetaStoreManagerFactory() {}

  @Inject protected Consumer<SchemaOptions> schemaInitializer;
  @Inject protected Consumer<RealmContext> realmInitializer;
  @Inject protected Supplier<DatabaseClient> clientSupplier;

  protected PrincipalSecretsGenerator secretsGenerator(
      String realmId, @Nullable RootCredentialsSet rootCredentialsSet) {
    if (rootCredentialsSet != null) {
      return PrincipalSecretsGenerator.bootstrap(realmId, rootCredentialsSet);
    } else {
      return PrincipalSecretsGenerator.RANDOM_SECRETS;
    }
  }

  @Override
  public synchronized PolarisMetaStoreManager getOrCreateMetaStoreManager(
      RealmContext realmContext) {
    if (!metaStoreManagerMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeRealmState(realmContext.getRealmIdentifier(), null);
      checkBootstrapped(realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    }
    return metaStoreManagerMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public BasePersistence getOrCreateSession(RealmContext realmContext) {
    if (!sessionSupplierMap.containsKey(realmContext.getRealmIdentifier())) {
      initializeRealmState(realmContext.getRealmIdentifier(), null);
    }
    checkBootstrapped(realmContext, metaStoreManagerMap.get(realmContext.getRealmIdentifier()));
    return sessionSupplierMap.get(realmContext.getRealmIdentifier()).get();
  }

  @Override
  public EntityCache getOrCreateEntityCache(RealmContext realmContext, RealmConfig realmConfig) {
    if (!entityCacheMap.containsKey(realmContext.getRealmIdentifier())) {
      PolarisMetaStoreManager manager = getOrCreateMetaStoreManager(realmContext);
      entityCacheMap.put(
          realmContext.getRealmIdentifier(), new InMemoryEntityCache(realmConfig, manager));
    }
    return entityCacheMap.get(realmContext.getRealmIdentifier());
  }

  @Override
  public Map<String, PrincipalSecretsResult> bootstrapRealms(
      Iterable<String> realms, RootCredentialsSet rootCredentialsSet) {
    SchemaOptions schemaOptions = ImmutableSchemaOptions.builder().build();
    BootstrapOptions bootstrapOptions =
        ImmutableBootstrapOptions.builder()
            .realms(realms)
            .rootCredentialsSet(rootCredentialsSet)
            .schemaOptions(schemaOptions)
            .build();
    return bootstrapRealms(bootstrapOptions);
  }

  @Override
  public Map<String, PrincipalSecretsResult> bootstrapRealms(BootstrapOptions bootstrapOptions) {
    HashMap<String, PrincipalSecretsResult> results = new HashMap<>();

    boolean schemaInitialized = false;

    final RootCredentialsSet rootCredentialsSet = bootstrapOptions.rootCredentialsSet();
    for (String realmId : bootstrapOptions.realms()) {
      // Some of the tests expect realm bootstrapping to be idempotent with respect to a
      // factory. This may not be what we want, but it's the semantic echoed in all existing
      // persistence implementations.
      if (metaStoreManagerMap.containsKey(realmId)) {
        LOGGER.info("Realm {} has already been bootstrapped by this factory. Skipping.", realmId);
        continue;
      }

      // The JDBC version does this for every realm, but that doesn't seem right as you only
      // need to do it once per set of realms as it is constant across all realms. We do this
      // once on the first time we encounter a non-bootstrapped realm.
      if (!schemaInitialized) {
        schemaInitializer.accept(bootstrapOptions.schemaOptions());
        schemaInitialized = true;
      }
      // Spanner actually has a realm parent table that needs to be initialized.
      realmInitializer.accept(() -> realmId);
      initializeRealmState(realmId, rootCredentialsSet);
      results.put(realmId, bootstrapServiceAndCreatePolarisPrincipalForRealm(() -> realmId));
    }
    return results;
  }

  protected void initializeRealmState(final String realmId, RootCredentialsSet rootCredentialsSet) {
    final RealmContext realmContext = () -> realmId;
    sessionSupplierMap.put(
        realmId,
        () -> {
          return new GoogleSpannerBasePersistenceImpl(
              clientSupplier,
              secretsGenerator(realmId, rootCredentialsSet),
              polarisStorageIntegrationProvider);
        });
    metaStoreManagerMap.put(realmId, new AtomicOperationMetaStoreManager(clock));
  }

  @Override
  public Map<String, BaseResult> purgeRealms(Iterable<String> realms) {
    Map<String, BaseResult> results = new HashMap<>();
    for (String realmId : realms) {
      RealmContext realmContext = () -> realmId;
      PolarisMetaStoreManager manager = getOrCreateMetaStoreManager(realmContext);
      BasePersistence session = getOrCreateSession(realmContext);

      PolarisCallContext callCtx =
          new PolarisCallContext(realmContext, session, polarisDiagnostics);
      BaseResult result = manager.purge(callCtx);
      results.put(realmId, result);
      sessionSupplierMap.remove(realmId);
      metaStoreManagerMap.remove(realmId);
    }
    return Map.copyOf(results);
  }

  protected PrincipalSecretsResult bootstrapServiceAndCreatePolarisPrincipalForRealm(
      RealmContext realmContext) {
    // We can assume the metastore manager has been created at this point
    PolarisMetaStoreManager manager = metaStoreManagerMap.get(realmContext.getRealmIdentifier());
    BasePersistence session = sessionSupplierMap.get(realmContext.getRealmIdentifier()).get();
    PolarisCallContext callCtx = new PolarisCallContext(realmContext, session, polarisDiagnostics);

    if (manager.findRootPrincipal(callCtx).isPresent()) {
      // Match JDBC in case integration tests rely on this exact format.
      String msg =
          "\n\n It appears this metastore manager has already been bootstrapped. "
              + "To continue bootstrapping, please first purge the metastore with the `purge` command. \n\n";
      LOGGER.error(msg);
      throw new IllegalArgumentException(msg);
    }

    manager.bootstrapPolarisService(callCtx);
    PrincipalEntity rootPrincipal = manager.findRootPrincipal(callCtx).orElseThrow();
    return manager.loadPrincipalSecrets(
        callCtx,
        rootPrincipal
            .getInternalPropertiesAsMap()
            .get(PolarisEntityConstants.getClientIdPropertyName()));
  }

  protected void checkBootstrapped(RealmContext realmContext, PolarisMetaStoreManager state) {
    PolarisMetaStoreManager manager = metaStoreManagerMap.get(realmContext.getRealmIdentifier());
    BasePersistence session = sessionSupplierMap.get(realmContext.getRealmIdentifier()).get();
    PolarisCallContext callCtx = new PolarisCallContext(realmContext, session, polarisDiagnostics);

    if (manager.findRootPrincipal(callCtx).isEmpty()) {
      // This exact format is needed to pass the purge tests.
      LOGGER.error(
          "\n\n Realm {} is not bootstrapped, could not load root principal. Please run Bootstrap command. \n\n",
          realmContext.getRealmIdentifier());
      throw new IllegalStateException(
          "Realm is not bootstrapped, please run server in bootstrap mode.");
    }
  }
}
