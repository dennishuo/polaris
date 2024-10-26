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
package org.apache.polaris.service.config;

import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.ws.rs.core.Context;
import java.time.Clock;
import java.util.HashMap;
import java.util.Set;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.AuthenticatedPolarisPrincipal;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PrincipalEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.PolarisTreeMapStore;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.api.IcebergRestCatalogApiService;
import org.apache.polaris.service.catalog.api.IcebergRestConfigurationApiService;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.catalog.api.impl.IcebergRestCatalogApiServiceImpl;
import org.apache.polaris.service.catalog.api.impl.IcebergRestConfigurationApiServiceImpl;
import org.apache.polaris.service.catalog.api.impl.IcebergRestOAuth2ApiServiceImpl;
import org.apache.polaris.service.context.RealmContextResolver;

public class PolarisQuarkusConfig {

  @Produces
  public Clock clock() {
    return Clock.systemDefaultZone();
  }

  // Polaris core beans - application scope

  @Produces
  public StorageCredentialCache storageCredentialCache() {
    return new StorageCredentialCache();
  }

  @Produces
  public PolarisAuthorizer polarisAuthorizer(PolarisConfigurationStore configurationStore) {
    return new PolarisAuthorizer(configurationStore);
  }

  @Produces
  public PolarisDiagnostics polarisDiagnostics() {
    return new PolarisDefaultDiagServiceImpl();
  }

  @Produces
  public PolarisTreeMapStore polarisTreeMapStore(PolarisDiagnostics diagnostics) {
    return new PolarisTreeMapStore(diagnostics);
  }

  @Produces
  public PolarisMetaStoreSession polarisMetaStoreSession(
      // FIXME should this return Supplier<PolarisMetaStoreSession>?
      PolarisTreeMapStore store, PolarisStorageIntegrationProvider storageIntegrationProvider) {
    // FIXME PolarisEclipseLinkMetaStoreSessionImpl
    return new PolarisTreeMapMetaStoreSessionImpl(store, storageIntegrationProvider);
  }

  // Polaris core beans - request scope

  @Produces
  @RequestScoped
  public RealmContext realmContext(
      @Context HttpServerRequest request, RealmContextResolver realmContextResolver) {
    // TODO query params and headers
    return realmContextResolver.resolveRealmContext(
        request.absoluteURI(),
        request.method().name(),
        request.path(),
        new HashMap<>(),
        new HashMap<>());
  }

  @Produces
  @RequestScoped
  public PolarisCallContext polarisCallContext(
      PolarisMetaStoreSession metaStore,
      PolarisDiagnostics diagnostics,
      PolarisConfigurationStore polarisConfigurationStore,
      Clock clock) {
    return new PolarisCallContext(metaStore, diagnostics, polarisConfigurationStore, clock);
  }

  @Produces
  @RequestScoped
  public CallContext callContext(RealmContext realmContext, PolarisCallContext polarisCallContext) {
    return CallContext.of(realmContext, polarisCallContext);
  }

  @Produces
  @RequestScoped
  public PolarisEntityManager polarisEntityManager(
      MetaStoreManagerFactory factory,
      RealmContext realmContext,
      PolarisMetaStoreSession metaStoreSession,
      StorageCredentialCache storageCredentialCache) {
    PolarisMetaStoreManager metaStoreManager = factory.getOrCreateMetaStoreManager(realmContext);
    return new PolarisEntityManager(
        metaStoreManager, () -> metaStoreSession, storageCredentialCache);
  }

  @Produces
  @RequestScoped
  public IcebergRestOAuth2ApiService icebergRestOAuth2ApiService() {
    return new IcebergRestOAuth2ApiServiceImpl();
  }

  @Produces
  @RequestScoped
  public IcebergRestConfigurationApiService icebergRestConfigurationApiService() {
    return new IcebergRestConfigurationApiServiceImpl();
  }

  @Produces
  @RequestScoped
  public IcebergRestCatalogApiService icebergRestCatalogApiService() {
    return new IcebergRestCatalogApiServiceImpl();
  }

  @Produces
  @RequestScoped
  public AuthenticatedPolarisPrincipal authenticatedPolarisPrincipal() {
    // FIXME OIDC
    PrincipalEntity principalEntity = new PrincipalEntity(null);
    return new AuthenticatedPolarisPrincipal(principalEntity, Set.of());
  }
}
