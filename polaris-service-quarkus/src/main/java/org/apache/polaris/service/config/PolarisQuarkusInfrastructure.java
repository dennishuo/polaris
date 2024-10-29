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
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDefaultDiagServiceImpl;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisTreeMapMetaStoreSessionImpl;
import org.apache.polaris.core.persistence.PolarisTreeMapStore;
import org.apache.polaris.core.storage.PolarisStorageIntegrationProvider;
import org.apache.polaris.core.storage.cache.StorageCredentialCache;
import org.apache.polaris.service.catalog.api.IcebergRestOAuth2ApiService;
import org.apache.polaris.service.catalog.api.impl.IcebergRestOAuth2ApiServiceImpl;
import org.apache.polaris.service.context.CallContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;

public class PolarisQuarkusInfrastructure {

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
    return realmContextResolver.resolveRealmContext(
        request.absoluteURI(),
        request.method().name(),
        request.path(),
        request.params().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll),
        request.headers().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll));
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
  public CallContext callContext(
      RealmContext realmContext,
      @Context HttpServerRequest request,
      CallContextResolver callContextResolver) {
    return callContextResolver.resolveCallContext(
        realmContext,
        request.method().name(),
        request.path(),
        request.params().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll),
        request.headers().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll));
  }

  @Produces
  @RequestScoped
  public PolarisMetaStoreManager polarisMetaStoreManager(
      MetaStoreManagerFactory factory, RealmContext realmContext) {
    return factory.getOrCreateMetaStoreManager(realmContext);
  }

  @Produces
  @RequestScoped
  public PolarisEntityManager polarisEntityManager(
      PolarisMetaStoreManager metaStoreManager,
      PolarisMetaStoreSession metaStoreSession,
      StorageCredentialCache storageCredentialCache) {
    return new PolarisEntityManager(
        metaStoreManager, () -> metaStoreSession, storageCredentialCache);
  }

  @Produces
  @RequestScoped
  public IcebergRestOAuth2ApiService icebergRestOAuth2ApiService() {
    // FIXME OIDC
    return new IcebergRestOAuth2ApiServiceImpl();
  }
}
