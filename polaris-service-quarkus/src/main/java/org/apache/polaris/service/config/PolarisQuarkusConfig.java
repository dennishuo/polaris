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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Context;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class PolarisQuarkusConfig {

  @Inject
  @ConfigProperty(name = "polaris.default-realms")
  Set<String> defaultRealms;

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
  public PolarisConfigurationStore polarisConfigurationStore() {
    return new DefaultConfigurationStore(new HashMap<>());
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

  // Required by PolarisStorageIntegrationProviderImpl
  // FIXME refactor this

  @Produces
  public AwsCredentialsProvider awsCredentialsProvider(
      @ConfigProperty(name = "polaris.storage.aws.awsAccessKey") String awsAccessKey,
      @ConfigProperty(name = "polaris.storage.aws.awsSecretKey") String awsSecretKey) {
    // FIXME configuration
    // FIXME optional bean
    if (!awsAccessKey.isBlank() && !awsSecretKey.isBlank()) {
      LoggerFactory.getLogger(PolarisQuarkusConfig.class)
          .warn("Using hard-coded AWS credentials - this is not recommended for production");
      return StaticCredentialsProvider.create(
          AwsBasicCredentials.create(awsAccessKey, awsSecretKey));
    }
    return null;
  }

  @Produces
  public Supplier<StsClient> stsClientSupplier(AwsCredentialsProvider awsCredentialsProvider) {
    return () -> {
      StsClientBuilder stsClientBuilder = StsClient.builder();
      if (awsCredentialsProvider != null) {
        stsClientBuilder.credentialsProvider(awsCredentialsProvider);
      }
      return stsClientBuilder.build();
    };
  }

  @Produces
  public Supplier<GoogleCredentials> gcpCredentialsSupplier(
      @ConfigProperty(name = "polaris.storage.gcp.token") String gcpAccessToken,
      @ConfigProperty(name = "polaris.storage.gcp.lifespan") Duration lifespan) {
    // FIXME configuration
    // FIXME optional bean
    return () -> {
      AccessToken accessToken =
          new AccessToken(gcpAccessToken, new Date(Instant.now().plus(lifespan).toEpochMilli()));
      return Optional.ofNullable(accessToken)
          .map(GoogleCredentials::create)
          .orElseGet(
              () -> {
                try {
                  return GoogleCredentials.getApplicationDefault();
                } catch (IOException e) {
                  throw new RuntimeException("Failed to get GCP credentials", e);
                }
              });
    };
  }
}
