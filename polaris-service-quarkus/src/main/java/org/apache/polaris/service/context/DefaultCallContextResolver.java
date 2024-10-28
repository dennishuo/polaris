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
package org.apache.polaris.service.context;

import static org.apache.polaris.service.context.DefaultRealmContextResolver.parseBearerTokenAsKvPairs;

import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For local/dev testing, this resolver simply expects a custom bearer-token format that is a
 * semicolon-separated list of colon-separated key/value pairs that constitute the realm properties.
 *
 * <p>Example: principal:data-engineer;password:test;realm:acct123
 */
@ApplicationScoped
@IfBuildProperty(name = "polaris.context.call-context-resolver.type", stringValue = "default")
public class DefaultCallContextResolver implements CallContextResolver {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCallContextResolver.class);

  public static final String PRINCIPAL_PROPERTY_KEY = "principal";
  public static final String PRINCIPAL_PROPERTY_DEFAULT_VALUE = "default-principal";

  private final RealmEntityManagerFactory entityManagerFactory;
  private final PolarisConfigurationStore configurationStore;
  private final PolarisDiagnostics diagServices;

  @Inject
  public DefaultCallContextResolver(
      RealmEntityManagerFactory entityManagerFactory,
      PolarisConfigurationStore configurationStore,
      PolarisDiagnostics diagServices) {
    this.entityManagerFactory = entityManagerFactory;
    this.configurationStore = configurationStore;
    this.diagServices = diagServices;
  }

  @Override
  public CallContext resolveCallContext(
      RealmContext realmContext,
      String method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers) {
    LOGGER
        .atDebug()
        .addKeyValue("realmContext", realmContext.getRealmIdentifier())
        .addKeyValue("method", method)
        .addKeyValue("path", path)
        .addKeyValue("queryParams", queryParams)
        .addKeyValue("headers", headers)
        .log("Resolving CallContext");
    Map<String, String> parsedProperties = parseBearerTokenAsKvPairs(headers);

    if (!parsedProperties.containsKey(PRINCIPAL_PROPERTY_KEY)) {
      LOGGER.warn(
          "Failed to parse {} from headers ({}); using {}",
          PRINCIPAL_PROPERTY_KEY,
          headers,
          PRINCIPAL_PROPERTY_DEFAULT_VALUE);
      parsedProperties.put(PRINCIPAL_PROPERTY_KEY, PRINCIPAL_PROPERTY_DEFAULT_VALUE);
    }

    PolarisEntityManager entityManager =
        entityManagerFactory.getOrCreateEntityManager(realmContext);

    PolarisMetaStoreSession metaStoreSession = entityManager.newMetaStoreSession();
    PolarisCallContext polarisContext =
        new PolarisCallContext(
            metaStoreSession,
            diagServices,
            configurationStore,
            Clock.system(ZoneId.systemDefault()));
    return CallContext.of(realmContext, polarisContext);
  }
}
