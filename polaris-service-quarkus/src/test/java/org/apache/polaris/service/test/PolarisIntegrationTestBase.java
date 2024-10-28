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
package org.apache.polaris.service.test;

import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.polaris.core.admin.model.GrantPrincipalRoleRequest;
import org.apache.polaris.core.admin.model.Principal;
import org.apache.polaris.core.admin.model.PrincipalRole;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.service.auth.TokenUtils;
import org.apache.polaris.service.context.CallContextResolver;
import org.apache.polaris.service.context.RealmContextResolver;
import org.apache.polaris.service.persistence.InMemoryPolarisMetaStoreManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PolarisIntegrationTestBase {

  @Inject MetaStoreManagerFactory metaStoreManagerFactory;
  @Inject RealmContextResolver realmContextResolver;
  @Inject CallContextResolver callContextResolver;
  @Inject ObjectMapper objectMapper;

  public record SnowmanCredentials(String clientId, String clientSecret) {}

  protected String realm;
  protected Client client;
  protected int localPort;
  protected Path testDir;
  protected PolarisPrincipalSecrets adminSecrets;
  protected SnowmanCredentials snowmanCredentials;
  protected String userToken;

  @BeforeAll
  public void setup(TestInfo testInfo) {
    // Generate unique realm using test name for each test since the tests can run in parallel
    realm = testInfo.getTestClass().orElseThrow().getName().replace('.', '_');
    client = ClientBuilder.newClient();
    localPort = Integer.getInteger("quarkus.http.port");
    testDir = Path.of("build/test_data/iceberg/" + realm);
    FileUtils.deleteQuietly(testDir.toFile());
    try {
      Files.createDirectories(testDir);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    fetchAdminSecrets();
    fetchPolarisToken();
    createSnowmanCredentials();
  }

  private void fetchPolarisToken() {
    userToken =
        TokenUtils.getTokenFromSecrets(
            client,
            localPort,
            adminSecrets.getPrincipalClientId(),
            adminSecrets.getMainSecret(),
            realm);
  }

  private void fetchAdminSecrets() {
    try {
      if (!(metaStoreManagerFactory instanceof InMemoryPolarisMetaStoreManagerFactory)) {
        metaStoreManagerFactory.bootstrapRealms(List.of(realm));
      }

      RealmContext realmContext =
          realmContextResolver.resolveRealmContext(
              "http://localhost", "GET", "/", Map.of(), Map.of(REALM_PROPERTY_KEY, realm));

      CallContext ctx =
          callContextResolver.resolveCallContext(realmContext, "GET", "/", Map.of(), Map.of());
      CallContext.setCurrentContext(ctx);
      PolarisMetaStoreManager metaStoreManager =
          metaStoreManagerFactory.getOrCreateMetaStoreManager(ctx.getRealmContext());
      PolarisMetaStoreManager.EntityResult principal =
          metaStoreManager.readEntityByName(
              ctx.getPolarisCallContext(),
              null,
              PolarisEntityType.PRINCIPAL,
              PolarisEntitySubType.NULL_SUBTYPE,
              PolarisEntityConstants.getRootPrincipalName());

      Map<String, String> propertiesMap = readInternalProperties(principal);
      adminSecrets =
          metaStoreManager
              .loadPrincipalSecrets(ctx.getPolarisCallContext(), propertiesMap.get("client_id"))
              .getPrincipalSecrets();
    } finally {
      CallContext.unsetCurrentContext();
    }
  }

  private void createSnowmanCredentials() {

    PrincipalRole principalRole = new PrincipalRole("catalog-admin");

    try (Response createPrResponse =
        client
            .target(
                String.format("http://localhost:%d/api/management/v1/principal-roles", localPort))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken)
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(principalRole))) {
      assertThat(createPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }

    Principal principal = new Principal("snowman");

    try (Response createPResponse =
        client
            .target(String.format("http://localhost:%d/api/management/v1/principals", localPort))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken) // how is token getting used?
            .header(REALM_PROPERTY_KEY, realm)
            .post(Entity.json(principal))) {
      assertThat(createPResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
      PrincipalWithCredentials snowmanWithCredentials =
          createPResponse.readEntity(PrincipalWithCredentials.class);
      try (Response rotateResp =
          client
              .target(
                  String.format(
                      "http://localhost:%d/api/management/v1/principals/%s/rotate",
                      localPort, "snowman"))
              .request(MediaType.APPLICATION_JSON)
              .header(
                  "Authorization",
                  "Bearer "
                      + TokenUtils.getTokenFromSecrets(
                          client,
                          localPort,
                          snowmanWithCredentials.getCredentials().getClientId(),
                          snowmanWithCredentials.getCredentials().getClientSecret(),
                          realm))
              .header(REALM_PROPERTY_KEY, realm)
              .post(Entity.json(snowmanWithCredentials))) {

        assertThat(rotateResp).returns(Response.Status.OK.getStatusCode(), Response::getStatus);

        // Use the rotated credentials.
        snowmanWithCredentials = rotateResp.readEntity(PrincipalWithCredentials.class);
      }
      snowmanCredentials =
          new SnowmanCredentials(
              snowmanWithCredentials.getCredentials().getClientId(),
              snowmanWithCredentials.getCredentials().getClientSecret());
    }
    try (Response assignPrResponse =
        client
            .target(
                String.format(
                    "http://localhost:%d/api/management/v1/principals/snowman/principal-roles",
                    localPort))
            .request("application/json")
            .header("Authorization", "Bearer " + userToken) // how is token getting used?
            .header(REALM_PROPERTY_KEY, realm)
            .put(Entity.json(new GrantPrincipalRoleRequest(principalRole)))) {
      assertThat(assignPrResponse)
          .returns(Response.Status.CREATED.getStatusCode(), Response::getStatus);
    }
  }

  @AfterAll
  public void tearDown() {
    if (!(metaStoreManagerFactory instanceof InMemoryPolarisMetaStoreManagerFactory)) {
      metaStoreManagerFactory.purgeRealms(List.of(realm));
    }
    FileUtils.deleteQuietly(testDir.toFile());
    client.close();
  }

  private Map<String, String> readInternalProperties(
      PolarisMetaStoreManager.EntityResult principal) {
    try {
      return objectMapper.readValue(
          principal.getEntity().getInternalProperties(), new TypeReference<>() {});
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
