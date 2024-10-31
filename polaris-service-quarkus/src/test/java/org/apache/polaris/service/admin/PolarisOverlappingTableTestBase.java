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
package org.apache.polaris.service.admin;

import static org.apache.polaris.service.admin.PolarisAuthzTestBase.SCHEMA;
import static org.apache.polaris.service.context.DefaultRealmContextResolver.REALM_PROPERTY_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import jakarta.inject.Inject;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.polaris.core.PolarisConfiguration;
import org.apache.polaris.core.admin.model.Catalog;
import org.apache.polaris.core.admin.model.CatalogProperties;
import org.apache.polaris.core.admin.model.CreateCatalogRequest;
import org.apache.polaris.core.admin.model.FileStorageConfigInfo;
import org.apache.polaris.core.admin.model.StorageConfigInfo;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.provider.Arguments;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class PolarisOverlappingTableTestBase {

  public enum CatalogType {
    DEFAULT,
    LAX,
    STRICT
  }

  private static final String baseLocation = "file:///tmp/PolarisOverlappingTableTest";

  @Inject PolarisIntegrationTestHelper testHelper;

  @BeforeAll
  public void setUp(TestInfo testInfo) {
    testHelper.setUp(testInfo);
  }

  protected String createCatalog(CatalogType catalogType) {
    String name = catalogType + "_" + UUID.randomUUID();
    CatalogProperties.Builder propertiesBuilder =
        CatalogProperties.builder()
            .setDefaultBaseLocation(String.format("%s/%s", baseLocation, name));
    if (catalogType != CatalogType.DEFAULT) {
      propertiesBuilder
          .addProperty(
              PolarisConfiguration.ALLOW_UNSTRUCTURED_TABLE_LOCATION.catalogConfig(),
              String.valueOf(catalogType == CatalogType.LAX))
          .addProperty(
              PolarisConfiguration.ALLOW_TABLE_LOCATION_OVERLAP.catalogConfig(),
              String.valueOf(catalogType == CatalogType.LAX));
    }
    StorageConfigInfo config =
        FileStorageConfigInfo.builder()
            .setStorageType(StorageConfigInfo.StorageTypeEnum.FILE)
            .build();
    Catalog catalogObject =
        new Catalog(
            Catalog.TypeEnum.INTERNAL,
            name,
            propertiesBuilder.build(),
            1725487592064L,
            1725487592064L,
            1,
            config);
    try (Response response =
        request("management/v1/catalogs")
            .post(Entity.json(new CreateCatalogRequest(catalogObject)))) {
      if (response.getStatus() != Response.Status.CREATED.getStatusCode()) {
        throw new IllegalStateException(
            "Failed to create catalog: " + response.readEntity(String.class));
      }
    }

    CreateNamespaceRequest createNamespaceRequest =
        CreateNamespaceRequest.builder().withNamespace(Namespace.of("ns")).build();
    try (Response response =
        request(String.format("catalog/v1/%s/namespaces", name))
            .post(Entity.json(createNamespaceRequest))) {
      if (response.getStatus() != Response.Status.OK.getStatusCode()) {
        throw new IllegalStateException(
            "Failed to create namespace: " + response.readEntity(String.class));
      }
    }
    return name;
  }

  private Response createTable(String catalog, String location) {
    CreateTableRequest createTableRequest =
        CreateTableRequest.builder()
            .withName("table_" + UUID.randomUUID())
            .withLocation(location)
            .withSchema(SCHEMA)
            .build();
    String prefix = String.format("catalog/v1/%s/namespaces/%s/tables", catalog, "ns");
    try (Response response = request(prefix).post(Entity.json(createTableRequest))) {
      return response;
    }
  }

  protected Stream<Arguments> getTestConfigs(
      Status defaultStatus, Status strictStatus, Status laxStatus) {
    return Stream.of(
        Arguments.of(CatalogType.DEFAULT, defaultStatus),
        Arguments.of(CatalogType.STRICT, strictStatus),
        Arguments.of(CatalogType.LAX, laxStatus));
  }

  private Invocation.Builder request(String prefix) {
    return testHelper
        .client
        .target(String.format("http://localhost:%d/api/%s", testHelper.localPort, prefix))
        .request("application/json")
        .header("Authorization", "Bearer " + testHelper.adminToken)
        .header(REALM_PROPERTY_KEY, testHelper.realm);
  }

  protected void testTableLocationRestrictions(CatalogType catalogType, Status status) {
    String catalog = createCatalog(catalogType);

    // Original table
    assertThat(createTable(catalog, String.format("%s/%s/%s/table_1", baseLocation, catalog, "ns")))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Unrelated path
    assertThat(createTable(catalog, String.format("%s/%s/%s/table_2", baseLocation, catalog, "ns")))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Trailing slash makes this not overlap with table_1
    assertThat(
            createTable(catalog, String.format("%s/%s/%s/table_100", baseLocation, catalog, "ns")))
        .returns(Response.Status.OK.getStatusCode(), Response::getStatus);

    // Repeat location
    assertThat(
            createTable(catalog, String.format("%s/%s/%s/table_100", baseLocation, catalog, "ns")))
        .returns(status.getStatusCode(), Response::getStatus);

    // Parent of existing location
    assertThat(createTable(catalog, String.format("%s/%s/%s", baseLocation, catalog, "ns")))
        .returns(status.getStatusCode(), Response::getStatus);

    // Child of existing location
    assertThat(
            createTable(
                catalog, String.format("%s/%s/%s/table_100/child", baseLocation, catalog, "ns")))
        .returns(status.getStatusCode(), Response::getStatus);

    // Outside the namespace
    assertThat(createTable(catalog, String.format("%s/%s", baseLocation, catalog)))
        .returns(status.getStatusCode(), Response::getStatus);

    // Outside the catalog
    assertThat(createTable(catalog, String.format("%s", baseLocation)))
        .returns(Response.Status.FORBIDDEN.getStatusCode(), Response::getStatus);
  }
}
