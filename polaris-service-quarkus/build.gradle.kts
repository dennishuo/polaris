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

import org.openapitools.generator.gradle.plugin.tasks.GenerateTask

plugins {
  alias(libs.plugins.quarkus)
  alias(libs.plugins.openapi.generator)
  id("polaris-server")
  id("application")
}

dependencies {
  implementation(project(":polaris-core"))

  implementation(platform(libs.iceberg.bom))
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  implementation(platform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-logging-json")
  implementation("io.quarkus:quarkus-rest")
  implementation("io.quarkus:quarkus-rest-jackson")
  implementation("io.quarkus:quarkus-smallrye-health")
  implementation("io.quarkus:quarkus-micrometer")
  implementation("io.quarkus:quarkus-opentelemetry")

  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.inject.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.ws.rs.api)

  // TODO this will removed as soon as dropwizard will be removed
  compileOnly(platform(libs.dropwizard.bom))
  compileOnly("io.dropwizard:dropwizard-core")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  implementation(libs.caffeine)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(libs.hadoop.client.api)

  implementation(libs.auth0.jwt)

  implementation(libs.bouncycastle.bcprov)

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")
  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:iam-policy-builder")
  implementation("software.amazon.awssdk:s3")

  implementation("io.quarkus:quarkus-micrometer-registry-prometheus")

  compileOnly(libs.swagger.annotations)

  testImplementation("org.apache.iceberg:iceberg-api:${libs.versions.iceberg.get()}:tests")
  testImplementation("org.apache.iceberg:iceberg-core:${libs.versions.iceberg.get()}:tests")

  testImplementation("org.apache.iceberg:iceberg-spark-3.5_2.12")
  testImplementation("org.apache.iceberg:iceberg-spark-extensions-3.5_2.12")
  testImplementation("org.apache.spark:spark-sql_2.12:3.5.1") {
    // exclude log4j dependencies
    exclude("org.apache.logging.log4j", "log4j-slf4j2-impl")
    exclude("org.apache.logging.log4j", "log4j-api")
    exclude("org.apache.logging.log4j", "log4j-1.2-api")
  }

  testImplementation("software.amazon.awssdk:glue")
  testImplementation("software.amazon.awssdk:kms")
  testImplementation("software.amazon.awssdk:dynamodb")
}

openApiGenerate {
  inputSpec = "$rootDir/spec/rest-catalog-open-api.yaml"
  generatorName = "jaxrs-resteasy"
  outputDir = "$projectDir/build/generated"
  apiPackage = "org.apache.polaris.service.catalog.api"
  ignoreFileOverride = "$rootDir/.openapi-generator-ignore"
  removeOperationIdPrefix = true
  templateDir = "$rootDir/server-templates"
  globalProperties.put("apis", "")
  globalProperties.put("models", "false")
  globalProperties.put("apiDocs", "false")
  globalProperties.put("modelTests", "false")
  configOptions.put("resourceName", "catalog")
  configOptions.put("useTags", "true")
  configOptions.put("useBeanValidation", "false")
  configOptions.put("sourceFolder", "src/main/java")
  configOptions.put("useJakartaEe", "true")
  openapiNormalizer.put("REFACTOR_ALLOF_WITH_PROPERTIES_ONLY", "true")
  additionalProperties.put("apiNamePrefix", "IcebergRest")
  additionalProperties.put("apiNameSuffix", "")
  additionalProperties.put("metricsPrefix", "polaris")
  serverVariables.put("basePath", "api/catalog")
  importMappings =
    mapOf(
      "CatalogConfig" to "org.apache.iceberg.rest.responses.ConfigResponse",
      "CommitTableResponse" to "org.apache.iceberg.rest.responses.LoadTableResponse",
      "CreateNamespaceRequest" to "org.apache.iceberg.rest.requests.CreateNamespaceRequest",
      "CreateNamespaceResponse" to "org.apache.iceberg.rest.responses.CreateNamespaceResponse",
      "CreateTableRequest" to "org.apache.iceberg.rest.requests.CreateTableRequest",
      "ErrorModel" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "GetNamespaceResponse" to "org.apache.iceberg.rest.responses.GetNamespaceResponse",
      "ListNamespacesResponse" to "org.apache.iceberg.rest.responses.ListNamespacesResponse",
      "ListTablesResponse" to "org.apache.iceberg.rest.responses.ListTablesResponse",
      "LoadTableResult" to "org.apache.iceberg.rest.responses.LoadTableResponse",
      "LoadViewResult" to "org.apache.iceberg.rest.responses.LoadTableResponse",
      "OAuthTokenResponse" to "org.apache.iceberg.rest.responses.OAuthTokenResponse",
      "OAuthErrorResponse" to "org.apache.iceberg.rest.responses.OAuthErrorResponse",
      "RenameTableRequest" to "org.apache.iceberg.rest.requests.RenameTableRequest",
      "ReportMetricsRequest" to "org.apache.iceberg.rest.requests.ReportMetricsRequest",
      "UpdateNamespacePropertiesRequest" to
        "org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest",
      "UpdateNamespacePropertiesResponse" to
        "org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse",
      "CommitTransactionRequest" to "org.apache.iceberg.rest.requests.CommitTransactionRequest",
      "CreateViewRequest" to "org.apache.iceberg.rest.requests.CreateViewRequest",
      "RegisterTableRequest" to "org.apache.iceberg.rest.requests.RegisterTableRequest",
      "IcebergErrorResponse" to "org.apache.iceberg.rest.responses.ErrorResponse",
      "OAuthError" to "org.apache.iceberg.rest.responses.ErrorResponse",

      // Custom types defined below
      "CommitViewRequest" to "org.apache.polaris.service.types.CommitViewRequest",
      "TokenType" to "org.apache.polaris.service.types.TokenType",
      "CommitTableRequest" to "org.apache.polaris.service.types.CommitTableRequest",
      "NotificationRequest" to "org.apache.polaris.service.types.NotificationRequest",
      "TableUpdateNotification" to "org.apache.polaris.service.types.TableUpdateNotification",
      "NotificationType" to "org.apache.polaris.service.types.NotificationType"
    )
}

val generatePolarisService by
  tasks.registering(GenerateTask::class) {
    inputSpec = "$rootDir/spec/polaris-management-service.yml"
    generatorName = "jaxrs-resteasy"
    outputDir = "$projectDir/build/generated"
    apiPackage = "org.apache.polaris.service.admin.api"
    modelPackage = "org.apache.polaris.core.admin.model"
    ignoreFileOverride = "$rootDir/.openapi-generator-ignore"
    removeOperationIdPrefix = true
    templateDir = "$rootDir/server-templates"
    globalProperties.put("apis", "")
    globalProperties.put("models", "false")
    globalProperties.put("apiDocs", "false")
    globalProperties.put("modelTests", "false")
    configOptions.put("useBeanValidation", "true")
    configOptions.put("sourceFolder", "src/main/java")
    configOptions.put("useJakartaEe", "true")
    configOptions.put("generateBuilders", "true")
    configOptions.put("generateConstructorWithAllArgs", "true")
    additionalProperties.put("apiNamePrefix", "Polaris")
    additionalProperties.put("apiNameSuffix", "Api")
    additionalProperties.put("metricsPrefix", "polaris")
    serverVariables.put("basePath", "api/v1")
  }

listOf("sourcesJar", "compileJava").forEach { task ->
  tasks.named(task) { dependsOn("openApiGenerate", generatePolarisService) }
}

sourceSets {
  main { java { srcDir(project.layout.buildDirectory.dir("generated/src/main/java")) } }
}
