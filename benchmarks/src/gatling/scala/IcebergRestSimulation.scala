import io.gatling.core.Predef._
import io.gatling.http.Predef._

import java.util.concurrent.atomic.AtomicInteger

class IcebergRestSimulation extends Simulation {
  // --------------------------------------------------------------------------------
  // Read parameters from system properties, with default values when possible
  // --------------------------------------------------------------------------------

  // The CLIENT_ID and CLIENT_SECRET must be provided otherwise we cannot authenticate
  private val clientId: String = sys.env("CLIENT_ID")
  private val clientSecret: String = sys.env("CLIENT_SECRET")

  // By default, we point to localhost
  private val baseUrl: String = sys.env.getOrElse("BASE_URL", "http://localhost:8181")

  private val concurrency: Int = sys.env.getOrElse("CONCURRENCY", "50").toInt

  // --------------------------------------------------------------------------------
  // Feeders
  // --------------------------------------------------------------------------------
  private val authenticationFeeder = Iterator.continually(
    Map(
      "clientId" -> clientId,
      "clientSecret" -> clientSecret
    )
  )


  // --------------------------------------------------------------------------------
  // Workload: Authenticate, then create catalog and namespace
  // --------------------------------------------------------------------------------
  private val createCatalogAndNamespace = scenario("Create catalog and namespace using the Iceberg REST API")
    // Authenticate using the /api/catalog/v1/oauth/tokens endpoint
    .tryMax(5) {
      exec(
        feed(authenticationFeeder)
          .exec(
            http("Authenticate")
              .post("/api/catalog/v1/oauth/tokens")
              .header("Content-Type", "application/x-www-form-urlencoded")
              .formParam("grant_type", "client_credentials")
              .formParam("client_id", "#{clientId}")
              .formParam("client_secret", "#{clientSecret}")
              .formParam("scope", "PRINCIPAL_ROLE:ALL")
              .check(status.is(200))
              .check(jsonPath("$.access_token").saveAs("accessToken"))
          )
      )
    }
    // Then create the catalog
    .exec(
      http("Create Catalog")
        .post("/api/management/v1/catalogs")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            """{
              |  "catalog": {
              |    "type": "INTERNAL",
              |    "name": "C_0",
              |    "properties": {
              |      "default-base-location": "file:///tmp/polaris2"
              |    },
              |    "storageConfigInfo": {
              |      "storageType": "FILE"
              |    }
              |  }
              |}""".stripMargin
          )
        )
        .check(status.is(201))
    )
    // Then create the namespace
    .exec(
      http("Create Namespace")
        .post("/api/catalog/v1/C_0/namespaces")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            """{
              |  "namespace": ["NS_0"]
              |}""".stripMargin
          )
        )
        .check(status.is(200))
    )

  // --------------------------------------------------------------------------------
  // Workload: Authenticate, then create tables
  // --------------------------------------------------------------------------------
  private val createTables = scenario("Create tables using the Iceberg REST API")
    // Authenticate using the /api/catalog/v1/oauth/tokens endpoint
    .tryMax(5) {
      exec(
        feed(authenticationFeeder)
          .exec(
            http("Authenticate")
              .post("/api/catalog/v1/oauth/tokens")
              .header("Content-Type", "application/x-www-form-urlencoded")
              .formParam("grant_type", "client_credentials")
              .formParam("client_id", "#{clientId}")
              .formParam("client_secret", "#{clientSecret}")
              .formParam("scope", "PRINCIPAL_ROLE:ALL")
              .check(status.is(200))
              .check(jsonPath("$.access_token").saveAs("accessToken"))
          )
      )
    }
    .rendezVous(concurrency)
    .exec(
      http("Create Table")
        .post("/api/catalog/v1/C_0/namespaces/NS_0/tables")
        .header("Authorization", "Bearer #{accessToken}")
        .header("Content-Type", "application/json")
        .body(
          StringBody(
            s"""{
               |  "name": "T_1000",
               |  "stage-create": false,
               |  "schema": {
               |    "type": "struct",
               |    "fields": [{ "id": 0, "name": "column0", "type": "int", "required": true }],
               |    "identifier-field-ids": [0]
               |  }
               |}""".stripMargin
          )
        )
    )

  // --------------------------------------------------------------------------------
  // Build up the HTTP protocol configuration and set up the simulation
  // --------------------------------------------------------------------------------
  private val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  setUp(
    createCatalogAndNamespace.inject(atOnceUsers(1))
      .andThen(createTables.inject(atOnceUsers(concurrency)))
  ).protocols(httpProtocol)
}
