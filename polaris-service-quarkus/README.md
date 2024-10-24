<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
 
   http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

This module contains the Polaris Service powered by Quarkus (instead of Dropwizard as in `polaris-service`).

The main differences:
* There's no need anymore to define Codehale metrics or Opentelemetry, Quarkus extension provides it "out of the box".
* The configuration is coming from Quarkus `application.properties`. It's possible to add a another configuration source if needed.
* The `PolarisHealthCheck` is not needed anymore, Quarkus provides it
* The `TimedApplicationEventListener` and `@TimedApi` are replaced by Quarkus `@Timed`:

```
@Timed(value = "call", extraTags = { "region", "test" })
```

* The bean injection is coming from Quarkus CDI (using `@Inject`) instead of injected by Dropwizard configuration. When we have several implementations of a bean, we can use the `@Default` annotation to define the default one.
* The rate limiter should stay (Quarkus provides a rate limiter but it's way difference from our needs in Polaris)
* The `PolarisJsonLayoutFactory` (log json layout) is not needed anymore, Quarkus log manager supports it "out of the box"
* `PolarisApplication` is not needed anymore as we use Quarkus CDI (and eventually `@Startup` to load a bean at startup)
* CORS classes are not needed anymore as it's managed by Quarkus
* The CLI (like bootstrap/purge realms) should be in separated module