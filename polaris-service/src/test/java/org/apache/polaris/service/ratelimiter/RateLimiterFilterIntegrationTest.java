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
package org.apache.polaris.service.ratelimiter;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.polaris.core.context.CallContext;
import org.apache.polaris.service.test.PolarisIntegrationTestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.threeten.extra.MutableClock;

/** Main integration tests for rate limiting */
@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestProfile(RateLimiterFilterIntegrationTest.Profile.class)
public class RateLimiterFilterIntegrationTest {

  private static final long REQUESTS_PER_SECOND = 5;
  private static final Duration WINDOW = Duration.ofSeconds(10);

  @Inject PolarisIntegrationTestHelper testHelper;

  @BeforeAll
  public void setUp(TestInfo testInfo) {
    QuarkusMock.installMockForType(
        new MockRealmTokenBucketRateLimiter(REQUESTS_PER_SECOND, WINDOW), RateLimiter.class);
    testHelper.setUp(testInfo);
  }

  @AfterAll
  public void tearDown() {
    testHelper.tearDown();
  }

  @Test
  public void testRateLimiter() {
    Consumer<Response.Status> requestAsserter = TestUtil.constructRequestAsserter(testHelper);
    CallContext.setCurrentContext(CallContext.of(() -> "myrealm", null));

    MutableClock clock = MockRealmTokenBucketRateLimiter.CLOCK;
    clock.add(WINDOW.multipliedBy(2)); // Clear any counters from before this test

    for (int i = 0; i < REQUESTS_PER_SECOND * WINDOW.getSeconds(); i++) {
      requestAsserter.accept(Response.Status.OK);
    }
    requestAsserter.accept(Response.Status.TOO_MANY_REQUESTS);

    clock.add(WINDOW.multipliedBy(4)); // Clear any counters from during this test
  }

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return Map.of(
          "polaris.rate-limiter.type", "realm-token-bucket",
          "polaris.rate-limiter.realm-token-bucket.requests-per-second",
              String.valueOf(REQUESTS_PER_SECOND),
          "polaris.rate-limiter.realm-token-bucket.window", WINDOW.toString());
    }
  }
}
