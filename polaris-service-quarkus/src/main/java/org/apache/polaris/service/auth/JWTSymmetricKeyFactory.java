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
package org.apache.polaris.service.auth;

import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.service.config.RealmEntityManagerFactory;
import org.apache.polaris.service.config.RuntimeCandidate;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
@RuntimeCandidate
@LookupIfProperty(
    name = "polaris.authentication.token-broker-factory.type",
    stringValue = "symmetric-key")
public class JWTSymmetricKeyFactory implements TokenBrokerFactory {

  private final RealmEntityManagerFactory realmEntityManagerFactory;
  private final Duration maxTokenGenerationInSeconds;

  private final Path file;
  private final String secret;

  public JWTSymmetricKeyFactory(
      RealmEntityManagerFactory realmEntityManagerFactory,
      @ConfigProperty(name = "polaris.authentication.token-broker-factory.max-token-generation")
          Duration maxTokenGenerationInSeconds,
      @ConfigProperty(name = "polaris.authentication.token-broker-factory.symmetric-key.secret")
          Optional<String> secret,
      @ConfigProperty(name = "polaris.authentication.token-broker-factory.symmetric-key.file")
          Optional<Path> file) {
    this.realmEntityManagerFactory = realmEntityManagerFactory;
    this.maxTokenGenerationInSeconds = maxTokenGenerationInSeconds;
    this.secret = secret.orElse(null);
    this.file = file.orElse(null);
    if (this.file == null && this.secret == null) {
      throw new IllegalStateException("Either file or secret must be set");
    }
  }

  @Override
  public TokenBroker apply(RealmContext realmContext) {
    Supplier<String> secretSupplier = secret != null ? () -> secret : readSecretFromDisk();
    return new JWTSymmetricKeyBroker(
        realmEntityManagerFactory.getOrCreateEntityManager(realmContext),
        (int) maxTokenGenerationInSeconds.toSeconds(),
        secretSupplier);
  }

  private Supplier<String> readSecretFromDisk() {
    return () -> {
      try {
        return Files.readString(file);
      } catch (IOException e) {
        throw new RuntimeException("Failed to read secret from file: " + file, e);
      }
    };
  }
}
