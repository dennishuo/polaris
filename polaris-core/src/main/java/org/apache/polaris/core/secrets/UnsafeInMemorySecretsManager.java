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
package org.apache.polaris.core.secrets;

import java.io.UnsupportedEncodingException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.polaris.core.entity.PolarisEntity;

/**
 * A minimal in-memory implementation of UserSecretsManager that should only be used for test and
 * development purposes.
 */
public class UnsafeInMemorySecretsManager implements UserSecretsManager {
  private final Map<String, String> rawSecretStore = new ConcurrentHashMap<>();
  private final SecureRandom rand = new SecureRandom();

  // Keys for information stored in referencePayload
  private static final String CIPHERTEXT_HASH = "ciphertext-hash";
  private static final String ENCRYPTION_KEY = "encryption-key";

  /** {@inheritDoc} */
  @Override
  public UserSecretReference writeSecret(String secret, PolarisEntity forEntity) {
    // For illustrative purposes and to exercise the control flow of requiring both the stored
    // secret as well as the secretReferencePayload to recover the original secret, we'll use
    // basic XOR encryption and store the randomly generated key in the reference payload.
    // A production implementation will typically use a standard crypto library if applicable.
    byte[] secretBytes;
    try {
      secretBytes = secret.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    byte[] oneTimeKey = new byte[secretBytes.length];
    byte[] cipherTextBytes = new byte[secretBytes.length];

    // Generate one-time key of length equal to the original secret's bytes.
    rand.nextBytes(oneTimeKey);

    // XOR all the bytes to generate the cipherText
    for (int i = 0; i < cipherTextBytes.length; ++i) {
      cipherTextBytes[i] = (byte) (secretBytes[i] ^ oneTimeKey[i]);
    }

    // Store as Base64 since raw bytes won't play well with non-invertible String behaviors
    // related to charset encodings.
    String encryptedSecretCipherTextBase64 = Base64.getEncoder().encodeToString(cipherTextBytes);
    String encryptedSecretKeyBase64 = Base64.getEncoder().encodeToString(oneTimeKey);

    String secretUrn;
    for (int secretOrdinal = 0; ; ++secretOrdinal) {
      secretUrn =
          String.format(
              "urn:polaris-secret:unsafe-in-memory:%d:%d", forEntity.getId(), secretOrdinal);

      // Store the base64-encoded encrypted ciphertext in the simulated "secret store".
      String existingSecret =
          rawSecretStore.putIfAbsent(secretUrn, encryptedSecretCipherTextBase64);

      // If there was already something stored under the current URN, continue to loop with
      // an incremented ordinal suffix until we find an unused URN.
      if (existingSecret == null) {
        break;
      }
    }

    Map<String, String> referencePayload = new HashMap<>();

    // Keep a hash to detect data corruption or tampering; String::hashCode is standardized and can
    // help detect systematic bugs causing corrupted secrets even if not secure against intentional
    // tampering.
    // A production implementation should use a cryptographic hash function instead if integrity
    // of the secret is an actual concern.
    referencePayload.put(
        CIPHERTEXT_HASH, Integer.toString(encryptedSecretCipherTextBase64.hashCode()));

    // Keep the randomly generated one-time-use encryption key in the reference payload.
    // A production implementation may choose to store an encryption key reference or URN if the
    // key is ever shared and/or the key isn't a one-time-pad of the same length as the source
    // secret.
    referencePayload.put(ENCRYPTION_KEY, encryptedSecretKeyBase64);
    UserSecretReference secretReference = new UserSecretReference(secretUrn, referencePayload);
    return secretReference;
  }

  /** {@inheritDoc} */
  @Override
  public String readSecret(UserSecretReference secretReference) {
    // TODO: Precondition checks and/or wire in PolarisDiagnostics
    String encryptedSecretCipherTextBase64 = rawSecretStore.get(secretReference.getUrn());
    if (encryptedSecretCipherTextBase64 == null) {
      // Secret at this URN no longer exists.
      return null;
    }

    String encryptedSecretKeyBase64 = secretReference.getReferencePayload().get(ENCRYPTION_KEY);

    // Validate integrity of the base64-encoded ciphertext which was retrieved from the secret
    // store against the hash we stored in the referencePayload.
    int expecteCipherTextBase64Hash =
        Integer.parseInt(secretReference.getReferencePayload().get(CIPHERTEXT_HASH));
    if (encryptedSecretCipherTextBase64.hashCode() != expecteCipherTextBase64Hash) {
      throw new IllegalArgumentException(
          String.format(
              "Ciphertext hash mismatch for URN %s; expected %d got %d",
              secretReference.getUrn(),
              expecteCipherTextBase64Hash,
              encryptedSecretCipherTextBase64.hashCode()));
    }

    byte[] cipherTextBytes = Base64.getDecoder().decode(encryptedSecretCipherTextBase64);
    byte[] oneTimeKey = Base64.getDecoder().decode(encryptedSecretKeyBase64);
    byte[] secretBytes = new byte[cipherTextBytes.length];

    // XOR all the bytes to recover the secret
    for (int i = 0; i < cipherTextBytes.length; ++i) {
      secretBytes[i] = (byte) (cipherTextBytes[i] ^ oneTimeKey[i]);
    }

    try {
      return new String(secretBytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteSecret(UserSecretReference secretReference) {
    rawSecretStore.remove(secretReference.getUrn());
  }
}
