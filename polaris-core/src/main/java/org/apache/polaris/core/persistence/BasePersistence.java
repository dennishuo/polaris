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
package org.apache.polaris.core.persistence;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.auth.PolarisGrantManager;
import org.apache.polaris.core.auth.PolarisSecretsManager;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.storage.PolarisCredentialVendor;

/** Core persistence APIs to be implemented by any persistence backend used for Polaris. */
public interface BasePersistence {
  /**
   * Lookup an entity by its name
   *
   * @param callCtx call context
   * @param ms meta store
   * @param entityActiveKey lookup key
   * @return the entity if it exists, null otherwise
   */
  @Nullable PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntitiesActiveKey entityActiveKey);

  /**
   * Resolve an entity by name. Can be a top-level entity like a catalog or an entity inside a
   * catalog like a namespace, a role, a table like entity, or a principal. If the entity is inside
   * a catalog, the parameter catalogPath must be specified
   *
   * @param callCtx call context
   * @param catalogPath path inside a catalog to that entity, rooted by the catalog. If null, the
   *     entity being resolved is a top-level account entity like a catalog.
   * @param entityType entity type
   * @param entitySubType entity subtype. Can be the special value ANY_SUBTYPE to match any
   *     subtypes. Else exact match on the subtype will be required.
   * @param name name of the entity, cannot be null
   * @return the result of the lookup operation. ENTITY_NOT_FOUND is returned if the specified
   *     entity is not found in the specified path. CONCURRENT_MODIFICATION_DETECTED_NEED_RETRY is
   *     returned if the specified catalog path cannot be resolved.
   */
  @Nonnull
  PolarisMetaStoreManager.EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name);
}
