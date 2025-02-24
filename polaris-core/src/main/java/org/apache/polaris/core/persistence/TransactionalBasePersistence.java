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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.AsyncTaskType;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.entity.PolarisEntityActiveRecord;
import org.apache.polaris.core.entity.PolarisEntityConstants;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntitySubType;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.entity.PolarisTaskConstants;
import org.apache.polaris.core.storage.PolarisCredentialProperty;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@code BasePersistence} based around a runInTransaction structure which
 * manually manages separate lower-level "tables" that are used as secondary indexes.
 */
public class TransactionalBasePersistence implements BasePersistence {
  /** {@inheritDoc} */
  @Override
  public @Nullable PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntitiesActiveKey entityActiveKey) {
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // ensure that the entity exists
    PolarisEntityActiveRecord entityActiveRecord = ms.lookupEntityActive(callCtx, entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        ms.lookupEntity(callCtx, entityActiveRecord.getCatalogId(), entityActiveRecord.getId());
    callCtx
        .getDiagServices()
        .checkNotNull(
            entity, "unexpected_not_found_entity", "entityActiveRecord={}", entityActiveRecord);

    // return it now
    return entity;
  }

  /**
   * See {@link #readEntityByName(PolarisCallContext, List, PolarisEntityType, PolarisEntitySubType,
   * String)}
   */
  private @Nonnull PolarisMetaStoreManager.EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisMetaStoreSession ms,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    // first resolve again the catalogPath to that entity
    PolarisEntityResolver resolver = new PolarisEntityResolver(callCtx, ms, catalogPath);

    // return if we failed to resolve
    if (resolver.isFailure()) {
      return new PolarisMetaStoreManager.EntityResult(BaseResult.ReturnStatus.CATALOG_PATH_CANNOT_BE_RESOLVED, null);
    }

    // now looking the entity by name
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(
            resolver.getCatalogIdOrNull(), resolver.getParentId(), entityType.getCode(), name);
    PolarisBaseEntity entity = this.lookupEntityByName(callCtx, entityActiveKey);

    // if found, check if subType really matches
    if (entity != null
        && entitySubType != PolarisEntitySubType.ANY_SUBTYPE
        && entity.getSubTypeCode() != entitySubType.getCode()) {
      entity = null;
    }

    // success, return what we found
    return (entity == null)
        ? new PolarisMetaStoreManager.EntityResult(BaseResult.ReturnStatus.ENTITY_NOT_FOUND, null)
        : new PolarisMetaStoreManager.EntityResult(entity);
  }

  /** {@inheritDoc} */
  @Override
  public @Nonnull PolarisMetaStoreManager.EntityResult readEntityByName(
      @Nonnull PolarisCallContext callCtx,
      @Nullable List<PolarisEntityCore> catalogPath,
      @Nonnull PolarisEntityType entityType,
      @Nonnull PolarisEntitySubType entitySubType,
      @Nonnull String name) {
    // get meta store we should be using
    PolarisMetaStoreSession ms = callCtx.getMetaStore();

    // run operation in a read/write transaction
    return ms.runInReadTransaction(
        callCtx, () -> readEntityByName(callCtx, ms, catalogPath, entityType, entitySubType, name));
  }
}
