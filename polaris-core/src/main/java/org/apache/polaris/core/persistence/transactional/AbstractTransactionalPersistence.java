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
package org.apache.polaris.core.persistence.transactional;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.entity.EntityNameLookupRecord;
import org.apache.polaris.core.entity.PolarisBaseEntity;
import org.apache.polaris.core.entity.PolarisChangeTrackingVersions;
import org.apache.polaris.core.entity.PolarisEntitiesActiveKey;
import org.apache.polaris.core.entity.PolarisEntityCore;
import org.apache.polaris.core.entity.PolarisEntityId;
import org.apache.polaris.core.entity.PolarisEntityType;
import org.apache.polaris.core.entity.PolarisGrantRecord;
import org.apache.polaris.core.entity.PolarisPrincipalSecrets;
import org.apache.polaris.core.persistence.EntityAlreadyExistsException;
import org.apache.polaris.core.persistence.RetryOnConcurrencyException;
import org.apache.polaris.core.storage.PolarisStorageConfigurationInfo;
import org.apache.polaris.core.storage.PolarisStorageIntegration;

/**
 * Extends BasePersistence to express a more "transaction-oriented" control flow for backing stores
 * which can support a runInTransaction semantic, while providing default implementations of some of
 * the BasePersistence methods in terms of lower-level methods that subclasses must implement.
 */
public abstract class AbstractTransactionalPersistence implements TransactionalPersistence {
  //
  // New abstract methods specific to this slice-based transactional persistence that subclasses
  // must implement to inherit implementations of lookup/write/delete
  //

  /**
   * Lookup an entity by entityActiveKey
   *
   * @param callCtx call context
   * @param entityActiveKey key by name
   * @return null if the specified entity does not exist or has been dropped.
   */
  @Nullable
  protected abstract EntityNameLookupRecord lookupEntityActive(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntitiesActiveKey entityActiveKey);

  /**
   * Write the base entity to the entities table. If there is a conflict (existing record with the
   * same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntities(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities_active table. If there is a conflict (existing record
   * with the same PK), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesActive(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Write the base entity to the entities change tracking table. If there is a conflict (existing
   * record with the same id), all attributes of the new record will replace the existing one.
   *
   * @param callCtx call context
   * @param entity entity record to write, potentially replacing an existing entity record with the
   *     same key
   */
  protected abstract void writeToEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity);

  /**
   * Delete the base entity from the entities table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntities(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities_active table.
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesActive(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  /**
   * Delete the base entity from the entities change tracking table
   *
   * @param callCtx call context
   * @param entity entity record to delete
   */
  protected abstract void deleteFromEntitiesChangeTracking(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisEntityCore entity);

  //
  // Implementations of the one-shot atomic BasePersistence methods which explicitly run
  // the in-transaction variants of methods in a new transaction.
  //

  /** {@inheritDoc} */
  @Override
  public long generateNewIdAtomically(@Nonnull PolarisCallContext callCtx) {
    return runInTransaction(callCtx, () -> this.generateNewId(callCtx));
  }

  /** Helper to perform the compare-and-swap semantics of a single writeEntity call. */
  private void checkConditionsForWriteEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      @Nullable PolarisBaseEntity originalEntity) {
    PolarisBaseEntity refreshedEntity =
        this.lookupEntity(callCtx, entity.getCatalogId(), entity.getId(), entity.getTypeCode());

    if (originalEntity == null) {
      if (refreshedEntity != null) {
        // If this is a "create", and we manage to look up an existing entity with already
        // the same id, where ids are uniquely reserved when generated, it means it's a
        // low-level retry possibly in the face of a transient connectivity failure to
        // the backend database.
        throw new EntityAlreadyExistsException(refreshedEntity);
      } else {
        // Successfully verified the entity doesn't already exist by-id, but for a "create"
        // we must also check for name-collection now.
        refreshedEntity =
            this.lookupEntityByName(
                callCtx,
                entity.getCatalogId(),
                entity.getParentId(),
                entity.getType().getCode(),
                entity.getName());
        if (refreshedEntity != null) {
          // Name-collision conflict.
          throw new EntityAlreadyExistsException(refreshedEntity);
        }
      }
    } else {
      // This is an "update".
      if (refreshedEntity == null
          || refreshedEntity.getEntityVersion() != originalEntity.getEntityVersion()
          || refreshedEntity.getGrantRecordsVersion() != originalEntity.getGrantRecordsVersion()) {
        // TODO: Better standardization of exception types, possibly make the ones that are
        // really part of the persistence contract be CheckedExceptions.
        throw new RetryOnConcurrencyException(
            "Entity '%s' id '%s' concurrently modified; expected version %s/%s got %s/%s",
            entity.getName(),
            entity.getId(),
            originalEntity.getEntityVersion(),
            originalEntity.getGrantRecordsVersion(),
            refreshedEntity != null ? refreshedEntity.getEntityVersion() : -1,
            refreshedEntity != null ? refreshedEntity.getGrantRecordsVersion() : -1);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntityAtomically(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    runActionInTransaction(
        callCtx,
        () -> {
          this.checkConditionsForWriteEntity(callCtx, entity, originalEntity);
          this.writeEntity(callCtx, entity, nameOrParentChanged, originalEntity);
        });
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntitiesAtomically(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    if (originalEntities != null) {
      callCtx
          .getDiagServices()
          .check(
              entities.size() == originalEntities.size(),
              "mismatched_entities_and_original_entities_size",
              "entities.size()={}, originalEntities.size()={}",
              entities.size(),
              originalEntities.size());
    }
    runActionInTransaction(
        callCtx,
        () -> {
          // Validate and write each one independently so that we can also detect conflicting
          // writes to the same entity id within a given batch (so that previously written
          // ones will be seen during validation of the later item).
          for (int i = 0; i < entities.size(); ++i) {
            PolarisBaseEntity entity = entities.get(i);
            PolarisBaseEntity originalEntity =
                originalEntities != null ? originalEntities.get(i) : null;
            boolean nameOrParentChanged =
                originalEntity == null
                    || !entity.getName().equals(originalEntity.getName())
                    || entity.getParentId() != originalEntity.getParentId();
            try {
              this.checkConditionsForWriteEntity(callCtx, entity, originalEntity);
            } catch (EntityAlreadyExistsException e) {
              // If the ids are equal then it is an idempotent-create-retry error, which counts
              // as a "success" for multi-entity commit purposes; name-collisions on different
              // ids counts as a true error that we rethrow.
              if (e.getExistingEntity().getId() != entity.getId()) {
                throw e;
              }
              // Else silently swallow the apparent create-retry
            }
            this.writeEntity(callCtx, entity, nameOrParentChanged, originalEntity);
          }
        });
  }

  /** {@inheritDoc} */
  @Override
  public void writeToGrantRecordsAtomically(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    runActionInTransaction(callCtx, () -> this.writeToGrantRecords(callCtx, grantRec));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteEntityAtomically(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    runActionInTransaction(callCtx, () -> this.deleteEntity(callCtx, entity));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFromGrantRecordsAtomically(
      @Nonnull PolarisCallContext callCtx, @Nonnull PolarisGrantRecord grantRec) {
    runActionInTransaction(callCtx, () -> this.deleteFromGrantRecords(callCtx, grantRec));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllEntityGrantRecordsAtomically(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisEntityCore entity,
      @Nonnull List<PolarisGrantRecord> grantsOnGrantee,
      @Nonnull List<PolarisGrantRecord> grantsOnSecurable) {
    runActionInTransaction(
        callCtx,
        () ->
            this.deleteAllEntityGrantRecords(callCtx, entity, grantsOnGrantee, grantsOnSecurable));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteAllAtomically(@Nonnull PolarisCallContext callCtx) {
    runActionInTransaction(callCtx, () -> this.deleteAll(callCtx));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisBaseEntity lookupEntityAtomically(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId, int typeCode) {
    return runInReadTransaction(
        callCtx, () -> this.lookupEntity(callCtx, catalogId, entityId, typeCode));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisBaseEntity lookupEntityByNameAtomically(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    return runInReadTransaction(
        callCtx, () -> this.lookupEntityByName(callCtx, catalogId, parentId, typeCode, name));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByNameAtomically(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    return runInReadTransaction(
        callCtx,
        () -> this.lookupEntityIdAndSubTypeByName(callCtx, catalogId, parentId, typeCode, name));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisBaseEntity> lookupEntitiesAtomically(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return runInReadTransaction(callCtx, () -> this.lookupEntities(callCtx, entityIds));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisChangeTrackingVersions> lookupEntityVersionsAtomically(
      @Nonnull PolarisCallContext callCtx, List<PolarisEntityId> entityIds) {
    return runInReadTransaction(callCtx, () -> this.lookupEntityVersions(callCtx, entityIds));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<EntityNameLookupRecord> listEntitiesAtomically(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType) {
    return runInReadTransaction(
        callCtx, () -> this.listEntities(callCtx, catalogId, parentId, entityType));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<EntityNameLookupRecord> listEntitiesAtomically(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter) {
    return runInReadTransaction(
        callCtx, () -> this.listEntities(callCtx, catalogId, parentId, entityType, entityFilter));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public <T> List<T> listEntitiesAtomically(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      @Nonnull PolarisEntityType entityType,
      int limit,
      @Nonnull Predicate<PolarisBaseEntity> entityFilter,
      @Nonnull Function<PolarisBaseEntity, T> transformer) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.listEntities(
                callCtx, catalogId, parentId, entityType, limit, entityFilter, transformer));
  }

  /** {@inheritDoc} */
  @Override
  public int lookupEntityGrantRecordsVersionAtomically(
      @Nonnull PolarisCallContext callCtx, long catalogId, long entityId) {
    return runInReadTransaction(
        callCtx, () -> this.lookupEntityGrantRecordsVersion(callCtx, catalogId, entityId));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisGrantRecord lookupGrantRecordAtomically(
      @Nonnull PolarisCallContext callCtx,
      long securableCatalogId,
      long securableId,
      long granteeCatalogId,
      long granteeId,
      int privilegeCode) {
    return runInReadTransaction(
        callCtx,
        () ->
            this.lookupGrantRecord(
                callCtx,
                securableCatalogId,
                securableId,
                granteeCatalogId,
                granteeId,
                privilegeCode));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisGrantRecord> loadAllGrantRecordsOnSecurableAtomically(
      @Nonnull PolarisCallContext callCtx, long securableCatalogId, long securableId) {
    return runInReadTransaction(
        callCtx,
        () -> this.loadAllGrantRecordsOnSecurable(callCtx, securableCatalogId, securableId));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public List<PolarisGrantRecord> loadAllGrantRecordsOnGranteeAtomically(
      @Nonnull PolarisCallContext callCtx, long granteeCatalogId, long granteeId) {
    return runInReadTransaction(
        callCtx, () -> this.loadAllGrantRecordsOnGrantee(callCtx, granteeCatalogId, granteeId));
  }

  /** {@inheritDoc} */
  @Override
  public boolean hasChildrenAtomically(
      @Nonnull PolarisCallContext callCtx,
      @Nullable PolarisEntityType optionalEntityType,
      long catalogId,
      long parentId) {
    return runInReadTransaction(
        callCtx, () -> this.hasChildren(callCtx, optionalEntityType, catalogId, parentId));
  }

  //
  // Implementations of the one-shot atomic IntegrationPersistence methods which explicitly run
  // the * variants of methods in a new transaction.
  //

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisPrincipalSecrets loadPrincipalSecretsAtomically(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId) {
    return runInReadTransaction(callCtx, () -> this.loadPrincipalSecrets(callCtx, clientId));
  }

  /** {@inheritDoc} */
  @Override
  @Nonnull
  public PolarisPrincipalSecrets generateNewPrincipalSecretsAtomically(
      @Nonnull PolarisCallContext callCtx, @Nonnull String principalName, long principalId) {
    return runInTransaction(
        callCtx, () -> this.generateNewPrincipalSecrets(callCtx, principalName, principalId));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisPrincipalSecrets rotatePrincipalSecretsAtomically(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull String clientId,
      long principalId,
      boolean reset,
      @Nonnull String oldSecretHash) {
    return runInTransaction(
        callCtx,
        () -> this.rotatePrincipalSecrets(callCtx, clientId, principalId, reset, oldSecretHash));
  }

  /** {@inheritDoc} */
  @Override
  public void deletePrincipalSecretsAtomically(
      @Nonnull PolarisCallContext callCtx, @Nonnull String clientId, long principalId) {
    runActionInTransaction(
        callCtx, () -> this.deletePrincipalSecrets(callCtx, clientId, principalId));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> createStorageIntegrationAtomically(
          @Nonnull PolarisCallContext callCtx,
          long catalogId,
          long entityId,
          PolarisStorageConfigurationInfo polarisStorageConfigurationInfo) {
    return runInTransaction(
        callCtx,
        () ->
            this.createStorageIntegration(
                callCtx, catalogId, entityId, polarisStorageConfigurationInfo));
  }

  /** {@inheritDoc} */
  @Override
  public <T extends PolarisStorageConfigurationInfo>
      void persistStorageIntegrationIfNeededAtomically(
          @Nonnull PolarisCallContext callCtx,
          @Nonnull PolarisBaseEntity entity,
          @Nullable PolarisStorageIntegration<T> storageIntegration) {
    runActionInTransaction(
        callCtx, () -> this.persistStorageIntegrationIfNeeded(callCtx, entity, storageIntegration));
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public <T extends PolarisStorageConfigurationInfo>
      PolarisStorageIntegration<T> loadPolarisStorageIntegrationAtomically(
          @Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    return runInReadTransaction(callCtx, () -> this.loadPolarisStorageIntegration(callCtx, entity));
  }

  //
  // Implementations of the in-transaction versions for basic write/delete/lookup using the
  // slice-based model supported by this class.
  //

  /** {@inheritDoc} */
  @Override
  public void writeEntity(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull PolarisBaseEntity entity,
      boolean nameOrParentChanged,
      @Nullable PolarisBaseEntity originalEntity) {
    writeToEntities(callCtx, entity);
    writeToEntitiesChangeTracking(callCtx, entity);

    if (nameOrParentChanged) {
      if (originalEntity != null) {
        // In our case, rename isn't automatically handled when the main "entities" slice
        // is updated; instead we must explicitly remove from the old entitiesActive
        // key as well.
        deleteFromEntitiesActive(callCtx, originalEntity);
      }
      writeToEntitiesActive(callCtx, entity);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void writeEntities(
      @Nonnull PolarisCallContext callCtx,
      @Nonnull List<PolarisBaseEntity> entities,
      @Nullable List<PolarisBaseEntity> originalEntities) {
    if (originalEntities != null) {
      callCtx
          .getDiagServices()
          .check(
              entities.size() == originalEntities.size(),
              "mismatched_entities_and_original_entities_size",
              "entities.size()={}, originalEntities.size()={}",
              entities.size(),
              originalEntities.size());
    }
    for (int i = 0; i < entities.size(); ++i) {
      PolarisBaseEntity entity = entities.get(i);
      PolarisBaseEntity originalEntity = originalEntities != null ? originalEntities.get(i) : null;
      boolean nameOrParentChanged =
          originalEntity == null
              || !entity.getName().equals(originalEntity.getName())
              || entity.getParentId() != originalEntity.getParentId();
      this.writeEntity(callCtx, entity, nameOrParentChanged, originalEntity);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void deleteEntity(@Nonnull PolarisCallContext callCtx, @Nonnull PolarisBaseEntity entity) {
    deleteFromEntitiesActive(callCtx, entity);
    deleteFromEntities(callCtx, entity);
    deleteFromEntitiesChangeTracking(callCtx, entity);
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public PolarisBaseEntity lookupEntityByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(catalogId, parentId, typeCode, name);

    // ensure that the entity exists
    EntityNameLookupRecord entityActiveRecord = lookupEntityActive(callCtx, entityActiveKey);

    // if not found, return null
    if (entityActiveRecord == null) {
      return null;
    }

    // lookup the entity, should be there
    PolarisBaseEntity entity =
        lookupEntity(
            callCtx,
            entityActiveRecord.getCatalogId(),
            entityActiveRecord.getId(),
            entityActiveRecord.getTypeCode());
    callCtx
        .getDiagServices()
        .checkNotNull(
            entity, "unexpected_not_found_entity", "entityActiveRecord={}", entityActiveRecord);

    // return it now
    return entity;
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public EntityNameLookupRecord lookupEntityIdAndSubTypeByName(
      @Nonnull PolarisCallContext callCtx,
      long catalogId,
      long parentId,
      int typeCode,
      @Nonnull String name) {
    PolarisEntitiesActiveKey entityActiveKey =
        new PolarisEntitiesActiveKey(catalogId, parentId, typeCode, name);
    return lookupEntityActive(callCtx, entityActiveKey);
  }
}
