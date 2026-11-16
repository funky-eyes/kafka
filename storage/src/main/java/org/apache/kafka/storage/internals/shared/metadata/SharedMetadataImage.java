/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.shared.metadata;

import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.BrokerSequenceValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.CleanupClaimedValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.CleanupDeletedValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.CommittedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.FencedClaimedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.FencedPreparedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.MetadataKey;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.ObjectCatalogSealValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.MetadataValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.PartitionLogStartValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.PreparedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.TombstoneValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * In-memory image rebuilt from the compacted shared-storage metadata topic.
 *
 * <p>The image is deliberately fail-closed. Authoritative getters are unavailable while the initial replay is still
 * recovering and after a live replay failure. Initial replay publishes only final COMMITTED winners;
 * live object tombstones without KRaft retirement authorization fail closed.
 * Cleanup fences use a distinct compacted key from object metadata. The
 * single metadata partition orders live COMMIT and cleanup records. On initial replay, however, compaction may have
 * removed an earlier record for either key, so their surviving order cannot prove the winner. A COMMIT and cleanup
 * fence for the same object therefore fail recovery closed instead of publishing an ambiguous reference.</p>
 */
public final class SharedMetadataImage {
    private static final long INITIAL_SEQUENCE = 1L;

    // Each object transition is still serialized by the single metadata
    // replay writer. Concurrent maps allow O(1) lock-free read probes without
    // contending with expensive catalog callbacks or whole-image snapshots.
    private final Map<Long, PreparedObject> preparedObjects = new ConcurrentHashMap<>();
    private final Map<Long, SharedObjectMetadata> committedObjects = new ConcurrentHashMap<>();
    private final Map<Long, CommitProvenance> committedObjectPositions = new HashMap<>();

    private record CommitProvenance(long offset, boolean fenced) {
    }
    private final Map<Long, CatalogSealEvidence> catalogSeals = new ConcurrentHashMap<>();
    private final Map<Long, CleanupObject> cleanupObjects = new ConcurrentHashMap<>();
    private final Map<Integer, Long> brokerSequenceWatermarks = new HashMap<>();
    private final Map<SharedPartitionId, Long> partitionLogStarts = new HashMap<>();
    private final Consumer<SharedObjectMetadata> committedObjectListener;
    private volatile State state = State.RECOVERING;
    private volatile Throwable failure;
    private long lastConsumedMetadataOffset = -1L;

    public SharedMetadataImage() {
        this(ignored -> { });
    }

    public SharedMetadataImage(Consumer<SharedObjectMetadata> committedObjectListener) {
        this.committedObjectListener = Objects.requireNonNull(committedObjectListener, "committedObjectListener");
    }

    public synchronized void apply(byte[] keyBytes, byte[] valueBytes) {
        applyWithOffset(keyBytes, valueBytes, -1L);
    }

    private void applyWithOffset(byte[] keyBytes, byte[] valueBytes, long recordOffset) {
        if (state == State.FAILED) {
            throw failedState();
        }
        try {
            MetadataKey key = SharedMetadataRecordCodec.decodeKey(keyBytes);
            MetadataValue value = SharedMetadataRecordCodec.decodeValue(key, valueBytes);
            switch (key.type()) {
                case OBJECT -> applyObject(key.id(), value, recordOffset);
                case OBJECT_CLEANUP -> applyCleanup(key.id(), value);
                case OBJECT_CATALOG_SEAL -> applyObjectCatalogSeal(key.id(), value, recordOffset);
                case BROKER_SEQUENCE -> applyBrokerSequence(Math.toIntExact(key.id()), value);
                case PARTITION_LOG_START -> applyPartitionLogStart(key.partition(), value);
            }
        } catch (RuntimeException | Error replayFailure) {
            markFailed(replayFailure);
            throw replayFailure;
        }
    }

    /**
     * Applies one record from the single authoritative Kafka metadata partition, retaining
     * the actual consumed offset in the same monitor as the image update.
     *
     * <p>Offsets can have gaps because the metadata topic is compacted. An offset
     * that is negative, duplicated, or behind a previously consumed record fails
     * the image closed. Direct in-memory calls to {@link #apply(byte[], byte[])}
     * are not associated with a Kafka consumer offset.</p>
     */
    public synchronized void applyFromMetadataLog(byte[] keyBytes, byte[] valueBytes, long offset) {
        if (state == State.FAILED) {
            throw failedState();
        }
        try {
            if (offset < 0L || offset <= lastConsumedMetadataOffset) {
                throw corruption(
                    "metadata consumer offset " + offset + " does not advance " + lastConsumedMetadataOffset
                );
            }
            applyWithOffset(keyBytes, valueBytes, offset);
            lastConsumedMetadataOffset = offset;
        } catch (RuntimeException | Error replayFailure) {
            markFailed(replayFailure);
            throw replayFailure;
        }
    }

    private void applyObject(long objectId, MetadataValue value, long recordOffset) {
        if (value == TombstoneValue.INSTANCE) {
            applyObjectTombstone(objectId);
            return;
        }
        if (value instanceof PreparedObjectValue prepared) {
            applyPreparedObject(objectId, prepared.createdTimeMs(), false);
            return;
        }
        if (value instanceof FencedPreparedObjectValue prepared) {
            applyPreparedObject(objectId, prepared.createdTimeMs(), true);
            return;
        }
        if (value instanceof FencedClaimedObjectValue claimed) {
            applyFencedClaim(objectId, claimed);
            return;
        }
        if (value instanceof CommittedObjectValue committed) {
            applyCommittedObject(objectId, committed, recordOffset);
            return;
        }
        throw corruption("object " + objectId + " has incompatible metadata value " + value.getClass().getName());
    }

    private void applyObjectTombstone(long objectId) {
        if (catalogSeals.containsKey(objectId)) {
            // A historical COMMITTED tombstone cannot erase a sealed
            // physical object while its authoritative lifetime is unresolved.
            throw corruption("Tombstone would erase a sealed COMMITTED object: " + objectId);
        }
        if (state == State.READY) {
            // Live object tombstones lack KRaft retirement authorization.
            // Fail closed rather than diverging from other replica indexes.
            throw corruption("Live object tombstone has no KRaft retirement authority: " + objectId);
        }
        // Initial recovery preserves historical compacted-topic tombstones.
        // No transient COMMITTED reference is exposed before markReady().
        preparedObjects.remove(objectId);
        committedObjects.remove(objectId);
        committedObjectPositions.remove(objectId);
    }

    private void applyPreparedObject(long objectId, long createdTimeMs, boolean fenced) {
        if (catalogSeals.containsKey(objectId)) {
            if (fenced) {
                throw corruption("Fenced PREPARE attempted to reuse sealed object " + objectId);
            }
            return;
        }
        CleanupObject cleanup = cleanupObjects.get(objectId);
        if (cleanup != null) {
            if (fenced || cleanup.fenced) {
                throw corruption("Fenced object " + objectId + " was prepared after its cleanup claim");
            }
            validateCleanupTimestamp(objectId, cleanup.createdTimeMs, createdTimeMs);
            return;
        }
        if (committedObjects.containsKey(objectId)) {
            throw corruption("object " + objectId + " regressed from COMMITTED to PREPARED");
        }
        PreparedObject candidate = new PreparedObject(objectId, createdTimeMs, fenced);
        PreparedObject existing = preparedObjects.putIfAbsent(objectId, candidate);
        if (existing != null && !existing.equals(candidate)) {
            throw corruption("object " + objectId + " was prepared with conflicting timestamps or protocol");
        }
    }

    private void applyFencedClaim(long objectId, FencedClaimedObjectValue claimed) {
        if (committedObjects.containsKey(objectId) || catalogSeals.containsKey(objectId)) {
            throw corruption("Fenced CLAIM after COMMIT for object " + objectId);
        }
        PreparedObject prepared = preparedObjects.get(objectId);
        if (prepared != null && !prepared.fenced) {
            throw corruption("Fenced CLAIM cannot retire a legacy PREPARED object " + objectId);
        }
        CleanupObject cleanup = cleanupObjects.get(objectId);
        if (cleanup != null && !cleanup.fenced) {
            throw corruption("Fenced CLAIM conflicts with legacy cleanup for object " + objectId);
        }
        validatePreparedCleanupTimestamp(objectId, claimed.createdTimeMs());
        applyCleanupState(objectId, new CleanupRecord(claimed.createdTimeMs(), CleanupState.CLAIMED, true));
    }

    private void applyCommittedObject(long objectId, CommittedObjectValue committed, long recordOffset) {
        SharedObjectMetadata metadata = committed.metadata();
        if (metadata.objectId() != objectId) {
            throw corruption(
                "object key " + objectId + " does not match committed metadata " + metadata.objectId());
        }
        CatalogSealEvidence seal = catalogSeals.get(objectId);
        if (seal != null) {
            // A late or retried COMMIT cannot publish any new reference after
            // its durable Kafka catalog seal. Identity mismatch means corrupt
            // metadata; a matching duplicate loses to the earlier seal.
            if (!committed.fenced()
                || !metadata.equals(committedObjects.get(objectId))
                || !seal.seal().descriptorSha256().equals(SharedMetadataRecordCodec.committedDescriptorSha256(metadata))) {
                throw corruption("Post-seal object COMMIT has a conflicting descriptor: " + objectId);
            }
            return;
        }
        if (suppressCommitAfterCleanup(objectId)) {
            return;
        }
        SharedObjectMetadata existing = committedObjects.putIfAbsent(objectId, metadata);
        if (existing != null && !existing.equals(metadata)) {
            throw corruption("object " + objectId + " has conflicting COMMITTED metadata");
        }
        if (existing != null && committedObjectPositions.containsKey(objectId)
            && committedObjectPositions.get(objectId).fenced() != committed.fenced()) {
            throw corruption("COMMIT changed its original v2 fencing provenance: " + objectId);
        }
        preparedObjects.remove(objectId);
        if (existing == null) {
            committedObjectPositions.put(objectId, new CommitProvenance(recordOffset, committed.fenced()));
        }
        if (existing == null && state == State.READY) {
            // Historical replay must finish before publishing its final
            // COMMITTED winners into the broker's remote read index.
            committedObjectListener.accept(metadata);
        }
    }

    private boolean suppressCommitAfterCleanup(long objectId) {
        CleanupObject cleanup = cleanupObjects.get(objectId);
        if (cleanup == null) {
            return false;
        }
        if (cleanup.fenced) {
            throw corruption("COMMIT after fenced CLAIM for object " + objectId);
        }
        if (state == State.RECOVERING) {
            throw corruption("Ambiguous compacted COMMIT and cleanup fence for object " + objectId);
        }
        // Legacy different-key cleanup won the live append order. Keep its
        // fence, but never let a late COMMIT publish new remote read references.
        return true;
    }

    private void applyObjectCatalogSeal(long objectId, MetadataValue value, long recordOffset) {
        if (!(value instanceof ObjectCatalogSealValue seal)) {
            throw corruption("Catalog seal may not be tombstoned or replaced by a different record type");
        }
        if (recordOffset < 0L || seal.originalCommitOffset() >= recordOffset) {
            throw corruption("Catalog seal requires a later, actual read-committed Kafka record offset");
        }
        SharedObjectMetadata original = committedObjects.get(objectId);
        CommitProvenance committedPosition = committedObjectPositions.get(objectId);
        if (original == null || committedPosition == null
            || committedPosition.offset() != seal.originalCommitOffset()) {
            // A compacted-away COMMIT, replay gap or forged position can
            // never be substituted by a bare sampled LSO.
            throw corruption("Catalog seal lacks the original replayed COMMITTED record: " + objectId);
        }
        // A plain legacy COMMIT carries no v2 fencing provenance. The
        // experimental catalog seal cannot certify it after compaction.
        if (!committedPosition.fenced()) {
            throw corruption("Catalog seal requires the original v2 fenced COMMIT: " + objectId);
        }
        if (original.objectSize() != seal.objectSize()
            || original.objectChecksum() != seal.objectChecksum()
            || !SharedMetadataRecordCodec.committedDescriptorSha256(original).equals(seal.descriptorSha256())) {
            throw corruption("Catalog seal changed an immutable packed object descriptor: " + objectId);
        }
        CatalogSealEvidence next = new CatalogSealEvidence(recordOffset, seal);
        CatalogSealEvidence prior = catalogSeals.get(objectId);
        if (prior != null && !prior.equals(next)) {
            throw corruption("Conflicting catalog seal generations for object " + objectId);
        }
        catalogSeals.putIfAbsent(objectId, next);
        // Keep existing COMMITTED references readable. This replay fact
        // DOES NOT retire primary/alternate indexes, drain readers, or
        // grant DELETE permission.
    }

    /** Read-only exact COMMIT/seal evidence, not cross-log Controller authority. */
    public synchronized Optional<CatalogSealEvidence> catalogSeal(long objectId) {
        requireReady();
        return Optional.ofNullable(catalogSeals.get(objectId));
    }

    public record CatalogSealEvidence(long sealRecordOffset, ObjectCatalogSealValue seal) {
        public CatalogSealEvidence {
            if (sealRecordOffset < 0L) {
                throw new IllegalArgumentException("Catalog seal needs an actual Kafka record offset");
            }
            Objects.requireNonNull(seal, "seal");
        }
    }

    private void applyCleanup(long objectId, MetadataValue value) {
        CleanupRecord cleanup = cleanupRecord(objectId, value);
        if (committedObjects.containsKey(objectId)) {
            if (state == State.RECOVERING) {
                throw corruption("Ambiguous compacted COMMIT and cleanup fence for object " + objectId);
            }
            // COMMIT has the lower authoritative offset, therefore the cleanup attempt lost and must be a no-op.
            return;
        }
        PreparedObject prepared = preparedObjects.get(objectId);
        if (prepared != null && prepared.fenced) {
            throw corruption("Legacy cleanup cannot claim fenced PREPARED object " + objectId);
        }
        validatePreparedCleanupTimestamp(objectId, cleanup.createdTimeMs());
        applyCleanupState(objectId, cleanup);
    }

    private CleanupRecord cleanupRecord(long objectId, MetadataValue value) {
        if (value == TombstoneValue.INSTANCE) {
            throw corruption("object cleanup fence must never be tombstoned for object " + objectId);
        }
        if (value instanceof CleanupClaimedValue claimed) {
            return new CleanupRecord(claimed.createdTimeMs(), CleanupState.CLAIMED);
        }
        if (value instanceof CleanupDeletedValue deleted) {
            return new CleanupRecord(deleted.createdTimeMs(), CleanupState.DELETED);
        }
        throw corruption("object " + objectId + " has incompatible cleanup metadata value");
    }

    private void validatePreparedCleanupTimestamp(long objectId, long createdTimeMs) {
        PreparedObject prepared = preparedObjects.get(objectId);
        if (prepared != null) {
            validateCleanupTimestamp(objectId, prepared.createdTimeMs(), createdTimeMs);
        }
    }

    private void applyCleanupState(long objectId, CleanupRecord candidate) {
        CleanupObject existing = cleanupObjects.get(objectId);
        if (existing != null) {
            validateCleanupTimestamp(objectId, existing.createdTimeMs, candidate.createdTimeMs());
            if (existing.fenced != candidate.fenced()) {
                throw corruption("Object " + objectId + " mixes fenced and legacy cleanup generations");
            }
            if (existing.state == CleanupState.DELETED && candidate.state() == CleanupState.CLAIMED) {
                // Duplicate/late claim cannot move a terminal fence backwards.
                return;
            }
        }
        cleanupObjects.put(objectId, new CleanupObject(candidate.createdTimeMs(), candidate.state(), candidate.fenced()));
        preparedObjects.remove(objectId);
    }

    private static void validateCleanupTimestamp(long objectId, long expected, long actual) {
        if (expected != actual) {
            throw corruption("object " + objectId + " has conflicting cleanup generations");
        }
    }

    private void applyBrokerSequence(int brokerId, MetadataValue value) {
        if (value == TombstoneValue.INSTANCE) {
            throw corruption("broker sequence watermark must never be tombstoned for broker " + brokerId);
        }
        if (!(value instanceof BrokerSequenceValue sequenceValue)) {
            throw corruption("broker " + brokerId + " has incompatible sequence metadata value");
        }
        long candidate = sequenceValue.reservedExclusiveSequence();
        Long existing = brokerSequenceWatermarks.get(brokerId);
        if (existing != null && candidate < existing) {
            throw corruption(
                "broker " + brokerId + " sequence watermark moved backwards from " + existing + " to " + candidate);
        }
        brokerSequenceWatermarks.put(brokerId, candidate);
    }

    private void applyPartitionLogStart(SharedPartitionId partition, MetadataValue value) {
        if (!(value instanceof PartitionLogStartValue advance)) {
            // A compacted-topic tombstone would erase the last known retirement fence.
            throw corruption("partition " + partition + " log-start fence must never be tombstoned");
        }
        long candidate = advance.startOffset();
        Long current = partitionLogStarts.get(partition);
        if (current != null && candidate < current) {
            throw corruption(
                "partition " + partition + " log-start offset moved backwards from " + current + " to " + candidate
            );
        }
        partitionLogStarts.put(partition, candidate);
    }

    public synchronized void markReady() {
        if (state == State.FAILED) {
            throw failedState();
        }
        if (state == State.READY) {
            return;
        }
        try {
            // Replay only final COMMITTED winners, without allocating an
            // additional object-sized snapshot on broker bootstrap.
            for (SharedObjectMetadata committed : committedObjects.values()) {
                committedObjectListener.accept(committed);
            }
            state = State.READY;
        } catch (RuntimeException | Error publicationFailure) {
            markFailed(publicationFailure);
            throw publicationFailure;
        }
    }

    public synchronized void markFailed(Throwable cause) {
        Objects.requireNonNull(cause, "cause");
        if (state != State.FAILED) {
            state = State.FAILED;
            failure = cause;
        } else if (failure != cause) {
            failure.addSuppressed(cause);
        }
    }

    public synchronized State state() {
        return state;
    }

    public synchronized boolean isReady() {
        return state == State.READY;
    }

    public synchronized Optional<Throwable> failure() {
        return Optional.ofNullable(failure);
    }

    public synchronized List<SharedObjectMetadata> committedObjects() {
        requireReady();
        List<SharedObjectMetadata> result = new ArrayList<>(committedObjects.values());
        result.sort(Comparator.comparingLong(SharedObjectMetadata::objectId));
        return List.copyOf(result);
    }

    public synchronized Optional<SharedObjectMetadata> committedObject(long objectId) {
        requireReady();
        return Optional.ofNullable(committedObjects.get(objectId));
    }

    /**
     * Existing object identity check; call only after a new uploader fenced
     * previous producer generations and caught up to post-fence Kafka LSO.
     */
    public boolean objectIdentityKnown(long objectId) {
        requireReady();
        if (objectId <= 0L) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        // PREPARED -> COMMITTED and PREPARED -> CLAIMED always install
        // the successor BEFORE removing the former. Thus readers cannot
        // observe an absent identity during either successful transition.
        boolean known = preparedObjects.containsKey(objectId) || committedObjects.containsKey(objectId)
            || cleanupObjects.containsKey(objectId) || catalogSeals.containsKey(objectId);
        requireReady();
        return known;
    }

    public synchronized List<ObjectMetadataStore.PreparedObject> preparedObjects() {
        requireReady();
        List<ObjectMetadataStore.PreparedObject> result = preparedObjects.values().stream()
            .map(prepared -> new ObjectMetadataStore.PreparedObject(prepared.objectId, prepared.createdTimeMs))
            .sorted(Comparator.comparingLong(ObjectMetadataStore.PreparedObject::objectId))
            .toList();
        return List.copyOf(result);
    }

    /** Only generation-fenced PREPARED objects can be considered for v2 arbitration. */
    public synchronized List<ObjectMetadataStore.PreparedObject> fencedPreparedObjects() {
        requireReady();
        return preparedObjects.values().stream()
            .filter(prepared -> prepared.fenced)
            .map(prepared -> new ObjectMetadataStore.PreparedObject(prepared.objectId, prepared.createdTimeMs))
            .sorted(Comparator.comparingLong(ObjectMetadataStore.PreparedObject::objectId))
            .toList();
    }

    /** Durable v2 CLAIM records remain retryable after late S3 PUT completion. */
    public synchronized List<ObjectMetadataStore.PreparedObject> fencedCleanupClaimedObjects() {
        requireReady();
        return cleanupObjects.entrySet().stream()
            .filter(entry -> entry.getValue().fenced && entry.getValue().state == CleanupState.CLAIMED)
            .map(entry -> new ObjectMetadataStore.PreparedObject(
                entry.getKey(), entry.getValue().createdTimeMs))
            .sorted(Comparator.comparingLong(ObjectMetadataStore.PreparedObject::objectId))
            .toList();
    }

    /** Targeted O(1) check; call only after post-fencing LSO catch-up. */
    public boolean fencedPrepared(long objectId, long expectedCreatedTimeMs) {
        requireReady();
        // The caller MUST first capture and apply a post-producer-fence
        // read-committed LSO. An arbitrary live map read is not GC authority.
        PreparedObject prepared = preparedObjects.get(objectId);
        boolean matches = prepared != null && prepared.fenced && prepared.createdTimeMs == expectedCreatedTimeMs;
        requireReady();
        return matches;
    }

    public synchronized List<ObjectMetadataStore.PreparedObject> cleanupClaimedObjects() {
        requireReady();
        List<ObjectMetadataStore.PreparedObject> result = cleanupObjects.entrySet().stream()
            .filter(entry -> entry.getValue().state == CleanupState.CLAIMED)
            .map(entry -> new ObjectMetadataStore.PreparedObject(entry.getKey(), entry.getValue().createdTimeMs))
            .sorted(Comparator.comparingLong(ObjectMetadataStore.PreparedObject::objectId))
            .toList();
        return List.copyOf(result);
    }

    public synchronized boolean cleanupClaimed(long objectId, long createdTimeMs) {
        requireReady();
        CleanupObject cleanup = cleanupObjects.get(objectId);
        return cleanup != null && cleanup.createdTimeMs == createdTimeMs && cleanup.state == CleanupState.CLAIMED;
    }

    public synchronized boolean cleanupFenced(long objectId, long createdTimeMs) {
        requireReady();
        CleanupObject cleanup = cleanupObjects.get(objectId);
        return cleanup != null && cleanup.createdTimeMs == createdTimeMs;
    }

    /**
     * Reads one object cleanup state in O(1), without copying the full claimed
     * inventory. This is replay evidence, not authorization to delete S3 data.
     * Only a CLAIMED state can be completed; DELETED is idempotently terminal.
     */
    public synchronized CleanupCompletionEvidence cleanupCompletionEvidence(long objectId) {
        requireReady();
        if (objectId <= 0L) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        CleanupObject cleanup = cleanupObjects.get(objectId);
        if (cleanup == null) {
            return new CleanupCompletionEvidence(CleanupCompletionStatus.ABSENT, -1L);
        }
        CleanupCompletionStatus status = cleanup.state == CleanupState.CLAIMED
            ? CleanupCompletionStatus.CLAIMED : CleanupCompletionStatus.DELETED;
        return new CleanupCompletionEvidence(status, cleanup.createdTimeMs);
    }

    public enum CleanupCompletionStatus {
        ABSENT,
        CLAIMED,
        DELETED
    }

    public record CleanupCompletionEvidence(CleanupCompletionStatus status, long createdTimeMs) {
        public CleanupCompletionEvidence {
            Objects.requireNonNull(status, "status");
            if ((status == CleanupCompletionStatus.ABSENT) != (createdTimeMs == -1L)
                || createdTimeMs < -1L) {
                throw new IllegalArgumentException("Invalid cleanup completion state or creation timestamp");
            }
        }
    }

    public synchronized long brokerReservedExclusiveSequence(int brokerId) {
        requireReady();
        if (brokerId < 0 || brokerId > BrokerObjectId.MAX_BROKER_ID) {
            throw new IllegalArgumentException(
                "brokerId must be in [0, " + BrokerObjectId.MAX_BROKER_ID + "]: " + brokerId);
        }
        return brokerSequenceWatermarks.getOrDefault(brokerId, INITIAL_SEQUENCE);
    }

    /**
     * Returns the latest replayed lower offset bound for a specific immutable topic ID and partition.
     *
     * <p>This is metadata only. It must not be used to evict a remote range or delete S3 data until
     * durable writer fencing, reader quiescence, and range retirement are implemented.</p>
     */
    public synchronized long partitionLogStartOffset(SharedPartitionId partition) {
        requireReady();
        Objects.requireNonNull(partition, "partition");
        return partitionLogStarts.getOrDefault(partition, 0L);
    }

    /**
     * Returns an immutable point-in-time copy of the explicitly replayed partition log-start
     * watermarks. An absent entry is different from a persisted offset of zero.
     *
     * <p>This is read-only evidence, not authority to retire remote references or delete S3 data.</p>
     */
    public synchronized Map<SharedPartitionId, Long> partitionLogStartsSnapshot() {
        requireReady();
        return Map.copyOf(partitionLogStarts);
    }

    /**
     * Reads one explicitly replayed partition log-start and the consumer provenance atomically.
     *
     * <p>The targeted lookup does not copy the COMMITTED-object inventory. Neither the
     * offset nor the watermark authenticates an exclusive writer or permits deletion.</p>
     */
    public synchronized PartitionLogStartEvidence partitionLogStartEvidence(SharedPartitionId partition) {
        requireReady();
        Objects.requireNonNull(partition, "partition");
        Long replayed = partitionLogStarts.get(partition);
        return new PartitionLogStartEvidence(
            lastConsumedMetadataOffset,
            replayed == null ? OptionalLong.empty() : OptionalLong.of(replayed)
        );
    }

    public record PartitionLogStartEvidence(long lastConsumedMetadataOffset, OptionalLong explicitLogStart) {
        public PartitionLogStartEvidence {
            if (lastConsumedMetadataOffset < -1L) {
                throw new IllegalArgumentException("lastConsumedMetadataOffset must be >= -1");
            }
            Objects.requireNonNull(explicitLogStart, "explicitLogStart");
        }
    }

    /**
     * Atomically captures one physically COMMITTED object's immutable identity
     * and only the explicitly persisted log-starts for its packed ranges.
     *
     * <p>Unlike a global retirement inventory, this targeted lookup does not
     * copy every COMMITTED object or unrelated partition watermark. Both
     * object existence and watermark values come from the same replay point.
     * Missing COMMITTED metadata is different from a logically expired object.
     * This is advisory evidence, never permission to DELETE physical bytes.</p>
     */
    public synchronized Optional<CommittedObjectEvidence> committedObjectRetirementEvidence(long objectId) {
        requireReady();
        if (objectId <= 0L) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        SharedObjectMetadata committed = committedObjects.get(objectId);
        if (committed == null) {
            return Optional.empty();
        }
        Map<SharedPartitionId, Long> relevantWatermarks = new HashMap<>();
        for (SharedObjectRange range : committed.ranges()) {
            Long explicit = partitionLogStarts.get(range.partition());
            if (explicit != null) {
                relevantWatermarks.put(range.partition(), explicit);
            }
        }
        return Optional.of(new CommittedObjectEvidence(
            committed, relevantWatermarks, lastConsumedMetadataOffset
        ));
    }

    public record CommittedObjectEvidence(
        SharedObjectMetadata metadata,
        Map<SharedPartitionId, Long> explicitLogStarts,
        long lastConsumedMetadataOffset
    ) {
        public CommittedObjectEvidence {
            Objects.requireNonNull(metadata, "metadata");
            explicitLogStarts = Map.copyOf(Objects.requireNonNull(explicitLogStarts, "explicitLogStarts"));
            if (lastConsumedMetadataOffset < -1L) {
                throw new IllegalArgumentException("lastConsumedMetadataOffset must be >= -1");
            }
        }
    }

    /**
     * Captures the replayed COMMITTED inventory and explicit partition log-start watermarks under one lock.
     *
     * <p>The immutable evidence is a single local replay point in time. The consumed metadata offset
     * is diagnostic provenance, not proof the consumer has reached Kafka's latest committed offset.
     * It is not a broker-generation fence, durable reference retirement, or permission to delete.
     * Live replay can advance after this method returns.</p>
     */
    public synchronized RetirementEvidenceSnapshot retirementEvidenceSnapshot() {
        requireReady();
        return new RetirementEvidenceSnapshot(committedObjects(), partitionLogStarts, lastConsumedMetadataOffset);
    }

    public record RetirementEvidenceSnapshot(
        List<SharedObjectMetadata> committedObjects,
        Map<SharedPartitionId, Long> partitionLogStarts,
        long lastConsumedMetadataOffset
    ) {
        public RetirementEvidenceSnapshot(
            List<SharedObjectMetadata> committedObjects,
            Map<SharedPartitionId, Long> partitionLogStarts
        ) {
            this(committedObjects, partitionLogStarts, -1L);
        }

        public RetirementEvidenceSnapshot {
            committedObjects = List.copyOf(Objects.requireNonNull(committedObjects, "committedObjects"));
            partitionLogStarts = Map.copyOf(Objects.requireNonNull(partitionLogStarts, "partitionLogStarts"));
            if (lastConsumedMetadataOffset < -1L) {
                throw new IllegalArgumentException("lastConsumedMetadataOffset must be >= -1");
            }
        }
    }

    private void requireReady() {
        if (state == State.RECOVERING) {
            throw new IllegalStateException("Shared metadata image is still recovering");
        }
        if (state == State.FAILED) {
            throw failedState();
        }
    }

    private IllegalStateException failedState() {
        return new IllegalStateException("Shared metadata image has failed and is no longer authoritative", failure);
    }

    private static IllegalStateException corruption(String message) {
        return new IllegalStateException("Corrupt shared-storage metadata image: " + message);
    }

    public enum State {
        RECOVERING,
        READY,
        FAILED
    }

    private enum CleanupState {
        CLAIMED,
        DELETED
    }

    private record PreparedObject(long objectId, long createdTimeMs, boolean fenced) {
    }

    private record CleanupObject(long createdTimeMs, CleanupState state, boolean fenced) {
    }

    private record CleanupRecord(long createdTimeMs, CleanupState state, boolean fenced) {
        private CleanupRecord(long createdTimeMs, CleanupState state) {
            this(createdTimeMs, state, false);
        }
    }
}
