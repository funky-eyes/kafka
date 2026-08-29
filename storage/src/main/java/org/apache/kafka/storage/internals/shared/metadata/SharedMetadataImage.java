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
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.MetadataKey;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.MetadataValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.PreparedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.TombstoneValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * In-memory image rebuilt from the compacted shared-storage metadata topic.
 *
 * <p>The image is deliberately fail-closed. Authoritative getters are unavailable while the initial replay is still
 * recovering and after a live replay failure. Cleanup fences use a distinct compacted key from object metadata. The
 * single metadata partition therefore supplies the total order between COMMIT and cleanup while compaction preserves
 * both winners' evidence. A cleanup fence that precedes a delayed COMMIT wins; a cleanup record after a committed object
 * is a losing claim and is ignored.</p>
 */
public final class SharedMetadataImage {
    private static final long INITIAL_SEQUENCE = 1L;

    private final Map<Long, PreparedObject> preparedObjects = new HashMap<>();
    private final Map<Long, SharedObjectMetadata> committedObjects = new HashMap<>();
    private final Map<Long, CleanupObject> cleanupObjects = new HashMap<>();
    private final Map<Integer, Long> brokerSequenceWatermarks = new HashMap<>();
    private final Consumer<SharedObjectMetadata> committedObjectListener;
    private State state = State.RECOVERING;
    private Throwable failure;

    public SharedMetadataImage() {
        this(ignored -> { });
    }

    public SharedMetadataImage(Consumer<SharedObjectMetadata> committedObjectListener) {
        this.committedObjectListener = Objects.requireNonNull(committedObjectListener, "committedObjectListener");
    }

    public synchronized void apply(byte[] keyBytes, byte[] valueBytes) {
        if (state == State.FAILED) {
            throw failedState();
        }
        MetadataKey key = SharedMetadataRecordCodec.decodeKey(keyBytes);
        MetadataValue value = SharedMetadataRecordCodec.decodeValue(key, valueBytes);
        switch (key.type()) {
            case OBJECT -> applyObject(key.id(), value);
            case OBJECT_CLEANUP -> applyCleanup(key.id(), value);
            case BROKER_SEQUENCE -> applyBrokerSequence(Math.toIntExact(key.id()), value);
        }
    }

    private void applyObject(long objectId, MetadataValue value) {
        if (value == TombstoneValue.INSTANCE) {
            preparedObjects.remove(objectId);
            committedObjects.remove(objectId);
            return;
        }
        if (value instanceof PreparedObjectValue prepared) {
            CleanupObject cleanup = cleanupObjects.get(objectId);
            if (cleanup != null) {
                // A cleanup key can survive compaction while an older PREPARE is rewritten or replayed. Never resurrect
                // a generation that is already fenced for physical deletion.
                if (cleanup.createdTimeMs != prepared.createdTimeMs()) {
                    throw corruption("object " + objectId + " cleanup fence conflicts with PREPARE timestamp");
                }
                return;
            }
            if (committedObjects.containsKey(objectId)) {
                throw corruption("object " + objectId + " regressed from COMMITTED to PREPARED");
            }
            PreparedObject candidate = new PreparedObject(objectId, prepared.createdTimeMs());
            PreparedObject existing = preparedObjects.putIfAbsent(objectId, candidate);
            if (existing != null && !existing.equals(candidate)) {
                throw corruption("object " + objectId + " was prepared with conflicting timestamps");
            }
            return;
        }
        if (value instanceof CommittedObjectValue committed) {
            SharedObjectMetadata metadata = committed.metadata();
            if (metadata.objectId() != objectId) {
                throw corruption(
                    "object key " + objectId + " does not match committed metadata " + metadata.objectId());
            }
            if (cleanupObjects.containsKey(objectId)) {
                // The cleanup record has an earlier offset in the authoritative partition, so this is a losing delayed
                // COMMIT. Keep the fence and ignore the object value; both keys remain available after compaction.
                return;
            }
            SharedObjectMetadata existing = committedObjects.putIfAbsent(objectId, metadata);
            if (existing != null && !existing.equals(metadata)) {
                throw corruption("object " + objectId + " has conflicting COMMITTED metadata");
            }
            preparedObjects.remove(objectId);
            if (existing == null) {
                committedObjectListener.accept(metadata);
            }
            return;
        }
        throw corruption("object " + objectId + " has incompatible metadata value " + value.getClass().getName());
    }

    private void applyCleanup(long objectId, MetadataValue value) {
        if (value == TombstoneValue.INSTANCE) {
            throw corruption("object cleanup fence must never be tombstoned for object " + objectId);
        }
        if (!(value instanceof CleanupClaimedValue) && !(value instanceof CleanupDeletedValue)) {
            throw corruption("object " + objectId + " has incompatible cleanup metadata value");
        }
        long createdTimeMs = value instanceof CleanupClaimedValue claimed
            ? claimed.createdTimeMs()
            : ((CleanupDeletedValue) value).createdTimeMs();

        if (committedObjects.containsKey(objectId)) {
            // COMMIT has the lower authoritative offset, therefore the cleanup attempt lost and must be a no-op.
            return;
        }

        PreparedObject prepared = preparedObjects.get(objectId);
        if (prepared != null && prepared.createdTimeMs() != createdTimeMs) {
            throw corruption("object " + objectId + " cleanup timestamp does not match PREPARE");
        }

        CleanupState candidateState = value instanceof CleanupDeletedValue
            ? CleanupState.DELETED
            : CleanupState.CLAIMED;
        CleanupObject existing = cleanupObjects.get(objectId);
        if (existing != null) {
            if (existing.createdTimeMs != createdTimeMs) {
                throw corruption("object " + objectId + " has conflicting cleanup generations");
            }
            if (existing.state == CleanupState.DELETED && candidateState == CleanupState.CLAIMED) {
                // Duplicate/late claim cannot move a terminal fence backwards.
                return;
            }
        }
        cleanupObjects.put(objectId, new CleanupObject(createdTimeMs, candidateState));
        preparedObjects.remove(objectId);
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

    public synchronized void markReady() {
        if (state == State.FAILED) {
            throw failedState();
        }
        state = State.READY;
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

    public synchronized List<ObjectMetadataStore.PreparedObject> preparedObjects() {
        requireReady();
        List<ObjectMetadataStore.PreparedObject> result = preparedObjects.values().stream()
            .map(prepared -> new ObjectMetadataStore.PreparedObject(prepared.objectId, prepared.createdTimeMs))
            .sorted(Comparator.comparingLong(ObjectMetadataStore.PreparedObject::objectId))
            .toList();
        return List.copyOf(result);
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

    public synchronized long brokerReservedExclusiveSequence(int brokerId) {
        requireReady();
        if (brokerId < 0 || brokerId > BrokerObjectId.MAX_BROKER_ID) {
            throw new IllegalArgumentException(
                "brokerId must be in [0, " + BrokerObjectId.MAX_BROKER_ID + "]: " + brokerId);
        }
        return brokerSequenceWatermarks.getOrDefault(brokerId, INITIAL_SEQUENCE);
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

    private record PreparedObject(long objectId, long createdTimeMs) {
    }

    private record CleanupObject(long createdTimeMs, CleanupState state) {
    }
}
