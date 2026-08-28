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

/**
 * In-memory image rebuilt from the compacted shared-storage metadata topic.
 *
 * <p>The image is deliberately fail-closed. Authoritative getters are unavailable until the initial replay reaches the
 * captured topic end offset and {@link #markReady()} is called. Live records may continue to be applied afterwards.
 * Illegal state regressions, such as re-preparing an already committed object or moving a broker sequence watermark
 * backwards, are treated as corruption rather than silently reconciled.</p>
 */
public final class SharedMetadataImage {
    private static final long INITIAL_SEQUENCE = 1L;

    private final Map<Long, PreparedObject> preparedObjects = new HashMap<>();
    private final Map<Long, SharedObjectMetadata> committedObjects = new HashMap<>();
    private final Map<Integer, Long> brokerSequenceWatermarks = new HashMap<>();
    private boolean ready;

    public synchronized void apply(byte[] keyBytes, byte[] valueBytes) {
        MetadataKey key = SharedMetadataRecordCodec.decodeKey(keyBytes);
        MetadataValue value = SharedMetadataRecordCodec.decodeValue(key, valueBytes);
        switch (key.type()) {
            case OBJECT -> applyObject(key.id(), value);
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
            SharedObjectMetadata existing = committedObjects.putIfAbsent(objectId, metadata);
            if (existing != null && !existing.equals(metadata)) {
                throw corruption("object " + objectId + " has conflicting COMMITTED metadata");
            }
            // A compacted topic may expose COMMIT without its earlier PREPARE, so PREPARE is not required here.
            preparedObjects.remove(objectId);
            return;
        }
        throw corruption("object " + objectId + " has incompatible metadata value " + value.getClass().getName());
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
        ready = true;
    }

    public synchronized boolean isReady() {
        return ready;
    }

    public synchronized List<SharedObjectMetadata> committedObjects() {
        requireReady();
        List<SharedObjectMetadata> result = new ArrayList<>(committedObjects.values());
        result.sort(Comparator.comparingLong(SharedObjectMetadata::objectId));
        return List.copyOf(result);
    }

    public synchronized List<PreparedObject> preparedObjects() {
        requireReady();
        List<PreparedObject> result = new ArrayList<>(preparedObjects.values());
        result.sort(Comparator.comparingLong(PreparedObject::objectId));
        return List.copyOf(result);
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
        if (!ready) {
            throw new IllegalStateException("Shared metadata image is not ready");
        }
    }

    private static IllegalStateException corruption(String message) {
        return new IllegalStateException("Corrupt shared-storage metadata image: " + message);
    }

    public record PreparedObject(long objectId, long createdTimeMs) {
    }
}
