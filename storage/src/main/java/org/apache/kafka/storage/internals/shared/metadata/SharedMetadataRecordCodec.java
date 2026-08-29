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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Versioned binary format stored in the compacted {@code __shared_storage_metadata} topic.
 *
 * <p>Keys deliberately have no version field: changing a compacted key encoding would create a new logical key and
 * leave stale records behind. Key layouts are therefore permanent. Values carry an explicit version and may evolve
 * compatibly. Java serialization and Kafka internal message classes are intentionally not used.</p>
 */
public final class SharedMetadataRecordCodec {
    private static final byte OBJECT_KEY = 1;
    private static final byte BROKER_SEQUENCE_KEY = 2;
    private static final byte OBJECT_CLEANUP_KEY = 3;
    private static final int OBJECT_KEY_BYTES = Byte.BYTES + Long.BYTES;
    private static final int BROKER_SEQUENCE_KEY_BYTES = Byte.BYTES + Integer.BYTES;

    private static final short VALUE_VERSION = 1;
    private static final byte OBJECT_PREPARED = 1;
    private static final byte OBJECT_COMMITTED = 2;
    private static final byte BROKER_SEQUENCE_RESERVED = 3;
    private static final byte OBJECT_CLEANUP_CLAIMED = 4;
    private static final byte OBJECT_CLEANUP_DELETED = 5;
    private static final int VALUE_HEADER_BYTES = Short.BYTES + Byte.BYTES;
    private static final int COMMITTED_OBJECT_HEADER_BYTES = Long.BYTES + Long.BYTES + Integer.BYTES;
    private static final int RANGE_BYTES = Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES +
        Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES;
    private static final long SEQUENCE_LIMIT_EXCLUSIVE = BrokerObjectId.MAX_SEQUENCE + 1L;

    private SharedMetadataRecordCodec() {
    }

    public static byte[] objectKey(long objectId) {
        return objectScopedKey(OBJECT_KEY, objectId);
    }

    /**
     * Uses a distinct compacted key from {@link #objectKey(long)} so a losing cleanup claim can never compact away a
     * winning COMMIT (or vice versa). Their offsets in the single metadata partition still provide the total order.
     */
    public static byte[] objectCleanupKey(long objectId) {
        return objectScopedKey(OBJECT_CLEANUP_KEY, objectId);
    }

    private static byte[] objectScopedKey(byte type, long objectId) {
        if (objectId <= 0) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        return ByteBuffer.allocate(OBJECT_KEY_BYTES)
            .put(type)
            .putLong(objectId)
            .array();
    }

    public static byte[] brokerSequenceKey(int brokerId) {
        validateBrokerId(brokerId);
        return ByteBuffer.allocate(BROKER_SEQUENCE_KEY_BYTES)
            .put(BROKER_SEQUENCE_KEY)
            .putInt(brokerId)
            .array();
    }

    public static MetadataKey decodeKey(byte[] keyBytes) {
        Objects.requireNonNull(keyBytes, "keyBytes");
        if (keyBytes.length < Byte.BYTES) {
            throw corruption("metadata key is empty");
        }
        ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
        byte type = buffer.get();
        if (type == OBJECT_KEY || type == OBJECT_CLEANUP_KEY) {
            requireLength(keyBytes.length, OBJECT_KEY_BYTES, "object key");
            long objectId = buffer.getLong();
            if (objectId <= 0) {
                throw corruption("object key contains non-positive objectId " + objectId);
            }
            return new MetadataKey(type == OBJECT_KEY ? KeyType.OBJECT : KeyType.OBJECT_CLEANUP, objectId);
        }
        if (type == BROKER_SEQUENCE_KEY) {
            requireLength(keyBytes.length, BROKER_SEQUENCE_KEY_BYTES, "broker sequence key");
            int brokerId = buffer.getInt();
            validateBrokerIdForDecode(brokerId);
            return new MetadataKey(KeyType.BROKER_SEQUENCE, brokerId);
        }
        throw corruption("unknown metadata key type " + type);
    }

    public static byte[] preparedObjectValue(long createdTimeMs) {
        validateCreatedTimeMs(createdTimeMs);
        return ByteBuffer.allocate(VALUE_HEADER_BYTES + Long.BYTES)
            .putShort(VALUE_VERSION)
            .put(OBJECT_PREPARED)
            .putLong(createdTimeMs)
            .array();
    }

    public static byte[] cleanupClaimedValue(long createdTimeMs) {
        return cleanupValue(OBJECT_CLEANUP_CLAIMED, createdTimeMs);
    }

    public static byte[] cleanupDeletedValue(long createdTimeMs) {
        return cleanupValue(OBJECT_CLEANUP_DELETED, createdTimeMs);
    }

    private static byte[] cleanupValue(byte type, long createdTimeMs) {
        validateCreatedTimeMs(createdTimeMs);
        return ByteBuffer.allocate(VALUE_HEADER_BYTES + Long.BYTES)
            .putShort(VALUE_VERSION)
            .put(type)
            .putLong(createdTimeMs)
            .array();
    }

    public static byte[] committedObjectValue(SharedObjectMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        if (metadata.objectId() <= 0) {
            throw new IllegalArgumentException("metadata objectId must be positive");
        }
        int rangeBytes;
        try {
            rangeBytes = Math.multiplyExact(metadata.ranges().size(), RANGE_BYTES);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("too many shared object ranges", e);
        }
        int totalBytes = Math.addExact(VALUE_HEADER_BYTES + COMMITTED_OBJECT_HEADER_BYTES, rangeBytes);
        ByteBuffer buffer = ByteBuffer.allocate(totalBytes)
            .putShort(VALUE_VERSION)
            .put(OBJECT_COMMITTED)
            .putLong(metadata.objectSize())
            .putLong(metadata.objectChecksum())
            .putInt(metadata.ranges().size());
        for (SharedObjectRange range : metadata.ranges()) {
            buffer.putLong(range.partition().topicIdHigh());
            buffer.putLong(range.partition().topicIdLow());
            buffer.putInt(range.partition().partition());
            buffer.putInt(range.leaderEpoch());
            buffer.putLong(range.offsets().startOffset());
            buffer.putLong(range.offsets().endOffset());
            buffer.putLong(range.objectPosition());
            buffer.putInt(range.objectLength());
            buffer.putLong(range.checksum());
        }
        return buffer.array();
    }

    public static byte[] brokerSequenceValue(long reservedExclusiveSequence) {
        validateReservedExclusiveSequence(reservedExclusiveSequence);
        return ByteBuffer.allocate(VALUE_HEADER_BYTES + Long.BYTES)
            .putShort(VALUE_VERSION)
            .put(BROKER_SEQUENCE_RESERVED)
            .putLong(reservedExclusiveSequence)
            .array();
    }

    /** A null value is the Kafka compacted-topic tombstone for the decoded key. */
    public static MetadataValue decodeValue(MetadataKey key, byte[] valueBytes) {
        Objects.requireNonNull(key, "key");
        if (valueBytes == null) {
            return TombstoneValue.INSTANCE;
        }
        if (valueBytes.length < VALUE_HEADER_BYTES) {
            throw corruption("metadata value is shorter than its header");
        }
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
        short version = buffer.getShort();
        if (version != VALUE_VERSION) {
            throw corruption("unsupported metadata value version " + version);
        }
        byte type = buffer.get();
        if (key.type() == KeyType.OBJECT) {
            if (type == OBJECT_PREPARED) {
                return new PreparedObjectValue(decodeCreatedTime(buffer, "prepared object value"));
            }
            if (type == OBJECT_COMMITTED) {
                return decodeCommittedObject(key.id(), buffer);
            }
            throw corruption("object key has incompatible value type " + type);
        }
        if (key.type() == KeyType.OBJECT_CLEANUP) {
            long createdTimeMs = decodeCreatedTime(buffer, "object cleanup value");
            if (type == OBJECT_CLEANUP_CLAIMED) {
                return new CleanupClaimedValue(createdTimeMs);
            }
            if (type == OBJECT_CLEANUP_DELETED) {
                return new CleanupDeletedValue(createdTimeMs);
            }
            throw corruption("object cleanup key has incompatible value type " + type);
        }
        if (type != BROKER_SEQUENCE_RESERVED) {
            throw corruption("broker sequence key has incompatible value type " + type);
        }
        requireRemaining(buffer, Long.BYTES, "broker sequence value");
        long reservedExclusiveSequence = buffer.getLong();
        validateReservedExclusiveSequenceForDecode(reservedExclusiveSequence);
        requireFullyConsumed(buffer, "broker sequence value");
        return new BrokerSequenceValue(reservedExclusiveSequence);
    }

    private static long decodeCreatedTime(ByteBuffer buffer, String description) {
        requireRemaining(buffer, Long.BYTES, description);
        long createdTimeMs = buffer.getLong();
        if (createdTimeMs < 0) {
            throw corruption(description + " has negative createdTimeMs");
        }
        requireFullyConsumed(buffer, description);
        return createdTimeMs;
    }

    private static CommittedObjectValue decodeCommittedObject(long objectId, ByteBuffer buffer) {
        requireAtLeastRemaining(buffer, COMMITTED_OBJECT_HEADER_BYTES, "committed object header");
        long objectSize = buffer.getLong();
        long objectChecksum = buffer.getLong();
        int rangeCount = buffer.getInt();
        if (objectSize <= 0 || rangeCount <= 0) {
            throw corruption("committed object has invalid size or range count");
        }
        long requiredRangeBytes;
        try {
            requiredRangeBytes = Math.multiplyExact((long) rangeCount, RANGE_BYTES);
        } catch (ArithmeticException e) {
            throw corruption("committed object range byte count overflow", e);
        }
        if (requiredRangeBytes != buffer.remaining()) {
            throw corruption(
                "committed object range bytes mismatch: expected=" + requiredRangeBytes +
                    ", actual=" + buffer.remaining());
        }
        List<SharedObjectRange> ranges = new ArrayList<>(rangeCount);
        for (int i = 0; i < rangeCount; i++) {
            SharedPartitionId partition = new SharedPartitionId(
                buffer.getLong(),
                buffer.getLong(),
                buffer.getInt()
            );
            int leaderEpoch = buffer.getInt();
            OffsetRange offsets = new OffsetRange(buffer.getLong(), buffer.getLong());
            long objectPosition = buffer.getLong();
            int objectLength = buffer.getInt();
            long checksum = buffer.getLong();
            try {
                ranges.add(new SharedObjectRange(
                    partition,
                    offsets,
                    leaderEpoch,
                    objectPosition,
                    objectLength,
                    checksum
                ));
            } catch (IllegalArgumentException e) {
                throw corruption("invalid committed object range", e);
            }
        }
        try {
            return new CommittedObjectValue(new SharedObjectMetadata(
                objectId,
                objectSize,
                objectChecksum,
                ranges
            ));
        } catch (IllegalArgumentException e) {
            throw corruption("invalid committed object metadata", e);
        }
    }

    private static void validateCreatedTimeMs(long createdTimeMs) {
        if (createdTimeMs < 0) {
            throw new IllegalArgumentException("createdTimeMs must be non-negative");
        }
    }

    private static void validateBrokerId(int brokerId) {
        if (brokerId < 0 || brokerId > BrokerObjectId.MAX_BROKER_ID) {
            throw new IllegalArgumentException(
                "brokerId must be in [0, " + BrokerObjectId.MAX_BROKER_ID + "]: " + brokerId);
        }
    }

    private static void validateBrokerIdForDecode(int brokerId) {
        try {
            validateBrokerId(brokerId);
        } catch (IllegalArgumentException e) {
            throw corruption("invalid brokerId in metadata key", e);
        }
    }

    private static void validateReservedExclusiveSequence(long value) {
        if (value < 1L || value > SEQUENCE_LIMIT_EXCLUSIVE) {
            throw new IllegalArgumentException(
                "reservedExclusiveSequence must be in [1, " + SEQUENCE_LIMIT_EXCLUSIVE + "]: " + value);
        }
    }

    private static void validateReservedExclusiveSequenceForDecode(long value) {
        try {
            validateReservedExclusiveSequence(value);
        } catch (IllegalArgumentException e) {
            throw corruption("invalid reserved sequence watermark", e);
        }
    }

    private static void requireLength(int actual, int expected, String description) {
        if (actual != expected) {
            throw corruption(description + " length must be " + expected + " bytes, got " + actual);
        }
    }

    private static void requireRemaining(ByteBuffer buffer, int expected, String description) {
        if (buffer.remaining() != expected) {
            throw corruption(description + " must contain " + expected + " remaining bytes, got " + buffer.remaining());
        }
    }

    private static void requireAtLeastRemaining(ByteBuffer buffer, int minimum, String description) {
        if (buffer.remaining() < minimum) {
            throw corruption(description + " must contain at least " + minimum + " remaining bytes, got " +
                buffer.remaining());
        }
    }

    private static void requireFullyConsumed(ByteBuffer buffer, String description) {
        if (buffer.hasRemaining()) {
            throw corruption(description + " has " + buffer.remaining() + " trailing bytes");
        }
    }

    private static IllegalArgumentException corruption(String message) {
        return new IllegalArgumentException("Corrupt shared-storage metadata: " + message);
    }

    private static IllegalArgumentException corruption(String message, Throwable cause) {
        return new IllegalArgumentException("Corrupt shared-storage metadata: " + message, cause);
    }

    public enum KeyType {
        OBJECT,
        OBJECT_CLEANUP,
        BROKER_SEQUENCE
    }

    public record MetadataKey(KeyType type, long id) {
        public MetadataKey {
            Objects.requireNonNull(type, "type");
        }
    }

    public interface MetadataValue {
    }

    public record PreparedObjectValue(long createdTimeMs) implements MetadataValue {
    }

    public record CommittedObjectValue(SharedObjectMetadata metadata) implements MetadataValue {
        public CommittedObjectValue {
            Objects.requireNonNull(metadata, "metadata");
        }
    }

    public record CleanupClaimedValue(long createdTimeMs) implements MetadataValue {
    }

    public record CleanupDeletedValue(long createdTimeMs) implements MetadataValue {
    }

    public record BrokerSequenceValue(long reservedExclusiveSequence) implements MetadataValue {
    }

    public enum TombstoneValue implements MetadataValue {
        INSTANCE
    }
}
