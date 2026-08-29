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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedMetadataRecordCodecTest {
    @Test
    void roundTripsPermanentObjectCleanupAndBrokerKeys() {
        long objectId = BrokerObjectId.compose(7, 123L);
        SharedMetadataRecordCodec.MetadataKey objectKey = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.objectKey(objectId)
        );
        assertEquals(SharedMetadataRecordCodec.KeyType.OBJECT, objectKey.type());
        assertEquals(objectId, objectKey.id());

        SharedMetadataRecordCodec.MetadataKey cleanupKey = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.objectCleanupKey(objectId)
        );
        assertEquals(SharedMetadataRecordCodec.KeyType.OBJECT_CLEANUP, cleanupKey.type());
        assertEquals(objectId, cleanupKey.id());

        SharedMetadataRecordCodec.MetadataKey brokerKey = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.brokerSequenceKey(7)
        );
        assertEquals(SharedMetadataRecordCodec.KeyType.BROKER_SEQUENCE, brokerKey.type());
        assertEquals(7L, brokerKey.id());
    }

    @Test
    void roundTripsPrepareCommitAndTombstoneValues() {
        long objectId = BrokerObjectId.compose(3, 44L);
        SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.objectKey(objectId)
        );

        SharedMetadataRecordCodec.PreparedObjectValue prepared = assertInstanceOf(
            SharedMetadataRecordCodec.PreparedObjectValue.class,
            SharedMetadataRecordCodec.decodeValue(
                key,
                SharedMetadataRecordCodec.preparedObjectValue(12_345L)
            )
        );
        assertEquals(12_345L, prepared.createdTimeMs());

        SharedObjectMetadata metadata = metadata(objectId);
        SharedMetadataRecordCodec.CommittedObjectValue committed = assertInstanceOf(
            SharedMetadataRecordCodec.CommittedObjectValue.class,
            SharedMetadataRecordCodec.decodeValue(
                key,
                SharedMetadataRecordCodec.committedObjectValue(metadata)
            )
        );
        assertEquals(metadata, committed.metadata());

        assertSame(
            SharedMetadataRecordCodec.TombstoneValue.INSTANCE,
            SharedMetadataRecordCodec.decodeValue(key, null)
        );
    }

    @Test
    void roundTripsCleanupClaimAndDeletedFenceValues() {
        long objectId = BrokerObjectId.compose(4, 45L);
        long createdTimeMs = 12_346L;
        SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.objectCleanupKey(objectId)
        );

        SharedMetadataRecordCodec.CleanupClaimedValue claimed = assertInstanceOf(
            SharedMetadataRecordCodec.CleanupClaimedValue.class,
            SharedMetadataRecordCodec.decodeValue(
                key,
                SharedMetadataRecordCodec.cleanupClaimedValue(createdTimeMs)
            )
        );
        assertEquals(createdTimeMs, claimed.createdTimeMs());

        SharedMetadataRecordCodec.CleanupDeletedValue deleted = assertInstanceOf(
            SharedMetadataRecordCodec.CleanupDeletedValue.class,
            SharedMetadataRecordCodec.decodeValue(
                key,
                SharedMetadataRecordCodec.cleanupDeletedValue(createdTimeMs)
            )
        );
        assertEquals(createdTimeMs, deleted.createdTimeMs());
    }

    @Test
    void roundTripsBrokerSequenceReservationIncludingLimit() {
        SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.brokerSequenceKey(BrokerObjectId.MAX_BROKER_ID)
        );
        long exclusiveLimit = BrokerObjectId.MAX_SEQUENCE + 1L;

        SharedMetadataRecordCodec.BrokerSequenceValue value = assertInstanceOf(
            SharedMetadataRecordCodec.BrokerSequenceValue.class,
            SharedMetadataRecordCodec.decodeValue(
                key,
                SharedMetadataRecordCodec.brokerSequenceValue(exclusiveLimit)
            )
        );

        assertEquals(exclusiveLimit, value.reservedExclusiveSequence());
    }

    @Test
    void rejectsCorruptOrIncompatibleRecords() {
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.decodeKey(new byte[] {99})
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.decodeKey(new byte[] {1, 0})
        );

        long objectId = BrokerObjectId.compose(1, 2L);
        SharedMetadataRecordCodec.MetadataKey objectKey = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.objectKey(objectId)
        );
        byte[] badVersion = SharedMetadataRecordCodec.preparedObjectValue(1L);
        ByteBuffer.wrap(badVersion).putShort((short) 99);
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.decodeValue(objectKey, badVersion)
        );

        SharedMetadataRecordCodec.MetadataKey cleanupKey = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.objectCleanupKey(objectId)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.decodeValue(
                cleanupKey,
                SharedMetadataRecordCodec.preparedObjectValue(1L)
            )
        );

        SharedMetadataRecordCodec.MetadataKey brokerKey = SharedMetadataRecordCodec.decodeKey(
            SharedMetadataRecordCodec.brokerSequenceKey(1)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.decodeValue(
                brokerKey,
                SharedMetadataRecordCodec.preparedObjectValue(1L)
            )
        );

        byte[] trailingPrepare = ByteBuffer.allocate(12)
            .put(SharedMetadataRecordCodec.preparedObjectValue(1L))
            .put((byte) 1)
            .array();
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.decodeValue(objectKey, trailingPrepare)
        );
    }

    @Test
    void rejectsInvalidSequenceReservationAndObjectIdentity() {
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.objectKey(0L)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.objectCleanupKey(0L)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.brokerSequenceKey(BrokerObjectId.MAX_BROKER_ID + 1)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.brokerSequenceValue(0L)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataRecordCodec.brokerSequenceValue(BrokerObjectId.MAX_SEQUENCE + 2L)
        );
    }

    private static SharedObjectMetadata metadata(long objectId) {
        SharedObjectRange first = new SharedObjectRange(
            new SharedPartitionId(1L, 2L, 0),
            new OffsetRange(10L, 20L),
            3,
            0L,
            120,
            111L
        );
        SharedObjectRange second = new SharedObjectRange(
            new SharedPartitionId(4L, 5L, 1),
            new OffsetRange(50L, 60L),
            4,
            120L,
            80,
            222L
        );
        return new SharedObjectMetadata(objectId, 200L, 999L, List.of(first, second));
    }
}
