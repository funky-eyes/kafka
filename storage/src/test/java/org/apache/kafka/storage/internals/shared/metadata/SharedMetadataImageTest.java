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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedMetadataImageTest {
    @Test
    void staysFailClosedUntilInitialReplayIsMarkedReady() {
        SharedMetadataImage image = new SharedMetadataImage();
        long objectId = BrokerObjectId.compose(1, 1L);
        image.apply(
            SharedMetadataRecordCodec.objectKey(objectId),
            SharedMetadataRecordCodec.committedObjectValue(metadata(objectId, 101L))
        );

        assertEquals(SharedMetadataImage.State.RECOVERING, image.state());
        assertFalse(image.isReady());
        assertThrows(IllegalStateException.class, image::committedObjects);
        assertThrows(IllegalStateException.class, image::preparedObjects);
        assertThrows(IllegalStateException.class, () -> image.brokerReservedExclusiveSequence(1));

        image.markReady();
        assertEquals(SharedMetadataImage.State.READY, image.state());
        assertTrue(image.isReady());
        assertEquals(List.of(metadata(objectId, 101L)), image.committedObjects());
    }

    @Test
    void liveReplayFailurePermanentlyFailsClosed() {
        SharedMetadataImage image = new SharedMetadataImage();
        long objectId = BrokerObjectId.compose(1, 2L);
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);
        image.apply(key, SharedMetadataRecordCodec.committedObjectValue(metadata(objectId, 102L)));
        image.markReady();

        RuntimeException cause = new RuntimeException("metadata consumer failed");
        image.markFailed(cause);

        assertEquals(SharedMetadataImage.State.FAILED, image.state());
        assertFalse(image.isReady());
        assertSame(cause, image.failure().orElseThrow());
        IllegalStateException readFailure = assertThrows(IllegalStateException.class, image::committedObjects);
        assertSame(cause, readFailure.getCause());
        IllegalStateException applyFailure = assertThrows(
            IllegalStateException.class,
            () -> image.apply(key, null)
        );
        assertSame(cause, applyFailure.getCause());
        assertThrows(IllegalStateException.class, image::markReady);
    }

    @Test
    void firstFailureRemainsPrimaryAndLaterFailuresAreSuppressed() {
        SharedMetadataImage image = new SharedMetadataImage();
        RuntimeException first = new RuntimeException("first");
        RuntimeException second = new RuntimeException("second");

        image.markFailed(first);
        image.markFailed(second);

        assertSame(first, image.failure().orElseThrow());
        assertEquals(1, first.getSuppressed().length);
        assertSame(second, first.getSuppressed()[0]);
    }

    @Test
    void replaysPrepareCommitAndTombstoneTransitions() {
        SharedMetadataImage image = new SharedMetadataImage();
        long objectId = BrokerObjectId.compose(2, 10L);
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);

        image.apply(key, SharedMetadataRecordCodec.preparedObjectValue(500L));
        image.markReady();
        assertEquals(
            List.of(new SharedMetadataImage.PreparedObject(objectId, 500L)),
            image.preparedObjects()
        );

        SharedObjectMetadata committed = metadata(objectId, 202L);
        image.apply(key, SharedMetadataRecordCodec.committedObjectValue(committed));
        assertTrue(image.preparedObjects().isEmpty());
        assertEquals(List.of(committed), image.committedObjects());

        image.apply(key, null);
        assertTrue(image.committedObjects().isEmpty());
        assertTrue(image.preparedObjects().isEmpty());
    }

    @Test
    void acceptsCompactedCommitWithoutEarlierPrepare() {
        SharedMetadataImage image = new SharedMetadataImage();
        long objectId = BrokerObjectId.compose(3, 20L);
        SharedObjectMetadata committed = metadata(objectId, 303L);

        image.apply(
            SharedMetadataRecordCodec.objectKey(objectId),
            SharedMetadataRecordCodec.committedObjectValue(committed)
        );
        image.markReady();

        assertEquals(List.of(committed), image.committedObjects());
        assertTrue(image.preparedObjects().isEmpty());
    }

    @Test
    void preservesOrphanPrepareForLaterGarbageCollection() {
        SharedMetadataImage image = new SharedMetadataImage();
        long objectId = BrokerObjectId.compose(4, 30L);
        image.apply(
            SharedMetadataRecordCodec.objectKey(objectId),
            SharedMetadataRecordCodec.preparedObjectValue(999L)
        );
        image.markReady();

        assertEquals(
            List.of(new SharedMetadataImage.PreparedObject(objectId, 999L)),
            image.preparedObjects()
        );
        assertTrue(image.committedObjects().isEmpty());
    }

    @Test
    void brokerSequenceWatermarkIsMonotonicAndNeverTombstoned() {
        SharedMetadataImage image = new SharedMetadataImage();
        byte[] key = SharedMetadataRecordCodec.brokerSequenceKey(7);
        image.apply(key, SharedMetadataRecordCodec.brokerSequenceValue(100L));
        image.apply(key, SharedMetadataRecordCodec.brokerSequenceValue(200L));
        image.markReady();

        assertEquals(200L, image.brokerReservedExclusiveSequence(7));
        assertEquals(1L, image.brokerReservedExclusiveSequence(8));
        assertThrows(
            IllegalStateException.class,
            () -> image.apply(key, SharedMetadataRecordCodec.brokerSequenceValue(150L))
        );
        assertThrows(IllegalStateException.class, () -> image.apply(key, null));
    }

    @Test
    void rejectsMetadataRegressionOrConflictingObjectReuse() {
        SharedMetadataImage image = new SharedMetadataImage();
        long objectId = BrokerObjectId.compose(5, 40L);
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);
        SharedObjectMetadata committed = metadata(objectId, 404L);
        image.apply(key, SharedMetadataRecordCodec.committedObjectValue(committed));

        assertThrows(
            IllegalStateException.class,
            () -> image.apply(key, SharedMetadataRecordCodec.preparedObjectValue(1_000L))
        );
        assertThrows(
            IllegalStateException.class,
            () -> image.apply(
                key,
                SharedMetadataRecordCodec.committedObjectValue(metadata(objectId, 405L))
            )
        );
    }

    private static SharedObjectMetadata metadata(long objectId, long checksum) {
        SharedObjectRange range = new SharedObjectRange(
            new SharedPartitionId(11L, 12L, 0),
            new OffsetRange(0L, 10L),
            3,
            0L,
            100,
            checksum + 1L
        );
        return new SharedObjectMetadata(objectId, 100L, checksum, List.of(range));
    }
}
