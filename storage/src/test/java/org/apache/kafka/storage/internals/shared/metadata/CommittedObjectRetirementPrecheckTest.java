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
import java.util.Map;

import static org.apache.kafka.storage.internals.shared.metadata.CommittedObjectRetirementPrecheck.Finding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommittedObjectRetirementPrecheckTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11L, 12L, 0);
    private static final SharedPartitionId OTHER = new SharedPartitionId(11L, 12L, 1);
    private static final SharedPartitionId RECREATED_TOPIC = new SharedPartitionId(11L, 13L, 0);

    @Test
    void missingAuthoritativeWatermarkBlocksEvenThoughRemoteObjectIsCommitted() {
        SharedMetadataImage image = readyImage();
        SharedObjectMetadata committed = object(11L, range(PARTITION, 10L, 20L, 0L));
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(committed);

        assertEquals(Finding.MISSING_WATERMARK, CommittedObjectRetirementPrecheck.assess(committed, image));
        assertTrue(index.referencesObject(11L), "Read-only precheck must not retire or delete live references");
        assertEquals(11L, index.find(PARTITION, 15L).orElseThrow().objectId());
    }

    @Test
    void exactExclusiveEndAtDurableLogStartIsLogicallyExpiredButDoesNotEvict() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 20L);
        image.markReady();
        SharedObjectMetadata committed = object(12L, range(PARTITION, 10L, 20L, 0L));
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(committed);

        assertEquals(
            Finding.ALL_RANGES_BELOW_LOG_START,
            CommittedObjectRetirementPrecheck.assess(committed, image)
        );
        assertTrue(index.referencesObject(12L), "Classification is not authority for physical deletion");
        assertEquals(12L, index.find(PARTITION, 15L).orElseThrow().objectId());
    }

    @Test
    void watermarkInsideRecordBatchMustNotTreatItsPhysicalBytesAsExpired() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 15L);
        image.markReady();

        assertEquals(
            Finding.HAS_UNRETIRED_RANGE,
            CommittedObjectRetirementPrecheck.assess(
                object(13L, range(PARTITION, 10L, 20L, 0L)), image
            )
        );
    }

    @Test
    void packedCrossPartitionObjectRequiresEveryExplicitLogStart() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 10L);
        image.markReady();
        SharedObjectMetadata packed = object(
            14L,
            range(PARTITION, 0L, 10L, 0L),
            range(OTHER, 20L, 40L, 10L)
        );

        assertEquals(Finding.MISSING_WATERMARK, CommittedObjectRetirementPrecheck.assess(packed, image));
        replayLogStart(image, OTHER, 30L);
        assertEquals(Finding.HAS_UNRETIRED_RANGE, CommittedObjectRetirementPrecheck.assess(packed, image));
        replayLogStart(image, OTHER, 40L);
        assertEquals(Finding.ALL_RANGES_BELOW_LOG_START, CommittedObjectRetirementPrecheck.assess(packed, image));
    }

    @Test
    void recreatedTopicIdDoesNotInheritOriginalTopicLogStart() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 100L);
        image.markReady();
        assertEquals(
            Finding.MISSING_WATERMARK,
            CommittedObjectRetirementPrecheck.assess(
                object(15L, range(RECREATED_TOPIC, 0L, 10L, 0L)), image
            )
        );
        assertEquals(
            Finding.MISSING_WATERMARK,
            CommittedObjectRetirementPrecheck.assess(
                object(16L, range(OTHER, 0L, 10L, 0L)), image
            )
        );
    }

    @Test
    void explicitlyPersistedZeroOffsetIsDistinctFromMissingEvidence() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 0L);
        image.markReady();

        assertEquals(
            Finding.HAS_UNRETIRED_RANGE,
            CommittedObjectRetirementPrecheck.assess(
                object(17L, range(PARTITION, 0L, 10L, 0L)), image
            )
        );
        assertEquals(
            Finding.MISSING_WATERMARK,
            CommittedObjectRetirementPrecheck.assess(
                object(18L, range(OTHER, 0L, 10L, 0L)), image
            )
        );
    }

    @Test
    void watermarkSnapshotIsImmutableAndCannotChangeAfterLiveReplay() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 10L);
        image.markReady();
        Map<SharedPartitionId, Long> original = image.partitionLogStartsSnapshot();
        assertEquals(10L, original.get(PARTITION).longValue());
        assertThrows(UnsupportedOperationException.class, () -> original.put(PARTITION, 999L));

        replayLogStart(image, PARTITION, 20L);
        assertEquals(10L, original.get(PARTITION).longValue());
        assertEquals(20L, image.partitionLogStartsSnapshot().get(PARTITION).longValue());
        assertEquals(1, image.partitionLogStartsSnapshot().size());
    }

    @Test
    void recoveringAndFailedMetadataImagesCannotSupplyReclamationEvidence() {
        SharedMetadataImage image = new SharedMetadataImage();
        SharedObjectMetadata object = object(19L, range(PARTITION, 0L, 10L, 0L));
        assertThrows(IllegalStateException.class, image::partitionLogStartsSnapshot);
        assertThrows(
            IllegalStateException.class,
            () -> CommittedObjectRetirementPrecheck.assess(object, image)
        );
        image.markFailed(new IllegalStateException("replay failure"));
        assertThrows(IllegalStateException.class, image::partitionLogStartsSnapshot);
        assertThrows(
            IllegalStateException.class,
            () -> CommittedObjectRetirementPrecheck.assess(object, image)
        );
    }

    private static SharedMetadataImage readyImage() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        return image;
    }

    private static void replayLogStart(SharedMetadataImage image, SharedPartitionId partition, long offset) {
        image.apply(
            SharedMetadataRecordCodec.partitionLogStartKey(partition),
            SharedMetadataRecordCodec.partitionLogStartValue(offset)
        );
    }

    private static SharedObjectMetadata object(long id, SharedObjectRange... ranges) {
        return new SharedObjectMetadata(id, 100L, 77L, List.of(ranges));
    }

    private static SharedObjectRange range(SharedPartitionId partition, long start, long end, long bytePosition) {
        return new SharedObjectRange(
            partition,
            new OffsetRange(start, end),
            3,
            bytePosition,
            Math.toIntExact(end - start),
            99L
        );
    }
}
