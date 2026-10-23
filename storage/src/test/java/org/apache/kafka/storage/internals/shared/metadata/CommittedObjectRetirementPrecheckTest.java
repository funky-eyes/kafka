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
        replayCommit(image, committed);

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
        replayCommit(image, committed);

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
        SharedObjectMetadata committed = object(13L, range(PARTITION, 10L, 20L, 0L));
        replayCommit(image, committed);
        image.markReady();

        assertEquals(Finding.HAS_UNRETIRED_RANGE, CommittedObjectRetirementPrecheck.assess(committed, image));
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
        replayCommit(image, packed);

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
        SharedObjectMetadata recreated = object(15L, range(RECREATED_TOPIC, 0L, 10L, 0L));
        SharedObjectMetadata unrelated = object(16L, range(OTHER, 0L, 10L, 0L));
        replayCommit(image, recreated);
        replayCommit(image, unrelated);
        image.markReady();
        assertEquals(Finding.MISSING_WATERMARK, CommittedObjectRetirementPrecheck.assess(recreated, image));
        assertEquals(Finding.MISSING_WATERMARK, CommittedObjectRetirementPrecheck.assess(unrelated, image));
    }

    @Test
    void explicitlyPersistedZeroOffsetIsDistinctFromMissingEvidence() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 0L);
        SharedObjectMetadata explicitZero = object(17L, range(PARTITION, 0L, 10L, 0L));
        SharedObjectMetadata missing = object(18L, range(OTHER, 0L, 10L, 0L));
        replayCommit(image, explicitZero);
        replayCommit(image, missing);
        image.markReady();

        assertEquals(Finding.HAS_UNRETIRED_RANGE, CommittedObjectRetirementPrecheck.assess(explicitZero, image));
        assertEquals(Finding.MISSING_WATERMARK, CommittedObjectRetirementPrecheck.assess(missing, image));
    }

    @Test
    void absentOrPreparedObjectCannotBeDeclaredRetiredUsingOldWatermarks() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 100L);
        image.markReady();
        SharedObjectMetadata unknown = object(20L, range(PARTITION, 10L, 20L, 0L));

        assertEquals(Finding.OBJECT_NOT_COMMITTED, CommittedObjectRetirementPrecheck.assess(unknown, image));
        image.apply(
            SharedMetadataRecordCodec.objectKey(unknown.objectId()),
            SharedMetadataRecordCodec.preparedObjectValue(1L)
        );
        assertEquals(Finding.OBJECT_NOT_COMMITTED, CommittedObjectRetirementPrecheck.assess(unknown, image));
        assertTrue(image.committedObjectRetirementEvidence(unknown.objectId()).isEmpty());
    }

    @Test
    void forgedDescriptorCannotBorrowAnotherCommittedPackedObjectsWatermarks() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 100L);
        replayLogStart(image, OTHER, 100L);
        SharedObjectMetadata authoritative = object(
            21L, range(PARTITION, 0L, 10L, 0L), range(OTHER, 20L, 40L, 10L)
        );
        replayCommit(image, authoritative);
        image.markReady();

        SharedObjectMetadata forged = object(21L, range(PARTITION, 0L, 10L, 0L));
        assertEquals(
            Finding.COMMITTED_DESCRIPTOR_MISMATCH,
            CommittedObjectRetirementPrecheck.assess(forged, image)
        );
        assertEquals(
            Finding.ALL_RANGES_BELOW_LOG_START,
            CommittedObjectRetirementPrecheck.assess(authoritative, image)
        );
    }

    @Test
    void historicalObjectTombstoneInvalidatesPreviouslyExpiredCommittedObject() {
        SharedMetadataImage image = new SharedMetadataImage();
        SharedObjectMetadata committed = object(22L, range(PARTITION, 0L, 20L, 0L));
        replayLogStart(image, PARTITION, 20L);
        replayCommit(image, committed);
        image.markReady();

        assertEquals(
            Finding.ALL_RANGES_BELOW_LOG_START,
            CommittedObjectRetirementPrecheck.assess(committed, image)
        );
        image.apply(SharedMetadataRecordCodec.objectKey(committed.objectId()), null);
        assertEquals(Finding.OBJECT_NOT_COMMITTED, CommittedObjectRetirementPrecheck.assess(committed, image));
    }

    @Test
    void targetedCommittedEvidenceIsImmutableAndLimitedToPackedPartitions() {
        SharedMetadataImage image = new SharedMetadataImage();
        replayLogStart(image, PARTITION, 20L);
        replayLogStart(image, OTHER, 30L);
        replayLogStart(image, RECREATED_TOPIC, 1_000L);
        SharedObjectMetadata packed = object(
            23L, range(PARTITION, 0L, 20L, 0L), range(OTHER, 20L, 40L, 20L)
        );
        replayCommit(image, packed);
        image.markReady();

        SharedMetadataImage.CommittedObjectEvidence snapshot =
            image.committedObjectRetirementEvidence(packed.objectId()).orElseThrow();
        assertEquals(packed, snapshot.metadata());
        assertEquals(2, snapshot.explicitLogStarts().size());
        assertEquals(30L, snapshot.explicitLogStarts().get(OTHER).longValue());
        assertThrows(
            UnsupportedOperationException.class,
            () -> snapshot.explicitLogStarts().put(OTHER, 900L)
        );

        replayLogStart(image, OTHER, 40L);
        assertEquals(30L, snapshot.explicitLogStarts().get(OTHER).longValue());
        assertEquals(
            Finding.ALL_RANGES_BELOW_LOG_START,
            CommittedObjectRetirementPrecheck.assess(packed, image)
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

    private static void replayCommit(SharedMetadataImage image, SharedObjectMetadata object) {
        image.apply(
            SharedMetadataRecordCodec.objectKey(object.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(object)
        );
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
