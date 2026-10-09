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

import static org.apache.kafka.storage.internals.shared.metadata.CommittedObjectRetirementPrecheck.Finding;
import static org.apache.kafka.storage.internals.shared.metadata.CommittedObjectRetirementPrecheck.ObjectFinding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedMetadataConsumerOffsetTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId OTHER_TOPIC = new SharedPartitionId(1L, 3L, 0);

    @Test
    void directReplayDoesNotFabricateMetadataConsumerOffset() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.apply(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(10L)
        );
        image.markReady();

        assertEquals(-1L, image.retirementEvidenceSnapshot().lastConsumedMetadataOffset());
        assertEquals(
            -1L,
            CommittedObjectRetirementPrecheck.assessCommittedSnapshot(image).lastConsumedMetadataOffset()
        );
    }

    @Test
    void compactedOffsetsMayHaveGapsAndStillDescribeOneConsistentImage() {
        SharedMetadataImage image = new SharedMetadataImage();
        SharedObjectMetadata object = object(2L, PARTITION, 0L, 10L);
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.objectKey(object.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(object),
            12L
        );
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(10L),
            37L
        );
        image.markReady();

        assertEquals(37L, image.retirementEvidenceSnapshot().lastConsumedMetadataOffset());
        assertEquals(
            new CommittedObjectRetirementPrecheck.AssessmentSnapshot(
                37L,
                List.of(new ObjectFinding(2L, Finding.ALL_RANGES_BELOW_LOG_START))
            ),
            CommittedObjectRetirementPrecheck.assessCommittedSnapshot(image)
        );
    }

    @Test
    void targetedEvidenceDistinguishesMissingWatermarkFromExplicitZero() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(0L),
            3L
        );
        image.markReady();

        assertEquals(3L, image.partitionLogStartEvidence(PARTITION).lastConsumedMetadataOffset());
        assertEquals(0L, image.partitionLogStartEvidence(PARTITION).explicitLogStart().orElseThrow());
        assertTrue(image.partitionLogStartEvidence(OTHER_TOPIC).explicitLogStart().isEmpty());
    }

    @Test
    void targetedEvidenceIsStableAfterLiveConsumerReplay() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(7L),
            3L
        );
        image.markReady();
        SharedMetadataImage.PartitionLogStartEvidence original = image.partitionLogStartEvidence(PARTITION);

        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(10L),
            11L
        );

        assertEquals(3L, original.lastConsumedMetadataOffset());
        assertEquals(7L, original.explicitLogStart().orElseThrow());
        assertEquals(11L, image.partitionLogStartEvidence(PARTITION).lastConsumedMetadataOffset());
        assertEquals(10L, image.partitionLogStartEvidence(PARTITION).explicitLogStart().orElseThrow());
    }

    @Test
    void zeroIsAValidFirstMetadataLogOffset() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(0L),
            0L
        );
        image.markReady();

        assertEquals(0L, image.retirementEvidenceSnapshot().lastConsumedMetadataOffset());
        assertEquals(0L, image.partitionLogStartOffset(PARTITION));
    }

    @Test
    void priorSnapshotRetainsOriginalOffsetAcrossLaterReplay() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(10L),
            5L
        );
        image.markReady();
        SharedMetadataImage.RetirementEvidenceSnapshot snapshot = image.retirementEvidenceSnapshot();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(20L),
            8L
        );

        assertEquals(5L, snapshot.lastConsumedMetadataOffset());
        assertEquals(10L, snapshot.partitionLogStarts().get(PARTITION).longValue());
        assertEquals(8L, image.retirementEvidenceSnapshot().lastConsumedMetadataOffset());
    }

    @Test
    void duplicateOrBackwardMetadataConsumerOffsetPermanentlyFailsClosed() {
        for (long regressed : List.of(-1L, 5L, 4L)) {
            SharedMetadataImage image = new SharedMetadataImage();
            image.applyFromMetadataLog(
                SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
                SharedMetadataRecordCodec.partitionLogStartValue(10L),
                5L
            );
            image.markReady();
            assertThrows(IllegalStateException.class, () -> image.applyFromMetadataLog(
                SharedMetadataRecordCodec.partitionLogStartKey(OTHER_TOPIC),
                SharedMetadataRecordCodec.partitionLogStartValue(10L),
                regressed
            ));
            assertEquals(SharedMetadataImage.State.FAILED, image.state());
            assertThrows(IllegalStateException.class, image::retirementEvidenceSnapshot);
            assertThrows(IllegalStateException.class,
                () -> CommittedObjectRetirementPrecheck.assessCommittedSnapshot(image));
        }
    }

    @Test
    void corruptKafkaMetadataRecordNeverAdvancesSnapshotOffset() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(10L),
            2L
        );
        image.markReady();

        assertThrows(IllegalStateException.class, () -> image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            null,
            3L
        ));
        assertEquals(SharedMetadataImage.State.FAILED, image.state());
        assertThrows(IllegalStateException.class, image::retirementEvidenceSnapshot);
    }

    @Test
    void snapshotOffsetDoesNotChangeObjectReachability() {
        SharedMetadataImage image = new SharedMetadataImage();
        SharedObjectMetadata live = object(20L, OTHER_TOPIC, 0L, 10L);
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.objectKey(live.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(live),
            100L
        );
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(100L),
            101L
        );
        image.markReady();

        assertEquals(
            List.of(new ObjectFinding(20L, Finding.MISSING_WATERMARK)),
            CommittedObjectRetirementPrecheck.assessCommittedSnapshot(image).findings()
        );
    }

    private static SharedObjectMetadata object(
        long id,
        SharedPartitionId partition,
        long start,
        long end
    ) {
        SharedObjectRange range = new SharedObjectRange(
            partition,
            new OffsetRange(start, end),
            3,
            0L,
            Math.toIntExact(end - start),
            99L
        );
        return new SharedObjectMetadata(id, 100L, 77L, List.of(range));
    }
}
