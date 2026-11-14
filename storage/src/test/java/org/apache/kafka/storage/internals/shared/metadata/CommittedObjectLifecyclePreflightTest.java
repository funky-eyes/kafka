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

import org.apache.kafka.storage.internals.shared.metadata.CommittedObjectLifecyclePreflight.Finding;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Snapshot;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A controller snapshot supplied by these tests is intentionally NOT an authenticated GC grant. */
class CommittedObjectLifecyclePreflightTest {
    private static final SharedPartitionId A = new SharedPartitionId(10L, 11L, 0);
    private static final SharedPartitionId B = new SharedPartitionId(10L, 11L, 1);
    private static final SharedPartitionId RECREATED = new SharedPartitionId(10L, 12L, 0);

    @Test
    void packedObjectNeedsBothControllerSnapshotsAndWholeRangeExpiry() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 20L, B, 40L));
        Map<SharedPartitionId, Snapshot> snapshots = Map.of(A, active(A, 20L), B, active(B, 40L));

        assertEquals(Finding.CANDIDATE_UNTRUSTED,
            CommittedObjectLifecyclePreflight.assess(packed, image, snapshots));
        assertTrue(image.committedObject(packed.objectId()).isPresent(),
            "Even matched caller supplied evidence must never evict committed metadata");

        assertEquals(Finding.LIVE_RANGE_REMAINS,
            CommittedObjectLifecyclePreflight.assess(packed, image,
                Map.of(A, active(A, 20L), B, active(B, 39L))));
        assertEquals(Finding.MISSING_PARTITION_AUTHORITY,
            CommittedObjectLifecyclePreflight.assess(packed, image, Map.of(A, active(A, 20L))));
    }

    @Test
    void topicRecreationCannotBorrowOldIncarnationWatermarks() {
        SharedObjectMetadata object = new SharedObjectMetadata(702L, 10L, 7L,
            List.of(range(RECREATED, 0L, 10L, 0L)));
        SharedMetadataImage image = image(object, Map.of(A, 100L));
        assertEquals(Finding.FOREIGN_PARTITION_AUTHORITY,
            CommittedObjectLifecyclePreflight.assess(object, image, Map.of(A, active(A, 100L))));
        assertEquals(Finding.METADATA_WATERMARK_NOT_REPLAYED,
            CommittedObjectLifecyclePreflight.assess(object, image,
                Map.of(RECREATED, active(RECREATED, 10L))));
    }

    @Test
    void terminalTopicIdDeletionCanRetireOnlyItsOwnPhysicalRanges() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 20L));
        assertEquals(Finding.CANDIDATE_UNTRUSTED,
            CommittedObjectLifecyclePreflight.assess(packed, image, Map.of(
                A, active(A, 20L),
                B, new Snapshot(B, 20L, 5, -1, OptionalLong.empty(), true))));
        assertEquals(Finding.METADATA_WATERMARK_NOT_REPLAYED,
            CommittedObjectLifecyclePreflight.assess(packed, image,
                Map.of(A, active(A, 20L), B, active(B, 40L))));
    }

    @Test
    void uncommittedAndUnreplayedSnapshotsFailClosed() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 20L, B, 40L));
        assertEquals(Finding.UNCOMMITTED_PARTITION_AUTHORITY,
            CommittedObjectLifecyclePreflight.assess(packed, image, Map.of(
                A, PartitionRetirementAuthorityModel.initial(A), B, active(B, 40L))));
        assertEquals(Finding.MISSING_EXPLICIT_LOG_START,
            CommittedObjectLifecyclePreflight.assess(packed, image, Map.of(
                A, new Snapshot(A, 2L, 5, 1, OptionalLong.empty(), false),
                B, active(B, 40L))));
        assertEquals(Finding.METADATA_WATERMARK_BEHIND_AUTHORITY,
            CommittedObjectLifecyclePreflight.assess(packed, image, Map.of(
                A, active(A, 21L), B, active(B, 40L))));
    }

    @Test
    void staleDescriptorAndAbsentCommittedObjectCannotInheritDeleteEligibility() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 20L, B, 40L));
        Map<SharedPartitionId, Snapshot> snapshots = Map.of(A, active(A, 20L), B, active(B, 40L));
        SharedObjectMetadata forged = new SharedObjectMetadata(
            packed.objectId(), packed.objectSize(), 1001L, packed.ranges());
        assertEquals(Finding.COMMITTED_DESCRIPTOR_MISMATCH,
            CommittedObjectLifecyclePreflight.assess(forged, image, snapshots));
        SharedObjectMetadata absent = new SharedObjectMetadata(
            888L, packed.objectSize(), packed.objectChecksum(), packed.ranges());
        assertEquals(Finding.OBJECT_NOT_COMMITTED,
            CommittedObjectLifecyclePreflight.assess(absent, image, snapshots));
    }

    @Test
    void callerEvidenceCanNeverMakeRecoveringMetadataAuthoritative() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage recovering = new SharedMetadataImage();
        assertThrows(IllegalStateException.class, () ->
            CommittedObjectLifecyclePreflight.assess(packed, recovering,
                Map.of(A, active(A, 20L), B, active(B, 40L))));
    }

    private static SharedMetadataImage image(SharedObjectMetadata object, Map<SharedPartitionId, Long> starts) {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.objectKey(object.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(object), 0L);
        long offset = 1L;
        for (Map.Entry<SharedPartitionId, Long> entry : starts.entrySet()) {
            image.applyFromMetadataLog(
                SharedMetadataRecordCodec.partitionLogStartKey(entry.getKey()),
                SharedMetadataRecordCodec.partitionLogStartValue(entry.getValue()), offset++);
        }
        image.markReady();
        return image;
    }

    private static Snapshot active(SharedPartitionId partition, long logStart) {
        return new Snapshot(partition, 15L, 5, 1, OptionalLong.of(logStart), false);
    }

    private static SharedObjectMetadata packed() {
        return new SharedObjectMetadata(701L, 20L, 7L, List.of(
            range(A, 10L, 20L, 0L),
            range(B, 30L, 40L, 10L)
        ));
    }

    private static SharedObjectRange range(
        SharedPartitionId partition, long first, long last, long position
    ) {
        return new SharedObjectRange(partition, new OffsetRange(first, last), 5,
            position, 10, 9L);
    }
}
