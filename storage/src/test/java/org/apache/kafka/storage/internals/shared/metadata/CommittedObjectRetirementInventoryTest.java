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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.storage.internals.shared.metadata.CommittedObjectRetirementPrecheck.Finding;
import static org.apache.kafka.storage.internals.shared.metadata.CommittedObjectRetirementPrecheck.ObjectFinding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommittedObjectRetirementInventoryTest {
    private static final SharedPartitionId FIRST = new SharedPartitionId(11L, 12L, 0);
    private static final SharedPartitionId SECOND = new SharedPartitionId(11L, 12L, 1);
    private static final SharedPartitionId RECREATED_TOPIC = new SharedPartitionId(11L, 13L, 0);

    @Test
    void inventoriesOnlyCommittedObjectsInStableObjectIdOrder() {
        SharedMetadataImage image = new SharedMetadataImage();
        commit(image, object(42L, range(FIRST, 0L, 10L, 0L)));
        commit(image, object(3L, range(SECOND, 20L, 30L, 0L)));
        image.apply(
            SharedMetadataRecordCodec.objectKey(99L),
            SharedMetadataRecordCodec.preparedObjectValue(1L)
        );
        advance(image, FIRST, 10L);
        image.markReady();

        assertEquals(
            List.of(
                new ObjectFinding(3L, Finding.MISSING_WATERMARK),
                new ObjectFinding(42L, Finding.ALL_RANGES_BELOW_LOG_START)
            ),
            CommittedObjectRetirementPrecheck.assessCommitted(image)
        );
    }

    @Test
    void oneLivePackedRangeProtectsTheEntireObject() {
        SharedMetadataImage image = new SharedMetadataImage();
        commit(image, object(
            10L,
            range(FIRST, 0L, 10L, 0L),
            range(SECOND, 10L, 20L, 10L)
        ));
        advance(image, FIRST, 10L);
        advance(image, SECOND, 15L);
        image.markReady();

        assertEquals(
            List.of(new ObjectFinding(10L, Finding.HAS_UNRETIRED_RANGE)),
            CommittedObjectRetirementPrecheck.assessCommitted(image)
        );
        advance(image, SECOND, 20L);
        assertEquals(
            List.of(new ObjectFinding(10L, Finding.ALL_RANGES_BELOW_LOG_START)),
            CommittedObjectRetirementPrecheck.assessCommitted(image)
        );
    }

    @Test
    void immutableReplaySnapshotCannotAcquireLaterWatermarksOrObjects() {
        SharedMetadataImage image = new SharedMetadataImage();
        SharedObjectMetadata first = object(2L, range(FIRST, 0L, 20L, 0L));
        commit(image, first);
        advance(image, FIRST, 10L);
        image.markReady();
        SharedMetadataImage.RetirementEvidenceSnapshot original = image.retirementEvidenceSnapshot();
        List<ObjectFinding> oldAssessment = CommittedObjectRetirementPrecheck.assessCommitted(image);

        assertThrows(UnsupportedOperationException.class, () -> original.committedObjects().clear());
        assertThrows(UnsupportedOperationException.class, () -> original.partitionLogStarts().put(FIRST, 999L));
        assertThrows(UnsupportedOperationException.class, () -> oldAssessment.clear());

        advance(image, FIRST, 20L);
        commit(image, object(3L, range(RECREATED_TOPIC, 0L, 10L, 0L)));
        assertEquals(10L, original.partitionLogStarts().get(FIRST).longValue());
        assertEquals(List.of(first), original.committedObjects());
        assertEquals(List.of(new ObjectFinding(2L, Finding.HAS_UNRETIRED_RANGE)), oldAssessment);
        assertEquals(
            List.of(
                new ObjectFinding(2L, Finding.ALL_RANGES_BELOW_LOG_START),
                new ObjectFinding(3L, Finding.MISSING_WATERMARK)
            ),
            CommittedObjectRetirementPrecheck.assessCommitted(image)
        );
    }

    @Test
    void evidenceRecordDefensivelyCopiesCallerOwnedCollections() {
        SharedObjectMetadata object = object(4L, range(FIRST, 0L, 10L, 0L));
        List<SharedObjectMetadata> objects = new ArrayList<>(List.of(object));
        Map<SharedPartitionId, Long> watermarks = new HashMap<>(Map.of(FIRST, 10L));
        SharedMetadataImage.RetirementEvidenceSnapshot evidence =
            new SharedMetadataImage.RetirementEvidenceSnapshot(objects, watermarks);

        objects.clear();
        watermarks.clear();
        assertEquals(List.of(object), evidence.committedObjects());
        assertEquals(10L, evidence.partitionLogStarts().get(FIRST).longValue());
    }

    @Test
    void classifyingAnExpiredCommittedObjectDoesNotMutateTheLiveIndex() {
        SharedMetadataImage image = new SharedMetadataImage();
        SharedObjectMetadata committed = object(8L, range(FIRST, 0L, 10L, 0L));
        commit(image, committed);
        advance(image, FIRST, 10L);
        image.markReady();
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(committed);

        assertEquals(
            List.of(new ObjectFinding(8L, Finding.ALL_RANGES_BELOW_LOG_START)),
            CommittedObjectRetirementPrecheck.assessCommitted(image)
        );
        assertTrue(index.referencesObject(8L));
        assertEquals(8L, index.find(FIRST, 5L).orElseThrow().objectId());
    }

    @Test
    void recoveringOrFailedImagesCannotProduceInventory() {
        SharedMetadataImage image = new SharedMetadataImage();
        assertThrows(IllegalStateException.class, image::retirementEvidenceSnapshot);
        assertThrows(IllegalStateException.class, () -> CommittedObjectRetirementPrecheck.assessCommitted(image));

        image.markReady();
        image.markFailed(new IllegalStateException("consumer replay failed"));
        assertThrows(IllegalStateException.class, image::retirementEvidenceSnapshot);
        assertThrows(IllegalStateException.class, () -> CommittedObjectRetirementPrecheck.assessCommitted(image));
    }

    private static void commit(SharedMetadataImage image, SharedObjectMetadata object) {
        image.apply(
            SharedMetadataRecordCodec.objectKey(object.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(object)
        );
    }

    private static void advance(SharedMetadataImage image, SharedPartitionId partition, long offset) {
        image.apply(
            SharedMetadataRecordCodec.partitionLogStartKey(partition),
            SharedMetadataRecordCodec.partitionLogStartValue(offset)
        );
    }

    private static SharedObjectMetadata object(long id, SharedObjectRange... ranges) {
        return new SharedObjectMetadata(id, 100L, 77L, List.of(ranges));
    }

    private static SharedObjectRange range(SharedPartitionId partition, long start, long end, long position) {
        return new SharedObjectRange(
            partition,
            new OffsetRange(start, end),
            3,
            position,
            Math.toIntExact(end - start),
            99L
        );
    }
}
