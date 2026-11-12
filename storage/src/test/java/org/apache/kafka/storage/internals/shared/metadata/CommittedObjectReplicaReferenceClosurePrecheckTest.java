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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.storage.internals.shared.metadata.CommittedObjectReplicaReferenceClosurePrecheck.Finding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exhaustive packed-range diagnostics, deliberately never GC authorization. */
class CommittedObjectReplicaReferenceClosurePrecheckTest {
    private static final SharedPartitionId A = new SharedPartitionId(11L, 12L, 0);
    private static final SharedPartitionId B = new SharedPartitionId(11L, 12L, 1);
    private static final SharedPartitionId RECREATED = new SharedPartitionId(11L, 13L, 0);
    private static final SharedPartitionId FOREIGN = new SharedPartitionId(11L, 99L, 0);

    @Test
    void packedCrossTopicRequiresAllDistinctPartitions() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        assertEquals(Finding.MISSING_PARTITION_REPORT,
            assess(packed, image, List.of(report(packed, A, offset), report(packed, B, offset))));
        assertEquals(Finding.MATCHED_UNTRUSTED, assess(packed, image, allReports(packed, offset)),
            "Only caller-supplied reports match; this result never authorizes deletion");
        assertTrue(image.committedObject(packed.objectId()).isPresent(),
            "Even a matching diagnostic must preserve the COMMITTED metadata");
    }

    @Test
    void repeatedRangesFromOnePartitionNeedExactlyOneConfirmation() {
        SharedObjectMetadata samePartition = object(902L,
            range(A, 0L, 10L, 0L), range(A, 10L, 20L, 10L));
        SharedMetadataImage image = image(samePartition, Map.of(A, 20L));
        long offset = snapshotOffset(image, samePartition);
        assertEquals(Finding.MATCHED_UNTRUSTED,
            assess(samePartition, image, List.of(report(samePartition, A, offset))));
        assertEquals(Finding.DUPLICATE_PARTITION_REPORT, assess(samePartition, image,
            List.of(report(samePartition, A, offset), report(samePartition, A, offset))));
    }

    @Test
    void duplicateOrForeignReceiptCannotReplaceMissingPackedPartition() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        assertEquals(Finding.DUPLICATE_PARTITION_REPORT, assess(packed, image, List.of(
            report(packed, A, offset), report(packed, B, offset), report(packed, A, offset))));
        assertEquals(Finding.FOREIGN_PARTITION_REPORT, assess(packed, image, List.of(
            report(packed, A, offset), report(packed, B, offset), report(packed, FOREIGN, offset))));
        ArrayList<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> withNull =
            new ArrayList<>(allReports(packed, offset));
        withNull.set(2, null);
        assertEquals(Finding.MISSING_PARTITION_REPORT, assess(packed, image, withNull));
    }

    @Test
    void committedDescriptorAndReportDescriptorMustMatchEntirePackedObject() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        SharedObjectMetadata forged = object(packed.objectId(),
            range(A, 0L, 10L, 0L));
        assertEquals(Finding.COMMITTED_DESCRIPTOR_MISMATCH,
            assess(forged, image, List.of(report(forged, A, offset))));
        assertEquals(Finding.REPORT_DESCRIPTOR_MISMATCH, assess(packed, image, List.of(
            report(packed, A, offset), report(forged, B, offset),
            report(packed, RECREATED, offset))));
    }

    @Test
    void metadataReplayOffsetMismatchCannotBorrowPreviousSnapshot() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        assertEquals(Finding.REPORT_REPLAY_OFFSET_MISMATCH, assess(packed, image, List.of(
            report(packed, A, offset), report(packed, B, offset + 1L),
            report(packed, RECREATED, offset))));
        assertEquals(Finding.MATCHED_UNTRUSTED, assess(packed, image, allReports(packed, offset)));
    }

    @Test
    void anyReaderWriterOrReferenceRemainingBlocksPackedClosure() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        for (int state = 0; state < 3; state++) {
            var blocked = new CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport(
                B, packed, offset, "round-B", state != 0, state != 1, state != 2);
            assertEquals(Finding.READER_WRITER_OR_REFERENCE_ACTIVE, assess(packed, image, List.of(
                report(packed, A, offset), blocked, report(packed, RECREATED, offset))));
        }
    }

    @Test
    void missingOrPartialWatermarkBlocksEvenCompleteReplicaReports() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage missing = image(packed, Map.of(A, 10L, B, 40L));
        assertEquals(Finding.LOGICAL_RANGES_NOT_RETIRED,
            assess(packed, missing, allReports(packed, snapshotOffset(missing, packed))));
        SharedMetadataImage partial = image(packed, Map.of(A, 10L, B, 39L, RECREATED, 10L));
        assertEquals(Finding.LOGICAL_RANGES_NOT_RETIRED,
            assess(packed, partial, allReports(packed, snapshotOffset(partial, packed))));
    }

    @Test
    void untrackedReplayAndInvalidReceiptsNeverPassAsEvidence() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = new SharedMetadataImage();
        image.apply(SharedMetadataRecordCodec.objectKey(packed.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(packed));
        for (Map.Entry<SharedPartitionId, Long> entry :
            Map.of(A, 10L, B, 40L, RECREATED, 10L).entrySet()) {
            image.apply(SharedMetadataRecordCodec.partitionLogStartKey(entry.getKey()),
                SharedMetadataRecordCodec.partitionLogStartValue(entry.getValue()));
        }
        image.markReady();
        assertEquals(Finding.UNTRACKED_METADATA_REPLAY, assess(packed, image, List.of()));
        assertEquals(Finding.OBJECT_NOT_COMMITTED, assess(object(999L, range(A, 0L, 10L, 0L)),
            image, List.of()));
        assertThrows(IllegalArgumentException.class, () ->
            report(packed, A, -1L));
        assertThrows(IllegalArgumentException.class, () ->
            new CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport(
                A, packed, 1L, " ", true, true, true));
    }

    @Test
    void primaryLocalReadIndexReferenceBlocksReportedRetirement() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed);

        assertEquals(Finding.MATCHED_UNTRUSTED,
            assess(packed, image, allReports(packed, offset)),
            "Caller-supplied reports alone do not observe a local read index");
        assertEquals(Finding.LOCAL_READ_INDEX_REFERENCE_ACTIVE,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, allReports(packed, offset), index));
        assertTrue(index.referencesObject(packed.objectId()));
        assertTrue(image.committedObject(packed.objectId()).isPresent());
    }

    @Test
    void alternateFallbackReferenceBlocksEvenWhenPrimaryScanIsFalse() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        RemoteObjectIndex index = new RemoteObjectIndex();
        SharedObjectMetadata preferred = new SharedObjectMetadata(
            900L, packed.objectSize(), packed.objectChecksum(), packed.ranges());
        index.add(preferred);
        index.add(packed);

        assertFalse(index.referencesObject(packed.objectId()),
            "The object is not the preferred physical copy on any packed range");
        assertTrue(index.referencesObjectIncludingAlternates(packed.objectId()),
            "The packed object is still usable as a read fallback");
        assertEquals(Finding.LOCAL_READ_INDEX_REFERENCE_ACTIVE,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, allReports(packed, offset), index));
    }

    @Test
    void unrelatedLocalIndexReferenceCannotInventGcAuthority() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(object(999L, range(A, 100L, 110L, 0L)));

        assertFalse(index.referencesObjectIncludingAlternates(packed.objectId()));
        assertEquals(Finding.MATCHED_UNTRUSTED,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, allReports(packed, offset), index),
            "No local indexed reference is still not Controller deletion authority");
        assertTrue(image.committedObject(packed.objectId()).isPresent());
    }

    @Test
    void localIndexAbsenceCannotOverrideStaleOrIncompleteMetadata() {
        SharedObjectMetadata packed = packed();
        RemoteObjectIndex empty = new RemoteObjectIndex();
        SharedMetadataImage incomplete = image(packed, Map.of(A, 10L, B, 39L, RECREATED, 10L));
        long offset = snapshotOffset(incomplete, packed);

        assertEquals(Finding.LOGICAL_RANGES_NOT_RETIRED,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, incomplete, allReports(packed, offset), empty));

        SharedMetadataImage complete = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long completeOffset = snapshotOffset(complete, packed);
        assertEquals(Finding.REPORT_REPLAY_OFFSET_MISMATCH,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, complete, allReports(packed, completeOffset + 1L), empty));
        assertThrows(NullPointerException.class,
            () -> CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, complete, allReports(packed, completeOffset), null));
    }

    @Test
    void alternateOnlyPublicationDuringClosureAssessmentFailsClosed() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        RemoteObjectIndex index = new RemoteObjectIndex();
        SharedObjectMetadata preferred = new SharedObjectMetadata(
            900L, packed.objectSize(), packed.objectChecksum(), packed.ranges());
        index.add(preferred);
        long primaryRevision = index.revision(A);

        Collection<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> reports =
            mutateOnIteration(allReports(packed, offset), () -> index.add(packed));
        assertEquals(Finding.LOCAL_READ_INDEX_CHANGED_DURING_ASSESSMENT,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, reports, index));
        assertEquals(primaryRevision, index.revision(A),
            "Alternate-only publication must be detected without a primary revision increment");
        assertFalse(index.referencesObject(packed.objectId()));
        assertTrue(index.referencesObjectIncludingAlternates(packed.objectId()));
        assertEquals(Finding.LOCAL_READ_INDEX_REFERENCE_ACTIVE,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, allReports(packed, offset), index));
    }

    @Test
    void unrelatedIndexPublicationDuringClosureAssessmentFailsClosed() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        RemoteObjectIndex index = new RemoteObjectIndex();
        Collection<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> reports =
            mutateOnIteration(allReports(packed, offset), () ->
                index.add(object(999L, range(A, 100L, 110L, 0L))));

        assertEquals(Finding.LOCAL_READ_INDEX_CHANGED_DURING_ASSESSMENT,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, reports, index));
        assertFalse(index.referencesObjectIncludingAlternates(packed.objectId()),
            "An unrelated reference should still force a retry after read-view publication");
    }

    @Test
    void metadataReplayDuringClosureAssessmentFailsClosed() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long offset = snapshotOffset(image, packed);
        RemoteObjectIndex index = new RemoteObjectIndex();
        Collection<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> reports =
            mutateOnIteration(allReports(packed, offset), () ->
                image.applyFromMetadataLog(
                    SharedMetadataRecordCodec.partitionLogStartKey(FOREIGN),
                    SharedMetadataRecordCodec.partitionLogStartValue(1L),
                    offset + 1L));

        assertEquals(Finding.METADATA_REPLAY_CHANGED_DURING_ASSESSMENT,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, reports, index));
        assertEquals(offset + 1L, snapshotOffset(image, packed));
        assertFalse(index.referencesObjectIncludingAlternates(packed.objectId()));

        // The direct advisory API must also detect a replay interleaving;
        // a report collection cannot turn an old snapshot into a match.
        SharedMetadataImage direct = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        long directOffset = snapshotOffset(direct, packed);
        Collection<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> rawReports =
            mutateOnIteration(allReports(packed, directOffset), () ->
                direct.applyFromMetadataLog(
                    SharedMetadataRecordCodec.partitionLogStartKey(FOREIGN),
                    SharedMetadataRecordCodec.partitionLogStartValue(1L),
                    directOffset + 1L));
        assertEquals(Finding.METADATA_REPLAY_CHANGED_DURING_ASSESSMENT,
            CommittedObjectReplicaReferenceClosurePrecheck.assess(packed, direct, rawReports));
    }

    @Test
    void stableMetadataAndPhysicalReadViewRemainOnlyUntrustedDiagnostics() {
        SharedObjectMetadata packed = packed();
        SharedMetadataImage image = image(packed, Map.of(A, 10L, B, 40L, RECREATED, 10L));
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(object(999L, range(A, 100L, 110L, 0L)));
        long offset = snapshotOffset(image, packed);
        assertEquals(Finding.MATCHED_UNTRUSTED,
            CommittedObjectReplicaReferenceClosurePrecheck.assessWithLocalReadIndex(
                packed, image, allReports(packed, offset), index));
        assertTrue(image.committedObject(packed.objectId()).isPresent(),
            "Even stable local diagnostics do not retire COMMITTED metadata");
    }

    private static SharedObjectMetadata packed() {
        return object(901L, range(A, 0L, 10L, 0L),
            range(B, 20L, 40L, 10L), range(RECREATED, 0L, 10L, 30L));
    }

    private static SharedObjectMetadata object(long objectId, SharedObjectRange... ranges) {
        return new SharedObjectMetadata(objectId, 100L, 77L, List.of(ranges));
    }

    private static SharedObjectRange range(
        SharedPartitionId partition, long start, long end, long position
    ) {
        return new SharedObjectRange(partition,
            new OffsetRange(start, end), 3, position, Math.toIntExact(end - start), 99L);
    }

    private static Collection<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> mutateOnIteration(
        List<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> reports,
        Runnable mutation
    ) {
        return new AbstractCollection<>() {
            private boolean mutated;

            @Override
            public Iterator<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> iterator() {
                if (!mutated) {
                    mutated = true;
                    mutation.run();
                }
                return reports.iterator();
            }

            @Override
            public int size() {
                return reports.size();
            }
        };
    }

    private static SharedMetadataImage image(
        SharedObjectMetadata object, Map<SharedPartitionId, Long> watermarks
    ) {
        SharedMetadataImage image = new SharedMetadataImage();
        long offset = 0L;
        for (Map.Entry<SharedPartitionId, Long> e : watermarks.entrySet()) {
            image.applyFromMetadataLog(SharedMetadataRecordCodec.partitionLogStartKey(e.getKey()),
                SharedMetadataRecordCodec.partitionLogStartValue(e.getValue()), offset++);
        }
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectKey(object.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(object), offset);
        image.markReady();
        return image;
    }

    private static long snapshotOffset(SharedMetadataImage image, SharedObjectMetadata object) {
        return image.committedObjectRetirementEvidence(object.objectId())
            .orElseThrow().lastConsumedMetadataOffset();
    }

    private static CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport report(
        SharedObjectMetadata object, SharedPartitionId partition, long offset
    ) {
        return new CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport(
            partition, object, offset, "round-" + partition.partition(), true, true, true);
    }

    private static List<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> allReports(
        SharedObjectMetadata object, long offset
    ) {
        return List.of(report(object, A, offset), report(object, B, offset),
            report(object, RECREATED, offset));
    }

    private static Finding assess(
        SharedObjectMetadata object,
        SharedMetadataImage image,
        List<CommittedObjectReplicaReferenceClosurePrecheck.PartitionReport> reports
    ) {
        return CommittedObjectReplicaReferenceClosurePrecheck.assess(object, image, reports);
    }
}
