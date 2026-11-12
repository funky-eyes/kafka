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

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Read-only, negative-first check of all logical partitions packed into a
 * single COMMITTED KSO2 physical object.
 *
 * <p>Logical expiration requires an explicit metadata-log watermark for
 * every range. Separately, every DISTINCT Topic ID/Partition in the exact
 * COMMITTED descriptor must submit a matching observation. Repeated ranges
 * from one partition do not justify duplicate observations. Checking only
 * the first partition or the writer's source partition is insufficient.</p>
 *
 * <p>Observations are caller-supplied and UNAUTHENTICATED. Reported replica
 * quiescence and reference retirement are not durable KRaft evidence.
 * Even MATCHED_UNTRUSTED cannot authorize metadata tombstones, index
 * eviction or physical COMMITTED object DELETE. There is no network or
 * Controller writer integration.</p>
 */
public final class CommittedObjectReplicaReferenceClosurePrecheck {
    private CommittedObjectReplicaReferenceClosurePrecheck() {
    }

    public enum Finding {
        OBJECT_NOT_COMMITTED,
        COMMITTED_DESCRIPTOR_MISMATCH,
        LOGICAL_RANGES_NOT_RETIRED,
        UNTRACKED_METADATA_REPLAY,
        MISSING_PARTITION_REPORT,
        FOREIGN_PARTITION_REPORT,
        DUPLICATE_PARTITION_REPORT,
        REPORT_DESCRIPTOR_MISMATCH,
        REPORT_REPLAY_OFFSET_MISMATCH,
        READER_WRITER_OR_REFERENCE_ACTIVE,
        LOCAL_READ_INDEX_REFERENCE_ACTIVE,
        LOCAL_READ_INDEX_CHANGED_DURING_ASSESSMENT,
        METADATA_REPLAY_CHANGED_DURING_ASSESSMENT,
        MATCHED_UNTRUSTED
    }

    /**
     * Diagnostic report for one DISTINCT logical partition of one physical
     * object. This is a value object, not an authenticated Controller ACK.
     * A roundId is local to the partition and need not match other partitions.
     */
    public record PartitionReport(
        SharedPartitionId partition,
        SharedObjectMetadata objectDescriptor,
        long observedMetadataOffset,
        String roundId,
        boolean reportedReadersDrained,
        boolean reportedWritersDrained,
        boolean reportedReferencesRetired
    ) {
        public PartitionReport {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(objectDescriptor, "objectDescriptor");
            Objects.requireNonNull(roundId, "roundId");
            if (observedMetadataOffset < 0L || roundId.isBlank()) {
                throw new IllegalArgumentException("A report needs a replay offset and nonblank round");
            }
        }
    }

    /**
     * Uses exactly ONE immutable, synchronized COMMITTED-object evidence
     * snapshot so descriptor, explicit watermarks and consumed log offset
     * are consistent. A following replay may invalidate the result; callers
     * MUST NOT interpret the returned value as a permanent retirement grant.
     */
    public static Finding assess(
        SharedObjectMetadata object,
        SharedMetadataImage image,
        Collection<PartitionReport> reports
    ) {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(reports, "reports");

        Optional<SharedMetadataImage.CommittedObjectEvidence> snapshot =
            image.committedObjectRetirementEvidence(object.objectId());
        if (snapshot.isEmpty()) {
            return Finding.OBJECT_NOT_COMMITTED;
        }
        SharedMetadataImage.CommittedObjectEvidence evidence = snapshot.get();
        if (!evidence.metadata().equals(object)) {
            return Finding.COMMITTED_DESCRIPTOR_MISMATCH;
        }
        if (CommittedObjectRetirementPrecheck.assessRanges(
            object, evidence.explicitLogStarts()
        ) != CommittedObjectRetirementPrecheck.Finding.ALL_RANGES_BELOW_LOG_START) {
            return Finding.LOGICAL_RANGES_NOT_RETIRED;
        }
        if (evidence.lastConsumedMetadataOffset() < 0L) {
            return Finding.UNTRACKED_METADATA_REPLAY;
        }
        Finding reported = assessReports(object, evidence.lastConsumedMetadataOffset(), reports);
        // A caller-controlled report collection can execute after the snapshot
        // and race new KRaft replay. Even the raw diagnostic must not describe
        // receipts from one metadata generation as matching another.
        if (!snapshot.equals(image.committedObjectRetirementEvidence(object.objectId()))) {
            return Finding.METADATA_REPLAY_CHANGED_DURING_ASSESSMENT;
        }
        return reported;
    }

    /**
     * Augments the existing advisory closure check with a live local index
     * negative guard for BOTH selected and equivalent fallback read paths.
     *
     * <p>For a packed KSO2 object, no local reader should still be able to
     * resolve any of its bytes via the primary or alternate physical index.
     * This is deliberately a veto only: an empty local index is NOT evidence
     * of other Brokers being quiescent, complete metadata replay, or durable
     * removal of references. The check double-reads metadata replay
     * provenance and the physical read view's publication generation;
     * a publication or replay observed during the check fails closed.
     * This is NOT an atomic cross-subsystem fence: a later commit may
     * still race after the final observation. The strongest possible
     * result remains MATCHED_UNTRUSTED.</p>
     */
    public static Finding assessWithLocalReadIndex(
        SharedObjectMetadata object,
        SharedMetadataImage image,
        Collection<PartitionReport> reports,
        RemoteObjectIndex localReadIndex
    ) {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(localReadIndex, "localReadIndex");
        // Optimistic local observations: no joint metadata/index lock exists.
        // Treat changes during assessment as negative results, not GC grants.
        Optional<SharedMetadataImage.CommittedObjectEvidence> initialEvidence =
            image.committedObjectRetirementEvidence(object.objectId());
        RemoteObjectIndex.PhysicalReadReferenceSnapshot initialIndex =
            localReadIndex.physicalReadReferenceSnapshot(object.objectId());
        Finding reported = assess(object, image, reports);
        if (reported != Finding.MATCHED_UNTRUSTED) {
            return reported;
        }
        if (!initialEvidence.equals(image.committedObjectRetirementEvidence(object.objectId()))) {
            return Finding.METADATA_REPLAY_CHANGED_DURING_ASSESSMENT;
        }
        RemoteObjectIndex.PhysicalReadReferenceSnapshot finalIndex =
            localReadIndex.physicalReadReferenceSnapshot(object.objectId());
        if (initialIndex.publicationGeneration() != finalIndex.publicationGeneration()) {
            return Finding.LOCAL_READ_INDEX_CHANGED_DURING_ASSESSMENT;
        }
        return finalIndex.referenced()
            ? Finding.LOCAL_READ_INDEX_REFERENCE_ACTIVE : Finding.MATCHED_UNTRUSTED;
    }

    private static Finding assessReports(
        SharedObjectMetadata object,
        long replayOffset,
        Collection<PartitionReport> reports
    ) {
        Set<SharedPartitionId> expected = new HashSet<>();
        for (SharedObjectRange range : object.ranges()) {
            expected.add(range.partition());
        }
        Set<SharedPartitionId> received = new HashSet<>();
        for (PartitionReport report : reports) {
            if (report == null) {
                return Finding.MISSING_PARTITION_REPORT;
            }
            if (!expected.contains(report.partition())) {
                return Finding.FOREIGN_PARTITION_REPORT;
            }
            if (!received.add(report.partition())) {
                return Finding.DUPLICATE_PARTITION_REPORT;
            }
            Finding finding = assessOne(report, object, replayOffset);
            if (finding != Finding.MATCHED_UNTRUSTED) {
                return finding;
            }
        }
        return expected.equals(received)
            ? Finding.MATCHED_UNTRUSTED : Finding.MISSING_PARTITION_REPORT;
    }

    private static Finding assessOne(
        PartitionReport report,
        SharedObjectMetadata object,
        long replayOffset
    ) {
        if (!object.equals(report.objectDescriptor())) {
            return Finding.REPORT_DESCRIPTOR_MISMATCH;
        }
        if (report.observedMetadataOffset() != replayOffset) {
            return Finding.REPORT_REPLAY_OFFSET_MISMATCH;
        }
        if (!report.reportedReadersDrained() || !report.reportedWritersDrained()
            || !report.reportedReferencesRetired()) {
            return Finding.READER_WRITER_OR_REFERENCE_ACTIVE;
        }
        return Finding.MATCHED_UNTRUSTED;
    }
}
