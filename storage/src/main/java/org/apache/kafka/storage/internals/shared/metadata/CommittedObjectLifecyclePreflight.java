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

import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Snapshot;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Fail-closed, READ-ONLY preflight for the controller-owned lifecycle protocol.
 *
 * <p>One KSO2 object can pack records from several immutable Topic IDs and
 * partitions. Retirement requires a distinct controller snapshot for each
 * referenced partition, and every physical range must be entirely expired
 * or belong to a terminally deleted Topic ID. An offset in a separate compacted
 * broker metadata topic cannot be substituted for a committed KRaft offset.</p>
 *
 * <p>The supplied snapshots are currently produced only by a pure reference
 * model, NOT authenticated by KRaft. Even CANDIDATE_UNTRUSTED is NEVER a
 * physical DELETE, metadata tombstone, remote-index eviction, or upload/read
 * fencing permit. A future controller must establish the snapshots' committed
 * provenance, monotonic leadership and durable retirement before using them.
 * This class deliberately cannot access an object store or mutate metadata.</p>
 */
public final class CommittedObjectLifecyclePreflight {
    private CommittedObjectLifecyclePreflight() {
    }

    public enum Finding {
        OBJECT_NOT_COMMITTED,
        COMMITTED_DESCRIPTOR_MISMATCH,
        METADATA_REPLAY_NOT_TRACKED,
        MISSING_PARTITION_AUTHORITY,
        FOREIGN_PARTITION_AUTHORITY,
        UNCOMMITTED_PARTITION_AUTHORITY,
        MISSING_EXPLICIT_LOG_START,
        METADATA_WATERMARK_NOT_REPLAYED,
        METADATA_WATERMARK_BEHIND_AUTHORITY,
        LIVE_RANGE_REMAINS,
        METADATA_REPLAY_CHANGED,
        CANDIDATE_UNTRUSTED
    }

    /**
     * Performs a conservative cross-partition check at one replayed metadata
     * instant. Caller-supplied authority snapshots are never certified here.
     */
    public static Finding assess(
        SharedObjectMetadata object,
        SharedMetadataImage image,
        Map<SharedPartitionId, Snapshot> controllerSnapshots
    ) {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(image, "image");
        Map<SharedPartitionId, Snapshot> claimed =
            Map.copyOf(Objects.requireNonNull(controllerSnapshots, "controllerSnapshots"));

        Optional<SharedMetadataImage.CommittedObjectEvidence> before =
            image.committedObjectRetirementEvidence(object.objectId());
        if (before.isEmpty()) {
            return Finding.OBJECT_NOT_COMMITTED;
        }
        SharedMetadataImage.CommittedObjectEvidence evidence = before.get();
        if (!evidence.metadata().equals(object)) {
            return Finding.COMMITTED_DESCRIPTOR_MISMATCH;
        }
        if (evidence.lastConsumedMetadataOffset() < 0L) {
            return Finding.METADATA_REPLAY_NOT_TRACKED;
        }

        Finding finding = assessRanges(object, evidence.explicitLogStarts(), claimed);
        if (!before.equals(image.committedObjectRetirementEvidence(object.objectId()))) {
            return Finding.METADATA_REPLAY_CHANGED;
        }
        return finding;
    }

    private static Finding assessRanges(
        SharedObjectMetadata object,
        Map<SharedPartitionId, Long> replayedLogStarts,
        Map<SharedPartitionId, Snapshot> claimed
    ) {
        Set<SharedPartitionId> expected = object.ranges().stream()
            .map(SharedObjectRange::partition)
            .collect(Collectors.toSet());
        if (!expected.containsAll(claimed.keySet())) {
            return Finding.FOREIGN_PARTITION_AUTHORITY;
        }
        if (!claimed.keySet().containsAll(expected)) {
            return Finding.MISSING_PARTITION_AUTHORITY;
        }

        for (SharedObjectRange range : object.ranges()) {
            Snapshot state = claimed.get(range.partition());
            if (state == null || !state.partition().equals(range.partition())) {
                return Finding.FOREIGN_PARTITION_AUTHORITY;
            }
            if (state.authorityOffset() < 0L) {
                return Finding.UNCOMMITTED_PARTITION_AUTHORITY;
            }
            if (state.terminallyDeleted()) {
                continue;
            }
            if (state.explicitLogStart().isEmpty()) {
                return Finding.MISSING_EXPLICIT_LOG_START;
            }
            long watermark = state.explicitLogStart().getAsLong();
            Long replayed = replayedLogStarts.get(range.partition());
            if (replayed == null) {
                return Finding.METADATA_WATERMARK_NOT_REPLAYED;
            }
            if (replayed < watermark) {
                return Finding.METADATA_WATERMARK_BEHIND_AUTHORITY;
            }
            // Kafka offsets are half-open on the object range; the inclusive
            // log-start must cover the entire RecordBatch before retirement.
            if (range.offsets().endOffset() > watermark) {
                return Finding.LIVE_RANGE_REMAINS;
            }
        }
        return Finding.CANDIDATE_UNTRUSTED;
    }
}
