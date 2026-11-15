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
package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.CommittedObjectRetirementRecord;
import org.apache.kafka.metadata.CommittedObjectRetirementState;
import org.apache.kafka.metadata.CommittedObjectRetirementState.Entry;
import org.apache.kafka.metadata.CommittedObjectRetirementState.Stage;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Snapshot-rollback-aware KRaft object-lifecycle ledger.
 *
 * <p>Only the already-recorded physical DELETE -> terminal DELETED edge
 * may emit an experimental internal KRaft record. This class cannot issue
 * a physical S3/MinIO DELETE or grant DELETE_READY.
 * The catalog position, descriptor digest and safe epoch in a replayed record
 * are only claims: no bridge currently authenticates the separate compacted
 * Kafka COMMITTED catalog, prevents late PUT/COMMIT, or proves every Broker
 * incarnation has drained. A future writer must establish those properties
 * inside a controller-serialized, version-negotiated protocol before the
 * worker can be wired into production.</p>
 */
final class CommittedObjectRetirementControlManager {
    private final TimelineHashMap<Long, Entry> entries;

    CommittedObjectRetirementControlManager(SnapshotRegistry registry) {
        entries = new TimelineHashMap<>(Objects.requireNonNull(registry, "registry"), 0);
    }

    void replayFromKRaft(
        CommittedObjectRetirementRecord record,
        Optional<OffsetAndEpoch> snapshotId,
        long recordOffset
    ) {
        Objects.requireNonNull(snapshotId, "snapshotId");
        Entry next = Entry.fromRecord(record);
        if (snapshotId.isPresent()) {
            // The snapshot contains a single latest value per physical object,
            // possibly a terminal state whose older transitions were compacted.
            // The position is historical, NOT the snapshot reader's current offset.
            if (next.acceptedKRaftOffset() >= snapshotId.get().offset()) {
                throw new IllegalStateException("Object retirement is ahead of its KRaft snapshot horizon");
            }
            Entry previous = entries.get(next.objectId());
            if (previous != null && !previous.equals(next)) {
                throw new IllegalStateException("Conflicting object retirement entries in KRaft snapshot");
            }
        } else {
            if (recordOffset < 0L || next.acceptedKRaftOffset() != recordOffset) {
                throw new IllegalStateException("Object retirement offset does not match KRaft append position");
            }
            CommittedObjectRetirementState.validateTransition(entries.get(next.objectId()), next);
        }
        entries.put(next.objectId(), next);
    }

    /**
     * Terminalize an already durably DELETE_RECORDED object in the same KRaft
     * event queue that serializes its lifecycle CAS.
     *
     * <p>This deliberately cannot emit RETIRE_INTENT, DELETE_READY, or
     * DELETE_RECORDED. The physical deletion and reference-fencing proofs
     * MUST have been established by separate authenticated authority before
     * DELETE_RECORDED can exist. The result is not a physical DELETE grant.
     *
     * <p>For one record, the controller's next write offset is the prepared
     * Raft record offset. The existing replay path fatally rejects an offset
     * mismatch rather than accepting an invented generation.</p>
     */
    ControllerResult<Boolean> finalizeRecordedDeletion(
        long objectId,
        long expectedRecordedOffset,
        long nextWriteOffset
    ) {
        if (objectId <= 0L || expectedRecordedOffset < 0L || nextWriteOffset < 0L) {
            throw new IllegalArgumentException("Object ID and KRaft offsets must be non-negative");
        }
        Entry previous = entries.get(objectId);
        if (previous == null || previous.stage() != Stage.DELETE_RECORDED
            || previous.acceptedKRaftOffset() != expectedRecordedOffset) {
            // A competing/controller-failover transition won, or physical
            // DELETE was never durably recorded. No record is emitted.
            return ControllerResult.of(List.of(), false);
        }
        if (nextWriteOffset <= previous.acceptedKRaftOffset()) {
            throw new IllegalStateException("New KRaft retirement offset must advance the generation");
        }
        CommittedObjectRetirementRecord record = previous.toRecord()
            .setPreviousKRaftOffset(previous.acceptedKRaftOffset())
            .setAcceptedKRaftOffset(nextWriteOffset)
            .setStage((byte) Stage.DELETED.wireId());
        CommittedObjectRetirementState.validateTransition(previous, Entry.fromRecord(record));
        return ControllerResult.atomicOf(
            List.of(new ApiMessageAndVersion(record, (short) 0)), true);
    }

    /** Each item is one already-recorded physical DELETE, never a delete grant. */
    record TerminalDeletion(long objectId, long expectedRecordedOffset) {
        TerminalDeletion {
            if (objectId <= 0L || expectedRecordedOffset < 0L) {
                throw new IllegalArgumentException("Invalid terminal deletion object or KRaft generation");
            }
        }
    }

    /**
     * Bound and atomically append multiple terminal bookkeeping records.
     *
     * <p>The controller event queue is the sole writer. A stale, missing or
     * not-yet-DELETE_RECORDED candidate vetoes the entire request. Adjacent
     * accepted offsets are derived from one serialized nextWriteOffset, and
     * each actual KRaft append/replay revalidates the exact log position.</p>
     */
    ControllerResult<Integer> finalizeRecordedDeletionBatch(
        List<TerminalDeletion> candidates, long nextWriteOffset
    ) {
        Objects.requireNonNull(candidates, "candidates");
        if (candidates.size() > 64 || nextWriteOffset < 0L) {
            throw new IllegalArgumentException("Invalid or unbounded KRaft terminal deletion batch");
        }
        if (candidates.isEmpty()) {
            return ControllerResult.of(List.of(), 0);
        }
        List<TerminalDeletion> requested = List.copyOf(candidates);
        Set<Long> unique = new HashSet<>();
        List<Entry> previous = new ArrayList<>(requested.size());
        for (TerminalDeletion candidate : requested) {
            if (!unique.add(candidate.objectId())) {
                throw new IllegalArgumentException("Duplicate physical object in KRaft terminal batch");
            }
            Entry stored = entries.get(candidate.objectId());
            if (stored == null || stored.stage() != Stage.DELETE_RECORDED
                || stored.acceptedKRaftOffset() != candidate.expectedRecordedOffset()) {
                // No partial writes: the caller must reread committed KRaft
                // state after any uncertain or losing compare-and-transition.
                return ControllerResult.of(List.of(), 0);
            }
            previous.add(stored);
        }
        List<ApiMessageAndVersion> records = new ArrayList<>(requested.size());
        for (int index = 0; index < requested.size(); index++) {
            Entry stored = previous.get(index);
            long accepted = Math.addExact(nextWriteOffset, index);
            if (accepted <= stored.acceptedKRaftOffset()) {
                throw new IllegalStateException("KRaft terminal batch generation did not advance");
            }
            CommittedObjectRetirementRecord record = stored.toRecord()
                .setPreviousKRaftOffset(stored.acceptedKRaftOffset())
                .setAcceptedKRaftOffset(accepted)
                .setStage((byte) Stage.DELETED.wireId());
            CommittedObjectRetirementState.validateTransition(stored, Entry.fromRecord(record));
            records.add(new ApiMessageAndVersion(record, (short) 0));
        }
        return ControllerResult.atomicOf(List.copyOf(records), records.size());
    }

    Entry get(long objectId) {
        return entries.get(objectId);
    }

}
