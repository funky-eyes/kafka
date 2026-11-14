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

import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.List;
import java.util.Objects;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Key;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Replica;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Span;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.UntrustedEvidence;

/**
 * Isolated V1-only timeline for UNTRUSTED WAL certificate diagnostics.
 *
 * Unlike the legacy V0 timeline, the stored KRaft offset is supplied by an
 * externally verified committed batch, not copied from the wire payload.
 * Standby QuorumController observes actual committed batches for untrusted
 * diagnostics only; there is no active Controller Writer, metadata image
 * serialization, Producer ACK consumer or COMMITTED physical GC authority.
 *
 * SnapshotRegistry supports speculative-state rollback; durable Kafka
 * snapshot serialization and restoration are NOT implemented here.
 */
final class WalReplicaDurabilityV1ReplayState {
    private final TimelineHashMap<Key, UntrustedEvidence> state;

    WalReplicaDurabilityV1ReplayState(SnapshotRegistry registry) {
        state = new TimelineHashMap<>(Objects.requireNonNull(registry, "registry"), 0);
    }

    /**
     * The metadata-version, feature, actual committed batch and wire-version
     * arguments must originate from the trusted KRaft replay path. A caller
     * supplying arbitrary values cannot authenticate itself or its WAL bytes.
     */
    void replayCommittedV1(
        WalReplicaDurabilityCertificateRecord record,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        WalReplicaDurabilityV1PositionPrecheck.CommitBatch committedBatch,
        long assignedRecordOffset
    ) {
        Key key = requireValidKey(record);
        UntrustedEvidence next = validateTransition(
            record, key, trustedWireVersion, metadataVersion, negotiatedExperimentalFeature,
            committedBatch, assignedRecordOffset, state.get(key));
        state.put(key, next);
    }

    /**
     * Pure, non-mutating transition check reusable by a committed-batch
     * preflight. A successful result remains untrusted diagnostic data.
     */
    static UntrustedEvidence validateTransition(
        WalReplicaDurabilityCertificateRecord record,
        Key key,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        WalReplicaDurabilityV1PositionPrecheck.CommitBatch committedBatch,
        long assignedRecordOffset,
        UntrustedEvidence previous
    ) {
        if (previous != null && !previous.key().equals(key)) {
            throw new IllegalStateException("foreign previous WAL certificate partition identity");
        }
        long previousOffset = previous == null ? -1 : previous.kraftOffset();
        if (!WalReplicaDurabilityV1PositionPrecheck.hasAssignedCommittedPosition(
            record, trustedWireVersion, metadataVersion, negotiatedExperimentalFeature,
            committedBatch, assignedRecordOffset, previousOffset)) {
            throw new IllegalStateException("V1 WAL certificate has no trusted committed position or current CAS");
        }
        UntrustedEvidence next = freeze(record, key, assignedRecordOffset);
        if (previous != null) {
            requireMonotonic(previous, next);
        }
        return next;
    }

    /**
     * Install an already preflighted committed KRaft batch's immutable
     * UNTRUSTED diagnostics, only AFTER every ordinary metadata message
     * in that same batch was replayed successfully by the standby Controller.
     * This must be called exclusively on the Controller event queue.
     *
     * These diagnostic records cannot grant Producer ACK, advance HW or
     * allow COMMITTED physical GC. The active Controller has no V1 Writer.
     */
    void applyPrevalidatedCommittedBatch(List<UntrustedEvidence> completePlan) {
        for (UntrustedEvidence evidence : List.copyOf(completePlan)) {
            state.put(evidence.key(), evidence);
        }
    }

    /**
     * Replace only this isolated UNTRUSTED diagnostic timeline with a
     * validated *complete* snapshot plan. The caller must supply all batches
     * from one trusted KRaft SnapshotReader, not a prefix or a Broker claim.
     *
     * Preflight validates every checkpoint, its wire version, snapshot bounds
     * and per-partition uniqueness BEFORE clearing prior state. A malformed
     * late checkpoint cannot partially overwrite earlier diagnostics.
     *
     * Neither this isolated replay state nor the plan is connected to
     * production QuorumController, MetadataImage, Producer ACK or GC.
     */
    void restoreUntrustedSnapshot(
        List<Batch<ApiMessageAndVersion>> completeSnapshotBatches,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        OffsetAndEpoch trustedSnapshotId
    ) {
        var plan = WalReplicaDurabilityV1SnapshotBatchPreflight.planUntrusted(
            completeSnapshotBatches, metadataVersion,
            negotiatedExperimentalFeature, trustedSnapshotId);
        state.clear();
        for (UntrustedEvidence evidence : plan.checkpoints()) {
            state.put(evidence.key(), evidence);
        }
    }

    /**
     * A fully replayed ordinary Kafka snapshot without a V1 checkpoint
     * replaces all previously observed diagnostic-only certificate data.
     * Bootstrap snapshots need no experimental feature or minimum offset.
     */
    void clearUntrustedAfterCheckpointFreeSnapshot() {
        state.clear();
    }

    /** Untrusted diagnostic only: never a durable receipt or ACK/GC permit. */
    UntrustedEvidence getUntrusted(Key key) {
        return state.get(key);
    }

    static Key requireValidKey(WalReplicaDurabilityCertificateRecord record) {
        try {
            WalReplicaDurabilityCertificateRecordPrecheck.requireWellFormedUntrustedRecord(record);
            return new Key(record.topicId(), record.partitionId());
        } catch (IllegalArgumentException ex) {
            throw new IllegalStateException("malformed V1 WAL certificate record", ex);
        }
    }

    private static UntrustedEvidence freeze(
        WalReplicaDurabilityCertificateRecord record,
        Key key,
        long assignedRecordOffset
    ) {
        List<Replica> replicas = record.replicaReceipts().stream()
            .map(r -> new Replica(r.brokerId(), r.brokerEpoch(), r.incarnationId(),
                r.storageGeneration(), r.durableEndOffset()))
            .toList();
        List<Span> spans = record.walSpans().stream()
            .map(s -> new Span(s.brokerId(), s.startOffset(), s.endOffset(), s.crc32c()))
            .toList();
        return new UntrustedEvidence(
            key, record.leaderEpoch(), record.partitionEpoch(), record.startOffset(),
            record.endOffset(), record.controllerEpoch(), assignedRecordOffset,
            replicas, spans);
    }

    private static void requireMonotonic(UntrustedEvidence previous, UntrustedEvidence next) {
        if (next.controllerEpoch() < previous.controllerEpoch()
            || next.endOffset() < previous.endOffset()) {
            throw new IllegalStateException("V1 WAL certificate regressed Controller generation or WAL end");
        }
        // A later certificate cannot skip an unproven Kafka-offset interval
        // between the previous durable end and its claimed starting offset.
        if (next.startOffset() > previous.endOffset()) {
            throw new IllegalStateException("V1 WAL certificate leaves a gap after prior durable range");
        }
        if (next.leaderEpoch() < previous.leaderEpoch()
            || next.partitionEpoch() < previous.partitionEpoch()) {
            throw new IllegalStateException("V1 WAL certificate regressed partition generation");
        }
        if (next.leaderEpoch() > previous.leaderEpoch()
            && next.partitionEpoch() <= previous.partitionEpoch()) {
            throw new IllegalStateException("V1 leader change without new partition epoch");
        }
    }
}
