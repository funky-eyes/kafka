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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.common.metadata.WalReplicaDurabilityV1CheckpointRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Snapshot restore is isolated, untrusted diagnostics; no Producer ACK or GC authority. */
class WalReplicaDurabilityV1SnapshotRestoreTest {
    private static final OffsetAndEpoch SNAPSHOT = new OffsetAndEpoch(1300, 10);

    private static WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt receipt(int id) {
        return new WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt()
            .setBrokerId(id).setBrokerEpoch(100L + id)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityV1CheckpointRecord.WalSpan span(int id) {
        return new WalReplicaDurabilityV1CheckpointRecord.WalSpan()
            .setBrokerId(id).setStartOffset(100).setEndOffset(200).setCrc32c(1);
    }

    private static WalReplicaDurabilityV1CheckpointRecord checkpoint(Uuid topic, long accepted) {
        return new WalReplicaDurabilityV1CheckpointRecord()
            .setTopicId(topic).setPartitionId(0).setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(200).setControllerEpoch(9)
            .setAcceptedRaftEpoch(9).setAcceptedKRaftOffset(accepted)
            .setPreviousKRaftOffset(-1)
            .setReplicaReceipts(List.of(receipt(1), receipt(2)))
            .setWalSpans(List.of(span(1), span(2)));
    }

    private static ApiMessageAndVersion wire(WalReplicaDurabilityV1CheckpointRecord record) {
        return new ApiMessageAndVersion(record, (short) 0);
    }

    private static Batch<ApiMessageAndVersion> batch(
        long offset, ApiMessageAndVersion... records
    ) {
        return Batch.data(offset, 9, 0, 1024, List.of(records));
    }

    private static WalReplicaDurabilityV1ReplayState timeline(SnapshotRegistry registry) {
        return new WalReplicaDurabilityV1ReplayState(registry);
    }

    private static WalReplicaDurabilityCertificateReplayState.Key key(Uuid topic) {
        return new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
    }

    private static void restore(
        WalReplicaDurabilityV1ReplayState state,
        OffsetAndEpoch snapshot,
        Batch<ApiMessageAndVersion>... batches
    ) {
        state.restoreUntrustedSnapshot(List.of(batches),
            MetadataVersion.IBP_4_4_IV0, true, snapshot);
    }

    @Test
    void restoredSnapshotPreservesOriginalOffsetForSubsequentCommittedV1Cas() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline(new SnapshotRegistry(new LogContext()));
        restore(state, SNAPSHOT, batch(0, wire(checkpoint(topic, 1000))));
        assertEquals(1000L, state.getUntrusted(key(topic)).kraftOffset());
        var next = new WalReplicaDurabilityCertificateRecord()
            .setTopicId(topic).setPartitionId(0).setLeaderEpoch(8).setPartitionEpoch(12)
            .setStartOffset(200).setEndOffset(220).setControllerEpoch(10)
            .setExpectedPreviousKRaftOffset(1000)
            .setReplicaReceipts(List.of(
                new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
                    .setBrokerId(1).setBrokerEpoch(101)
                    .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
                    .setDurableEndOffset(220),
                new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
                    .setBrokerId(2).setBrokerEpoch(102)
                    .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
                    .setDurableEndOffset(220)))
            .setWalSpans(List.of(
                new WalReplicaDurabilityCertificateRecord.WalSpan()
                    .setBrokerId(1).setStartOffset(200).setEndOffset(220).setCrc32c(1),
                new WalReplicaDurabilityCertificateRecord.WalSpan()
                    .setBrokerId(2).setStartOffset(200).setEndOffset(220).setCrc32c(2)));
        state.replayCommittedV1(next, (short) 1, MetadataVersion.IBP_4_4_IV0,
            true, new WalReplicaDurabilityV1PositionPrecheck.CommitBatch(1400, 1400, 10, 1401),
            1400);
        assertEquals(1400L, state.getUntrusted(key(topic)).kraftOffset());
        assertEquals(220L, state.getUntrusted(key(topic)).endOffset());
    }

    @Test
    void malformedLateCheckpointCannotClearOrPartiallyReplacePriorState() {
        Uuid original = Uuid.randomUuid();
        Uuid added = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline(new SnapshotRegistry(new LogContext()));
        restore(state, SNAPSHOT, batch(0, wire(checkpoint(original, 1000))));
        var previous = state.getUntrusted(key(original));
        var forgedFuture = checkpoint(Uuid.randomUuid(), 1600);
        assertThrows(IllegalArgumentException.class, () -> restore(state, SNAPSHOT,
            batch(0, wire(checkpoint(added, 1200))),
            batch(2, wire(forgedFuture))));
        assertEquals(previous, state.getUntrusted(key(original)));
        assertNull(state.getUntrusted(key(added)));
    }

    @Test
    void completeSnapshotReplacesOldTopicsWithoutBorrowingRecreatedTopicIdentity() {
        Uuid oldTopic = Uuid.randomUuid();
        Uuid retainedTopic = Uuid.randomUuid();
        Uuid recreatedTopic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline(new SnapshotRegistry(new LogContext()));
        restore(state, SNAPSHOT, batch(0, wire(checkpoint(oldTopic, 1000)),
            wire(checkpoint(retainedTopic, 1001))));
        restore(state, new OffsetAndEpoch(1700, 11),
            batch(0, wire(checkpoint(retainedTopic, 1200)),
                wire(checkpoint(recreatedTopic, 1400))));
        assertNull(state.getUntrusted(key(oldTopic)));
        assertEquals(1200L, state.getUntrusted(key(retainedTopic)).kraftOffset());
        assertEquals(1400L, state.getUntrusted(key(recreatedTopic)).kraftOffset());
    }

    @Test
    void snapshotRegistryRollbackRestoresPreviousDiagnosticStateAfterReplacement() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        WalReplicaDurabilityV1ReplayState state = timeline(registry);
        Uuid topic = Uuid.randomUuid();
        restore(state, SNAPSHOT, batch(0, wire(checkpoint(topic, 1000))));
        var original = state.getUntrusted(key(topic));
        registry.idempotentCreateSnapshot(1300);
        restore(state, new OffsetAndEpoch(1700, 11),
            batch(0, wire(checkpoint(topic, 1400))));
        assertEquals(1400L, state.getUntrusted(key(topic)).kraftOffset());
        registry.revertToSnapshot(1300);
        assertEquals(original, state.getUntrusted(key(topic)));
    }

    @Test
    void emptySnapshotClearsStaleDiagnosticsButInvalidBoundaryCannotMutateState() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline(new SnapshotRegistry(new LogContext()));
        restore(state, SNAPSHOT, batch(0, wire(checkpoint(topic, 1000))));
        var original = state.getUntrusted(key(topic));
        assertThrows(IllegalArgumentException.class, () ->
            state.restoreUntrustedSnapshot(List.of(), MetadataVersion.IBP_4_4_IV0, true,
                new OffsetAndEpoch(0, 9)));
        assertEquals(original, state.getUntrusted(key(topic)));
        restore(state, SNAPSHOT);
        assertNull(state.getUntrusted(key(topic)));
        // Ordinary metadata is safe to ignore only when there is no WAL
        // certificate record hidden among the snapshot entries.
        restore(state, SNAPSHOT, batch(0,
            new ApiMessageAndVersion(new NoOpRecord(), (short) 0)));
        assertNull(state.getUntrusted(key(topic)));
    }
}
