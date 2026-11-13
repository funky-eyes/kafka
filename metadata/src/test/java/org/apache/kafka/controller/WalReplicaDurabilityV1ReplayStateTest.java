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
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests immutable diagnostics, not Broker durability or a Producer ACK grant. */
class WalReplicaDurabilityV1ReplayStateTest {
    private static final Uuid TOPIC = Uuid.randomUuid();

    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(int broker, long end) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(broker).setBrokerEpoch(100 + broker)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(end);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int broker, long end) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(end).setCrc32c(42);
    }

    private static WalReplicaDurabilityCertificateRecord record(
        Uuid topic, int leaderEpoch, int partitionEpoch, long end, long controllerEpoch, long expectedPrevious
    ) {
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(topic).setPartitionId(0)
            .setLeaderEpoch(leaderEpoch).setPartitionEpoch(partitionEpoch)
            .setStartOffset(100).setEndOffset(end).setControllerEpoch(controllerEpoch)
            .setExpectedPreviousKRaftOffset(expectedPrevious)
            .setReplicaReceipts(List.of(receipt(1, end), receipt(2, end)))
            .setWalSpans(List.of(span(1, end), span(2, end)));
    }

    private static WalReplicaDurabilityV1PositionPrecheck.CommitBatch batch(
        long first, long last, int epoch
    ) {
        return new WalReplicaDurabilityV1PositionPrecheck.CommitBatch(first, last, epoch, last + 1);
    }

    private static WalReplicaDurabilityV1ReplayState state(SnapshotRegistry registry) {
        return new WalReplicaDurabilityV1ReplayState(registry);
    }

    private static WalReplicaDurabilityCertificateReplayState.Key key(Uuid topic) {
        return new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
    }

    private static void replay(
        WalReplicaDurabilityV1ReplayState state,
        WalReplicaDurabilityCertificateRecord record,
        long offset
    ) {
        state.replayCommittedV1(record, (short) 1, MetadataVersion.IBP_4_4_IV0, true,
            batch(offset, offset, (int) record.controllerEpoch()), offset);
    }

    @Test
    void trustedAssignedOffsetNotMutableWireClaimAdvancesV1Cas() {
        WalReplicaDurabilityV1ReplayState state = state(new SnapshotRegistry(new LogContext()));
        replay(state, record(TOPIC, 7, 11, 200, 9, -1), 1000);
        var original = state.getUntrusted(key(TOPIC));
        assertEquals(1000L, original.kraftOffset());
        assertEquals(200L, original.endOffset());

        replay(state, record(TOPIC, 7, 11, 220, 9, 1000), 1002);
        assertEquals(1002L, state.getUntrusted(key(TOPIC)).kraftOffset());
        assertEquals(220L, state.getUntrusted(key(TOPIC)).endOffset());
        assertEquals(1000L, original.kraftOffset());
    }

    @Test
    void uncommittedWrongVersionAndStalePreviousCasFailClosed() {
        WalReplicaDurabilityV1ReplayState state = state(new SnapshotRegistry(new LogContext()));
        replay(state, record(TOPIC, 7, 11, 200, 9, -1), 1000);
        var original = state.getUntrusted(key(TOPIC));
        var next = record(TOPIC, 7, 11, 220, 9, 1000);

        assertThrows(IllegalStateException.class, () -> state.replayCommittedV1(
            next, (short) 0, MetadataVersion.IBP_4_4_IV0, true, batch(1001, 1001, 9), 1001));
        assertThrows(IllegalStateException.class, () -> state.replayCommittedV1(
            next, (short) 1, MetadataVersion.LATEST_PRODUCTION, true, batch(1001, 1001, 9), 1001));
        assertThrows(IllegalStateException.class, () -> state.replayCommittedV1(
            next, (short) 1, MetadataVersion.IBP_4_4_IV0, false, batch(1001, 1001, 9), 1001));
        assertThrows(IllegalStateException.class, () -> state.replayCommittedV1(
            next, (short) 1, MetadataVersion.IBP_4_4_IV0, true, batch(999, 1000, 9), 1001));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 7, 11, 220, 9, 999), 1001));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 7, 11, 220, 9, 1000), 1000));
        assertEquals(original, state.getUntrusted(key(TOPIC)));
    }

    @Test
    void staleControllerAndPartitionGenerationsNeverOverwriteTimeline() {
        WalReplicaDurabilityV1ReplayState state = state(new SnapshotRegistry(new LogContext()));
        replay(state, record(TOPIC, 8, 12, 220, 9, -1), 1000);
        var original = state.getUntrusted(key(TOPIC));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 8, 12, 230, 8, 1000), 1001));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 7, 12, 230, 9, 1000), 1001));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 8, 11, 230, 9, 1000), 1001));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 9, 12, 230, 9, 1000), 1001));
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(TOPIC, 8, 12, 210, 9, 1000), 1001));
        assertEquals(original, state.getUntrusted(key(TOPIC)));
        replay(state, record(TOPIC, 9, 13, 230, 10, 1000), 1002);
        assertEquals(230L, state.getUntrusted(key(TOPIC)).endOffset());
    }

    @Test
    void snapshotRegistryRollbackRestoresPreviousV1Cas() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        WalReplicaDurabilityV1ReplayState state = state(registry);
        replay(state, record(TOPIC, 7, 11, 200, 9, -1), 1000);
        var first = state.getUntrusted(key(TOPIC));
        registry.idempotentCreateSnapshot(1000);

        replay(state, record(TOPIC, 7, 11, 220, 9, 1000), 1001);
        registry.idempotentCreateSnapshot(1001);
        assertEquals(1001L, state.getUntrusted(key(TOPIC)).kraftOffset());

        registry.revertToSnapshot(1000);
        assertEquals(first, state.getUntrusted(key(TOPIC)));
        replay(state, record(TOPIC, 7, 11, 225, 9, 1000), 1002);
        assertEquals(1002L, state.getUntrusted(key(TOPIC)).kraftOffset());
    }

    @Test
    void nestedMutableReceiptsAndTopicRecreationRemainIsolated() {
        WalReplicaDurabilityV1ReplayState state = state(new SnapshotRegistry(new LogContext()));
        var source = record(TOPIC, 7, 11, 200, 9, -1);
        Uuid incarnation = source.replicaReceipts().get(0).incarnationId();
        replay(state, source, 1000);
        var frozen = state.getUntrusted(key(TOPIC));

        source.replicaReceipts().get(0).setIncarnationId(Uuid.randomUuid());
        source.walSpans().get(0).setEndOffset(400);
        source.setEndOffset(400);
        assertEquals(incarnation, frozen.replicas().get(0).incarnationId());
        assertEquals(200L, frozen.spans().get(0).endOffset());
        assertThrows(UnsupportedOperationException.class, () -> frozen.replicas().clear());

        Uuid recreated = Uuid.randomUuid();
        assertNull(state.getUntrusted(key(recreated)));
        replay(state, record(recreated, 0, 0, 120, 10, -1), 1001);
        assertEquals(120L, state.getUntrusted(key(recreated)).endOffset());
        assertEquals(200L, state.getUntrusted(key(TOPIC)).endOffset());
        assertTrue(state.getUntrusted(key(recreated)).replicas().size() == 2);
        assertThrows(IllegalStateException.class, () -> replay(
            state, record(Uuid.ZERO_UUID, 0, 0, 120, 10, -1), 1002));
    }
}
