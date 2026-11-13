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
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Replay diagnostics only: none of these tests authorize producer ACK or GC.
 */
class WalReplicaDurabilityCertificateReplayStateTest {
    private static final Uuid TOPIC = Uuid.randomUuid();

    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(int id, long end) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(id)
            .setBrokerEpoch(id + 100)
            .setIncarnationId(Uuid.randomUuid())
            .setStorageGeneration(1)
            .setDurableEndOffset(end);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int id, long start, long end) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(id).setStartOffset(start).setEndOffset(end).setCrc32c(90210L);
    }

    private static WalReplicaDurabilityCertificateRecord record(
        Uuid topicId, int leaderEpoch, int partitionEpoch, long endOffset, long kraftOffset, long controllerEpoch
    ) {
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setLeaderEpoch(leaderEpoch)
            .setPartitionEpoch(partitionEpoch)
            .setStartOffset(100)
            .setEndOffset(endOffset)
            .setControllerEpoch(controllerEpoch)
            .setKRaftOffset(kraftOffset)
            .setReplicaReceipts(List.of(receipt(1, endOffset), receipt(2, endOffset)))
            .setWalSpans(List.of(span(1, 100, endOffset), span(2, 100, endOffset)));
    }

    private static WalReplicaDurabilityCertificateReplayState manager(SnapshotRegistry registry) {
        return new WalReplicaDurabilityCertificateReplayState(registry);
    }

    private static void replay(
        WalReplicaDurabilityCertificateReplayState manager,
        WalReplicaDurabilityCertificateRecord record
    ) {
        manager.replayFromKRaft(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.empty(), record.kRaftOffset(), record.kRaftOffset() + 2);
    }

    private static WalReplicaDurabilityCertificateReplayState.Key key(Uuid topic) {
        return new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
    }

    @Test
    void committedLogReplayFreezesGeneratedRecordAndItsNestedMutableEntries() {
        WalReplicaDurabilityCertificateReplayState manager =
            manager(new SnapshotRegistry(new LogContext()));
        WalReplicaDurabilityCertificateRecord source = record(TOPIC, 7, 11, 200, 1000, 3);
        Uuid originalIncarnation = source.replicaReceipts().get(0).incarnationId();
        replay(manager, source);
        var immutable = manager.getUntrusted(key(TOPIC));
        assertEquals(1000L, immutable.kraftOffset());
        assertEquals(200L, immutable.endOffset());
        assertEquals(originalIncarnation, immutable.replicas().get(0).incarnationId());
        source.setEndOffset(400);
        source.replicaReceipts().get(0).setIncarnationId(Uuid.randomUuid());
        source.walSpans().get(0).setEndOffset(400);
        assertEquals(200L, immutable.endOffset());
        assertEquals(originalIncarnation, immutable.replicas().get(0).incarnationId());
        assertEquals(200L, immutable.spans().get(0).endOffset());
        assertThrows(UnsupportedOperationException.class, () -> immutable.replicas().clear());
        assertThrows(UnsupportedOperationException.class, () -> immutable.spans().clear());
    }

    @Test
    void untrustedOrUncommittedSourceCannotOverwriteExistingReplayState() {
        WalReplicaDurabilityCertificateReplayState manager =
            manager(new SnapshotRegistry(new LogContext()));
        replay(manager, record(TOPIC, 7, 11, 200, 1000, 3));
        var original = manager.getUntrusted(key(TOPIC));
        var newer = record(TOPIC, 7, 11, 210, 1001, 3);
        assertThrows(IllegalStateException.class, () -> manager.replayFromKRaft(newer, (short) 0,
            MetadataVersion.IBP_4_4_IV0, false, Optional.empty(), 1001, 1003));
        assertThrows(IllegalStateException.class, () -> manager.replayFromKRaft(newer, (short) 0,
            MetadataVersion.LATEST_PRODUCTION, true, Optional.empty(), 1001, 1003));
        assertThrows(IllegalStateException.class, () -> manager.replayFromKRaft(newer, (short) 0,
            MetadataVersion.IBP_4_4_IV0, true, Optional.empty(), 1000, 1003));
        assertThrows(IllegalStateException.class, () -> manager.replayFromKRaft(newer, (short) 0,
            MetadataVersion.IBP_4_4_IV0, true, Optional.empty(), 1001, 1001));
        assertEquals(original, manager.getUntrusted(key(TOPIC)));
        // Even a plausible record at the first KRaft log position must not
        // enter the V0 diagnostic timeline if the wire version is V1.
        var firstOffset = record(Uuid.randomUuid(), 0, 0, 120, 0, 3);
        assertThrows(IllegalStateException.class, () -> manager.replayFromKRaft(
            firstOffset, (short) 1, MetadataVersion.IBP_4_4_IV0, true,
            Optional.empty(), 0, 1));
    }

    @Test
    void snapshotRegistryRollbackRestoresPriorFrozenCertificate() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        WalReplicaDurabilityCertificateReplayState manager = manager(registry);
        replay(manager, record(TOPIC, 7, 11, 200, 1000, 3));
        var original = manager.getUntrusted(key(TOPIC));
        registry.idempotentCreateSnapshot(1000);
        replay(manager, record(TOPIC, 8, 12, 220, 1001, 4));
        registry.idempotentCreateSnapshot(1001);
        assertEquals(220L, manager.getUntrusted(key(TOPIC)).endOffset());
        registry.revertToSnapshot(1000);
        assertEquals(original, manager.getUntrusted(key(TOPIC)));
    }

    @Test
    void staleControllerLeaderAndPartitionGenerationsNeverReplaceNewerEvidence() {
        WalReplicaDurabilityCertificateReplayState manager =
            manager(new SnapshotRegistry(new LogContext()));
        replay(manager, record(TOPIC, 8, 12, 220, 1000, 4));
        var original = manager.getUntrusted(key(TOPIC));
        var staleController = record(TOPIC, 8, 12, 230, 1001, 3);
        var staleLeader = record(TOPIC, 7, 12, 230, 1001, 4);
        var stalePartition = record(TOPIC, 8, 11, 230, 1001, 4);
        var leaderWithoutPartitionChange = record(TOPIC, 9, 12, 230, 1001, 4);
        var regressedEnd = record(TOPIC, 8, 12, 210, 1001, 4);
        var reusedKRaftOffset = record(TOPIC, 8, 12, 230, 1000, 4);
        for (var forged : List.of(staleController, staleLeader, stalePartition,
            leaderWithoutPartitionChange, regressedEnd, reusedKRaftOffset)) {
            assertThrows(IllegalStateException.class, () -> replay(manager, forged));
            assertEquals(original, manager.getUntrusted(key(TOPIC)));
        }
        replay(manager, record(TOPIC, 9, 13, 240, 1001, 5));
        assertEquals(240L, manager.getUntrusted(key(TOPIC)).endOffset());
    }

    @Test
    void topicRecreationAndSnapshotReplayNeverBorrowAnotherPartitionsEvidence() {
        WalReplicaDurabilityCertificateReplayState manager =
            manager(new SnapshotRegistry(new LogContext()));
        var historical = record(TOPIC, 7, 11, 200, 1000, 3);
        manager.replayFromKRaft(historical, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.of(new OffsetAndEpoch(1001, 9)), 999, 1001);
        assertEquals(200L, manager.getUntrusted(key(TOPIC)).endOffset());
        Uuid newTopic = Uuid.randomUuid();
        assertNull(manager.getUntrusted(key(newTopic)));
        assertNull(manager.getUntrusted(new WalReplicaDurabilityCertificateReplayState.Key(TOPIC, 1)));
        replay(manager, record(newTopic, 0, 0, 150, 1001, 4));
        assertEquals(150L, manager.getUntrusted(key(newTopic)).endOffset());
        assertEquals(200L, manager.getUntrusted(key(TOPIC)).endOffset());

        WalReplicaDurabilityCertificateReplayState failed =
            manager(new SnapshotRegistry(new LogContext()));
        assertThrows(IllegalStateException.class, () -> failed.replayFromKRaft(historical, (short) 0,
            MetadataVersion.IBP_4_4_IV0, true,
            Optional.of(new OffsetAndEpoch(1000, 9)), 999, 1001));
        assertNull(failed.getUntrusted(key(TOPIC)));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificateReplayState.Key(Uuid.ZERO_UUID, 0));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificateReplayState.Key(TOPIC, -1));
        assertFalse(TOPIC.equals(newTopic));
        assertTrue(manager.getUntrusted(key(TOPIC)).replicas().size() == 2);
    }
}
