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
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityV1StandbyCommitReplay.replayCommittedBatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Real-batch-shape standby replay remains untrusted and never releases acks=all. */
class WalReplicaDurabilityV1StandbyCommitReplayTest {
    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(
        int broker, long end
    ) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(broker).setBrokerEpoch(100L + broker)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(end);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int broker, long end) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(end).setCrc32c(1);
    }

    private static ApiMessageAndVersion certificate(
        Uuid topic, long end, long expectedPrevious, short version
    ) {
        var record = new WalReplicaDurabilityCertificateRecord()
            .setTopicId(topic).setPartitionId(0)
            .setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(end).setControllerEpoch(9)
            .setExpectedPreviousKRaftOffset(expectedPrevious)
            .setReplicaReceipts(List.of(receipt(1, end), receipt(2, end)))
            .setWalSpans(List.of(span(1, end), span(2, end)));
        return new ApiMessageAndVersion(record, version);
    }

    private static ApiMessageAndVersion normal() {
        return new ApiMessageAndVersion(new NoOpRecord(), (short) 0);
    }

    private static Batch<ApiMessageAndVersion> batch(
        long baseOffset, ApiMessageAndVersion... messages
    ) {
        return Batch.data(baseOffset, 9, 0, 1024, List.of(messages));
    }

    private static WalReplicaDurabilityV1ReplayState timeline() {
        return new WalReplicaDurabilityV1ReplayState(new SnapshotRegistry(new LogContext()));
    }

    private static WalReplicaDurabilityCertificateReplayState.Key key(Uuid topic) {
        return new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
    }

    @Test
    void trustedStandbyBatchKeepsRealOffsetsAndDoesNotOrdinarilyReplayCertificates() {
        Uuid topic = Uuid.randomUuid();
        var timeline = timeline();
        List<Long> ordinaryOffsets = new ArrayList<>();
        var committed = batch(500, normal(), certificate(topic, 200, -1, (short) 1),
            normal(), certificate(topic, 220, 501, (short) 1));
        replayCommittedBatch(committed, () -> MetadataVersion.IBP_4_4_IV0,
            () -> true, timeline, (msg, offset) -> ordinaryOffsets.add(offset));
        assertEquals(List.of(500L, 502L), ordinaryOffsets);
        assertEquals(503L, timeline.getUntrusted(key(topic)).kraftOffset());
        assertEquals(220L, timeline.getUntrusted(key(topic)).endOffset());
    }

    @Test
    void ordinaryCommittedMetadataDoesNotConsultExperimentalFeatureOrMutateTimeline() {
        var timeline = timeline();
        List<Long> replayed = new ArrayList<>();
        replayCommittedBatch(batch(900, normal(), normal()),
            () -> {
                throw new AssertionError("No feature check for ordinary metadata");
            },
            () -> {
                throw new AssertionError("Production ordinary metadata must not enable WAL certificates");
            },
            timeline, (msg, offset) -> replayed.add(offset));
        assertEquals(List.of(900L, 901L), replayed);
        assertNull(timeline.getUntrusted(key(Uuid.randomUuid())));
    }

    @Test
    void lateStaleCasOrWrongWireFailsBeforeAnyOrdinaryReplay() {
        Uuid topic = Uuid.randomUuid();
        var timeline = timeline();
        replayCommittedBatch(batch(500, certificate(topic, 200, -1, (short) 1)),
            () -> MetadataVersion.IBP_4_4_IV0,
            () -> true, timeline, (msg, offset) -> { });
        var original = timeline.getUntrusted(key(topic));
        List<Long> ordinary = new ArrayList<>();
        assertThrows(IllegalStateException.class, () -> replayCommittedBatch(
            batch(600, normal(), certificate(topic, 220, 499, (short) 1)),
            () -> MetadataVersion.IBP_4_4_IV0, () -> true,
            timeline, (msg, offset) -> ordinary.add(offset)));
        assertThrows(IllegalArgumentException.class, () -> replayCommittedBatch(
            batch(600, normal(), certificate(topic, 220, 500, (short) 0)),
            () -> MetadataVersion.IBP_4_4_IV0, () -> true,
            timeline, (msg, offset) -> ordinary.add(offset)));
        assertEquals(List.of(), ordinary);
        assertEquals(original, timeline.getUntrusted(key(topic)));
    }

    @Test
    void failedOrdinaryMetadataReplayCannotInstallPrevalidatedCertificateEvidence() {
        Uuid topic = Uuid.randomUuid();
        var timeline = timeline();
        var committed = batch(1000, certificate(topic, 200, -1, (short) 1), normal());
        assertThrows(IllegalStateException.class, () -> replayCommittedBatch(
            committed, () -> MetadataVersion.IBP_4_4_IV0,
            () -> true, timeline, (msg, offset) -> {
                throw new IllegalStateException("injected ordinary replay failure");
            }));
        assertNull(timeline.getUntrusted(key(topic)));
    }

    @Test
    void snapshotCheckpointInLogAndDisabledFeatureAreRejectedFailClosed() {
        Uuid topic = Uuid.randomUuid();
        var timeline = timeline();
        var checkpointInLog = new ApiMessageAndVersion(
            new WalReplicaDurabilityV1CheckpointRecord(), (short) 0);
        assertThrows(IllegalStateException.class, () -> replayCommittedBatch(
            batch(0, checkpointInLog), () -> MetadataVersion.IBP_4_4_IV0,
            () -> true, timeline, (msg, offset) -> { }));
        assertThrows(IllegalStateException.class, () -> replayCommittedBatch(
            batch(500, certificate(topic, 200, -1, (short) 1)),
            () -> MetadataVersion.IBP_4_4_IV0,
            () -> false, timeline, (msg, offset) -> { }));
        assertNull(timeline.getUntrusted(key(topic)));
    }
}
