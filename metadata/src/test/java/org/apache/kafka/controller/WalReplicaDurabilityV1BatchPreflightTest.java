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
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.FenceBrokerRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityV1BatchPreflight.planUntrusted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Preflight produces untrusted diagnostics, never an acks=all or physical GC permit. */
class WalReplicaDurabilityV1BatchPreflightTest {
    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(int id, long end) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(id).setBrokerEpoch(id + 100L).setIncarnationId(Uuid.randomUuid())
            .setStorageGeneration(1).setDurableEndOffset(end);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int broker, long end) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(end).setCrc32c(1234);
    }

    private static WalReplicaDurabilityCertificateRecord certificate(
        Uuid topic, long end, long expectedPrevious, long controllerEpoch
    ) {
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(topic).setPartitionId(0)
            .setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(end)
            .setControllerEpoch(controllerEpoch)
            .setExpectedPreviousKRaftOffset(expectedPrevious)
            .setReplicaReceipts(List.of(receipt(1, end), receipt(2, end)))
            .setWalSpans(List.of(span(1, end), span(2, end)));
    }

    private static ApiMessageAndVersion v1(
        Uuid topic, long end, long expectedPrevious
    ) {
        return new ApiMessageAndVersion(certificate(topic, end, expectedPrevious, 9), (short) 1);
    }

    private static ApiMessageAndVersion regular() {
        return new ApiMessageAndVersion(new NoOpRecord(), (short) 0);
    }

    private static Batch<ApiMessageAndVersion> batch(long base, ApiMessageAndVersion... records) {
        return Batch.data(base, 9, 0, 1024, List.of(records));
    }

    private static WalReplicaDurabilityV1ReplayState timeline() {
        return new WalReplicaDurabilityV1ReplayState(new SnapshotRegistry(new LogContext()));
    }

    private static WalReplicaDurabilityCertificateReplayState.Key key(Uuid topic) {
        return new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
    }

    private static List<WalReplicaDurabilityCertificateReplayState.UntrustedEvidence> plan(
        Batch<ApiMessageAndVersion> committed, WalReplicaDurabilityV1ReplayState timeline
    ) {
        return planUntrusted(committed, MetadataVersion.IBP_4_4_IV0, true, timeline);
    }

    @Test
    void multipleSamePartitionCertificatesUseStagedCasWithoutMutatingTimeline() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState diagnostics = timeline();
        var planned = plan(batch(1000, v1(topic, 200, -1), regular(),
            v1(topic, 220, 1000)), diagnostics);
        assertEquals(2, planned.size());
        assertEquals(1000L, planned.get(0).kraftOffset());
        assertEquals(1002L, planned.get(1).kraftOffset());
        assertEquals(220L, planned.get(1).endOffset());
        assertNull(diagnostics.getUntrusted(key(topic)));
        assertThrows(UnsupportedOperationException.class, () -> planned.clear());
    }

    @Test
    void invalidLateCertificateCannotPartiallyApplyEarlierBatchEvidence() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState diagnostics = timeline();
        diagnostics.replayCommittedV1(certificate(topic, 200, -1, 9), (short) 1,
            MetadataVersion.IBP_4_4_IV0, true,
            new WalReplicaDurabilityV1PositionPrecheck.CommitBatch(900, 900, 9, 901), 900);
        var prior = diagnostics.getUntrusted(key(topic));
        assertThrows(IllegalStateException.class, () -> plan(batch(1000,
            v1(topic, 210, 900), v1(topic, 220, 999)), diagnostics));
        // A newer certificate must not jump over offsets not covered by its
        // predecessor, even if the new individual WAL span is contiguous.
        var gap = certificate(topic, 400, 900, 9).setStartOffset(250);
        gap.walSpans().forEach(span -> span.setStartOffset(250));
        assertThrows(IllegalStateException.class, () -> plan(batch(1002,
            new ApiMessageAndVersion(gap, (short) 1)), diagnostics));
        assertEquals(prior, diagnostics.getUntrusted(key(topic)));
        assertEquals(900L, diagnostics.getUntrusted(key(topic)).kraftOffset());
    }

    @Test
    void mixedFeatureAndReplicaIdentityChangesRequirePerRecordControllerReplay() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState diagnostics = timeline();
        var feature = new ApiMessageAndVersion(new FeatureLevelRecord()
            .setName("wal.replica.durability.version").setFeatureLevel((short) 0), (short) 0);
        var topicMutation = new ApiMessageAndVersion(new TopicRecord(), (short) 0);
        var brokerMutation = new ApiMessageAndVersion(new FenceBrokerRecord(), (short) 0);
        for (var mutation : List.of(feature, topicMutation, brokerMutation)) {
            assertThrows(IllegalStateException.class, () ->
                plan(batch(1000, v1(topic, 200, -1), mutation), diagnostics));
            assertNull(diagnostics.getUntrusted(key(topic)));
        }
    }

    @Test
    void revokedFeatureOldWireAndWrongRaftEpochFailBeforeAnyMutation() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState diagnostics = timeline();
        var original = certificate(topic, 200, -1, 9);
        var committed = batch(1000, new ApiMessageAndVersion(original, (short) 1));
        assertThrows(IllegalStateException.class, () -> planUntrusted(
            committed, MetadataVersion.LATEST_PRODUCTION, true, diagnostics));
        assertThrows(IllegalStateException.class, () -> planUntrusted(
            committed, MetadataVersion.IBP_4_4_IV0, false, diagnostics));
        original.setControllerEpoch(8);
        assertThrows(IllegalStateException.class, () -> plan(committed, diagnostics));
        original.setControllerEpoch(9);
        assertThrows(IllegalArgumentException.class, () -> plan(
            batch(0, new ApiMessageAndVersion(original, (short) 0)), diagnostics));
        assertThrows(IllegalArgumentException.class, () -> plan(
            batch(0, new ApiMessageAndVersion(original, (short) 2)), diagnostics));
        assertNull(diagnostics.getUntrusted(key(topic)));
    }

    @Test
    void separateTopicsKeepIndependentCasAndFreezeMutableSourceData() {
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState diagnostics = timeline();
        var original = certificate(first, 200, -1, 9);
        var planned = plan(batch(1000,
            new ApiMessageAndVersion(original, (short) 1),
            v1(second, 210, -1)), diagnostics);
        original.setEndOffset(999);
        original.replicaReceipts().get(0).setDurableEndOffset(999);
        assertEquals(2, planned.size());
        assertEquals(200L, planned.get(0).endOffset());
        assertEquals(200L, planned.get(0).replicas().get(0).durableEndOffset());
        assertEquals(1001L, planned.get(1).kraftOffset());
        assertTrue(planned.get(0).key().topicId().equals(first));
        assertTrue(planned.get(1).key().topicId().equals(second));
        assertNull(diagnostics.getUntrusted(key(first)));
        assertNull(diagnostics.getUntrusted(key(second)));
    }
}
