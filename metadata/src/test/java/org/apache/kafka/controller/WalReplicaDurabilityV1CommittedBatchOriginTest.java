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
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityV1CommittedBatchOrigin.inspectCommittedDataBatch;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** No test in this class grants a producer ACK or enables physical GC. */
class WalReplicaDurabilityV1CommittedBatchOriginTest {
    private static WalReplicaDurabilityCertificateRecord certificate(Uuid topic) {
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(topic).setPartitionId(0).setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(200).setControllerEpoch(9)
            .setExpectedPreviousKRaftOffset(-1)
            .setReplicaReceipts(List.of(
                receipt(1), receipt(2)))
            .setWalSpans(List.of(span(1), span(2)));
    }

    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(int broker) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(broker).setBrokerEpoch(100L + broker)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int broker) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(200).setCrc32c(1234L);
    }

    private static ApiMessageAndVersion v1(Uuid topic) {
        return new ApiMessageAndVersion(certificate(topic), (short) 1);
    }

    private static ApiMessageAndVersion noOp() {
        return new ApiMessageAndVersion(new NoOpRecord(), (short) 0);
    }

    private static Batch<ApiMessageAndVersion> batch(
        long base, int epoch, List<ApiMessageAndVersion> messages
    ) {
        return Batch.data(base, epoch, 0, 1024, messages);
    }

    @Test
    void assignsExactOffsetsAcrossMixedMetadataRecords() {
        var topic = Uuid.randomUuid();
        var messages = List.of(
            new ApiMessageAndVersion(new FeatureLevelRecord().setName("metadata.version")
                .setFeatureLevel((short) 0), (short) 0),
            v1(topic), noOp(), v1(topic));
        var positions = inspectCommittedDataBatch(batch(500, 9, messages));
        assertEquals(2, positions.size());
        assertEquals(1, positions.get(0).index());
        assertEquals(501L, positions.get(0).assignedOffset());
        assertEquals(3, positions.get(1).index());
        assertEquals(503L, positions.get(1).assignedOffset());
        assertEquals(500L, positions.get(0).committedBatch().baseOffset());
        assertEquals(503L, positions.get(0).committedBatch().lastOffset());
        assertEquals(504L, positions.get(0).committedBatch().committedNextOffset());
        assertEquals(9, positions.get(0).committedBatch().raftEpoch());
        assertEquals(1, positions.get(0).wireVersion());
        assertThrows(UnsupportedOperationException.class, () -> positions.clear());
    }

    @Test
    void nonCertificateMetadataNeverCreatesReplayAuthority() {
        var positions = inspectCommittedDataBatch(batch(10, 9, List.of(noOp(), noOp())));
        assertTrue(positions.isEmpty());
        var ordinary = new ApiMessageAndVersion(
            new FeatureLevelRecord().setName("metadata.version").setFeatureLevel((short) 0),
            (short) 0);
        assertTrue(inspectCommittedDataBatch(batch(10, 9, List.of(ordinary))).isEmpty());
    }

    @Test
    void legacyV0AndUnknownWireVersionsAreRejectedEvenAtOffsetZero() {
        var legacy = new ApiMessageAndVersion(certificate(Uuid.randomUuid()), (short) 0);
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(0, 9, List.of(legacy))));
        var unknown = new ApiMessageAndVersion(certificate(Uuid.randomUuid()), (short) 2);
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(0, 9, List.of(unknown))));
    }

    @Test
    void nullNegativeAndOverflowingKRaftBatchCoordinatesFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> inspectCommittedDataBatch(null));
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(-1, 9, List.of(v1(Uuid.randomUuid())))));
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(5, -1, List.of(v1(Uuid.randomUuid())))));
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(Long.MAX_VALUE, 9, List.of(v1(Uuid.randomUuid())))));
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(Long.MAX_VALUE - 1, 9,
                List.of(v1(Uuid.randomUuid()), noOp(), noOp()))));
        List<ApiMessageAndVersion> badMessages = new ArrayList<>();
        badMessages.add(null);
        assertThrows(IllegalArgumentException.class, () ->
            inspectCommittedDataBatch(batch(5, 9, badMessages)));
    }

    @Test
    void observedBatchPositionCanOnlyPopulateUntrustedDiagnosticReplay() {
        Uuid topic = Uuid.randomUuid();
        var source = certificate(topic);
        var records = List.of(noOp(), new ApiMessageAndVersion(source, (short) 1));
        var positions = inspectCommittedDataBatch(batch(1000, 9, records));
        assertEquals(1, positions.size());
        var p = positions.get(0);
        var diagnostics = new WalReplicaDurabilityV1ReplayState(
            new SnapshotRegistry(new LogContext()));
        diagnostics.replayCommittedV1(
            source, p.wireVersion(), MetadataVersion.IBP_4_4_IV0, true,
            p.committedBatch(), p.assignedOffset());
        var key = new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
        assertEquals(1001L, diagnostics.getUntrusted(key).kraftOffset());
        source.setEndOffset(500);
        assertEquals(200L, diagnostics.getUntrusted(key).endOffset());
        assertFalse(diagnostics.getUntrusted(key).spans().isEmpty());
    }
}
