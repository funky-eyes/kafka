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
import org.apache.kafka.common.metadata.WalReplicaDurabilityV1CheckpointRecord;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityV1SnapshotBatchPreflight.planUntrusted;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Snapshot checkpoint plans remain untrusted and cannot grant Producer ACK or GC. */
class WalReplicaDurabilityV1SnapshotBatchPreflightTest {
    private static final OffsetAndEpoch SNAPSHOT_ID = new OffsetAndEpoch(1300, 10);

    private static WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt receipt(int id) {
        return new WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt()
            .setBrokerId(id).setBrokerEpoch(id + 100L)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityV1CheckpointRecord.WalSpan span(int broker) {
        return new WalReplicaDurabilityV1CheckpointRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(200).setCrc32c(1234);
    }

    private static WalReplicaDurabilityV1CheckpointRecord checkpoint(Uuid topic, long accepted) {
        return new WalReplicaDurabilityV1CheckpointRecord()
            .setTopicId(topic).setPartitionId(0)
            .setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(200)
            .setControllerEpoch(9).setAcceptedRaftEpoch(9)
            .setAcceptedKRaftOffset(accepted).setPreviousKRaftOffset(-1)
            .setReplicaReceipts(List.of(receipt(1), receipt(2)))
            .setWalSpans(List.of(span(1), span(2)));
    }

    private static ApiMessageAndVersion wire(WalReplicaDurabilityV1CheckpointRecord checkpoint) {
        return new ApiMessageAndVersion(checkpoint, (short) 0);
    }

    private static Batch<ApiMessageAndVersion> batch(long base, ApiMessageAndVersion... records) {
        return Batch.data(base, 9, 0, 1024, List.of(records));
    }

    private static WalReplicaDurabilityV1SnapshotBatchPreflight.UntrustedSnapshotPlan plan(
        List<Batch<ApiMessageAndVersion>> batches
    ) {
        return planUntrusted(batches, MetadataVersion.IBP_4_4_IV0, true, SNAPSHOT_ID);
    }

    @Test
    void immutableCheckpointsAcrossBatchesUseOriginalAcceptedOffsetsNotSnapshotPseudoOffsets() {
        Uuid topicOne = Uuid.randomUuid();
        Uuid topicTwo = Uuid.randomUuid();
        var first = checkpoint(topicOne, 1000);
        var batches = List.of(
            batch(0, new ApiMessageAndVersion(new NoOpRecord(), (short) 0), wire(first)),
            batch(11, wire(checkpoint(topicTwo, 1200))));
        var result = plan(batches);
        assertEquals(SNAPSHOT_ID, result.snapshotId());
        assertEquals(2, result.checkpoints().size());
        assertEquals(1000L, result.checkpoints().get(0).kraftOffset());
        assertEquals(1200L, result.checkpoints().get(1).kraftOffset());
        assertNotEquals(topicOne, result.checkpoints().get(1).key().topicId());
        first.setEndOffset(500);
        first.replicaReceipts().get(0).setDurableEndOffset(500);
        assertEquals(200L, result.checkpoints().get(0).endOffset());
        assertEquals(200L, result.checkpoints().get(0).replicas().get(0).durableEndOffset());
        assertThrows(UnsupportedOperationException.class, () -> result.checkpoints().clear());
    }

    @Test
    void duplicateTopicPartitionCheckpointsAcrossBatchesRejectEntireSnapshot() {
        Uuid topic = Uuid.randomUuid();
        var first = checkpoint(topic, 1000);
        var duplicate = checkpoint(topic, 1100).setPreviousKRaftOffset(1000);
        assertThrows(IllegalStateException.class, () -> plan(List.of(
            batch(5, wire(first)), batch(10, wire(duplicate)))));
        var otherPartition = checkpoint(topic, 1100).setPartitionId(1);
        var allowed = plan(List.of(batch(5, wire(first)), batch(10, wire(otherPartition))));
        assertEquals(2, allowed.checkpoints().size());
    }

    @Test
    void futureOriginalOffsetAndUnsupportedWireVersionRejectEntireSnapshot() {
        Uuid topic = Uuid.randomUuid();
        var valid = wire(checkpoint(topic, 1000));
        var future = wire(checkpoint(Uuid.randomUuid(), 1300));
        assertThrows(IllegalArgumentException.class, () ->
            plan(List.of(batch(0, valid), batch(1, future))));
        var unsupported = new ApiMessageAndVersion(checkpoint(Uuid.randomUuid(), 1200), (short) 1);
        assertThrows(IllegalArgumentException.class, () ->
            plan(List.of(batch(0, valid), batch(1, unsupported))));
        // An incremental V1 certificate is NOT a snapshot checkpoint:
        // its pseudo-offset cannot recover the source's assigned log offset.
        var incremental = new ApiMessageAndVersion(
            new WalReplicaDurabilityCertificateRecord(), (short) 1);
        assertThrows(IllegalStateException.class, () ->
            plan(List.of(batch(0, valid), batch(1, incremental))));
    }

    @Test
    void ordinaryMetadataAndEmptyBatchesNeverCreateCheckpointEvidence() {
        var feature = new ApiMessageAndVersion(
            new FeatureLevelRecord().setName("metadata.version").setFeatureLevel((short) 0),
            (short) 0);
        var empty = plan(List.of(batch(0, feature), batch(1, new ApiMessageAndVersion(
            new NoOpRecord(), (short) 0))));
        assertTrue(empty.checkpoints().isEmpty());
        var productionOnly = planUntrusted(List.of(batch(0, feature)),
            MetadataVersion.LATEST_PRODUCTION, false, SNAPSHOT_ID);
        assertTrue(productionOnly.checkpoints().isEmpty());
        assertTrue(plan(List.of()).checkpoints().isEmpty());
    }

    @Test
    void nullMalformedAndRevokedFeatureInputsCannotExposeCheckpointPlan() {
        var source = wire(checkpoint(Uuid.randomUuid(), 1000));
        var batches = List.of(batch(0, source));
        assertThrows(IllegalArgumentException.class, () ->
            planUntrusted(null, MetadataVersion.IBP_4_4_IV0, true, SNAPSHOT_ID));
        assertThrows(IllegalArgumentException.class, () ->
            planUntrusted(batches, null, true, SNAPSHOT_ID));
        assertThrows(IllegalArgumentException.class, () ->
            planUntrusted(batches, MetadataVersion.IBP_4_4_IV0, true, null));
        assertThrows(IllegalArgumentException.class, () ->
            planUntrusted(batches, MetadataVersion.IBP_4_4_IV0, false, SNAPSHOT_ID));
        var invalid = wire(checkpoint(Uuid.randomUuid(), 1000).setTopicId(null));
        assertThrows(IllegalArgumentException.class, () -> plan(List.of(batch(0, invalid))));
        List<Batch<ApiMessageAndVersion>> malformedBatches = new ArrayList<>();
        malformedBatches.add(null);
        assertThrows(IllegalArgumentException.class, () -> plan(malformedBatches));
        List<ApiMessageAndVersion> nullMessages = new ArrayList<>();
        nullMessages.add(null);
        assertThrows(IllegalArgumentException.class, () ->
            plan(List.of(Batch.data(0, 9, 0, 1024, nullMessages))));
    }
}
