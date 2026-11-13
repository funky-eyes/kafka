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
import org.apache.kafka.common.metadata.WalReplicaDurabilityV1CheckpointRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityV1SnapshotCheckpointPrecheck.inspectUntrustedSnapshotCheckpoint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WalReplicaDurabilityV1SnapshotCheckpointPrecheckTest {
    private static WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt receipt(int id) {
        return new WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt()
            .setBrokerId(id).setBrokerEpoch(id + 100L)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityV1CheckpointRecord.WalSpan span(int id) {
        return new WalReplicaDurabilityV1CheckpointRecord.WalSpan()
            .setBrokerId(id).setStartOffset(100).setEndOffset(200).setCrc32c(1234);
    }

    private static WalReplicaDurabilityV1CheckpointRecord checkpoint(Uuid topic) {
        return new WalReplicaDurabilityV1CheckpointRecord()
            .setTopicId(topic).setPartitionId(0)
            .setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(200)
            .setControllerEpoch(9).setAcceptedRaftEpoch(9)
            .setAcceptedKRaftOffset(1000).setPreviousKRaftOffset(999)
            .setReplicaReceipts(List.of(receipt(1), receipt(2)))
            .setWalSpans(List.of(span(1), span(2)));
    }

    private static WalReplicaDurabilityCertificateReplayState.UntrustedEvidence inspect(
        WalReplicaDurabilityV1CheckpointRecord record
    ) {
        return inspectUntrustedSnapshotCheckpoint(
            record, (short) 0, MetadataVersion.IBP_4_4_IV0,
            true, new OffsetAndEpoch(1200, 10));
    }

    @Test
    void snapshotCheckpointV0BinaryRoundTripPreservesAssignedOffsetAndEvidence() {
        var source = checkpoint(Uuid.randomUuid());
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buf = ByteBuffer.allocate(source.size(cache, (short) 0));
        source.write(new ByteBufferAccessor(buf), cache, (short) 0);
        buf.flip();
        var decoded = new WalReplicaDurabilityV1CheckpointRecord();
        decoded.read(new ByteBufferAccessor(buf), (short) 0);
        assertEquals(source, decoded);
        assertEquals(1000L, inspect(decoded).kraftOffset());
        assertEquals(2, inspect(decoded).replicas().size());
        assertEquals(2, inspect(decoded).spans().size());
    }

    @Test
    void snapshotReaderBoundaryAndExperimentalFeatureAreRequired() {
        var record = checkpoint(Uuid.randomUuid());
        assertThrows(IllegalArgumentException.class, () ->
            inspectUntrustedSnapshotCheckpoint(record, (short) 1,
                MetadataVersion.IBP_4_4_IV0, true, new OffsetAndEpoch(1200, 10)));
        assertThrows(IllegalArgumentException.class, () ->
            inspectUntrustedSnapshotCheckpoint(record, (short) 0,
                MetadataVersion.LATEST_PRODUCTION, true, new OffsetAndEpoch(1200, 10)));
        assertThrows(IllegalArgumentException.class, () ->
            inspectUntrustedSnapshotCheckpoint(record, (short) 0,
                MetadataVersion.IBP_4_4_IV0, false, new OffsetAndEpoch(1200, 10)));
        assertThrows(IllegalArgumentException.class, () ->
            inspectUntrustedSnapshotCheckpoint(record, (short) 0,
                MetadataVersion.IBP_4_4_IV0, true, null));
        assertThrows(IllegalArgumentException.class, () ->
            inspectUntrustedSnapshotCheckpoint(record, (short) 0,
                MetadataVersion.IBP_4_4_IV0, true, new OffsetAndEpoch(1000, 10)));
    }

    @Test
    void forgedFutureAndRegressedOriginalCertificateOffsetsFailClosed() {
        var record = checkpoint(Uuid.randomUuid());
        assertThrows(IllegalArgumentException.class, () ->
            inspect(record.setAcceptedKRaftOffset(1200)));
        record.setAcceptedKRaftOffset(1000).setPreviousKRaftOffset(1000);
        assertThrows(IllegalArgumentException.class, () -> inspect(record));
        record.setPreviousKRaftOffset(-2);
        assertThrows(IllegalArgumentException.class, () -> inspect(record));
        record.setPreviousKRaftOffset(999).setAcceptedRaftEpoch(11);
        assertThrows(IllegalArgumentException.class, () -> inspect(record));
        record.setAcceptedRaftEpoch(9).setControllerEpoch(8);
        assertThrows(IllegalArgumentException.class, () -> inspect(record));
        record.setControllerEpoch(9).setPreviousKRaftOffset(-1);
        assertEquals(1000L, inspect(record).kraftOffset());
    }

    @Test
    void foreignOrMalformedReplicaAndWalClaimsCannotBecomeCheckpointEvidence() {
        var record = checkpoint(Uuid.randomUuid());
        record.replicaReceipts().get(1)
            .setIncarnationId(record.replicaReceipts().get(0).incarnationId());
        assertThrows(IllegalArgumentException.class, () -> inspect(record));
        record = checkpoint(Uuid.randomUuid());
        record.walSpans().get(0).setCrc32c(-1);
        var badCrc = record;
        assertThrows(IllegalArgumentException.class, () -> inspect(badCrc));
        var missingIdentity = checkpoint(Uuid.ZERO_UUID);
        assertThrows(IllegalArgumentException.class, () -> inspect(missingIdentity));
        var missingTopic = checkpoint(Uuid.randomUuid()).setTopicId(null);
        assertThrows(IllegalArgumentException.class, () -> inspect(missingTopic));
        var missingReplicas = checkpoint(Uuid.randomUuid()).setReplicaReceipts(null);
        assertThrows(IllegalArgumentException.class, () -> inspect(missingReplicas));
    }

    @Test
    void immutableDiagnosticCheckpointDoesNotAliasGeneratedMutableRecordOrTopic() {
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        var source = checkpoint(first);
        var result = inspect(source);
        var originalIncarnation = source.replicaReceipts().get(0).incarnationId();
        source.setEndOffset(900);
        source.replicaReceipts().get(0).setIncarnationId(Uuid.randomUuid());
        source.walSpans().get(0).setEndOffset(900);
        assertEquals(200L, result.endOffset());
        assertEquals(200L, result.spans().get(0).endOffset());
        assertEquals(originalIncarnation, result.replicas().get(0).incarnationId());
        assertThrows(UnsupportedOperationException.class, () -> result.replicas().clear());
        assertNotEquals(result.key(), inspect(checkpoint(second)).key());
    }
}
