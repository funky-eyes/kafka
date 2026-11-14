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
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** V1 diagnostics are never physical WAL durability or Producer ACK permits. */
class WalReplicaDurabilityV1SnapshotReaderObserverTest {
    private static final OffsetAndEpoch SNAPSHOT = new OffsetAndEpoch(1300, 10);

    private static WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt receipt(int broker) {
        return new WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt()
            .setBrokerId(broker).setBrokerEpoch(broker + 100L)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityV1CheckpointRecord.WalSpan span(int broker) {
        return new WalReplicaDurabilityV1CheckpointRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(200).setCrc32c(1234);
    }

    private static WalReplicaDurabilityV1CheckpointRecord checkpoint(Uuid topic, long accepted) {
        return new WalReplicaDurabilityV1CheckpointRecord()
            .setTopicId(topic).setPartitionId(0).setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(200).setControllerEpoch(9)
            .setAcceptedRaftEpoch(9).setAcceptedKRaftOffset(accepted)
            .setPreviousKRaftOffset(-1)
            .setReplicaReceipts(java.util.List.of(receipt(1), receipt(2)))
            .setWalSpans(java.util.List.of(span(1), span(2)));
    }

    private static ApiMessageAndVersion wire(WalReplicaDurabilityV1CheckpointRecord record) {
        return new ApiMessageAndVersion(record, (short) 0);
    }

    private static WalReplicaDurabilityV1ReplayState timeline() {
        return new WalReplicaDurabilityV1ReplayState(new SnapshotRegistry(new LogContext()));
    }

    private static WalReplicaDurabilityCertificateReplayState.Key key(Uuid topic) {
        return new WalReplicaDurabilityCertificateReplayState.Key(topic, 0);
    }

    private static void complete(
        WalReplicaDurabilityV1SnapshotReaderObserver observer,
        OffsetAndEpoch id,
        WalReplicaDurabilityV1ReplayState state
    ) {
        observer.restoreAfterCompleteSnapshot(id, () -> MetadataVersion.IBP_4_4_IV0,
            () -> true, state);
    }

    @Test
    void standbySnapshotCollectsOnlyCheckpointsAndRestoresOriginalLogOffset() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline();
        WalReplicaDurabilityV1SnapshotReaderObserver observer =
            new WalReplicaDurabilityV1SnapshotReaderObserver();
        assertFalse(observer.captureIfCheckpoint(
            new ApiMessageAndVersion(new NoOpRecord(), (short) 0)));
        assertTrue(observer.captureIfCheckpoint(wire(checkpoint(topic, 1000))));
        assertNull(state.getUntrusted(key(topic)));
        complete(observer, SNAPSHOT, state);
        assertEquals(1000L, state.getUntrusted(key(topic)).kraftOffset());
        assertEquals(200L, state.getUntrusted(key(topic)).endOffset());
    }

    @Test
    void ordinaryBootstrapSnapshotAtOffsetZeroNeverRequiresExperimentalFeature() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline();
        var populated = new WalReplicaDurabilityV1SnapshotReaderObserver();
        assertTrue(populated.captureIfCheckpoint(wire(checkpoint(topic, 1000))));
        complete(populated, SNAPSHOT, state);
        var empty = new WalReplicaDurabilityV1SnapshotReaderObserver();
        assertFalse(empty.captureIfCheckpoint(new ApiMessageAndVersion(
            new NoOpRecord(), (short) 0)));
        empty.restoreAfterCompleteSnapshot(new OffsetAndEpoch(0, 0),
            () -> {
                throw new AssertionError("ordinary bootstrap must not read experimental metadata");
            },
            () -> {
                throw new AssertionError("ordinary bootstrap must not negotiate V1 WAL");
            }, state);
        assertNull(state.getUntrusted(key(topic)));
    }

    @Test
    void malformedLateOrDuplicateCheckpointFailsBeforeDiagnosticReplacement() {
        Uuid topic = Uuid.randomUuid();
        Uuid added = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline();
        var initial = new WalReplicaDurabilityV1SnapshotReaderObserver();
        initial.captureIfCheckpoint(wire(checkpoint(topic, 1000)));
        complete(initial, SNAPSHOT, state);
        var prior = state.getUntrusted(key(topic));
        var invalid = new WalReplicaDurabilityV1SnapshotReaderObserver();
        invalid.captureIfCheckpoint(wire(checkpoint(added, 1100)));
        invalid.captureIfCheckpoint(wire(checkpoint(Uuid.randomUuid(), 1400)));
        assertThrows(IllegalArgumentException.class, () -> complete(invalid, SNAPSHOT, state));
        assertEquals(prior, state.getUntrusted(key(topic)));
        assertNull(state.getUntrusted(key(added)));
        var duplicate = new WalReplicaDurabilityV1SnapshotReaderObserver();
        duplicate.captureIfCheckpoint(wire(checkpoint(added, 1100)));
        duplicate.captureIfCheckpoint(wire(checkpoint(added, 1150)));
        assertThrows(IllegalStateException.class, () -> complete(duplicate, SNAPSHOT, state));
        assertEquals(prior, state.getUntrusted(key(topic)));
    }

    @Test
    void incrementalCertificateCannotMasqueradeAsSnapshotCheckpointOrWrongWire() {
        Uuid topic = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline();
        var observer = new WalReplicaDurabilityV1SnapshotReaderObserver();
        var incremental = new ApiMessageAndVersion(
            new WalReplicaDurabilityCertificateRecord(), (short) 1);
        assertThrows(IllegalStateException.class, () -> observer.captureIfCheckpoint(incremental));
        assertThrows(IllegalArgumentException.class, () -> observer.captureIfCheckpoint(null));
        assertTrue(observer.captureIfCheckpoint(new ApiMessageAndVersion(
            checkpoint(topic, 1000), (short) 1)));
        assertThrows(IllegalArgumentException.class, () -> complete(observer, SNAPSHOT, state));
        assertNull(state.getUntrusted(key(topic)));
    }

    @Test
    void restoredEvidenceIsImmutableAndTopicIncarnationsRemainDistinct() {
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        WalReplicaDurabilityV1ReplayState state = timeline();
        var source = checkpoint(first, 1000);
        var observer = new WalReplicaDurabilityV1SnapshotReaderObserver();
        observer.captureIfCheckpoint(wire(source));
        observer.captureIfCheckpoint(wire(checkpoint(second, 1100)));
        complete(observer, SNAPSHOT, state);
        source.setEndOffset(900);
        source.replicaReceipts().get(0).setDurableEndOffset(900);
        source.walSpans().get(0).setEndOffset(900);
        assertEquals(200L, state.getUntrusted(key(first)).endOffset());
        assertEquals(200L, state.getUntrusted(key(first)).replicas().get(0).durableEndOffset());
        assertEquals(200L, state.getUntrusted(key(first)).spans().get(0).endOffset());
        assertEquals(1100L, state.getUntrusted(key(second)).kraftOffset());
    }
}
