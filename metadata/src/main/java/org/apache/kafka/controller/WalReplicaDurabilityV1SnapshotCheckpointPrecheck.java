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
import org.apache.kafka.common.metadata.WalReplicaDurabilityV1CheckpointRecord;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Key;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Replica;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Span;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.UntrustedEvidence;

/**
 * Read-only, snapshot-only V1 certificate checkpoint prerequisite.
 *
 * A V1 log certificate carries only its previous-record CAS. To continue its
 * chain after metadata log compaction, a snapshot needs the last accepted
 * original KRaft log position and the complete UNTRUSTED receipt data.
 *
 * The checkpoint payload is NOT self-authenticating. Only a future trusted
 * KRaft SnapshotReader callback and an enabled experimental feature may
 * provide context; this is not hooked into QuorumController, snapshot
 * serialization, a producer ACK, or physical GC.
 */
final class WalReplicaDurabilityV1SnapshotCheckpointPrecheck {
    private WalReplicaDurabilityV1SnapshotCheckpointPrecheck() {
    }

    /**
     * Make a deep, immutable copy after checking externally supplied snapshot
     * bounds and the underlying independent replica span claims. Success
     * is necessary but never sufficient to establish durable WAL bytes.
     */
    static UntrustedEvidence inspectUntrustedSnapshotCheckpoint(
        WalReplicaDurabilityV1CheckpointRecord checkpoint,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        OffsetAndEpoch trustedSnapshotId
    ) {
        requireSnapshotContext(checkpoint, trustedWireVersion, metadataVersion,
            negotiatedExperimentalFeature, trustedSnapshotId);
        WalReplicaDurabilityCertificateRecord evidence = copyAsUntrustedV1(checkpoint);
        WalReplicaDurabilityCertificateRecordPrecheck.requireWellFormedUntrustedRecord(evidence);
        return deepFreeze(checkpoint);
    }

    private static void requireSnapshotContext(
        WalReplicaDurabilityV1CheckpointRecord checkpoint,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        OffsetAndEpoch trustedSnapshotId
    ) {
        if (checkpoint == null || metadataVersion == null || trustedSnapshotId == null
            || trustedWireVersion != 0 || !negotiatedExperimentalFeature) {
            throw new IllegalArgumentException("untrusted or disabled experimental WAL checkpoint");
        }
        if (!metadataVersion.isAtLeast(MetadataVersion.IBP_4_4_IV0)) {
            throw new IllegalArgumentException("WAL checkpoint requires experimental metadata version 4.4");
        }
        requireSnapshotPosition(checkpoint, trustedSnapshotId);
    }

    private static void requireSnapshotPosition(
        WalReplicaDurabilityV1CheckpointRecord checkpoint,
        OffsetAndEpoch snapshot
    ) {
        if (snapshot.offset() <= 0 || snapshot.epoch() < 0) {
            throw new IllegalArgumentException("invalid trusted KRaft snapshot horizon");
        }
        if (checkpoint.acceptedKRaftOffset() < 0
            || checkpoint.acceptedKRaftOffset() >= snapshot.offset()
            || checkpoint.previousKRaftOffset() < -1
            || checkpoint.previousKRaftOffset() >= checkpoint.acceptedKRaftOffset()) {
            throw new IllegalArgumentException("checkpoint's claimed last accepted log offset is invalid");
        }
        if (checkpoint.acceptedRaftEpoch() < 0
            || checkpoint.acceptedRaftEpoch() > snapshot.epoch()
            || checkpoint.controllerEpoch() != checkpoint.acceptedRaftEpoch()) {
            throw new IllegalArgumentException("checkpoint's historical Raft epoch is invalid");
        }
    }

    private static WalReplicaDurabilityCertificateRecord copyAsUntrustedV1(
        WalReplicaDurabilityV1CheckpointRecord checkpoint
    ) {
        List<WalReplicaDurabilityCertificateRecord.ReplicaReceipt> replicas = new ArrayList<>();
        if (checkpoint.replicaReceipts() == null || checkpoint.walSpans() == null) {
            throw new IllegalArgumentException("missing checkpoint WAL evidence lists");
        }
        for (var receipt : checkpoint.replicaReceipts()) {
            if (receipt == null) {
                throw new IllegalArgumentException("null checkpoint replica receipt");
            }
            replicas.add(new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
                .setBrokerId(receipt.brokerId()).setBrokerEpoch(receipt.brokerEpoch())
                .setIncarnationId(receipt.incarnationId())
                .setStorageGeneration(receipt.storageGeneration())
                .setDurableEndOffset(receipt.durableEndOffset()));
        }
        List<WalReplicaDurabilityCertificateRecord.WalSpan> spans = new ArrayList<>();
        for (var span : checkpoint.walSpans()) {
            if (span == null) {
                throw new IllegalArgumentException("null checkpoint WAL span");
            }
            spans.add(new WalReplicaDurabilityCertificateRecord.WalSpan()
                .setBrokerId(span.brokerId()).setStartOffset(span.startOffset())
                .setEndOffset(span.endOffset()).setCrc32c(span.crc32c()));
        }
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(checkpoint.topicId()).setPartitionId(checkpoint.partitionId())
            .setLeaderEpoch(checkpoint.leaderEpoch()).setPartitionEpoch(checkpoint.partitionEpoch())
            .setStartOffset(checkpoint.startOffset()).setEndOffset(checkpoint.endOffset())
            .setControllerEpoch(checkpoint.controllerEpoch())
            .setExpectedPreviousKRaftOffset(checkpoint.previousKRaftOffset())
            .setReplicaReceipts(replicas).setWalSpans(spans);
    }

    private static UntrustedEvidence deepFreeze(WalReplicaDurabilityV1CheckpointRecord checkpoint) {
        List<Replica> replicas = checkpoint.replicaReceipts().stream()
            .map(r -> new Replica(r.brokerId(), r.brokerEpoch(),
                r.incarnationId(), r.storageGeneration(), r.durableEndOffset()))
            .toList();
        List<Span> spans = checkpoint.walSpans().stream()
            .map(s -> new Span(s.brokerId(), s.startOffset(), s.endOffset(), s.crc32c()))
            .toList();
        return new UntrustedEvidence(new Key(checkpoint.topicId(), checkpoint.partitionId()),
            checkpoint.leaderEpoch(), checkpoint.partitionEpoch(),
            checkpoint.startOffset(), checkpoint.endOffset(),
            checkpoint.controllerEpoch(), checkpoint.acceptedKRaftOffset(),
            replicas, spans);
    }
}
