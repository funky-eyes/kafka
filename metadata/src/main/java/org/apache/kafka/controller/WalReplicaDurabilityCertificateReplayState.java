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
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Isolated, rollback-aware read model for EXPERIMENTAL KRaft WAL certificate
 * records. No Controller production replay dispatch or record emitter calls
 * this class. Stored evidence remains UNTRUSTED and is never ACK/GC authority.
 */
final class WalReplicaDurabilityCertificateReplayState {
    record Key(Uuid topicId, int partition) {
        Key {
            Objects.requireNonNull(topicId);
            if (Uuid.ZERO_UUID.equals(topicId) || partition < 0) {
                throw new IllegalArgumentException("invalid WAL certificate replay key");
            }
        }
    }

    record Replica(int brokerId, long brokerEpoch, Uuid incarnationId,
                   long storageGeneration, long durableEndOffset) {
    }

    record Span(int brokerId, long startOffset, long endOffset, long crc32c) {
    }

    /** Frozen copy: generated metadata records and nested entries are mutable. */
    record UntrustedEvidence(
        Key key, int leaderEpoch, int partitionEpoch, long startOffset,
        long endOffset, long controllerEpoch, long kraftOffset,
        List<Replica> replicas, List<Span> spans
    ) {
        UntrustedEvidence {
            Objects.requireNonNull(key);
            replicas = List.copyOf(replicas);
            spans = List.copyOf(spans);
        }
    }

    private final TimelineHashMap<Key, UntrustedEvidence> state;

    WalReplicaDurabilityCertificateReplayState(SnapshotRegistry registry) {
        state = new TimelineHashMap<>(Objects.requireNonNull(registry, "registry"), 0);
    }

    /**
     * Accept only a structurally valid experimental record at a trusted,
     * committed KRaft log position or within an actual snapshot horizon.
     * The caller MUST obtain these inputs from KRaft, not the record itself.
     * No caller can treat a successful replay as durable Broker proof.
     */
    void replayFromKRaft(
        WalReplicaDurabilityCertificateRecord record,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        Optional<OffsetAndEpoch> snapshotId,
        long observedRecordOffset,
        long committedNextOffset
    ) {
        if (!WalReplicaDurabilityCertificateRecordPrecheck.hasCommittedReplayOrigin(
            record, trustedWireVersion, metadataVersion, negotiatedExperimentalFeature,
            snapshotId, observedRecordOffset, committedNextOffset)) {
            throw new IllegalStateException("untrusted or uncommitted WAL certificate replay source");
        }
        UntrustedEvidence next = freeze(record);
        UntrustedEvidence previous = state.get(next.key());
        if (previous != null) {
            requireMonotonicReplay(previous, next);
        }
        state.put(next.key(), next);
    }

    /**
     * Diagnostics only. No ACK, high-watermark, physical GC or other
     * production code may consume this untrusted data as authorization.
     */
    UntrustedEvidence getUntrusted(Key key) {
        return state.get(key);
    }

    private static UntrustedEvidence freeze(WalReplicaDurabilityCertificateRecord record) {
        List<Replica> replicas = record.replicaReceipts().stream()
            .map(r -> new Replica(r.brokerId(), r.brokerEpoch(),
                r.incarnationId(), r.storageGeneration(), r.durableEndOffset()))
            .toList();
        List<Span> spans = record.walSpans().stream()
            .map(s -> new Span(s.brokerId(), s.startOffset(), s.endOffset(), s.crc32c()))
            .toList();
        return new UntrustedEvidence(
            new Key(record.topicId(), record.partitionId()), record.leaderEpoch(),
            record.partitionEpoch(), record.startOffset(), record.endOffset(),
            record.controllerEpoch(), record.kRaftOffset(), replicas, spans);
    }

    private static void requireMonotonicReplay(UntrustedEvidence previous, UntrustedEvidence next) {
        if (next.kraftOffset() <= previous.kraftOffset()
            || next.endOffset() < previous.endOffset()
            || next.controllerEpoch() < previous.controllerEpoch()) {
            throw new IllegalStateException("stale or regressed KRaft WAL certificate replay");
        }
        if (next.leaderEpoch() < previous.leaderEpoch()
            || next.partitionEpoch() < previous.partitionEpoch()) {
            throw new IllegalStateException("regressed WAL certificate leader or partition epoch");
        }
        if (next.leaderEpoch() > previous.leaderEpoch()
            && next.partitionEpoch() <= previous.partitionEpoch()) {
            throw new IllegalStateException("leader change without new KRaft partition epoch");
        }
    }
}
