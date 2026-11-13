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

import org.apache.kafka.common.metadata.WalReplicaDurabilityV1CheckpointRecord;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Key;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.UntrustedEvidence;

/**
 * Isolated, read-only preflight for experimental V1 WAL certificate snapshot
 * checkpoints. A Kafka metadata snapshot can contain many batches. It must
 * contain at most one accepted checkpoint for each immutable TopicId/partition
 * across ALL its batches, not just within one BatchReader iteration.
 *
 * A snapshot batch's local record offsets are not the original KRaft WAL
 * certificate append positions. Only the checkpoint's original accepted
 * offset and the trusted SnapshotReader horizon are examined.
 *
 * This helper CANNOT authenticate an arbitrary list as a KRaft snapshot and
 * is not called from production QuorumController/MetadataImage. Successful
 * preflight is untrusted diagnostic data, NEVER Producer ACK/HW/GC authority.
 */
final class WalReplicaDurabilityV1SnapshotBatchPreflight {
    private static final short CHECKPOINT_API_KEY = new WalReplicaDurabilityV1CheckpointRecord().apiKey();
    private static final int MAX_UNTRUSTED_CHECKPOINTS = 65_536;

    record UntrustedSnapshotPlan(OffsetAndEpoch snapshotId, List<UntrustedEvidence> checkpoints) {
        UntrustedSnapshotPlan {
            Objects.requireNonNull(snapshotId, "snapshotId");
            checkpoints = List.copyOf(checkpoints);
        }
    }

    private WalReplicaDurabilityV1SnapshotBatchPreflight() {
    }

    /**
     * Scan the complete snapshot before exposing any diagnostic checkpoint
     * evidence. Duplicates in separate batches fail the entire plan.
     */
    static UntrustedSnapshotPlan planUntrusted(
        List<Batch<ApiMessageAndVersion>> snapshotBatches,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        OffsetAndEpoch trustedSnapshotId
    ) {
        if (snapshotBatches == null || metadataVersion == null || trustedSnapshotId == null) {
            throw new IllegalArgumentException("missing trusted WAL snapshot preflight context");
        }
        List<UntrustedEvidence> checkpoints = new ArrayList<>();
        Set<Key> seen = new HashSet<>();
        for (Batch<ApiMessageAndVersion> batch : snapshotBatches) {
            scanSnapshotBatch(batch, metadataVersion, negotiatedExperimentalFeature,
                trustedSnapshotId, checkpoints, seen);
        }
        return new UntrustedSnapshotPlan(trustedSnapshotId, checkpoints);
    }

    private static void scanSnapshotBatch(
        Batch<ApiMessageAndVersion> batch,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        OffsetAndEpoch trustedSnapshotId,
        List<UntrustedEvidence> checkpoints,
        Set<Key> seen
    ) {
        if (batch == null || batch.records() == null) {
            throw new IllegalArgumentException("missing KRaft snapshot data batch");
        }
        for (ApiMessageAndVersion entry : batch.records()) {
            if (entry == null || entry.message() == null) {
                throw new IllegalArgumentException("null metadata record within KRaft snapshot");
            }
            if (entry.message().apiKey() != CHECKPOINT_API_KEY) {
                continue;
            }
            if (!(entry.message() instanceof WalReplicaDurabilityV1CheckpointRecord checkpoint)) {
                throw new IllegalArgumentException("checkpoint API key does not match checkpoint message");
            }
            addSnapshotCheckpoint(checkpoint, entry.version(), metadataVersion,
                negotiatedExperimentalFeature, trustedSnapshotId, checkpoints, seen);
        }
    }

    private static void addSnapshotCheckpoint(
        WalReplicaDurabilityV1CheckpointRecord checkpoint,
        short wireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        OffsetAndEpoch trustedSnapshotId,
        List<UntrustedEvidence> checkpoints,
        Set<Key> seen
    ) {
        if (checkpoints.size() >= MAX_UNTRUSTED_CHECKPOINTS) {
            throw new IllegalArgumentException("too many untrusted WAL snapshot checkpoints");
        }
        UntrustedEvidence evidence = WalReplicaDurabilityV1SnapshotCheckpointPrecheck
            .inspectUntrustedSnapshotCheckpoint(
                checkpoint, wireVersion, metadataVersion,
                negotiatedExperimentalFeature, trustedSnapshotId);
        if (!seen.add(evidence.key())) {
            throw new IllegalStateException("duplicate V1 WAL checkpoint for same TopicId and partition");
        }
        checkpoints.add(evidence);
    }
}
