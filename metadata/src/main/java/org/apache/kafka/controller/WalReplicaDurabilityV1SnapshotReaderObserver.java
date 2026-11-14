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
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Captures only experimental V1 WAL checkpoint records from the ACTUAL KRaft
 * standby SnapshotReader callback. Ordinary metadata must continue through
 * the existing Controller replay path. Do not buffer the full metadata image.
 *
 * A snapshot-local batch offset is NOT the original assigned incremental V1
 * certificate offset. That trusted historical value is explicitly stored in
 * the validated snapshot-only checkpoint envelope.
 *
 * Not a snapshot writer, durability/fsync proof, Producer ACK/HW permit or
 * COMMITTED physical GC authorization.
 */
final class WalReplicaDurabilityV1SnapshotReaderObserver {
    private static final short CHECKPOINT_API_KEY =
        new WalReplicaDurabilityV1CheckpointRecord().apiKey();
    private static final short INCREMENTAL_API_KEY =
        new WalReplicaDurabilityCertificateRecord().apiKey();
    private static final int MAX_CHECKPOINT_RECORDS = 65_536;
    // Limit cumulative nested receipts and WAL spans as well as record count.
    // A snapshot with many large checkpoints must not amplify standby memory.
    private static final long MAX_CHECKPOINT_CLAIMS = 262_144L;

    private final List<ApiMessageAndVersion> captured = new ArrayList<>();
    private long capturedClaimCount;
    // The trusted SnapshotReader callback creates one observer per snapshot.
    // One failed or completed restore must never be retried on the same capture.
    private boolean restoreStarted;

    /**
     * Called once for each record from the actual standby SnapshotReader.
     *
     * @return true for a captured checkpoint which ordinary metadata replay
     *         must skip, false for a normal metadata record to replay.
     */
    boolean captureIfCheckpoint(ApiMessageAndVersion entry) {
        if (restoreStarted) {
            throw new IllegalStateException("WAL snapshot observer cannot capture after restore begins");
        }
        if (entry == null || entry.message() == null) {
            throw new IllegalArgumentException("null record in trusted snapshot callback");
        }
        short key = entry.message().apiKey();
        if (key == INCREMENTAL_API_KEY) {
            throw new IllegalStateException(
                "incremental WAL certificate record cannot be replayed as snapshot checkpoint");
        }
        if (key != CHECKPOINT_API_KEY) {
            return false;
        }
        if (!(entry.message() instanceof WalReplicaDurabilityV1CheckpointRecord checkpoint)) {
            throw new IllegalArgumentException("snapshot V1 checkpoint API key and record type mismatch");
        }
        if (captured.size() >= MAX_CHECKPOINT_RECORDS) {
            throw new IllegalArgumentException("too many WAL checkpoint entries in metadata snapshot");
        }
        if (checkpoint.replicaReceipts() == null || checkpoint.walSpans() == null) {
            throw new IllegalArgumentException("missing WAL checkpoint receipt or span claims");
        }
        long newClaims = (long) checkpoint.replicaReceipts().size() + checkpoint.walSpans().size();
        if (newClaims > MAX_CHECKPOINT_CLAIMS - capturedClaimCount) {
            throw new IllegalArgumentException("too many buffered WAL checkpoint receipt and span claims");
        }
        // Snapshot records are mutable generated messages. Do not retain an
        // alias which could change between this callback and final preflight.
        ApiMessageAndVersion frozen = duplicateCheckpoint(checkpoint, entry.version());
        captured.add(frozen);
        capturedClaimCount += newClaims;
        return true;
    }

    private static ApiMessageAndVersion duplicateCheckpoint(
        WalReplicaDurabilityV1CheckpointRecord checkpoint,
        short wireVersion
    ) {
        List<WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt> receipts =
            new ArrayList<>(checkpoint.replicaReceipts().size());
        for (var receipt : checkpoint.replicaReceipts()) {
            if (receipt == null) {
                throw new IllegalArgumentException("null WAL checkpoint replica receipt");
            }
            receipts.add(new WalReplicaDurabilityV1CheckpointRecord.ReplicaReceipt()
                .setBrokerId(receipt.brokerId())
                .setBrokerEpoch(receipt.brokerEpoch())
                .setIncarnationId(receipt.incarnationId())
                .setStorageGeneration(receipt.storageGeneration())
                .setDurableEndOffset(receipt.durableEndOffset()));
        }
        List<WalReplicaDurabilityV1CheckpointRecord.WalSpan> spans =
            new ArrayList<>(checkpoint.walSpans().size());
        for (var span : checkpoint.walSpans()) {
            if (span == null) {
                throw new IllegalArgumentException("null WAL checkpoint span claim");
            }
            spans.add(new WalReplicaDurabilityV1CheckpointRecord.WalSpan()
                .setBrokerId(span.brokerId())
                .setStartOffset(span.startOffset())
                .setEndOffset(span.endOffset())
                .setCrc32c(span.crc32c()));
        }
        var copy = new WalReplicaDurabilityV1CheckpointRecord()
            .setTopicId(checkpoint.topicId())
            .setPartitionId(checkpoint.partitionId())
            .setLeaderEpoch(checkpoint.leaderEpoch())
            .setPartitionEpoch(checkpoint.partitionEpoch())
            .setStartOffset(checkpoint.startOffset())
            .setEndOffset(checkpoint.endOffset())
            .setControllerEpoch(checkpoint.controllerEpoch())
            .setAcceptedKRaftOffset(checkpoint.acceptedKRaftOffset())
            .setPreviousKRaftOffset(checkpoint.previousKRaftOffset())
            .setAcceptedRaftEpoch(checkpoint.acceptedRaftEpoch())
            .setReplicaReceipts(receipts)
            .setWalSpans(spans);
        return new ApiMessageAndVersion(copy, wireVersion);
    }

    /**
     * Invoke ONLY once, after all ordinary snapshot metadata records replay
     * successfully, from the Controller event queue's trusted snapshot load.
     *
     * A checkpoint-free snapshot is a legitimate production/default-off
     * snapshot, including the offset-zero bootstrap snapshot. Never require
     * an experimental feature or reject the ordinary snapshot ID in this
     * case; simply clear stale non-authorizing diagnostic entries.
     *
     * A snapshot containing checkpoint evidence must meet the strict,
     * feature-negotiated v0 checkpoint and snapshot horizon requirements.
     */
    void restoreAfterCompleteSnapshot(
        OffsetAndEpoch trustedSnapshotId,
        Supplier<MetadataVersion> currentMetadataVersion,
        BooleanSupplier negotiatedExperimentalFeature,
        WalReplicaDurabilityV1ReplayState diagnosticTimeline
    ) {
        Objects.requireNonNull(trustedSnapshotId, "trustedSnapshotId");
        Objects.requireNonNull(currentMetadataVersion, "currentMetadataVersion");
        Objects.requireNonNull(negotiatedExperimentalFeature, "negotiatedExperimentalFeature");
        Objects.requireNonNull(diagnosticTimeline, "diagnosticTimeline");
        if (restoreStarted) {
            throw new IllegalStateException("WAL snapshot observer can restore only once");
        }
        // Burn the one-shot restore token before any feature check, validation,
        // or state mutation; a malformed snapshot must not be retried.
        restoreStarted = true;
        if (captured.isEmpty()) {
            diagnosticTimeline.clearUntrustedAfterCheckpointFreeSnapshot();
            return;
        }
        // We retain only checkpoint entries (<= 65,536), not ordinary
        // metadata batches. The synthesized batch's pseudo-offset is NEVER
        // used as an original certificate offset by the full-snapshot
        // preflight or by the replay timeline.
        Batch<ApiMessageAndVersion> checkpointData = Batch.data(
            0, 0, 0L, 0, List.copyOf(captured));
        diagnosticTimeline.restoreUntrustedSnapshot(
            List.of(checkpointData), currentMetadataVersion.get(),
            negotiatedExperimentalFeature.getAsBoolean(), trustedSnapshotId);
    }
}
