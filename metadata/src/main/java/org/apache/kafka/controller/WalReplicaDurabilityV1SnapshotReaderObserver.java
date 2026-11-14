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

    private final List<ApiMessageAndVersion> captured = new ArrayList<>();

    /**
     * Called once for each record from the actual standby SnapshotReader.
     *
     * @return true for a captured checkpoint which ordinary metadata replay
     *         must skip, false for a normal metadata record to replay.
     */
    boolean captureIfCheckpoint(ApiMessageAndVersion entry) {
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
        if (!(entry.message() instanceof WalReplicaDurabilityV1CheckpointRecord)) {
            throw new IllegalArgumentException("snapshot V1 checkpoint API key and record type mismatch");
        }
        if (captured.size() >= MAX_CHECKPOINT_RECORDS) {
            throw new IllegalArgumentException("too many WAL checkpoint entries in metadata snapshot");
        }
        captured.add(entry);
        return true;
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
