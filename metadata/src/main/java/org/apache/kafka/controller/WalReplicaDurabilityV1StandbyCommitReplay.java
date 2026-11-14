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

import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.UntrustedEvidence;

/**
 * Standby-only KRaft commit callback replay. V1 certificate records are
 * read-only UNTRUSTED diagnostics, not MetadataImage or Producer ACK state.
 * The KRaft Batch is trusted ONLY when invoked from QuorumMetaLogListener's
 * real handleCommit callback. Never invoke this from prepareAppend or Broker
 * input; these objects carry no self-authenticating provenance.
 */
final class WalReplicaDurabilityV1StandbyCommitReplay {
    @FunctionalInterface
    interface OrdinaryMetadataReplay {
        void replay(ApiMessageAndVersion message, long assignedOffset);
    }

    private WalReplicaDurabilityV1StandbyCommitReplay() {
    }

    /**
     * First validates all WAL V1 receipts/CAS in the committed batch.
     * Replays regular metadata records in original record order. Installs
     * immutable, non-authorizing certificate diagnostics ONLY if every
     * ordinary metadata record has completed without throwing.
     *
     * Ordinary batches without WAL certificates do not consult experimental
     * version/feature state or touch the diagnostics.
     */
    static void replayCommittedBatch(
        Batch<ApiMessageAndVersion> committedBatch,
        Supplier<MetadataVersion> currentMetadataVersion,
        BooleanSupplier negotiatedExperimentalFeature,
        WalReplicaDurabilityV1ReplayState diagnostics,
        OrdinaryMetadataReplay ordinaryReplay
    ) {
        Objects.requireNonNull(committedBatch, "committedBatch");
        Objects.requireNonNull(currentMetadataVersion, "currentMetadataVersion");
        Objects.requireNonNull(negotiatedExperimentalFeature, "negotiatedExperimentalFeature");
        Objects.requireNonNull(diagnostics, "diagnostics");
        Objects.requireNonNull(ordinaryReplay, "ordinaryReplay");

        boolean hasCertificate = false;
        for (ApiMessageAndVersion entry : committedBatch.records()) {
            if (entry == null || entry.message() == null) {
                throw new IllegalArgumentException("null record in committed KRaft batch");
            }
            if (entry.message() instanceof WalReplicaDurabilityV1CheckpointRecord) {
                throw new IllegalStateException("snapshot-only V1 checkpoint in the committed metadata log");
            }
            if (entry.message() instanceof WalReplicaDurabilityCertificateRecord) {
                hasCertificate = true;
            }
        }
        List<UntrustedEvidence> plan = hasCertificate
            ? WalReplicaDurabilityV1BatchPreflight.planUntrusted(
                committedBatch, currentMetadataVersion.get(),
                negotiatedExperimentalFeature.getAsBoolean(), diagnostics)
            : List.of();

        int index = 0;
        for (ApiMessageAndVersion entry : committedBatch.records()) {
            if (!(entry.message() instanceof WalReplicaDurabilityCertificateRecord)) {
                long recordOffset = committedBatch.baseOffset() + index;
                try {
                    ordinaryReplay.replay(entry, recordOffset);
                } catch (RuntimeException e) {
                    throw new IllegalStateException(
                        "Unable to replay ordinary metadata record at committed KRaft offset " +
                            recordOffset, e);
                }
            }
            index++;
        }
        diagnostics.applyPrevalidatedCommittedBatch(plan);
    }
}
