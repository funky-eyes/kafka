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

import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.Key;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateReplayState.UntrustedEvidence;

/**
 * Pure, non-mutating preflight of all V1 WAL certificate claims within one
 * already-committed KRaft batch. No caller is wired into QuorumController;
 * this is not certificate issuance, a Broker fsync attestation, an ACK
 * authorization, a high-watermark update, or COMMITTED physical GC.
 *
 * A batch's trusted provenance still MUST be supplied by an actual KRaft
 * handleCommit callback. Passing an arbitrary Batch object does not
 * authenticate the callback, the certificate, or the WAL bytes.
 */
final class WalReplicaDurabilityV1BatchPreflight {
    private WalReplicaDurabilityV1BatchPreflight() {
    }

    /**
     * Validates the entire batch against existing read-only V1 timeline
     * evidence, including multiple certificates for the same partition.
     * Even if a late certificate fails, nothing is written into the timeline.
     * Returned immutable evidence is an UNTRUSTED plan, never an ACK permit.
     */
    static List<UntrustedEvidence> planUntrusted(
        Batch<ApiMessageAndVersion> committedBatch,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        WalReplicaDurabilityV1ReplayState diagnosticTimeline
    ) {
        if (metadataVersion == null || diagnosticTimeline == null) {
            throw new IllegalArgumentException("missing WAL certificate feature or diagnostic timeline");
        }
        var positions = WalReplicaDurabilityV1CommittedBatchOrigin.inspectCommittedDataBatch(
            committedBatch);
        if (positions.isEmpty()) {
            return List.of();
        }
        rejectMixedIdentityChanges(committedBatch.records());
        Map<Key, UntrustedEvidence> staged = new HashMap<>();
        List<UntrustedEvidence> plan = new ArrayList<>();
        for (var position : positions) {
            var record = (WalReplicaDurabilityCertificateRecord) committedBatch.records()
                .get(position.index()).message();
            Key key = WalReplicaDurabilityV1ReplayState.requireValidKey(record);
            UntrustedEvidence previous = staged.containsKey(key) ? staged.get(key)
                : diagnosticTimeline.getUntrusted(key);
            UntrustedEvidence next = WalReplicaDurabilityV1ReplayState.validateTransition(
                record, key, position.wireVersion(), metadataVersion,
                negotiatedExperimentalFeature, position.committedBatch(),
                position.assignedOffset(), previous);
            plan.add(next);
            staged.put(key, next);
        }
        return List.copyOf(plan);
    }

    /**
     * A single externally supplied feature level and metadata version cannot
     * represent interleaved feature, partition or Broker identity mutations
     * inside a batch. Fail closed until production replay provides a trusted
     * per-record state transition and a fully authenticated durability input.
     */
    private static void rejectMixedIdentityChanges(List<ApiMessageAndVersion> records) {
        for (ApiMessageAndVersion entry : records) {
            if (mutatesIdentityOrFeature(MetadataRecordType.fromId(entry.message().apiKey()))) {
                throw new IllegalStateException(
                    "WAL certificate batch interleaves feature or replica identity mutations");
            }
        }
    }

    private static boolean mutatesIdentityOrFeature(MetadataRecordType type) {
        return switch (type) {
            case FEATURE_LEVEL_RECORD,
                TOPIC_RECORD,
                REMOVE_TOPIC_RECORD,
                PARTITION_RECORD,
                PARTITION_CHANGE_RECORD,
                REGISTER_BROKER_RECORD,
                UNREGISTER_BROKER_RECORD,
                BROKER_REGISTRATION_CHANGE_RECORD,
                FENCE_BROKER_RECORD,
                UNFENCE_BROKER_RECORD,
                CLEAR_ELR_RECORD -> true;
            default -> false;
        };
    }
}
