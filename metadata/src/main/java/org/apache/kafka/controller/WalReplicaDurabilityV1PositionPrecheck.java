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
import org.apache.kafka.server.common.MetadataVersion;

/**
 * V1-only structural check for an externally observed, committed KRaft
 * position. Unlike experimental V0, V1 never claims its own future append
 * offset in the record being appended.
 *
 * This is NOT an issuer, Broker WAL durability attestation, Controller replay
 * dispatcher, ACK grant or physical-GC permit. The Controller must populate
 * all observed batch coordinates from its KRaft commit callback, not from a
 * Broker, a speculative prepareAppend call, or certificate payload fields.
 */
final class WalReplicaDurabilityV1PositionPrecheck {
    /**
     * Values must come from an actually committed KRaft batch and be checked
     * against its trusted commit callback. This record cannot authenticate
     * those sources by itself.
     */
    record CommitBatch(long baseOffset, long lastOffset, int raftEpoch, long committedNextOffset) {
        CommitBatch {
            if (baseOffset < 0 || lastOffset < baseOffset || raftEpoch < 0
                || committedNextOffset <= lastOffset) {
                throw new IllegalArgumentException("invalid committed KRaft batch boundaries");
            }
        }
    }

    private WalReplicaDurabilityV1PositionPrecheck() {
    }

    /**
     * The V1 record encodes ONLY an expected previous accepted KRaft offset,
     * which is knowable before append. Its own real append offset must be
     * provided by the committed Raft batch (never by the record).
     *
     * A positive result is necessary but NEVER sufficient for ACK.
     * Snapshot replay is not supported by this V1 prerequisite yet.
     */
    static boolean hasAssignedCommittedPosition(
        WalReplicaDurabilityCertificateRecord record,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        CommitBatch trustedCommittedBatch,
        long trustedRecordOffset,
        long trustedPreviousCertificateOffset
    ) {
        // The actual schema version comes from KRaft's ApiMessageAndVersion,
        // not from the mutable generated record. V0 with an offset of zero
        // would otherwise be indistinguishable from a V1 default field.
        if (trustedWireVersion != 1 || record == null || metadataVersion == null
            || trustedCommittedBatch == null || !negotiatedExperimentalFeature
            || !metadataVersion.isAtLeast(MetadataVersion.IBP_4_4_IV0)) {
            return false;
        }
        try {
            WalReplicaDurabilityCertificateRecordPrecheck.requireWellFormedUntrustedRecord(record);
        } catch (IllegalArgumentException ex) {
            return false;
        }
        if (trustedPreviousCertificateOffset < -1
            || record.expectedPreviousKRaftOffset() != trustedPreviousCertificateOffset
            || trustedPreviousCertificateOffset >= trustedRecordOffset) {
            return false;
        }
        // V0's self-claimed KRaftOffset is absent from the V1 wire image.
        // Fail closed for in-memory objects carrying an inconsistent V0 claim.
        if (record.kRaftOffset() != 0) {
            return false;
        }
        return trustedRecordOffset >= trustedCommittedBatch.baseOffset()
            && trustedRecordOffset <= trustedCommittedBatch.lastOffset()
            && trustedRecordOffset < trustedCommittedBatch.committedNextOffset();
    }
}
