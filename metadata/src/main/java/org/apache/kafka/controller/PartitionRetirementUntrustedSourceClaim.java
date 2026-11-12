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
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;

import java.util.Objects;

/**
 * Unauthenticated, broker-reported native source/WAL window to be compared
 * with the Controller's live registration and the proposed retirement CAS.
 *
 * <p>This is intentionally NOT a source log-start certificate. Any caller
 * could invent every field, including a positive local coverage flag. It is
 * a fail-closed shape/agreement check only. No network RPC, verified sender
 * identity, KRaft writer or COMMITTED physical GC is provided.</p>
 */
record PartitionRetirementUntrustedSourceClaim(
    Key key,
    int brokerId,
    long brokerEpoch,
    Uuid brokerIncarnationId,
    int sourceLeaderEpoch,
    long localLeadershipGeneration,
    long nativeLogStartOffset,
    long nativeHighWatermark,
    long nativeLogEndOffset,
    long observedWalMutationRevision,
    long observedWalAdmissionRevision,
    boolean reportsCompleteLocalWalWindow
) {
    PartitionRetirementUntrustedSourceClaim {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
        if (brokerId < 0 || brokerEpoch < 0L || Uuid.ZERO_UUID.equals(brokerIncarnationId)) {
            throw new IllegalArgumentException("Source claim requires a complete broker identity");
        }
        if (sourceLeaderEpoch < 0 || localLeadershipGeneration <= 0L) {
            throw new IllegalArgumentException("Source claim requires an epoch-aware local leader");
        }
        if (nativeLogStartOffset < 0L || nativeHighWatermark < nativeLogStartOffset
            || nativeLogEndOffset < nativeHighWatermark) {
            throw new IllegalArgumentException("Source claim has an impossible native Kafka window");
        }
        if (observedWalMutationRevision < 0L || observedWalAdmissionRevision < 0L) {
            throw new IllegalArgumentException("In-flight or unknown WAL generation is not a stable claim");
        }
        if (reportsCompleteLocalWalWindow && nativeHighWatermark == nativeLogStartOffset) {
            throw new IllegalArgumentException("An empty Kafka source window cannot prove recovered DATA");
        }
    }

    boolean matchesRetirementCandidate(PartitionRetirementCandidatePrecheck.Candidate request) {
        return key.equals(request.key())
            && brokerId == request.brokerId()
            && brokerEpoch == request.brokerEpoch()
            && brokerIncarnationId.equals(request.brokerIncarnationId())
            && sourceLeaderEpoch == request.sourceLeaderEpoch();
    }
}
