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
package org.apache.kafka.common.replica;

import org.apache.kafka.common.Uuid;

import java.util.Objects;

/**
 * Transport-neutral shapes for a future broker-to-controller Source/WAL
 * diagnostic probe. They are not Kafka protocol APIs or authenticated claims.
 *
 * <p>Neither a caller-supplied nonce nor a complete local indexed WAL window
 * proves broker provenance, replicated durability or COMMITTED GC eligibility.
 * Never use these immutable, freely constructible records as write permits.</p>
 */
public final class PartitionRetirementSourceProbe {
    private PartitionRetirementSourceProbe() {
    }

    /** Exact Controller-issued probe coordinates that a broker must recheck. */
    public record Request(
        Uuid nonce,
        String clusterId,
        Uuid topicId,
        int partitionId,
        int brokerId,
        long brokerEpoch,
        Uuid brokerIncarnationId,
        int sourceLeaderEpoch,
        long expectedPreviousAuthorityOffset,
        long nextAuthorityOffset,
        long claimedLogStartOffset
    ) {
        public Request {
            Objects.requireNonNull(nonce, "nonce");
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(topicId, "topicId");
            Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
            if (Uuid.ZERO_UUID.equals(nonce) || Uuid.ZERO_UUID.equals(topicId)
                || Uuid.ZERO_UUID.equals(brokerIncarnationId) || clusterId.isBlank()) {
                throw new IllegalArgumentException("Probe requires real nonce, cluster and broker identities");
            }
            if (partitionId < 0 || brokerId < 0 || brokerEpoch < 0L || sourceLeaderEpoch < 0) {
                throw new IllegalArgumentException("Probe requires a valid registered source leader");
            }
            if (expectedPreviousAuthorityOffset < -1L || nextAuthorityOffset <= expectedPreviousAuthorityOffset
                || nextAuthorityOffset < 0L || claimedLogStartOffset < 0L) {
                throw new IllegalArgumentException("Probe requires a valid authority CAS and log start");
            }
        }
    }

    /**
     * Unauthenticated reply. A broker may build this ONLY from a fresh native
     * Kafka source and backend-indexed WAL observation; a Controller must
     * independently validate an authenticated sender, nonce, current
     * registration and current KRaft authority before considering any write.
     */
    public record Report(
        Request request,
        long localLeadershipGeneration,
        long nativeLogStartOffset,
        long nativeHighWatermark,
        long nativeLogEndOffset,
        long observedWalMutationRevision,
        long observedWalAdmissionRevision
    ) {
        public Report {
            Objects.requireNonNull(request, "request");
            if (localLeadershipGeneration <= 0L || observedWalMutationRevision < 0L
                || observedWalAdmissionRevision < 0L) {
                throw new IllegalArgumentException("A source probe cannot report unknown WAL generations");
            }
            if (nativeLogStartOffset != request.claimedLogStartOffset()
                || nativeHighWatermark <= nativeLogStartOffset
                || nativeLogEndOffset < nativeHighWatermark) {
                throw new IllegalArgumentException("A source probe cannot report a different or empty window");
            }
        }
    }
}
