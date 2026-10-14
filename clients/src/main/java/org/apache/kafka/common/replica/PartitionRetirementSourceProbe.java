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
 * Java 11 compatible transport-neutral Source/WAL diagnostic shapes.
 * No class here authenticates a Broker, signs a claim, authorizes KRaft
 * writes, or grants permission to retire a COMMITTED object.
 */
public final class PartitionRetirementSourceProbe {
    private PartitionRetirementSourceProbe() {
    }

    public static final class Request {
        private final Uuid nonce;
        private final String clusterId;
        private final Uuid topicId;
        private final int partitionId;
        private final int brokerId;
        private final long brokerEpoch;
        private final Uuid brokerIncarnationId;
        private final int sourceLeaderEpoch;
        private final long expectedPreviousAuthorityOffset;
        private final long nextAuthorityOffset;
        private final long claimedLogStartOffset;

        public Request(
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
            this.nonce = nonce;
            this.clusterId = clusterId;
            this.topicId = topicId;
            this.partitionId = partitionId;
            this.brokerId = brokerId;
            this.brokerEpoch = brokerEpoch;
            this.brokerIncarnationId = brokerIncarnationId;
            this.sourceLeaderEpoch = sourceLeaderEpoch;
            this.expectedPreviousAuthorityOffset = expectedPreviousAuthorityOffset;
            this.nextAuthorityOffset = nextAuthorityOffset;
            this.claimedLogStartOffset = claimedLogStartOffset;
        }

        public Uuid nonce() {
            return nonce;
        }

        public String clusterId() {
            return clusterId;
        }

        public Uuid topicId() {
            return topicId;
        }

        public int partitionId() {
            return partitionId;
        }

        public int brokerId() {
            return brokerId;
        }

        public long brokerEpoch() {
            return brokerEpoch;
        }

        public Uuid brokerIncarnationId() {
            return brokerIncarnationId;
        }

        public int sourceLeaderEpoch() {
            return sourceLeaderEpoch;
        }

        public long expectedPreviousAuthorityOffset() {
            return expectedPreviousAuthorityOffset;
        }

        public long nextAuthorityOffset() {
            return nextAuthorityOffset;
        }

        public long claimedLogStartOffset() {
            return claimedLogStartOffset;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                if (!(nonce.equals(that.nonce)
                && clusterId.equals(that.clusterId)
                && topicId.equals(that.topicId))) {
                return false;
            }
            if (!(partitionId == that.partitionId
                && brokerId == that.brokerId
                && brokerEpoch == that.brokerEpoch)) {
                return false;
            }
            if (!(brokerIncarnationId.equals(that.brokerIncarnationId)
                && sourceLeaderEpoch == that.sourceLeaderEpoch
                && expectedPreviousAuthorityOffset == that.expectedPreviousAuthorityOffset)) {
                return false;
            }
            if (!(nextAuthorityOffset == that.nextAuthorityOffset
                && claimedLogStartOffset == that.claimedLogStartOffset)) {
                return false;
            }
            return true;
            }
            if (!(other instanceof Request)) {
                return false;
            }
            Request that = (Request) other;
            return nonce.equals(that.nonce)
                && clusterId.equals(that.clusterId)
                && topicId.equals(that.topicId)
                && partitionId == that.partitionId
                && brokerId == that.brokerId
                && brokerEpoch == that.brokerEpoch
                && brokerIncarnationId.equals(that.brokerIncarnationId)
                && sourceLeaderEpoch == that.sourceLeaderEpoch
                && expectedPreviousAuthorityOffset == that.expectedPreviousAuthorityOffset
                && nextAuthorityOffset == that.nextAuthorityOffset
                && claimedLogStartOffset == that.claimedLogStartOffset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                nonce,
                clusterId,
                topicId,
                partitionId,
                brokerId,
                brokerEpoch,
                brokerIncarnationId,
                sourceLeaderEpoch,
                expectedPreviousAuthorityOffset,
                nextAuthorityOffset,
                claimedLogStartOffset
            );
        }
    }

    public static final class Report {
        private final Request request;
        private final long localLeadershipGeneration;
        private final long nativeLogStartOffset;
        private final long nativeHighWatermark;
        private final long nativeLogEndOffset;
        private final long observedWalMutationRevision;
        private final long observedWalAdmissionRevision;

        public Report(
            Request request,
            long localLeadershipGeneration,
            long nativeLogStartOffset,
            long nativeHighWatermark,
            long nativeLogEndOffset,
            long observedWalMutationRevision,
            long observedWalAdmissionRevision
        ) {
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
            this.request = request;
            this.localLeadershipGeneration = localLeadershipGeneration;
            this.nativeLogStartOffset = nativeLogStartOffset;
            this.nativeHighWatermark = nativeHighWatermark;
            this.nativeLogEndOffset = nativeLogEndOffset;
            this.observedWalMutationRevision = observedWalMutationRevision;
            this.observedWalAdmissionRevision = observedWalAdmissionRevision;
        }

        public Request request() {
            return request;
        }

        public long localLeadershipGeneration() {
            return localLeadershipGeneration;
        }

        public long nativeLogStartOffset() {
            return nativeLogStartOffset;
        }

        public long nativeHighWatermark() {
            return nativeHighWatermark;
        }

        public long nativeLogEndOffset() {
            return nativeLogEndOffset;
        }

        public long observedWalMutationRevision() {
            return observedWalMutationRevision;
        }

        public long observedWalAdmissionRevision() {
            return observedWalAdmissionRevision;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                if (!(request.equals(that.request)
                && localLeadershipGeneration == that.localLeadershipGeneration
                && nativeLogStartOffset == that.nativeLogStartOffset)) {
                return false;
            }
            if (!(nativeHighWatermark == that.nativeHighWatermark
                && nativeLogEndOffset == that.nativeLogEndOffset
                && observedWalMutationRevision == that.observedWalMutationRevision)) {
                return false;
            }
            if (!(observedWalAdmissionRevision == that.observedWalAdmissionRevision)) {
                return false;
            }
            return true;
            }
            if (!(other instanceof Report)) {
                return false;
            }
            Report that = (Report) other;
            return request.equals(that.request)
                && localLeadershipGeneration == that.localLeadershipGeneration
                && nativeLogStartOffset == that.nativeLogStartOffset
                && nativeHighWatermark == that.nativeHighWatermark
                && nativeLogEndOffset == that.nativeLogEndOffset
                && observedWalMutationRevision == that.observedWalMutationRevision
                && observedWalAdmissionRevision == that.observedWalAdmissionRevision;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                request,
                localLeadershipGeneration,
                nativeLogStartOffset,
                nativeHighWatermark,
                nativeLogEndOffset,
                observedWalMutationRevision,
                observedWalAdmissionRevision
            );
        }
    }
}
