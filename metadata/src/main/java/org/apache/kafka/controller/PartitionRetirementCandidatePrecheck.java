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
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.metadata.PartitionRegistration;

import java.util.Objects;

/**
 * Controller-event-queue preflight for an eventual KRaft retirement CAS writer.
 *
 * <p>The checks use current authoritative KRaft broker registration, partition
 * leadership and replayed retirement generation. This class is deliberately
 * NON-EMITTING: a matching result explicitly says source LogStart evidence is
 * UNVERIFIED. There is no Kafka metadata producer, external API, or deletion grant.
 * A future writer must atomically recheck every condition inside its controller
 * event and authenticate the source-log-start before appending any metadata.</p>
 */
final class PartitionRetirementCandidatePrecheck {
    private PartitionRetirementCandidatePrecheck() {
    }

    record Candidate(
        Key key,
        int brokerId,
        long brokerEpoch,
        Uuid brokerIncarnationId,
        int sourceLeaderEpoch,
        long expectedPreviousAuthorityOffset,
        long nextAuthorityOffset,
        long claimedLogStartOffset
    ) {
        Candidate {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
            if (brokerId < 0 || brokerEpoch < 0 || Uuid.ZERO_UUID.equals(brokerIncarnationId)) {
                throw new IllegalArgumentException("A candidate must bind a registered broker incarnation");
            }
            if (sourceLeaderEpoch < 0 || expectedPreviousAuthorityOffset < -1L
                || nextAuthorityOffset < 0L || claimedLogStartOffset < 0L) {
                throw new IllegalArgumentException("Invalid authority CAS or claimed log-start coordinates");
            }
        }

        Value proposedValue() {
            return new Value(
                nextAuthorityOffset, sourceLeaderEpoch, brokerId, claimedLogStartOffset,
                false, brokerEpoch, brokerIncarnationId
            );
        }
    }

    enum Finding {
        FEATURE_NOT_NEGOTIATED,
        WRONG_KRAFT_WRITE_POSITION,
        STALE_AUTHORITY_VERSION,
        TOPIC_TERMINALLY_DELETED,
        LEADER_REGISTRATION_MISMATCH,
        BROKER_NOT_REGISTERED,
        BROKER_FENCED,
        BROKER_SHUTTING_DOWN,
        BROKER_EPOCH_MISMATCH,
        BROKER_INCARNATION_MISMATCH,
        REJECTED_AUTHORITY_TRANSITION,
        SOURCE_LOG_START_NOT_VERIFIED
    }

    static Finding assess(
        FeatureControlManager featureControl,
        ClusterControlManager clusterControl,
        ReplicationControlManager replicationControl,
        PartitionRetirementControlManager authorityControl,
        Candidate candidate,
        long nextWriteOffset
    ) {
        Objects.requireNonNull(featureControl, "featureControl");
        Objects.requireNonNull(clusterControl, "clusterControl");
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(authorityControl, "authorityControl");
        Objects.requireNonNull(candidate, "candidate");
        if (!featureControl.isPartitionRetirementAuthorityEnabled()) {
            return Finding.FEATURE_NOT_NEGOTIATED;
        }
        // One controller-timeline snapshot is shared by both the generation CAS and
        // subsequent leader/epoch/watermark transition validation. Do not look the
        // state up again after validating the expected authority offset.
        Value previous = authorityControl.get(candidate.key());
        Finding check = checkCAS(previous, candidate, nextWriteOffset);
        if (check != null) {
            return check;
        }
        check = checkPartition(replicationControl, candidate);
        if (check != null) {
            return check;
        }
        check = checkBroker(clusterControl.registration(candidate.brokerId()), candidate);
        if (check != null) {
            return check;
        }
        try {
            PartitionRetirementAuthorityState.validateAdvance(previous, candidate.proposedValue());
        } catch (IllegalStateException rejected) {
            return Finding.REJECTED_AUTHORITY_TRANSITION;
        }
        // No trusted source-log-start adapter exists yet. This is never an
        // authorization result even when every current controller claim matches.
        return Finding.SOURCE_LOG_START_NOT_VERIFIED;
    }

    private static Finding checkCAS(Value previous, Candidate candidate, long nextWriteOffset) {
        if (nextWriteOffset < 0L || candidate.nextAuthorityOffset() != nextWriteOffset) {
            return Finding.WRONG_KRAFT_WRITE_POSITION;
        }
        long current = previous == null ? -1L : previous.authorityOffset();
        if (candidate.expectedPreviousAuthorityOffset() != current) {
            return Finding.STALE_AUTHORITY_VERSION;
        }
        if (previous != null && previous.terminallyDeleted()) {
            return Finding.TOPIC_TERMINALLY_DELETED;
        }
        return null;
    }

    private static Finding checkPartition(
        ReplicationControlManager replicationControl,
        Candidate candidate
    ) {
        PartitionRegistration registration = replicationControl.getPartition(
            candidate.key().topicId(), candidate.key().partitionId()
        );
        return PartitionRetirementControllerPrecheck.assessRegistration(
            registration, candidate.brokerId(), candidate.sourceLeaderEpoch()
        ) == PartitionRetirementControllerPrecheck.Finding.CONTROLLER_IMAGE_MATCH
            ? null : Finding.LEADER_REGISTRATION_MISMATCH;
    }

    private static Finding checkBroker(BrokerRegistration registration, Candidate candidate) {
        if (registration == null) {
            return Finding.BROKER_NOT_REGISTERED;
        }
        if (registration.fenced()) {
            return Finding.BROKER_FENCED;
        }
        if (registration.inControlledShutdown()) {
            return Finding.BROKER_SHUTTING_DOWN;
        }
        if (registration.epoch() != candidate.brokerEpoch()) {
            return Finding.BROKER_EPOCH_MISMATCH;
        }
        // Old/malformed broker registrations can have no incarnation UUID. A
        // missing registration proof is a deterministic fence, not an NPE.
        if (!candidate.brokerIncarnationId().equals(registration.incarnationId())) {
            return Finding.BROKER_INCARNATION_MISMATCH;
        }
        return null;
    }
}
