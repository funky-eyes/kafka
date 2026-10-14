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
package org.apache.kafka.server;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData.SourceWalProbeChallenge;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;
import org.apache.kafka.metadata.BrokerState;

import java.util.Objects;
import java.util.Optional;

/**
 * Read-only validation for an unstable heartbeat v3 Controller challenge.
 *
 * <p>A matched challenge is not authentication or proof of WAL persistence.
 * This gate never schedules source sampling or sends a Source/WAL report.
 * A unique broker-to-controller principal and Controller one-use nonce
 * validation must be enforced before the report path may be enabled.</p>
 */
final class BrokerHeartbeatSourceProbeChallengeGate {
    enum Finding {
        ABSENT,
        VERSION_NOT_NEGOTIATED,
        BROKER_NOT_RUNNING,
        IDENTITY_MISMATCH,
        MALFORMED,
        MATCHED_UNTRUSTED
    }

    private BrokerHeartbeatSourceProbeChallengeGate() {
    }

    static Finding inspect(
        BrokerHeartbeatResponseData response,
        short requestVersion,
        String clusterId,
        int brokerId,
        long brokerEpoch,
        Uuid incarnationId,
        BrokerState state,
        boolean registered
    ) {
        Objects.requireNonNull(response, "response");
        SourceWalProbeChallenge challenge = response.sourceWalProbeChallenge();
        if (challenge == null) {
            return Finding.ABSENT;
        }
        // Reject future versions until their challenge semantics have been reviewed.
        if (requestVersion != 3) {
            return Finding.VERSION_NOT_NEGOTIATED;
        }
        if (!registered || state != BrokerState.RUNNING || brokerEpoch < 0L) {
            return Finding.BROKER_NOT_RUNNING;
        }
        if (!sameRegistration(challenge, clusterId, brokerId, brokerEpoch, incarnationId)) {
            return Finding.IDENTITY_MISMATCH;
        }
        if (!shapeValid(challenge)) {
            return Finding.MALFORMED;
        }
        // A syntactically matched, self-contained challenge is still not
        // permission to send or accept an unauthenticated source report.
        return Finding.MATCHED_UNTRUSTED;
    }

    static Optional<PartitionRetirementSourceProbe.Request> diagnosticRequest(
        BrokerHeartbeatResponseData response,
        short requestVersion,
        String clusterId,
        int brokerId,
        long brokerEpoch,
        Uuid incarnationId,
        BrokerState state,
        boolean registered
    ) {
        Finding finding = inspect(
            response, requestVersion, clusterId, brokerId, brokerEpoch, incarnationId, state, registered
        );
        if (finding != Finding.MATCHED_UNTRUSTED) {
            return Optional.empty();
        }
        SourceWalProbeChallenge wire = response.sourceWalProbeChallenge();
        return Optional.of(new PartitionRetirementSourceProbe.Request(
            wire.nonce(), wire.clusterId(), wire.topicId(), wire.partitionId(),
            wire.brokerId(), wire.brokerEpoch(), wire.brokerIncarnationId(),
            wire.sourceLeaderEpoch(), wire.expectedPreviousAuthorityOffset(),
            wire.nextAuthorityOffset(), wire.claimedLogStartOffset()
        ));
    }

    private static boolean sameRegistration(
        SourceWalProbeChallenge challenge,
        String clusterId,
        int brokerId,
        long brokerEpoch,
        Uuid incarnationId
    ) {
        return clusterId != null && clusterId.equals(challenge.clusterId())
            && brokerId == challenge.brokerId() && brokerEpoch == challenge.brokerEpoch()
            && incarnationId != null && incarnationId.equals(challenge.brokerIncarnationId());
    }

    private static boolean shapeValid(SourceWalProbeChallenge challenge) {
        if (challenge.nonce() == null || challenge.clusterId() == null
            || challenge.topicId() == null || challenge.brokerIncarnationId() == null) {
            return false;
        }
        try {
            new PartitionRetirementSourceProbe.Request(
                challenge.nonce(), challenge.clusterId(), challenge.topicId(),
                challenge.partitionId(), challenge.brokerId(), challenge.brokerEpoch(),
                challenge.brokerIncarnationId(), challenge.sourceLeaderEpoch(),
                challenge.expectedPreviousAuthorityOffset(), challenge.nextAuthorityOffset(),
                challenge.claimedLogStartOffset()
            );
            return true;
        } catch (IllegalArgumentException invalid) {
            return false;
        }
    }
}
