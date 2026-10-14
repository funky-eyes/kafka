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
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.BrokerLifecycleManager.BrokerHeartbeatSourceProbeChallengeGate;
import org.apache.kafka.server.BrokerLifecycleManager.BrokerHeartbeatSourceProbeChallengeGate.Finding;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerHeartbeatSourceProbeChallengeGateTest {
    private static final Uuid INCARNATION = new Uuid(5L, 6L);

    @Test
    void missingChallengeCannotChangeNormalBrokerHeartbeat() {
        assertEquals(Finding.ABSENT, inspect(new BrokerHeartbeatResponseData()));
    }

    @Test
    void controllerMustNegotiateExplicitUnstableVersionThree() {
        assertEquals(Finding.VERSION_NOT_NEGOTIATED, inspect(challenge(), (short) 2,
            "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, true));
        assertEquals(Finding.VERSION_NOT_NEGOTIATED, inspect(challenge(), (short) 4,
            "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, true));
    }

    @Test
    void unregisteredOrFencedBrokerCannotAcceptChallenge() {
        assertEquals(Finding.BROKER_NOT_RUNNING, inspect(challenge(), (short) 3,
            "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, false));
        assertEquals(Finding.BROKER_NOT_RUNNING, inspect(challenge(), (short) 3,
            "cluster-a", 3, 54L, INCARNATION, BrokerState.RECOVERY, true));
        assertEquals(Finding.BROKER_NOT_RUNNING, inspect(challenge(), (short) 3,
            "cluster-a", 3, -1L, INCARNATION, BrokerState.RUNNING, true));
    }

    @Test
    void wrongClusterOrBrokerIdCannotBorrowAnotherBrokerChallenge() {
        assertEquals(Finding.IDENTITY_MISMATCH, inspect(challenge(), (short) 3,
            "cluster-b", 3, 54L, INCARNATION, BrokerState.RUNNING, true));
        assertEquals(Finding.IDENTITY_MISMATCH, inspect(challenge(), (short) 3,
            "cluster-a", 4, 54L, INCARNATION, BrokerState.RUNNING, true));
    }

    @Test
    void brokerEpochOrIncarnationAbaCannotResurrectOldChallenge() {
        assertEquals(Finding.IDENTITY_MISMATCH, inspect(challenge(), (short) 3,
            "cluster-a", 3, 55L, INCARNATION, BrokerState.RUNNING, true));
        assertEquals(Finding.IDENTITY_MISMATCH, inspect(challenge(), (short) 3,
            "cluster-a", 3, 54L, new Uuid(5L, 7L), BrokerState.RUNNING, true));
    }

    @Test
    void invalidNonceOrRecreatedTopicFailsBeforeDiagnosticDecode() {
        BrokerHeartbeatResponseData nonce = challenge();
        nonce.sourceWalProbeChallenge().setNonce(Uuid.ZERO_UUID);
        assertEquals(Finding.MALFORMED, inspect(nonce));
        BrokerHeartbeatResponseData topic = challenge();
        topic.sourceWalProbeChallenge().setTopicId(Uuid.ZERO_UUID);
        assertEquals(Finding.MALFORMED, inspect(topic));
    }

    @Test
    void invalidAuthorityCasOrSourceLogStartIsMalformed() {
        BrokerHeartbeatResponseData cas = challenge();
        cas.sourceWalProbeChallenge().setNextAuthorityOffset(-1L);
        assertEquals(Finding.MALFORMED, inspect(cas));
        BrokerHeartbeatResponseData start = challenge();
        start.sourceWalProbeChallenge().setClaimedLogStartOffset(-1L);
        assertEquals(Finding.MALFORMED, inspect(start));
    }

    @Test
    void missingClusterAndRequiredUuidAreMalformedOrMismatched() {
        BrokerHeartbeatResponseData noCluster = challenge();
        noCluster.sourceWalProbeChallenge().setClusterId(null);
        assertEquals(Finding.IDENTITY_MISMATCH, inspect(noCluster));
        BrokerHeartbeatResponseData noTopic = challenge();
        noTopic.sourceWalProbeChallenge().setTopicId(null);
        assertEquals(Finding.MALFORMED, inspect(noTopic));
    }

    @Test
    void validChallengeIsStillUntrustedAndNeverSelfAuthorizesGc() {
        assertEquals(Finding.MATCHED_UNTRUSTED, inspect(challenge()));
        var diagnostic = BrokerHeartbeatSourceProbeChallengeGate.diagnosticRequest(
            challenge(), (short) 3, "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, true
        ).orElseThrow();
        assertEquals(new Uuid(1L, 2L), diagnostic.nonce());
        assertEquals(new Uuid(3L, 4L), diagnostic.topicId());
        assertEquals(8, diagnostic.sourceLeaderEpoch());
        assertEquals(100L, diagnostic.nextAuthorityOffset());
        assertEquals(0L, diagnostic.claimedLogStartOffset());
    }

    @Test
    void rejectedChallengeNeverYieldsLocalSamplingRequest() {
        assertTrue(BrokerHeartbeatSourceProbeChallengeGate.diagnosticRequest(
            challenge(), (short) 2, "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, true
        ).isEmpty());
        assertFalse(BrokerHeartbeatSourceProbeChallengeGate.diagnosticRequest(
            challenge(), (short) 3, "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, true
        ).isEmpty());
    }

    private static Finding inspect(BrokerHeartbeatResponseData response) {
        return inspect(response, (short) 3, "cluster-a", 3, 54L, INCARNATION, BrokerState.RUNNING, true);
    }

    private static Finding inspect(
        BrokerHeartbeatResponseData response,
        short apiVersion,
        String clusterId,
        int brokerId,
        long brokerEpoch,
        Uuid incarnation,
        BrokerState state,
        boolean registered
    ) {
        return BrokerHeartbeatSourceProbeChallengeGate.inspect(
            response, apiVersion, clusterId, brokerId, brokerEpoch, incarnation, state, registered
        );
    }

    private static BrokerHeartbeatResponseData challenge() {
        return new BrokerHeartbeatResponseData()
            .setSourceWalProbeChallenge(new SourceWalProbeChallenge()
                .setNonce(new Uuid(1L, 2L))
                .setClusterId("cluster-a")
                .setTopicId(new Uuid(3L, 4L))
                .setPartitionId(0)
                .setBrokerId(3)
                .setBrokerEpoch(54L)
                .setBrokerIncarnationId(INCARNATION)
                .setSourceLeaderEpoch(8)
                .setExpectedPreviousAuthorityOffset(-1L)
                .setNextAuthorityOffset(100L)
                .setClaimedLogStartOffset(0L));
    }
}
