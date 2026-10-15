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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Candidate;
import org.apache.kafka.controller.PartitionRetirementSourceProbeRegistry.Challenge;
import org.apache.kafka.controller.PartitionRetirementSourceProbeRegistry.Finding;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementSourceProbeRegistryTest {
    private static final KafkaPrincipal BROKER_THREE = broker("broker-three");
    private static final KafkaPrincipal BROKER_FOUR = broker("broker-four");
    private static final PartitionRetirementBrokerPrincipalPolicy POLICY =
        new PartitionRetirementBrokerPrincipalPolicy(Map.of(3, BROKER_THREE, 4, BROKER_FOUR));
    private static final Candidate CANDIDATE = candidate(3, 54L, new Uuid(10L, 11L), 8, 20L);

    @Test
    void correctBrokerGetsUniqueNoncesButNoSourceAuthority() {
        var registry = registry();
        Challenge first = issue(registry, 100L);
        Challenge second = issue(registry, 100L);
        assertNotEquals(first.nonce(), second.nonce());
        assertEquals("cluster-a", first.clusterId());
        assertEquals(CANDIDATE, first.candidate());
        assertEquals(Finding.MATCHED_UNTRUSTED, registry.consume(first, CANDIDATE, BROKER_THREE, 101L));
        assertEquals(Finding.MATCHED_UNTRUSTED, registry.consume(second, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void pendingChallengeIsReadOnlyAndRequiresExactBrokerPrincipal() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        assertEquals(issued,
            registry.pendingForHeartbeat(3, 54L, BROKER_THREE, 101L).orElseThrow());
        assertTrue(registry.pendingForHeartbeat(4, 54L, BROKER_THREE, 101L).isEmpty());
        assertTrue(registry.pendingForHeartbeat(3, 54L, BROKER_FOUR, 101L).isEmpty());
        assertTrue(registry.pendingForHeartbeat(3, 54L, KafkaPrincipal.ANONYMOUS, 101L).isEmpty());
        assertTrue(registry.pendingForHeartbeat(3, 54L, null, 101L).isEmpty());
        assertEquals(Finding.MATCHED_UNTRUSTED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void heartbeatOfferMustRejectOldBrokerEpochAndExpiredNonce() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        assertTrue(registry.pendingForHeartbeat(3, 55L, BROKER_THREE, 101L).isEmpty());
        assertTrue(registry.pendingForHeartbeat(3, -1L, BROKER_THREE, 101L).isEmpty());
        assertTrue(registry.pendingForHeartbeat(-1, 54L, BROKER_THREE, 101L).isEmpty());
        long expiry = 100L + PartitionRetirementSourceProbeRegistry.MAX_AGE_NS;
        assertTrue(registry.pendingForHeartbeat(3, 54L, BROKER_THREE, expiry).isEmpty());
        assertEquals(Finding.EXPIRED, registry.consume(issued, CANDIDATE, BROKER_THREE, expiry));
    }

    @Test
    void consumedProbeCannotBeReofferedOnLaterHeartbeat() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        assertEquals(Finding.MATCHED_UNTRUSTED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 101L));
        assertTrue(registry.pendingForHeartbeat(3, 54L, BROKER_THREE, 102L).isEmpty());
    }

    @Test
    void controllerLeadershipChangeRevokesHeartbeatOffers() {
        var registry = registry();
        issue(registry, 100L);
        assertTrue(registry.pendingForHeartbeat(3, 54L, BROKER_THREE, 101L).isPresent());
        registry.invalidateAll();
        assertTrue(registry.pendingForHeartbeat(3, 54L, BROKER_THREE, 102L).isEmpty());
    }

    @Test
    void replayedNonceCannotBeConsumedTwice() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        assertEquals(Finding.MATCHED_UNTRUSTED, registry.consume(issued, CANDIDATE, BROKER_THREE, 101L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED, registry.consume(issued, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void expiredProbeFailsAtExactMaximumAge() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        long expiredAt = 100L + PartitionRetirementSourceProbeRegistry.MAX_AGE_NS;
        assertEquals(Finding.EXPIRED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, expiredAt));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 101L));
    }

    @Test
    void consumedNonceFromWrongPrincipalCannotBeRecoveredByLegitimateBroker() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        assertEquals(Finding.PRINCIPAL_MISMATCH,
            registry.consume(issued, CANDIDATE, BROKER_FOUR, 101L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void brokerReregistrationOrLeaderEpochChangeInvalidatesPendingProbe() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        Candidate newIncarnation = candidate(3, 55L, new Uuid(12L, 13L), 8, 20L);
        assertEquals(Finding.CANDIDATE_CHANGED,
            registry.consume(issued, newIncarnation, BROKER_THREE, 101L));
        Challenge second = issue(registry, 102L);
        Candidate newLeader = candidate(3, 54L, new Uuid(10L, 11L), 9, 20L);
        assertEquals(Finding.CANDIDATE_CHANGED,
            registry.consume(second, newLeader, BROKER_THREE, 103L));
    }

    @Test
    void changedKRaftWritePositionInvalidatesOldProbe() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        Candidate newOffset = candidate(3, 54L, new Uuid(10L, 11L), 8, 21L);
        assertEquals(Finding.CANDIDATE_CHANGED,
            registry.consume(issued, newOffset, BROKER_THREE, 101L));
    }

    @Test
    void forgedClusterOrCandidateInEnvelopeBurnsTheNonce() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        Challenge forged = new Challenge(issued.nonce(), "cluster-b", issued.candidate());
        assertEquals(Finding.ENVELOPE_MISMATCH,
            registry.consume(forged, CANDIDATE, BROKER_THREE, 101L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void missingNonceAndUnknownNonceBothFailClosed() {
        var registry = registry();
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(null, CANDIDATE, BROKER_THREE, 100L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(new Challenge(Uuid.randomUuid(), "cluster-a", CANDIDATE),
                CANDIDATE, BROKER_THREE, 100L));
    }

    @Test
    void anonymousOrWrongBrokerPrincipalCannotIssueAProbe() {
        var registry = registry();
        assertTrue(registry.issue(CANDIDATE, KafkaPrincipal.ANONYMOUS, POLICY, 100L).isEmpty());
        assertTrue(registry.issue(CANDIDATE, BROKER_FOUR, POLICY, 100L).isEmpty());
        assertTrue(registry.issue(CANDIDATE, null, POLICY, 100L).isEmpty());
    }

    @Test
    void noConfiguredBrokerPrincipalMeansNoChallengeCanBeIssued() {
        var registry = registry();
        assertTrue(registry.issue(CANDIDATE, BROKER_THREE,
            PartitionRetirementBrokerPrincipalPolicy.DENY_ALL, 100L).isEmpty());
    }

    @Test
    void pendingCapacityIsBoundedAndExpiredEntriesCanBePruned() {
        var registry = registry();
        for (int i = 0; i < PartitionRetirementSourceProbeRegistry.MAX_PENDING; i++) {
            assertTrue(registry.issue(CANDIDATE, BROKER_THREE, POLICY, 100L).isPresent());
        }
        assertFalse(registry.issue(CANDIDATE, BROKER_THREE, POLICY, 100L).isPresent());
        assertTrue(registry.issue(CANDIDATE, BROKER_THREE, POLICY,
            100L + PartitionRetirementSourceProbeRegistry.MAX_AGE_NS).isPresent());
    }

    @Test
    void controllerEpochChangeRevokesAllOutstandingNonces() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        registry.invalidateAll();
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 101L));
    }

    @Test
    void restartedControllerHasNoKnowledgeOfFormerProcessNonce() {
        Challenge issued = issue(registry(), 100L);
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry().consume(issued, CANDIDATE, BROKER_THREE, 101L));
    }

    @Test
    void monotonicNanotimeMayBeNegativeOrWrapAcrossSignedLongBoundary() {
        var registry = registry();
        long createdAt = Long.MAX_VALUE - 1L;
        Challenge issued = issue(registry, createdAt);
        assertEquals(Finding.MATCHED_UNTRUSTED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, Long.MIN_VALUE + 5L));
    }

    @Test
    void backwardsNanotimeCannotMakeAnExpiredProbeFresh() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        assertEquals(Finding.EXPIRED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 99L));
    }

    @Test
    void malformedChallengeIdentifiersAndMissingCandidatesFailConstruction() {
        assertThrows(IllegalArgumentException.class,
            () -> new Challenge(Uuid.ZERO_UUID, "cluster-a", CANDIDATE));
        assertThrows(IllegalArgumentException.class,
            () -> new Challenge(Uuid.randomUuid(), "", CANDIDATE));
        assertThrows(NullPointerException.class,
            () -> new Challenge(Uuid.randomUuid(), "cluster-a", null));
    }

    @Test
    void challengeMapsToSharedProtocolRequestWithoutLosingCasCoordinates() {
        Challenge original = issue(registry(), 100L);
        var request = original.toProtocolRequest();
        assertEquals(original.nonce(), request.nonce());
        assertEquals("cluster-a", request.clusterId());
        assertEquals(CANDIDATE.key().topicId(), request.topicId());
        assertEquals(CANDIDATE.key().partitionId(), request.partitionId());
        assertEquals(CANDIDATE.brokerEpoch(), request.brokerEpoch());
        assertEquals(CANDIDATE.brokerIncarnationId(), request.brokerIncarnationId());
        assertEquals(CANDIDATE.sourceLeaderEpoch(), request.sourceLeaderEpoch());
        assertEquals(CANDIDATE.expectedPreviousAuthorityOffset(),
            request.expectedPreviousAuthorityOffset());
        assertEquals(CANDIDATE.nextAuthorityOffset(), request.nextAuthorityOffset());
        assertEquals(original, Challenge.fromProtocolRequest(request));
    }

    @Test
    void issuedChallengeRoundTripsThroughHeartbeatVersionThreeButNeverAuthorizesGc() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        var response = new BrokerHeartbeatResponseData()
            .setSourceWalProbeChallenge(issued.toHeartbeatChallenge());
        var parsed = new BrokerHeartbeatResponseData(
            MessageUtil.toByteBufferAccessor(response, (short) 3), (short) 3
        );
        Challenge reconstructed = Challenge.fromHeartbeatChallenge(parsed.sourceWalProbeChallenge());
        assertEquals(issued, reconstructed);
        assertEquals(Finding.MATCHED_UNTRUSTED,
            registry.consume(reconstructed, CANDIDATE, BROKER_THREE, 101L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(reconstructed, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void malformedHeartbeatChallengeCannotBecomeTypedSourceProbe() {
        Challenge issued = issue(registry(), 100L);
        var noNonce = issued.toHeartbeatChallenge().setNonce(null);
        assertThrows(IllegalArgumentException.class, () -> Challenge.fromHeartbeatChallenge(noNonce));
        var noTopic = issued.toHeartbeatChallenge().setTopicId(null);
        assertThrows(IllegalArgumentException.class, () -> Challenge.fromHeartbeatChallenge(noTopic));
        var badEpoch = issued.toHeartbeatChallenge().setBrokerEpoch(-1L);
        assertThrows(IllegalArgumentException.class, () -> Challenge.fromHeartbeatChallenge(badEpoch));
        var badCas = issued.toHeartbeatChallenge().setNextAuthorityOffset(-1L);
        assertThrows(IllegalArgumentException.class, () -> Challenge.fromHeartbeatChallenge(badCas));
    }

    @Test
    void forgedWireFieldBurnsNonceAndPreventsLaterValidConsumption() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        var forged = issued.toHeartbeatChallenge().setBrokerIncarnationId(new Uuid(99L, 100L));
        Challenge reconstructed = Challenge.fromHeartbeatChallenge(forged);
        assertEquals(Finding.ENVELOPE_MISMATCH,
            registry.consume(reconstructed, CANDIDATE, BROKER_THREE, 101L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED,
            registry.consume(issued, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void unstableChallengeNeverDowngradesIntoStableHeartbeatReply() {
        Challenge issued = issue(registry(), 100L);
        var response = new BrokerHeartbeatResponseData()
            .setSourceWalProbeChallenge(issued.toHeartbeatChallenge());
        assertThrows(UnsupportedVersionException.class,
            () -> MessageUtil.toByteBufferAccessor(response, (short) 2));
    }

    @Test
    void sourceProbeWithRecreatedTopicIdCannotMatchOriginalChallenge() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        var request = issued.toProtocolRequest();
        var foreign = new org.apache.kafka.common.replica.PartitionRetirementSourceProbe.Request(
            request.nonce(), request.clusterId(), new Uuid(7L, 8L), request.partitionId(),
            request.brokerId(), request.brokerEpoch(), request.brokerIncarnationId(),
            request.sourceLeaderEpoch(), request.expectedPreviousAuthorityOffset(),
            request.nextAuthorityOffset(), request.claimedLogStartOffset()
        );
        assertEquals(Finding.ENVELOPE_MISMATCH, registry.consume(
            Challenge.fromProtocolRequest(foreign), CANDIDATE, BROKER_THREE, 101L));
        assertEquals(Finding.UNKNOWN_OR_ALREADY_USED, registry.consume(
            issued, CANDIDATE, BROKER_THREE, 102L));
    }

    @Test
    void claimedAuthorityGenerationCannotBeChangedAcrossWireConversion() {
        var registry = registry();
        Challenge issued = issue(registry, 100L);
        var request = issued.toProtocolRequest();
        var forged = new org.apache.kafka.common.replica.PartitionRetirementSourceProbe.Request(
            request.nonce(), request.clusterId(), request.topicId(), request.partitionId(),
            request.brokerId(), request.brokerEpoch(), request.brokerIncarnationId(),
            request.sourceLeaderEpoch(), request.expectedPreviousAuthorityOffset(),
            request.nextAuthorityOffset() + 1L, request.claimedLogStartOffset()
        );
        assertEquals(Finding.ENVELOPE_MISMATCH, registry.consume(
            Challenge.fromProtocolRequest(forged), CANDIDATE, BROKER_THREE, 101L));
    }

    private static PartitionRetirementSourceProbeRegistry registry() {
        return new PartitionRetirementSourceProbeRegistry("cluster-a");
    }

    private static Challenge issue(PartitionRetirementSourceProbeRegistry registry, long nowNs) {
        return registry.issue(CANDIDATE, BROKER_THREE, POLICY, nowNs).orElseThrow();
    }

    private static Candidate candidate(int brokerId, long brokerEpoch, Uuid incarnation, int leaderEpoch,
                                        long writePosition) {
        return new Candidate(new Key(new Uuid(1L, 2L), 0), brokerId, brokerEpoch, incarnation,
            leaderEpoch, -1L, writePosition, 0L);
    }

    private static KafkaPrincipal broker(String name) {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name);
    }
}
