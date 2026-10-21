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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Candidate;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Finding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Controller state and broker-registration witnesses, with no authority record emitter. */
class PartitionRetirementCandidatePrecheckTest {
    private static final Key KEY = new Key(Uuid.randomUuid(), 0);
    private static final Uuid INCARNATION = Uuid.randomUuid();

    @Test
    void matchingKRaftBrokerAndLeaderStillCannotAttestSourceLogStart() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_LOG_START_NOT_VERIFIED, evaluate(f, candidate()));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void disabledClusterFeatureRejectsAnyCandidate() {
        Fixture f = fixture();
        when(f.features.isPartitionRetirementAuthorityEnabled()).thenReturn(false);
        assertEquals(Finding.FEATURE_NOT_NEGOTIATED, evaluate(f, candidate()));
    }

    @Test
    void speculativeOrWrongKRaftAppendPositionIsRejected() {
        Fixture f = fixture();
        assertEquals(Finding.WRONG_KRAFT_WRITE_POSITION, assess(f, candidate(), 19L));
        assertEquals(Finding.WRONG_KRAFT_WRITE_POSITION, assess(f, candidate(), -1L));
    }

    @Test
    void staleExpectedGenerationCannotOverrideTheCurrentControllerState() {
        Fixture f = fixture();
        f.authority.replay(record(prior(30L)));
        assertEquals(Finding.STALE_AUTHORITY_VERSION, evaluate(f, candidate()));
        assertEquals(10L, f.authority.get(KEY).authorityOffset());
    }

    @Test
    void terminalTopicDeleteCannotBeReopenedEvenWithNewLeaderEpoch() {
        Fixture f = fixture();
        f.authority.replay(record(new Value(10L, 7, -1, 30L, true)));
        Candidate next = request(1, 300L, INCARNATION, 8, 10L, 20L, 50L);
        assertEquals(Finding.TOPIC_TERMINALLY_DELETED, assess(f, next, 20L));
    }

    @Test
    void recreatedOrMissingTopicRegistrationFailsClosed() {
        Fixture f = fixture();
        when(f.replicas.getPartition(KEY.topicId(), 0)).thenReturn(null);
        assertEquals(Finding.LEADER_REGISTRATION_MISMATCH, evaluate(f, candidate()));
    }

    @Test
    void staleSourceLeaderEpochFailsDespiteCurrentBroker() {
        Fixture f = fixture();
        when(f.replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition(1, 8));
        assertEquals(Finding.LEADER_REGISTRATION_MISMATCH, evaluate(f, candidate()));
    }

    @Test
    void controllerCandidateCannotUseStableLeaderEpochDuringReplicaReassignment() {
        Fixture f = fixture();
        PartitionRegistration changing = partition(1, 7).merge(new PartitionChangeRecord()
            .setReplicas(List.of(1, 2, 3, 4))
            .setAddingReplicas(List.of(4)));
        // Kafka retains the same leaderEpoch for this topology change.
        assertEquals(7, changing.leaderEpoch);
        assertEquals(8, changing.partitionEpoch);
        when(f.replicas.getPartition(KEY.topicId(), 0)).thenReturn(changing);
        assertEquals(Finding.LEADER_REGISTRATION_MISMATCH, evaluate(f, candidate()));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void missingBrokerRegistrationFailsClosed() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(null);
        assertEquals(Finding.BROKER_NOT_REGISTERED, evaluate(f, candidate()));
    }

    @Test
    void registrationPayloadWithWrongBrokerIdCannotSatisfyCas() {
        Fixture f = fixture();
        BrokerRegistration wrongId = new BrokerRegistration.Builder()
            .setId(2)
            .setEpoch(300L)
            .setIncarnationId(INCARNATION)
            .setFenced(false)
            .setInControlledShutdown(false)
            .build();
        when(f.cluster.registration(1)).thenReturn(wrongId);
        assertEquals(Finding.BROKER_ID_MISMATCH, evaluate(f, candidate()));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void fencedBrokerCannotSatisfyPrecommitCas() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(broker(300L, INCARNATION, true, false));
        assertEquals(Finding.BROKER_FENCED, evaluate(f, candidate()));
    }

    @Test
    void brokerInControlledShutdownCannotSatisfyPrecommitCas() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(broker(300L, INCARNATION, false, true));
        assertEquals(Finding.BROKER_SHUTTING_DOWN, evaluate(f, candidate()));
    }

    @Test
    void restartedBrokerEpochCannotReusePriorIncarnationClaim() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(broker(301L, INCARNATION, false, false));
        assertEquals(Finding.BROKER_EPOCH_MISMATCH, evaluate(f, candidate()));
    }

    @Test
    void sameBrokerIdAndEpochWithNewIncarnationIsNotSameOwner() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(broker(300L, Uuid.randomUuid(), false, false));
        assertEquals(Finding.BROKER_INCARNATION_MISMATCH, evaluate(f, candidate()));
    }

    @Test
    void requestedLogStartCannotRegressFromReplayedControllerWatermark() {
        Fixture f = fixture();
        f.authority.replay(record(prior(60L)));
        Candidate request = request(1, 300L, INCARNATION, 7, 10L, 20L, 40L);
        assertEquals(Finding.REJECTED_AUTHORITY_TRANSITION, evaluate(f, request));
        assertEquals(60L, f.authority.get(KEY).logStartOffset());
    }

    @Test
    void aNewBrokerIncarnationCannotReuseTheSamePartitionLeaderEpoch() {
        Fixture f = fixture();
        f.authority.replay(record(prior(30L)));
        Uuid replaced = Uuid.randomUuid();
        when(f.cluster.registration(1)).thenReturn(broker(301L, replaced, false, false));
        Candidate request = request(1, 301L, replaced, 7, 10L, 20L, 50L);
        assertEquals(Finding.REJECTED_AUTHORITY_TRANSITION, evaluate(f, request));
    }

    @Test
    void freshLeaderEpochCanPassRegistrationCasButStillNeedsSourceProof() {
        Fixture f = fixture();
        f.authority.replay(record(prior(30L)));
        Uuid replaced = Uuid.randomUuid();
        when(f.cluster.registration(1)).thenReturn(broker(301L, replaced, false, false));
        when(f.replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition(1, 8));
        Candidate request = request(1, 301L, replaced, 8, 10L, 20L, 50L);
        assertEquals(Finding.SOURCE_LOG_START_NOT_VERIFIED, evaluate(f, request));
        assertEquals(10L, f.authority.get(KEY).authorityOffset());
    }

    @Test
    void proposedKRaftPositionCannotReuseExpectedPreviousAuthorityOffset() {
        assertThrows(IllegalArgumentException.class, () ->
            request(1, 300L, INCARNATION, 7, 20L, 20L, 50L));
    }

    @Test
    void proposedKRaftPositionCannotMoveBehindExpectedPreviousAuthorityOffset() {
        assertThrows(IllegalArgumentException.class, () ->
            request(1, 300L, INCARNATION, 7, 30L, 20L, 50L));
    }

    @Test
    void zeroTopicIdCannotBeUsedAsRetirementCasIdentity() {
        assertThrows(IllegalArgumentException.class, () ->
            new Candidate(new Key(Uuid.ZERO_UUID, 0),
                1, 300L, INCARNATION, 7, -1L, 20L, 50L));
    }

    @Test
    void forgedOrPartialBrokerIdentityIsRejectedAtRequestBoundary() {
        assertThrows(IllegalArgumentException.class, () ->
            request(1, -1L, INCARNATION, 7, -1L, 20L, 50L));
        assertThrows(IllegalArgumentException.class, () ->
            request(1, 300L, Uuid.ZERO_UUID, 7, -1L, 20L, 50L));
        assertThrows(IllegalArgumentException.class, () ->
            request(1, 300L, INCARNATION, -1, -1L, 20L, 50L));
        assertThrows(IllegalArgumentException.class, () ->
            request(1, 300L, INCARNATION, 7, -2L, 20L, 50L));
    }

    @Test
    void brokerRegistrationWithoutIncarnationFailsClosedInsteadOfThrowing() {
        Fixture f = fixture();
        BrokerRegistration registration = mock(BrokerRegistration.class);
        // Mockito's default int zero is a different, valid BrokerId.
        // Exercise the missing-incarnation path with the correct broker identity.
        when(registration.id()).thenReturn(1);
        when(registration.epoch()).thenReturn(300L);
        when(registration.incarnationId()).thenReturn(null);
        when(f.cluster.registration(1)).thenReturn(registration);
        assertEquals(Finding.BROKER_INCARNATION_MISMATCH, evaluate(f, candidate()));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void brokerRegistrationWithReservedIncarnationFailsClosed() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(broker(300L, Uuid.ZERO_UUID, false, false));
        assertEquals(Finding.BROKER_INCARNATION_MISMATCH, evaluate(f, candidate()));
    }

    @Test
    void authorityChangesAfterPreflightInvalidateTheOriginalGeneration() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_LOG_START_NOT_VERIFIED, evaluate(f, candidate()));
        f.authority.replay(record(prior(30L)));
        assertEquals(Finding.STALE_AUTHORITY_VERSION, evaluate(f, candidate()));
        assertEquals(10L, f.authority.get(KEY).authorityOffset());
    }

    @Test
    void leaderElectionAfterPreflightInvalidatesTheOldCandidate() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_LOG_START_NOT_VERIFIED, evaluate(f, candidate()));
        when(f.replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition(2, 8));
        assertEquals(Finding.LEADER_REGISTRATION_MISMATCH, evaluate(f, candidate()));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void matchingSourceWindowClaimStillNeverAuthorizesKRaftRetirement() {
        Fixture f = fixture();
        Candidate request = candidate();
        assertEquals(
            Finding.SOURCE_LOG_START_NOT_VERIFIED,
            assessClaim(f, request, sourceClaim(request))
        );
        assertNull(f.authority.get(KEY));
    }

    @Test
    void missingUntrustedSourceClaimCannotPassControllerAgreementGate() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_MISSING, assessClaim(f, candidate(), null));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void incompleteLocalCoverageCannotInventDurableKafkaSourceBytes() {
        Fixture f = fixture();
        Candidate request = candidate();
        assertEquals(Finding.SOURCE_CLAIM_LOCAL_COVERAGE_INCOMPLETE,
            assessClaim(f, request, untrusted(
                request.key(), 1, 300L, INCARNATION, 7, 50L, false)));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void claimForAnotherImmutableTopicIdDoesNotMatchCurrentAuthorityKey() {
        Fixture f = fixture();
        Candidate request = candidate();
        assertEquals(Finding.SOURCE_CLAIM_IDENTITY_MISMATCH,
            assessClaim(f, request, untrusted(
                new Key(Uuid.randomUuid(), 0), 1, 300L, INCARNATION, 7, 50L, true)));
    }

    @Test
    void claimForAnotherPartitionCannotBorrowRetirementAuthority() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_IDENTITY_MISMATCH,
            assessClaim(f, candidate(), untrusted(
                new Key(KEY.topicId(), 1), 1, 300L, INCARNATION, 7, 50L, true)));
    }

    @Test
    void claimForDifferentBrokerIdCannotBorrowTheCurrentLeader() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_IDENTITY_MISMATCH,
            assessClaim(f, candidate(), untrusted(KEY, 2, 300L, INCARNATION, 7, 50L, true)));
    }

    @Test
    void claimFromOldBrokerEpochCannotBorrowCurrentRegistration() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_IDENTITY_MISMATCH,
            assessClaim(f, candidate(), untrusted(KEY, 1, 299L, INCARNATION, 7, 50L, true)));
    }

    @Test
    void claimFromDifferentBrokerIncarnationCannotPassIdentityAgreement() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_IDENTITY_MISMATCH,
            assessClaim(f, candidate(), untrusted(KEY, 1, 300L, Uuid.randomUuid(), 7, 50L, true)));
    }

    @Test
    void claimFromOldKafkaSourceLeaderEpochCannotPassIdentityAgreement() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_IDENTITY_MISMATCH,
            assessClaim(f, candidate(), untrusted(KEY, 1, 300L, INCARNATION, 6, 50L, true)));
    }

    @Test
    void nativeKafkaLogStartDisagreementCannotBeOverriddenByCoverageFlag() {
        Fixture f = fixture();
        assertEquals(Finding.SOURCE_CLAIM_LOG_START_MISMATCH,
            assessClaim(f, candidate(), untrusted(KEY, 1, 300L, INCARNATION, 7, 51L, true)));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void malformedNativeHighWatermarkOrLogEndOffsetsFailAtClaimBoundary() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, 50L, 49L, 70L, 2L, 4L, true));
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, 50L, 70L, 69L, 2L, 4L, true));
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, -1L, 70L, 80L, 2L, 4L, true));
    }

    @Test
    void emptyNativeKafkaSourceWindowCannotAdvertiseDurableDataCoverage() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, 50L, 50L, 50L, 2L, 4L, true));
        // Empty source observations are allowed only as explicitly incomplete
        // diagnostic claims; they must never become DATA coverage evidence.
        var empty = new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, 50L, 50L, 50L, 2L, 4L, false);
        assertEquals(Finding.SOURCE_CLAIM_LOCAL_COVERAGE_INCOMPLETE,
            assessClaim(fixture(), candidate(), empty));
    }

    @Test
    void unknownOrInflightWalRevisionsCannotBeTreatedAsStableProof() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, 50L, 70L, 80L, -1L, 4L, true));
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 9L, 50L, 70L, 80L, 2L, -1L, true));
    }

    @Test
    void unversionedLeaderTicketAndIncompleteBrokerIdentityCannotFormClaim() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, 7, 0L, 50L, 70L, 80L, 2L, 4L, true));
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, INCARNATION, -1, 9L, 50L, 70L, 80L, 2L, 4L, true));
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, -1L, INCARNATION, 7, 9L, 50L, 70L, 80L, 2L, 4L, true));
        assertThrows(IllegalArgumentException.class, () -> new PartitionRetirementUntrustedSourceClaim(
            KEY, 1, 300L, Uuid.ZERO_UUID, 7, 9L, 50L, 70L, 80L, 2L, 4L, true));
    }

    @Test
    void disabledFeatureRemainsDominantEvenWithMatchingPositiveLocalClaim() {
        Fixture f = fixture();
        when(f.features.isPartitionRetirementAuthorityEnabled()).thenReturn(false);
        assertEquals(Finding.FEATURE_NOT_NEGOTIATED,
            assessClaim(f, candidate(), sourceClaim(candidate())));
    }

    @Test
    void changedLiveBrokerRegistrationWinsOverCallerSuppliedClaim() {
        Fixture f = fixture();
        Candidate request = candidate();
        var copied = sourceClaim(request);
        assertEquals(Finding.SOURCE_LOG_START_NOT_VERIFIED, assessClaim(f, request, copied));
        when(f.cluster.registration(1)).thenReturn(broker(301L, Uuid.randomUuid(), false, false));
        assertEquals(Finding.BROKER_EPOCH_MISMATCH, assessClaim(f, request, copied));
        assertNull(f.authority.get(KEY));
    }

    @Test
    void changedCurrentKRaftLeaderRejectsPreviouslyMatchingSourceClaim() {
        Fixture f = fixture();
        Candidate request = candidate();
        var copied = sourceClaim(request);
        assertEquals(Finding.SOURCE_LOG_START_NOT_VERIFIED, assessClaim(f, request, copied));
        when(f.replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition(2, 8));
        assertEquals(Finding.LEADER_REGISTRATION_MISMATCH, assessClaim(f, request, copied));
    }

    @Test
    void authorityReplayChangeAndWrongKRaftPositionOverridePositiveClaim() {
        Fixture f = fixture();
        Candidate request = candidate();
        var copied = sourceClaim(request);
        assertEquals(Finding.WRONG_KRAFT_WRITE_POSITION,
            PartitionRetirementCandidatePrecheck.assessUntrustedSourceClaim(
                f.features, f.cluster, f.replicas, f.authority, request, 21L, copied
            ));
        f.authority.replay(record(prior(30L)));
        assertEquals(Finding.STALE_AUTHORITY_VERSION, assessClaim(f, request, copied));
        assertEquals(10L, f.authority.get(KEY).authorityOffset());
    }

    private static Finding assessClaim(
        Fixture f,
        Candidate request,
        PartitionRetirementUntrustedSourceClaim claim
    ) {
        return PartitionRetirementCandidatePrecheck.assessUntrustedSourceClaim(
            f.features, f.cluster, f.replicas, f.authority, request, 20L, claim
        );
    }

    private static PartitionRetirementUntrustedSourceClaim sourceClaim(Candidate request) {
        return untrusted(request.key(), request.brokerId(), request.brokerEpoch(),
            request.brokerIncarnationId(), request.sourceLeaderEpoch(),
            request.claimedLogStartOffset(), true);
    }

    private static PartitionRetirementUntrustedSourceClaim untrusted(
        Key key, int brokerId, long brokerEpoch, Uuid incarnation,
        int leaderEpoch, long logStart, boolean covered
    ) {
        return new PartitionRetirementUntrustedSourceClaim(
            key, brokerId, brokerEpoch, incarnation, leaderEpoch, 9L,
            logStart, Math.max(70L, logStart), Math.max(80L, logStart), 2L, 4L, covered
        );
    }

    private static PartitionRetirementCandidatePrecheck.Candidate candidate() {
        return request(1, 300L, INCARNATION, 7, -1L, 20L, 50L);
    }

    private static PartitionRetirementCandidatePrecheck.Candidate request(
        int brokerId, long brokerEpoch, Uuid incarnation, int leaderEpoch,
        long priorAuthorityOffset, long nextAuthorityOffset, long logStart
    ) {
        return new PartitionRetirementCandidatePrecheck.Candidate(
            KEY, brokerId, brokerEpoch, incarnation, leaderEpoch,
            priorAuthorityOffset, nextAuthorityOffset, logStart
        );
    }

    private static Finding evaluate(Fixture f, PartitionRetirementCandidatePrecheck.Candidate candidate) {
        return assess(f, candidate, 20L);
    }

    private static Finding assess(
        Fixture f, PartitionRetirementCandidatePrecheck.Candidate candidate, long nextWriteOffset
    ) {
        return PartitionRetirementCandidatePrecheck.assess(
            f.features, f.cluster, f.replicas, f.authority, candidate, nextWriteOffset
        );
    }

    private static Fixture fixture() {
        FeatureControlManager features = mock(FeatureControlManager.class);
        ClusterControlManager cluster = mock(ClusterControlManager.class);
        ReplicationControlManager replicas = mock(ReplicationControlManager.class);
        PartitionRetirementControlManager authority = new PartitionRetirementControlManager(
            new SnapshotRegistry(new LogContext())
        );
        when(features.isPartitionRetirementAuthorityEnabled()).thenReturn(true);
        when(replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition(1, 7));
        when(cluster.registration(1)).thenReturn(broker(300L, INCARNATION, false, false));
        return new Fixture(features, cluster, replicas, authority);
    }

    private static PartitionRegistration partition(int leader, int epoch) {
        return new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3})
            .setDirectories(DirectoryId.unassignedArray(3))
            .setIsr(new int[] {1, 2})
            .setLeader(leader)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(epoch)
            .setPartitionEpoch(epoch)
            .build();
    }

    private static BrokerRegistration broker(
        long brokerEpoch, Uuid incarnation, boolean fenced, boolean shuttingDown
    ) {
        return new BrokerRegistration.Builder()
            .setId(1)
            .setEpoch(brokerEpoch)
            .setIncarnationId(incarnation)
            .setFenced(fenced)
            .setInControlledShutdown(shuttingDown)
            .build();
    }

    private static Value prior(long watermark) {
        return new Value(10L, 7, 1, watermark, false, 300L, INCARNATION);
    }

    private static org.apache.kafka.common.metadata.PartitionRetirementAuthorityRecord record(Value value) {
        return PartitionRetirementAuthorityState.record(KEY, value);
    }

    private record Fixture(
        FeatureControlManager features,
        ClusterControlManager cluster,
        ReplicationControlManager replicas,
        PartitionRetirementControlManager authority
    ) {
    }
}
