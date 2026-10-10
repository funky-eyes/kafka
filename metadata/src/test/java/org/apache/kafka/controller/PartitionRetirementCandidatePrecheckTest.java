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
    void missingBrokerRegistrationFailsClosed() {
        Fixture f = fixture();
        when(f.cluster.registration(1)).thenReturn(null);
        assertEquals(Finding.BROKER_NOT_REGISTERED, evaluate(f, candidate()));
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
