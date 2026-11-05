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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.metadata.authorizer.MockAuthorizableRequestContext;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Candidate;
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Finding;
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Report;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unauthenticated, controller-volatile correlation; never a GC authority. */
class PartitionRetirementReplicaRoundRegistryTest {
    private static final Key KEY = new Key(Uuid.randomUuid(), 0);
    private static final Uuid[] INCARNATIONS = {
        Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid()
    };
    private static final Candidate CANDIDATE =
        new Candidate(KEY, 1, 101L, INCARNATIONS[0], 7, 11, -1L, 20L, 50L);
    private static final long OBJECT_ID = 901L;
    private static final long NOW_NS = 10_000_000L;
    private static final KafkaPrincipal BROKER_ONE =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unique-replica-one");
    private static final KafkaPrincipal BROKER_TWO =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unique-replica-two");
    private static final KafkaPrincipal BROKER_THREE =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unique-replica-three");
    private static final PartitionRetirementBrokerPrincipalPolicy BOUND =
        new PartitionRetirementBrokerPrincipalPolicy(
            Map.of(1, BROKER_ONE, 2, BROKER_TWO, 3, BROKER_THREE));


    @Test
    void fullAssignedReplicaRoundCanOnlyProduceOneUntrustedDiagnostic() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry registry = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge challenge =
            issue(registry, f, OBJECT_ID, NOW_NS);
        assertEquals(3, challenge.assignedReplicas().size(),
            "The third assigned replica is outside ISR and must still report");
        assertTrue(registry.recordReport(challenge.roundId(), report(challenge, 1), NOW_NS));
        assertTrue(registry.recordReport(challenge.roundId(), report(challenge, 2), NOW_NS));
        assertTrue(registry.recordReport(challenge.roundId(), report(challenge, 3), NOW_NS));
        assertEquals(Optional.of(Finding.MATCHED_UNTRUSTED),
            registry.consume(challenge.roundId(), f.replicas(), f.cluster(), NOW_NS));
        assertTrue(registry.consume(challenge.roundId(), f.replicas(), f.cluster(), NOW_NS).isEmpty());
        assertFalse(registry.recordReport(challenge.roundId(), report(challenge, 1), NOW_NS));
        assertEquals(0, registry.pendingCount());
    }

    @Test
    void reissueRevokesOldRoundAndDuplicatesCannotOverwriteReceipt() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry registry = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge oldRound = issue(registry, f, OBJECT_ID, NOW_NS);
        assertTrue(registry.recordReport(oldRound.roundId(), report(oldRound, 1), NOW_NS));
        PartitionRetirementReplicaRoundRegistry.Challenge nextRound =
            issue(registry, f, OBJECT_ID, NOW_NS + 1L);
        assertNotEquals(oldRound.roundId(), nextRound.roundId());
        assertEquals(1, registry.pendingCount());
        assertFalse(registry.recordReport(oldRound.roundId(), report(oldRound, 2), NOW_NS + 1L));
        assertFalse(registry.recordReport(nextRound.roundId(), report(oldRound, 1), NOW_NS + 1L));
        assertTrue(registry.consume(oldRound.roundId(), f.replicas(), f.cluster(), NOW_NS + 1L).isEmpty());
        assertTrue(registry.recordReport(nextRound.roundId(), report(nextRound, 1), NOW_NS + 1L));
        assertFalse(registry.recordReport(nextRound.roundId(), report(nextRound, 1), NOW_NS + 1L));
        assertTrue(registry.recordReport(nextRound.roundId(), report(nextRound, 2), NOW_NS + 1L));
        assertTrue(registry.recordReport(nextRound.roundId(), report(nextRound, 3), NOW_NS + 1L));
        assertEquals(Optional.of(Finding.MATCHED_UNTRUSTED),
            registry.consume(nextRound.roundId(), f.replicas(), f.cluster(), NOW_NS + 1L));
    }

    @Test
    void incompleteOrForeignBrokerReceiptsFailClosedAndBurnRound() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry registry = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge challenge = issue(registry, f, OBJECT_ID, NOW_NS);
        assertFalse(registry.recordReport(challenge.roundId(), report(challenge, 4), NOW_NS));
        assertTrue(registry.recordReport(challenge.roundId(), report(challenge, 1), NOW_NS));
        assertTrue(registry.recordReport(challenge.roundId(), report(challenge, 2), NOW_NS));
        assertEquals(Optional.of(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT),
            registry.consume(challenge.roundId(), f.replicas(), f.cluster(), NOW_NS));
        assertFalse(registry.recordReport(challenge.roundId(), report(challenge, 3), NOW_NS));
        assertTrue(registry.consume(challenge.roundId(), f.replicas(), f.cluster(), NOW_NS).isEmpty());
    }

    @Test
    void brokerReregistrationAndPartitionEpochChangeInvalidateCollectedRound() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry registry = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge first = issue(registry, f, OBJECT_ID, NOW_NS);
        recordAll(registry, first, NOW_NS);
        when(f.cluster().registration(2)).thenReturn(
            broker(2, 102L, Uuid.randomUuid()));
        assertEquals(Optional.of(Finding.BROKER_IDENTITY_MISMATCH),
            registry.consume(first.roundId(), f.replicas(), f.cluster(), NOW_NS));

        PartitionRetirementReplicaRoundRegistry.Challenge second = issue(registry, f, OBJECT_ID, NOW_NS + 1L);
        recordAll(registry, second, NOW_NS + 1L);
        PartitionRegistration changed = partition().merge(new PartitionChangeRecord().setIsr(List.of(1)));
        assertEquals(7, changed.leaderEpoch);
        assertEquals(12, changed.partitionEpoch);
        when(f.replicas().getPartition(KEY.topicId(), 0)).thenReturn(changed);
        assertEquals(Optional.of(Finding.KRAFT_PARTITION_CHANGED),
            registry.consume(second.roundId(), f.replicas(), f.cluster(), NOW_NS + 1L));
    }

    @Test
    void expiryAndControllerLeadershipResetRevokeAllVolatileRounds() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry registry = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge expired = issue(registry, f, OBJECT_ID, NOW_NS);
        long afterTtl = NOW_NS + PartitionRetirementReplicaRoundRegistry.MAX_AGE_NS + 1L;
        assertFalse(registry.recordReport(expired.roundId(), report(expired, 1), afterTtl));
        assertTrue(registry.consume(expired.roundId(), f.replicas(), f.cluster(), afterTtl).isEmpty());
        PartitionRetirementReplicaRoundRegistry.Challenge cleared = issue(registry, f, OBJECT_ID, afterTtl);
        registry.invalidateAll();
        assertEquals(0, registry.pendingCount());
        assertFalse(registry.recordReport(cleared.roundId(), report(cleared, 1), afterTtl));
        assertTrue(registry.consume(cleared.roundId(), f.replicas(), f.cluster(), afterTtl).isEmpty());
    }

    @Test
    void perSourceCapacityAndInvalidPhysicalCoordinatesFailClosed() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry registry = new PartitionRetirementReplicaRoundRegistry();
        assertThrows(IllegalArgumentException.class, () ->
            registry.issue(f.replicas(), CANDIDATE, 0L, NOW_NS));
        for (int i = 0; i < PartitionRetirementReplicaRoundRegistry.MAX_PENDING_PER_SOURCE; i++) {
            assertTrue(registry.issue(f.replicas(), CANDIDATE, OBJECT_ID + i, NOW_NS).isPresent());
        }
        assertEquals(PartitionRetirementReplicaRoundRegistry.MAX_PENDING_PER_SOURCE,
            registry.pendingCount());
        assertTrue(registry.issue(f.replicas(), CANDIDATE, 100_000L, NOW_NS).isEmpty());
        // A new round for the same object revokes the previous one rather than growing capacity.
        assertTrue(registry.issue(f.replicas(), CANDIDATE, OBJECT_ID, NOW_NS + 1L).isPresent());
        assertEquals(PartitionRetirementReplicaRoundRegistry.MAX_PENDING_PER_SOURCE,
            registry.pendingCount());
    }

    @Test
    void authenticatedReplicaIngressStillProducesOnlyUntrustedFinding() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry rounds = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge round = issue(rounds, f, OBJECT_ID, NOW_NS);
        for (int brokerId = 1; brokerId <= 3; brokerId++) {
            assertTrue(recordAuthenticated(rounds, f, round,
                report(round, brokerId), trusted(brokerId), NOW_NS));
        }
        assertEquals(Optional.of(Finding.MATCHED_UNTRUSTED),
            rounds.consume(round.roundId(), f.replicas(), f.cluster(), NOW_NS));
        assertFalse(recordAuthenticated(rounds, f, round, report(round, 1), trusted(1), NOW_NS));
    }

    @Test
    void foreignAnonymousAndUnmappedPrincipalsCannotClaimAnotherReplicaSlot() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry rounds = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge round = issue(rounds, f, OBJECT_ID, NOW_NS);
        Report one = report(round, 1);
        assertFalse(recordAuthenticated(rounds, f, round, one, trusted(2), NOW_NS));
        assertFalse(recordAuthenticated(rounds, f, round, one,
            context((short) 3, KafkaPrincipal.ANONYMOUS, SecurityProtocol.SASL_SSL, false), NOW_NS));
        assertFalse(rounds.recordAuthenticated(
            round.roundId(), one, trusted(1), PartitionRetirementBrokerPrincipalPolicy.DENY_ALL,
            f.replicas(), f.cluster(), NOW_NS));
        assertTrue(recordAuthenticated(rounds, f, round, one, trusted(1), NOW_NS));
        assertFalse(recordAuthenticated(rounds, f, round, one, trusted(1), NOW_NS),
            "An authenticated duplicate must not overwrite an already accepted observation");
    }

    @Test
    void plaintextForwardedTokenSyntheticAndWrongVersionCannotEnterRound() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry rounds = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge round = issue(rounds, f, OBJECT_ID, NOW_NS);
        Report one = report(round, 1);
        for (SecurityProtocol protocol : List.of(SecurityProtocol.PLAINTEXT,
            SecurityProtocol.SASL_PLAINTEXT)) {
            assertFalse(recordAuthenticated(rounds, f, round, one,
                context((short) 3, BROKER_ONE, protocol, false), NOW_NS));
        }
        assertFalse(recordAuthenticated(rounds, f, round, one,
            context((short) 3, BROKER_ONE, SecurityProtocol.SASL_SSL, true), NOW_NS));
        KafkaPrincipal token = new KafkaPrincipal(BROKER_ONE.getPrincipalType(),
            BROKER_ONE.getName(), true);
        assertFalse(recordAuthenticated(rounds, f, round, one,
            context((short) 3, token, SecurityProtocol.SASL_SSL, false), NOW_NS));
        for (short version : new short[] {2, 4}) {
            assertFalse(recordAuthenticated(rounds, f, round, one,
                context(version, BROKER_ONE, SecurityProtocol.SASL_SSL, false), NOW_NS));
        }
        assertFalse(recordAuthenticated(rounds, f, round, one,
            new ControllerRequestContext(
                trusted(1).requestHeader(), BROKER_ONE, OptionalLong.empty()), NOW_NS));
        ControllerRequestContext foreignApi = new ControllerRequestContext(
            new MockAuthorizableRequestContext.Builder()
                .setRequestType(ApiKeys.ALTER_CONFIGS)
                .setRequestVersion((short) 3)
                .setPrincipal(BROKER_ONE)
                .setSecurityProtocol(SecurityProtocol.SASL_SSL)
                .build(), OptionalLong.empty());
        assertFalse(recordAuthenticated(rounds, f, round, one, foreignApi, NOW_NS));
        assertTrue(recordAuthenticated(rounds, f, round, one, trusted(1), NOW_NS));
    }

    @Test
    void brokerIncarnationAndKraftTopologyMustBeCurrentAtIngress() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry rounds = new PartitionRetirementReplicaRoundRegistry();
        PartitionRetirementReplicaRoundRegistry.Challenge round = issue(rounds, f, OBJECT_ID, NOW_NS);
        Report second = report(round, 2);
        when(f.cluster().registration(2)).thenReturn(broker(2, 103L, Uuid.randomUuid()));
        assertFalse(recordAuthenticated(rounds, f, round, second, trusted(2), NOW_NS));
        when(f.cluster().registration(2)).thenReturn(broker(2, 102L, INCARNATIONS[1]));
        PartitionRegistration changed = partition().merge(
            new PartitionChangeRecord().setIsr(List.of(1)));
        when(f.replicas().getPartition(KEY.topicId(), 0)).thenReturn(changed);
        assertFalse(recordAuthenticated(rounds, f, round, second, trusted(2), NOW_NS));
        when(f.replicas().getPartition(KEY.topicId(), 0)).thenReturn(partition());
        assertTrue(recordAuthenticated(rounds, f, round, second, trusted(2), NOW_NS));
        assertFalse(recordAuthenticated(rounds, f, round, second, trusted(2), NOW_NS));
    }

    @Test
    void monotonicClockWrapAcceptsShortElapsedButRejectsExactTtl() {
        Fixture f = fixture();
        PartitionRetirementReplicaRoundRegistry rounds = new PartitionRetirementReplicaRoundRegistry();
        long issuedNearWrap = Long.MAX_VALUE - 20L;
        long shortElapsed = Long.MIN_VALUE + 20L;
        PartitionRetirementReplicaRoundRegistry.Challenge wrap =
            issue(rounds, f, OBJECT_ID, issuedNearWrap);
        assertTrue(recordAuthenticated(rounds, f, wrap,
            report(wrap, 1), trusted(1), shortElapsed));
        assertEquals(Optional.of(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT),
            rounds.consume(wrap.roundId(), f.replicas(), f.cluster(), shortElapsed));

        PartitionRetirementReplicaRoundRegistry.Challenge boundary =
            issue(rounds, f, OBJECT_ID, NOW_NS);
        long expiredAt = NOW_NS + PartitionRetirementReplicaRoundRegistry.MAX_AGE_NS;
        assertFalse(recordAuthenticated(rounds, f, boundary,
            report(boundary, 1), trusted(1), expiredAt));
        assertTrue(rounds.consume(boundary.roundId(), f.replicas(), f.cluster(), expiredAt).isEmpty());
    }

    private static boolean recordAuthenticated(
        PartitionRetirementReplicaRoundRegistry registry,
        Fixture fixture,
        PartitionRetirementReplicaRoundRegistry.Challenge round,
        Report report,
        ControllerRequestContext context,
        long nowNs
    ) {
        return registry.recordAuthenticated(round.roundId(), report, context, BOUND,
            fixture.replicas(), fixture.cluster(), nowNs);
    }

    private static ControllerRequestContext trusted(int brokerId) {
        KafkaPrincipal principal = switch (brokerId) {
            case 1 -> BROKER_ONE;
            case 2 -> BROKER_TWO;
            case 3 -> BROKER_THREE;
            default -> KafkaPrincipal.ANONYMOUS;
        };
        return context((short) 3, principal, SecurityProtocol.SASL_SSL, false);
    }

    private static ControllerRequestContext context(
        short version,
        KafkaPrincipal principal,
        SecurityProtocol protocol,
        boolean forwarded
    ) {
        return ControllerRequestContextUtil.brokerDiagnosticContext(
            version, principal, protocol, forwarded);
    }

    private static PartitionRetirementReplicaRoundRegistry.Challenge issue(
        PartitionRetirementReplicaRoundRegistry registry, Fixture f, long objectId, long nowNs
    ) {
        return registry.issue(f.replicas(), CANDIDATE, objectId, nowNs).orElseThrow();
    }

    private static void recordAll(
        PartitionRetirementReplicaRoundRegistry registry,
        PartitionRetirementReplicaRoundRegistry.Challenge challenge,
        long nowNs
    ) {
        for (int id = 1; id <= 3; id++) {
            assertTrue(registry.recordReport(challenge.roundId(), report(challenge, id), nowNs));
        }
    }

    private static Report report(PartitionRetirementReplicaRoundRegistry.Challenge challenge, int brokerId) {
        Uuid incarnation = brokerId <= 3 ? INCARNATIONS[brokerId - 1] : Uuid.randomUuid();
        return new Report(
            challenge.candidate().key(), challenge.objectId(), brokerId,
            100L + brokerId, incarnation, challenge.candidate().sourceLeaderEpoch(),
            challenge.candidate().sourcePartitionEpoch(),
            challenge.candidate().expectedPreviousAuthorityOffset(),
            challenge.candidate().nextAuthorityOffset(), brokerId,
            challenge.roundId(), true, true);
    }

    private static Fixture fixture() {
        ReplicationControlManager replicas = mock(ReplicationControlManager.class);
        ClusterControlManager cluster = mock(ClusterControlManager.class);
        when(replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition());
        for (int id = 1; id <= 3; id++) {
            when(cluster.registration(id)).thenReturn(broker(id, 100L + id, INCARNATIONS[id - 1]));
        }
        return new Fixture(replicas, cluster);
    }

    private static PartitionRegistration partition() {
        return new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3})
            .setDirectories(DirectoryId.unassignedArray(3))
            .setIsr(new int[] {1, 2})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(7)
            .setPartitionEpoch(11)
            .build();
    }

    private static BrokerRegistration broker(int brokerId, long epoch, Uuid incarnation) {
        return new BrokerRegistration.Builder()
            .setId(brokerId)
            .setEpoch(epoch)
            .setIncarnationId(incarnation)
            .setFenced(false)
            .setInControlledShutdown(false)
            .build();
    }

    private record Fixture(ReplicationControlManager replicas, ClusterControlManager cluster) {
    }
}
