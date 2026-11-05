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
import org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Candidate;
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Finding;
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Report;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

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
