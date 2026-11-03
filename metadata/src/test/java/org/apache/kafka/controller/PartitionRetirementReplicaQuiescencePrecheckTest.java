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
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Report;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Finding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Complete-replica diagnostic only: no authenticated GC or KRaft record emission. */
class PartitionRetirementReplicaQuiescencePrecheckTest {
    private static final Key KEY = new Key(Uuid.randomUuid(), 0);
    private static final long OBJECT_ID = 901L;
    private static final Uuid[] INCARNATIONS = {
        Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid()
    };
    private static final Candidate CANDIDATE =
        new Candidate(KEY, 1, 101L, INCARNATIONS[0], 7, 11, -1L, 20L, 50L);

    @Test
    void allAssignedReplicasIncludingOutOfIsrAreRequired() {
        Fixture f = fixture();
        assertEquals(Finding.MATCHED_UNTRUSTED, assess(f, reports()));
        assertEquals(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT, assess(f, List.of(report(1), report(2))),
            "Replica 3 is not in ISR but is still assigned and may read the object");
    }

    @Test
    void duplicatedOrForeignBrokerCannotCompleteReplicaSet() {
        Fixture f = fixture();
        assertEquals(Finding.DUPLICATE_REPLICA_REPORT,
            assess(f, List.of(report(1), report(1), report(3))));
        assertEquals(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT,
            assess(f, List.of(report(1), report(2), report(4))));
    }

    @Test
    void isrChangeLeaderElectionAndReassignmentInvalidateEpochBoundReports() {
        Fixture f = fixture();
        PartitionRegistration changedIsr = partition().merge(
            new PartitionChangeRecord().setIsr(List.of(1))
        );
        assertEquals(7, changedIsr.leaderEpoch);
        assertEquals(12, changedIsr.partitionEpoch);
        when(f.replicas().getPartition(KEY.topicId(), 0)).thenReturn(changedIsr);
        assertEquals(Finding.KRAFT_PARTITION_CHANGED, assess(f, reports()));

        PartitionRegistration newLeader = registration(2, 8, 12, new int[] {1, 2});
        when(f.replicas().getPartition(KEY.topicId(), 0)).thenReturn(newLeader);
        assertEquals(Finding.KRAFT_PARTITION_CHANGED, assess(f, reports()));

        PartitionRegistration reassigning = partition().merge(new PartitionChangeRecord()
            .setReplicas(List.of(1, 2, 3, 4))
            .setDirectories(Uuid.toList(DirectoryId.unassignedArray(4)))
            .setAddingReplicas(List.of(4)));
        when(f.replicas().getPartition(KEY.topicId(), 0)).thenReturn(reassigning);
        assertEquals(Finding.KRAFT_PARTITION_CHANGED, assess(f, reports()));
    }

    @Test
    void missingFencedOrShuttingDownBrokerFailsClosed() {
        Fixture f = fixture();
        when(f.cluster().registration(3)).thenReturn(null);
        assertEquals(Finding.BROKER_NOT_ELIGIBLE, assess(f, reports()));
        when(f.cluster().registration(3)).thenReturn(
            broker(3, 103L, INCARNATIONS[2], true, false));
        assertEquals(Finding.BROKER_NOT_ELIGIBLE, assess(f, reports()));
        when(f.cluster().registration(3)).thenReturn(
            broker(3, 103L, INCARNATIONS[2], false, true));
        assertEquals(Finding.BROKER_NOT_ELIGIBLE, assess(f, reports()));
        when(f.cluster().registration(3)).thenReturn(
            broker(3, 103L, Uuid.ZERO_UUID, false, false));
        assertEquals(Finding.BROKER_NOT_ELIGIBLE, assess(f, reports()));
    }

    @Test
    void reusedBrokerIdCannotBorrowOlderEpochOrIncarnationReport() {
        Fixture f = fixture();
        Candidate staleLeader = new Candidate(
            KEY, 1, 999L, Uuid.randomUuid(), 7, 11, -1L, 20L, 50L);
        assertEquals(Finding.BROKER_IDENTITY_MISMATCH,
            PartitionRetirementReplicaQuiescencePrecheck.assess(
                f.replicas(), f.cluster(), staleLeader, OBJECT_ID, reports()),
            "A current replica report must not validate an old source candidate");
        when(f.cluster().registration(2)).thenReturn(
            broker(2, 200L, INCARNATIONS[1], false, false));
        assertEquals(Finding.BROKER_IDENTITY_MISMATCH, assess(f, reports()));
        when(f.cluster().registration(2)).thenReturn(
            broker(2, 102L, Uuid.randomUuid(), false, false));
        assertEquals(Finding.BROKER_IDENTITY_MISMATCH, assess(f, reports()));
        when(f.cluster().registration(2)).thenReturn(
            broker(9, 102L, INCARNATIONS[1], false, false));
        assertEquals(Finding.BROKER_NOT_ELIGIBLE, assess(f, reports()));
    }

    @Test
    void pendingLogicalReadOrPhysicalUploadBlocksLocalDrain() {
        Fixture f = fixture();
        assertEquals(Finding.LOCAL_IO_NOT_DRAINED,
            assess(f, List.of(report(1), report(2, false, true), report(3))));
        assertEquals(Finding.LOCAL_IO_NOT_DRAINED,
            assess(f, List.of(report(1), report(2, true, false), report(3))));
        assertEquals(Finding.MATCHED_UNTRUSTED, assess(f, reports()),
            "Matching self-reported drain is never authenticated GC permission");
    }

    @Test
    void physicalObjectTopicAndAuthorityCasCannotCrossBorrowEvidence() {
        Fixture f = fixture();
        assertEquals(Finding.OBJECT_OR_AUTHORITY_MISMATCH,
            assess(f, List.of(report(1), report(2, KEY, 902L, 7, 11, -1L, 20L, true, true),
                report(3))));
        Key recreated = new Key(Uuid.randomUuid(), 0);
        assertEquals(Finding.OBJECT_OR_AUTHORITY_MISMATCH,
            assess(f, List.of(report(1), report(2, recreated, OBJECT_ID, 7, 11, -1L, 20L,
                true, true), report(3))));
        assertEquals(Finding.OBJECT_OR_AUTHORITY_MISMATCH,
            assess(f, List.of(report(1), report(2, KEY, OBJECT_ID, 7, 11, 10L, 20L, true, true),
                report(3))));
        assertEquals(Finding.OBJECT_OR_AUTHORITY_MISMATCH,
            assess(f, List.of(report(1), report(2, KEY, OBJECT_ID, 7, 11, -1L, 21L, true, true),
                report(3))));
        assertEquals(Finding.SOURCE_EPOCH_MISMATCH,
            assess(f, List.of(report(1), report(2, KEY, OBJECT_ID, 7, 12, -1L, 20L, true, true),
                report(3))));
    }

    @Test
    void missingMalformedOrNullReportNeverPassesPrecheck() {
        Fixture f = fixture();
        assertEquals(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT, assess(f, List.of()));
        ArrayList<Report> incomplete = new ArrayList<>(reports());
        incomplete.set(1, null);
        assertEquals(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT, assess(f, incomplete));
        assertThrows(IllegalArgumentException.class,
            () -> PartitionRetirementReplicaQuiescencePrecheck.assess(
                f.replicas(), f.cluster(), CANDIDATE, 0L, reports()));
        assertThrows(IllegalArgumentException.class,
            () -> new Report(KEY, OBJECT_ID, 2, 102L, Uuid.ZERO_UUID, 7, 11,
                -1L, 20L, 1L, true, true));
        assertThrows(IllegalArgumentException.class,
            () -> new Report(KEY, OBJECT_ID, 2, 102L, INCARNATIONS[1], 7, 11,
                -1L, 20L, 0L, true, true));
        assertTrue(assess(f, reports()) == Finding.MATCHED_UNTRUSTED);
    }

    private static Finding assess(Fixture f, List<Report> reports) {
        return PartitionRetirementReplicaQuiescencePrecheck.assess(
            f.replicas(), f.cluster(), CANDIDATE, OBJECT_ID, reports
        );
    }

    private static List<Report> reports() {
        return List.of(report(1), report(2), report(3));
    }

    private static Report report(int id) {
        return report(id, true, true);
    }

    private static Report report(int id, boolean reads, boolean writes) {
        return report(id, KEY, OBJECT_ID, 7, 11, -1L, 20L, reads, writes);
    }

    private static Report report(
        int id,
        Key key,
        long objectId,
        int leaderEpoch,
        int partitionEpoch,
        long previous,
        long next,
        boolean reads,
        boolean writes
    ) {
        return new Report(key, objectId, id, 100L + id, INCARNATIONS[id - 1],
            leaderEpoch, partitionEpoch, previous, next, 1L, reads, writes);
    }

    private static Fixture fixture() {
        ReplicationControlManager replicas = mock(ReplicationControlManager.class);
        ClusterControlManager cluster = mock(ClusterControlManager.class);
        when(replicas.getPartition(KEY.topicId(), 0)).thenReturn(partition());
        for (int id = 1; id <= 3; id++) {
            when(cluster.registration(id)).thenReturn(
                broker(id, 100L + id, INCARNATIONS[id - 1], false, false));
        }
        return new Fixture(replicas, cluster);
    }

    private static PartitionRegistration partition() {
        return registration(1, 7, 11, new int[] {1, 2});
    }

    private static PartitionRegistration registration(
        int leader,
        int leaderEpoch,
        int partitionEpoch,
        int[] isr
    ) {
        return new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3})
            .setDirectories(DirectoryId.unassignedArray(3))
            .setIsr(isr)
            .setLeader(leader)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(leaderEpoch)
            .setPartitionEpoch(partitionEpoch)
            .build();
    }

    private static BrokerRegistration broker(
        int id,
        long epoch,
        Uuid incarnation,
        boolean fenced,
        boolean shuttingDown
    ) {
        return new BrokerRegistration.Builder()
            .setId(id)
            .setEpoch(epoch)
            .setIncarnationId(incarnation)
            .setFenced(fenced)
            .setInControlledShutdown(shuttingDown)
            .build();
    }

    private record Fixture(
        ReplicationControlManager replicas,
        ClusterControlManager cluster
    ) {
    }
}
