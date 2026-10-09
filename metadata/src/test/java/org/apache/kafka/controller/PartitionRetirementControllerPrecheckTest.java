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
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;

import org.junit.jupiter.api.Test;

import static org.apache.kafka.controller.PartitionRetirementControllerPrecheck.Finding;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Negative-first regression witnesses for the current KRaft partition-registration seam. */
class PartitionRetirementControllerPrecheckTest {
    @Test
    void currentRecoveredLeaderInIsrMatchesButIsNotWriteAuthorization() {
        assertEquals(
            Finding.CONTROLLER_IMAGE_MATCH,
            assess(registration(1, 20, LeaderRecoveryState.RECOVERED, new int[] {1, 2}), 1, 20)
        );
    }

    @Test
    void missingPartitionOrDeletedTopicIsNotAnAuthoritySource() {
        assertEquals(Finding.PARTITION_NOT_PRESENT, assess(null, 1, 20));
    }

    @Test
    void noActiveLeaderIsRejected() {
        assertEquals(
            Finding.NO_ACTIVE_LEADER,
            assess(registration(NO_LEADER, 20, LeaderRecoveryState.RECOVERED, new int[] {2}), 1, 20)
        );
    }

    @Test
    void previousLeaderEpochIsRejectedEvenWhenBrokerMatches() {
        assertEquals(
            Finding.KAFKA_LEADER_EPOCH_MISMATCH,
            assess(registration(1, 21, LeaderRecoveryState.RECOVERED, new int[] {1}), 1, 20)
        );
    }

    @Test
    void futureClaimedEpochIsRejectedEvenWhenBrokerMatches() {
        assertEquals(
            Finding.KAFKA_LEADER_EPOCH_MISMATCH,
            assess(registration(1, 20, LeaderRecoveryState.RECOVERED, new int[] {1}), 1, 21)
        );
    }

    @Test
    void formerBrokerCannotClaimTheNewLeaderEpoch() {
        assertEquals(
            Finding.BROKER_NOT_LEADER,
            assess(registration(2, 21, LeaderRecoveryState.RECOVERED, new int[] {1, 2}), 1, 21)
        );
    }

    @Test
    void uncleanElectionStillRecoveringDoesNotQualify() {
        assertEquals(
            Finding.LEADER_STILL_RECOVERING,
            assess(registration(1, 21, LeaderRecoveryState.RECOVERING, new int[] {1}), 1, 21)
        );
    }

    @Test
    void leaderOutsideIsrDoesNotQualify() {
        assertEquals(
            Finding.LEADER_NOT_IN_ISR,
            assess(registration(1, 20, LeaderRecoveryState.RECOVERED, new int[] {2}), 1, 20)
        );
    }

    @Test
    void leaderOutsideReplicaSetDoesNotQualify() {
        PartitionRegistration invalid = new PartitionRegistration.Builder()
            .setReplicas(new int[] {2, 3})
            .setDirectories(DirectoryId.unassignedArray(2))
            .setIsr(new int[] {1, 2})
            .setLeader(1)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(20)
            .setPartitionEpoch(20)
            .build();

        assertEquals(Finding.LEADER_NOT_IN_ISR, assess(invalid, 1, 20));
    }

    @Test
    void invalidBrokerAndEpochInputsFailFast() {
        PartitionRegistration current = registration(
            1, 20, LeaderRecoveryState.RECOVERED, new int[] {1}
        );
        assertThrows(IllegalArgumentException.class, () -> assess(current, -1, 20));
        assertThrows(IllegalArgumentException.class, () -> assess(current, 1, -1));
    }

    @Test
    void controllerLookupRevalidatesCurrentLeaderAfterEpochTransition() {
        ReplicationControlManager controller = mock(ReplicationControlManager.class);
        Uuid topicId = Uuid.randomUuid();
        when(controller.getPartition(topicId, 0)).thenReturn(
            registration(1, 20, LeaderRecoveryState.RECOVERED, new int[] {1, 2}),
            registration(2, 21, LeaderRecoveryState.RECOVERED, new int[] {1, 2})
        );
        assertEquals(
            Finding.CONTROLLER_IMAGE_MATCH,
            PartitionRetirementControllerPrecheck.assess(controller, topicId, 0, 1, 20)
        );
        assertEquals(
            Finding.KAFKA_LEADER_EPOCH_MISMATCH,
            PartitionRetirementControllerPrecheck.assess(controller, topicId, 0, 1, 20)
        );
        verify(controller, times(2)).getPartition(topicId, 0);
    }

    @Test
    void deletedTopicIdCannotBorrowRecreatedTopicRegistration() {
        ReplicationControlManager controller = mock(ReplicationControlManager.class);
        Uuid deletedTopicId = Uuid.randomUuid();
        Uuid recreatedTopicId = Uuid.randomUuid();
        when(controller.getPartition(recreatedTopicId, 0)).thenReturn(
            registration(1, 21, LeaderRecoveryState.RECOVERED, new int[] {1})
        );
        assertEquals(
            Finding.PARTITION_NOT_PRESENT,
            PartitionRetirementControllerPrecheck.assess(controller, deletedTopicId, 0, 1, 21)
        );
        assertEquals(
            Finding.CONTROLLER_IMAGE_MATCH,
            PartitionRetirementControllerPrecheck.assess(controller, recreatedTopicId, 0, 1, 21)
        );
    }

    @Test
    void invalidControllerLookupArgumentsFailClosed() {
        ReplicationControlManager controller = mock(ReplicationControlManager.class);
        Uuid topicId = Uuid.randomUuid();
        assertThrows(NullPointerException.class, () ->
            PartitionRetirementControllerPrecheck.assess(null, topicId, 0, 1, 20));
        assertThrows(NullPointerException.class, () ->
            PartitionRetirementControllerPrecheck.assess(controller, null, 0, 1, 20));
        assertThrows(IllegalArgumentException.class, () ->
            PartitionRetirementControllerPrecheck.assess(controller, topicId, -1, 1, 20));
        assertThrows(IllegalArgumentException.class, () ->
            PartitionRetirementControllerPrecheck.assess(controller, topicId, 0, 1, -1));
    }

    private static Finding assess(PartitionRegistration registration, int brokerId, int epoch) {
        return PartitionRetirementControllerPrecheck.assessRegistration(registration, brokerId, epoch);
    }

    private static PartitionRegistration registration(
        int leader,
        int leaderEpoch,
        LeaderRecoveryState recoveryState,
        int[] isr
    ) {
        return new PartitionRegistration.Builder()
            .setReplicas(new int[] {1, 2, 3})
            .setDirectories(DirectoryId.unassignedArray(3))
            .setIsr(isr)
            .setLeader(leader)
            .setLeaderRecoveryState(recoveryState)
            .setLeaderEpoch(leaderEpoch)
            .setPartitionEpoch(leaderEpoch)
            .build();
    }
}
