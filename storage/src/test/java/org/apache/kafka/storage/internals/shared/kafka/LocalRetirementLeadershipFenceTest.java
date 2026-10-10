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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalRetirementLeadershipFenceTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId OTHER_PARTITION = new SharedPartitionId(1L, 2L, 1);
    private static final SharedPartitionId RECREATED_TOPIC = new SharedPartitionId(1L, 3L, 0);

    @Test
    void unknownOrFollowerNeverIssuesALeaderTicket() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        fence.onFollower(PARTITION);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
    }

    @Test
    void demotionImmediatelyInvalidatesCapturedTicket() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        var ticket = fence.captureLeader(PARTITION).orElseThrow();
        assertTrue(fence.stillLeader(ticket));

        fence.onFollower(PARTITION);
        assertFalse(fence.stillLeader(ticket));
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
    }

    @Test
    void abaLeaderFollowerLeaderCannotResurrectOldTicket() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        var old = fence.captureLeader(PARTITION).orElseThrow();
        fence.onFollower(PARTITION);
        fence.onLeader(PARTITION);
        var current = fence.captureLeader(PARTITION).orElseThrow();

        assertNotEquals(old.localGeneration(), current.localGeneration());
        assertFalse(fence.stillLeader(old));
        assertTrue(fence.stillLeader(current));
    }

    @Test
    void repeatedLeaderNotificationFencesOldLocalWork() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        var old = fence.captureLeader(PARTITION).orElseThrow();
        fence.onLeader(PARTITION);
        assertFalse(fence.stillLeader(old));
        assertTrue(fence.stillLeader(fence.captureLeader(PARTITION).orElseThrow()));
    }

    @Test
    void removalAndReassignmentCannotReusePreviousLocalGeneration() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        var old = fence.captureLeader(PARTITION).orElseThrow();
        fence.onRemoved(PARTITION);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        assertFalse(fence.stillLeader(old));

        // Reassignment after removal must carry a fresh explicit Kafka epoch.
        // The legacy callback cannot distinguish a genuine assignment from a stale one.
        fence.onLeader(PARTITION);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        fence.onLeader(PARTITION, 1);
        var readded = fence.captureEpochLeader(PARTITION).orElseThrow();
        assertNotEquals(old.localGeneration(), readded.localGeneration());
        assertFalse(fence.stillLeader(old));
    }

    @Test
    void recreatedTopicAndOtherPartitionAreIndependent() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        var old = fence.captureLeader(PARTITION).orElseThrow();
        fence.onLeader(OTHER_PARTITION);
        fence.onLeader(RECREATED_TOPIC);

        assertTrue(fence.stillLeader(old));
        assertTrue(fence.captureLeader(OTHER_PARTITION).isPresent());
        assertTrue(fence.captureLeader(RECREATED_TOPIC).isPresent());
        fence.onRemoved(PARTITION);
        assertTrue(fence.captureLeader(RECREATED_TOPIC).isPresent());
    }

    @Test
    void sourceKafkaEpochIsCapturedByEpochAwareLeaderTicket() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 7);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();
        assertTrue(fence.stillLeader(ticket));
        assertTrue(ticket.leaderEpoch() == 7);
    }

    @Test
    void unversionedLeaderCallbacksNeverClaimKafkaEpochAuthority() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        assertTrue(fence.captureLeader(PARTITION).isPresent());
        assertTrue(fence.captureEpochLeader(PARTITION).isEmpty());
        assertTrue(fence.captureLeader(PARTITION).orElseThrow().leaderEpoch() == -1);
    }

    @Test
    void staleLowerEpochCannotReclaimLocalLeadershipAfterDemotion() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 10);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();
        fence.onFollower(PARTITION, 11);
        fence.onLeader(PARTITION, 10);

        assertFalse(fence.stillLeader(ticket));
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        fence.onLeader(PARTITION, 12);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
        assertTrue(fence.captureEpochLeader(PARTITION).orElseThrow().leaderEpoch() == 12);
    }

    @Test
    void lowerEpochDoesNotReplaceCurrentKnownLeader() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 10);
        var original = fence.captureEpochLeader(PARTITION).orElseThrow();
        fence.onLeader(PARTITION, 9);

        assertFalse(fence.stillLeader(original));
        assertTrue(fence.captureEpochLeader(PARTITION).isEmpty());
        fence.onLeader(PARTITION, 12);
        assertTrue(fence.captureEpochLeader(PARTITION).orElseThrow().leaderEpoch() == 12);
    }

    @Test
    void unversionedReelectionCannotErasePreviouslyKnownEpoch() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 12);
        fence.onFollower(PARTITION);
        fence.onLeader(PARTITION);

        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        fence.onLeader(PARTITION, 13);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
    }

    @Test
    void equalEpochLeaderCallbackAfterFollowerCannotUndoDemotion() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 15);
        var first = fence.captureEpochLeader(PARTITION).orElseThrow();
        fence.onFollower(PARTITION, 15);
        fence.onLeader(PARTITION, 15);

        assertFalse(fence.stillLeader(first));
        assertTrue(fence.captureEpochLeader(PARTITION).isEmpty());
        fence.onLeader(PARTITION, 16);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
        assertTrue(fence.captureEpochLeader(PARTITION).orElseThrow().leaderEpoch() == 16);
    }

    @Test
    void sameEpochDuplicateLeaderCallbackOnlyRefreshesLocalGeneration() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 21);
        var previous = fence.captureEpochLeader(PARTITION).orElseThrow();

        fence.onLeader(PARTITION, 21);
        var next = fence.captureEpochLeader(PARTITION).orElseThrow();
        assertFalse(fence.stillLeader(previous));
        assertNotEquals(previous.localGeneration(), next.localGeneration());
        assertTrue(fence.stillLeader(next));
        assertTrue(next.leaderEpoch() == 21);
    }

    @Test
    void invalidExplicitKafkaEpochIsRejected() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        assertThrows(IllegalArgumentException.class, () -> fence.onLeader(PARTITION, -1));
        assertThrows(IllegalArgumentException.class, () -> fence.onFollower(PARTITION, -1));
        assertTrue(fence.captureEpochLeader(PARTITION).isEmpty());
    }

    @Test
    void removedEpochTombstoneFencesEqualAndLowerLeaderCallbacks() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 20);
        var first = fence.captureEpochLeader(PARTITION).orElseThrow();
        fence.onRemoved(PARTITION);

        fence.onLeader(PARTITION, 19);
        fence.onLeader(PARTITION, 20);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        assertFalse(fence.stillLeader(first));

        fence.onLeader(PARTITION, 21);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
        assertFalse(fence.stillLeader(first));
    }

    @Test
    void removedUnversionedAssignmentRequiresExplicitEpochForReadmission() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        fence.onRemoved(PARTITION);
        fence.onLeader(PARTITION);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());

        fence.onLeader(PARTITION, 0);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
    }

    @Test
    void removedPartitionTracksLaterFollowerEpochBeforeReassignment() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 10);
        fence.onRemoved(PARTITION);
        fence.onFollower(PARTITION, 15);
        fence.onLeader(PARTITION, 14);
        fence.onLeader(PARTITION, 15);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());

        fence.onLeader(PARTITION, 16);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
    }

    @Test
    void repeatedRemovalKeepsLatestFenceAndInvalidatesPriorLeaderTickets() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 1);
        var first = fence.captureEpochLeader(PARTITION).orElseThrow();
        fence.onRemoved(PARTITION);
        fence.onRemoved(PARTITION);

        assertTrue(fence.captureLeader(PARTITION).isEmpty());
        assertFalse(fence.stillLeader(first));
        fence.onLeader(PARTITION, 1);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());

        fence.onLeader(PARTITION, 2);
        assertTrue(fence.captureEpochLeader(PARTITION).isPresent());
    }

    @Test
    void deletedTopicIdTombstoneDoesNotFenceRecreatedTopicId() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 30);
        fence.onRemoved(PARTITION);

        fence.onLeader(RECREATED_TOPIC, 0);
        assertTrue(fence.captureEpochLeader(RECREATED_TOPIC).isPresent());
        fence.onLeader(PARTITION, 30);
        assertTrue(fence.captureLeader(PARTITION).isEmpty());
    }

    @Test
    void ticketFromAnotherFenceInstanceNeverValidates() {
        LocalRetirementLeadershipFence first = new LocalRetirementLeadershipFence();
        LocalRetirementLeadershipFence second = new LocalRetirementLeadershipFence();
        first.onLeader(PARTITION);
        second.onLeader(PARTITION);

        var token = first.captureLeader(PARTITION).orElseThrow();
        assertFalse(second.stillLeader(token));
        assertTrue(first.stillLeader(token));
    }
}
