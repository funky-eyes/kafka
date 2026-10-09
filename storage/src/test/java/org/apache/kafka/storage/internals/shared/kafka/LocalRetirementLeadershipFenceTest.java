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

        fence.onLeader(PARTITION);
        var readded = fence.captureLeader(PARTITION).orElseThrow();
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
