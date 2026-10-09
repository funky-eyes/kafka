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

import org.apache.kafka.storage.internals.shared.metadata.PartitionLogStartAdvancePrecheck;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementEpochPrecheckTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);

    @Test
    void matchingSourceEpochOnlyProducesAnAdvisoryValueFinding() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();

        var result = check(fence, ticket, 5, imageWithWatermark(10L), 20L, 20L, 2L);
        assertEquals(PartitionRetirementEpochPrecheck.Status.LOCAL_EPOCH_OBSERVATION_MATCH, result.status());
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.VALUE_DOMAIN_CANDIDATE,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void legacyLeadershipNotificationIsNeverAnEpochProof() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION);
        var ticket = fence.captureLeader(PARTITION).orElseThrow();

        var result = check(fence, ticket, 5, readyImage(), 0L, 0L, -1L);
        assertEquals(PartitionRetirementEpochPrecheck.Status.KAFKA_EPOCH_UNKNOWN, result.status());
        assertTrue(result.valueFinding().isEmpty());
    }

    @Test
    void observedNewEpochInvalidatesOldEpochCandidateEvenIfLocallyLeader() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();

        var result = check(fence, ticket, 6, readyImage(), 0L, 0L, -1L);
        assertEquals(PartitionRetirementEpochPrecheck.Status.KAFKA_EPOCH_MISMATCH, result.status());
        assertTrue(result.valueFinding().isEmpty());
    }

    @Test
    void staleTicketCannotBeRevivedAfterAba() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var previous = fence.captureEpochLeader(PARTITION).orElseThrow();
        fence.onFollower(PARTITION, 6);
        fence.onLeader(PARTITION, 7);

        var result = check(fence, previous, 5, readyImage(), 0L, 0L, -1L);
        assertEquals(PartitionRetirementEpochPrecheck.Status.LOCAL_TICKET_STALE, result.status());
        assertTrue(result.valueFinding().isEmpty());
    }

    @Test
    void matchedEpochCannotOverrideMissingInitialZero() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();

        var result = check(fence, ticket, 5, readyImage(), 10L, 10L, -1L);
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.INITIAL_ZERO_REQUIRED,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void matchedEpochCannotOverrideReplayHorizon() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();

        var result = check(fence, ticket, 5, imageWithWatermark(10L), 20L, 20L, 3L);
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.METADATA_REPLAY_BEHIND,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void matchedEpochCannotOverrideSourceLogStartUpperBound() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();

        var result = check(fence, ticket, 5, imageWithWatermark(10L), 30L, 20L, 2L);
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.EXCEEDS_OBSERVED_KAFKA_LOG_START,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void corruptedMetadataImageFailsClosedAfterLocalEpochMatch() {
        LocalRetirementLeadershipFence fence = new LocalRetirementLeadershipFence();
        fence.onLeader(PARTITION, 5);
        var ticket = fence.captureEpochLeader(PARTITION).orElseThrow();
        SharedMetadataImage image = readyImage();
        image.markFailed(new IllegalStateException("metadata consumer stopped"));

        assertThrows(IllegalStateException.class, () ->
            check(fence, ticket, 5, image, 0L, 0L, -1L)
        );
    }

    private static PartitionRetirementEpochPrecheck.Assessment check(
        LocalRetirementLeadershipFence fence,
        LocalRetirementLeadershipFence.LeaderTicket ticket,
        int observedEpoch,
        SharedMetadataImage image,
        long requested,
        long observedLogStart,
        long horizon
    ) {
        return PartitionRetirementEpochPrecheck.assess(
            fence, ticket, observedEpoch, image, requested, observedLogStart, horizon
        );
    }

    private static SharedMetadataImage readyImage() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        return image;
    }

    private static SharedMetadataImage imageWithWatermark(long watermark) {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(watermark),
            2L
        );
        image.markReady();
        return image;
    }
}
