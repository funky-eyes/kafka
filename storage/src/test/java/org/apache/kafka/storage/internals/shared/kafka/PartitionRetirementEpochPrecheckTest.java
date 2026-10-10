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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.shared.metadata.PartitionLogStartAdvancePrecheck;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PartitionRetirementEpochPrecheckTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final Uuid TOPIC_ID = new Uuid(1L, 2L);
    private static final TopicIdPartition KAFKA_PARTITION =
        new TopicIdPartition(TOPIC_ID, new TopicPartition("shared-topic", 0));

    @TempDir
    Path tempDir;

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

    @Test
    void nativeSourceWindowProducesOnlyAdvisoryValueFinding() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        var result = nativeCheck(roles, nativeLog(TOPIC_ID, 0, goodNativeWindow()),
            imageWithWatermark(10L), 20L, 2L);
        assertEquals(PartitionRetirementEpochPrecheck.Status.LOCAL_EPOCH_OBSERVATION_MATCH, result.status());
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.VALUE_DOMAIN_CANDIDATE,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void nativeSourceRejectsWrongTopicIncarnationOrPartition() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        var image = readyImage();
        assertEquals(
            PartitionRetirementEpochPrecheck.Status.NATIVE_SOURCE_WINDOW_UNKNOWN,
            nativeCheck(roles, nativeLog(new Uuid(3L, 4L), 0, goodNativeWindow()),
                image, 0L, -1L).status()
        );
        assertEquals(
            PartitionRetirementEpochPrecheck.Status.NATIVE_SOURCE_WINDOW_UNKNOWN,
            nativeCheck(roles, nativeLog(TOPIC_ID, 1, goodNativeWindow()),
                image, 0L, -1L).status()
        );
    }

    @Test
    void nativeSourceRejectsLegacyEpochAndUnknownWindow() throws IOException {
        SharedPartitionRoleListener unversioned = nativeRoles();
        unversioned.onLeadershipChange(List.of(KAFKA_PARTITION), List.of());
        assertEquals(
            PartitionRetirementEpochPrecheck.Status.NATIVE_SOURCE_WINDOW_UNKNOWN,
            nativeCheck(unversioned, nativeLog(TOPIC_ID, 0, goodNativeWindow()),
                readyImage(), 0L, -1L).status()
        );
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        assertEquals(
            PartitionRetirementEpochPrecheck.Status.NATIVE_SOURCE_WINDOW_UNKNOWN,
            nativeCheck(roles, nativeLog(TOPIC_ID, 0, Optional.empty()),
                readyImage(), 0L, -1L).status()
        );
    }

    @Test
    void nativeSourceDemotionDuringKafkaReadFailsClosed() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        SharedUnifiedLog log = nativeLog(TOPIC_ID, 0, goodNativeWindow());
        when(log.captureNativeSourceWindow()).thenAnswer(ignored -> {
            roles.onLeadershipChangeWithEpochs(Map.of(), Map.of(KAFKA_PARTITION, 9));
            return goodNativeWindow();
        });
        var result = nativeCheck(roles, log, readyImage(), 0L, -1L);
        assertEquals(PartitionRetirementEpochPrecheck.Status.NATIVE_SOURCE_WINDOW_UNKNOWN, result.status());
        assertTrue(result.valueFinding().isEmpty());
    }

    @Test
    void nativeSourceCannotBypassMetadataReplayHorizon() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        var result = nativeCheck(roles, nativeLog(TOPIC_ID, 0, goodNativeWindow()),
            imageWithWatermark(10L), 20L, 3L);
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.METADATA_REPLAY_BEHIND,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void nativeSourceCannotProposeLogStartBeyondActualKafkaLogStart() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        var result = nativeCheck(roles, nativeLog(TOPIC_ID, 0, goodNativeWindow()),
            imageWithWatermark(10L), 30L, 2L);
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.EXCEEDS_OBSERVED_KAFKA_LOG_START,
            result.valueFinding().orElseThrow()
        );
    }

    @Test
    void nativeSourceRoleChangeDuringMetadataReadDiscardsAdvisoryResult() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        SharedMetadataImage image = mock(SharedMetadataImage.class);
        when(image.partitionLogStartEvidence(PARTITION)).thenAnswer(ignored -> {
            roles.onLeadershipChangeWithEpochs(Map.of(), Map.of(KAFKA_PARTITION, 9));
            return new SharedMetadataImage.PartitionLogStartEvidence(2L, OptionalLong.of(10L));
        });
        var result = nativeCheck(roles, nativeLog(TOPIC_ID, 0, goodNativeWindow()),
            image, 20L, 2L);
        assertEquals(
            PartitionRetirementEpochPrecheck.Status.LOCAL_ROLE_CHANGED_DURING_METADATA_CHECK,
            result.status()
        );
        assertTrue(result.valueFinding().isEmpty());
    }

    @Test
    void nativeSourceFailedMetadataReplayRemainsFailClosed() throws IOException {
        SharedPartitionRoleListener roles = nativeRoles();
        elect(roles, 8);
        SharedMetadataImage image = readyImage();
        image.markFailed(new IllegalStateException("metadata replay unavailable"));
        SharedUnifiedLog log = nativeLog(TOPIC_ID, 0, goodNativeWindow());
        assertThrows(IllegalStateException.class, () ->
            nativeCheck(roles, log, image, 20L, 2L)
        );
    }

    private SharedPartitionRoleListener nativeRoles() {
        SharedStorageConfiguration configuration = SharedStorageConfiguration.from(new StorageExtensionContext(
            Map.of(), List.of(tempDir.toFile()), 1, new MockTime()
        ));
        return new SharedPartitionRoleListener(configuration, new SharedCommitProgress());
    }

    private static void elect(SharedPartitionRoleListener roles, int epoch) {
        roles.onLeadershipChangeWithEpochs(Map.of(KAFKA_PARTITION, epoch), Map.of());
    }

    private static Optional<SharedUnifiedLog.NativeSourceWindow> goodNativeWindow() {
        return Optional.of(new SharedUnifiedLog.NativeSourceWindow(20L, 50L, 80L));
    }

    private static SharedUnifiedLog nativeLog(
        Uuid topicId,
        int partition,
        Optional<SharedUnifiedLog.NativeSourceWindow> window
    ) throws IOException {
        SharedUnifiedLog log = mock(SharedUnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.of(topicId));
        when(log.topicPartition()).thenReturn(new TopicPartition("shared-topic", partition));
        when(log.captureNativeSourceWindow()).thenReturn(window);
        return log;
    }

    private static PartitionRetirementEpochPrecheck.Assessment nativeCheck(
        SharedPartitionRoleListener roles,
        SharedUnifiedLog log,
        SharedMetadataImage image,
        long requestedLogStart,
        long requiredMetadataOffset
    ) throws IOException {
        return PartitionRetirementEpochPrecheck.assessNativeSource(
            roles, log, PARTITION, image, requestedLogStart, requiredMetadataOffset
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
