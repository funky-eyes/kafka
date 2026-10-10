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
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SourceLogStartObservationTest {
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final TopicIdPartition KAFKA_PARTITION =
        new TopicIdPartition(TOPIC_ID, new TopicPartition("shared-topic", 0));
    private static final SharedPartitionId SHARED_PARTITION = new SharedPartitionId(
        TOPIC_ID.getMostSignificantBits(), TOPIC_ID.getLeastSignificantBits(), 0
    );

    @TempDir
    Path tempDir;

    @Test
    void capturesCurrentEpochAndNativeKafkaSourceBounds() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        SharedUnifiedLog log = mockedLog(
            TOPIC_ID, 0, Optional.of(new SharedUnifiedLog.NativeSourceWindow(20L, 50L, 80L))
        );

        var observation = SourceLogStartObservation.capture(roles, log, SHARED_PARTITION).orElseThrow();

        assertEquals(SHARED_PARTITION, observation.partition());
        assertEquals(8, observation.sourceLeaderEpoch());
        assertEquals(20L, observation.offsets().logStartOffset());
        assertEquals(50L, observation.offsets().highWatermark());
        assertEquals(80L, observation.offsets().logEndOffset());
    }

    @Test
    void legacyLeaderWithoutKRaftEpochCannotMakeAReadOnlyObservation() throws IOException {
        SharedPartitionRoleListener roles = roles();
        roles.onLeadershipChange(List.of(KAFKA_PARTITION), List.of());

        assertTrue(SourceLogStartObservation.capture(
            roles, mockedLog(TOPIC_ID, 0, goodWindow()), SHARED_PARTITION
        ).isEmpty());
    }

    @Test
    void demotedLeaderCannotCaptureNativeSourceEvidence() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        roles.onLeadershipChangeWithEpochs(Map.of(), Map.of(KAFKA_PARTITION, 9));

        assertTrue(SourceLogStartObservation.capture(
            roles, mockedLog(TOPIC_ID, 0, goodWindow()), SHARED_PARTITION
        ).isEmpty());
    }

    @Test
    void topicNameReuseWithNewTopicIdCannotBorrowOldLocalObservation() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        assertTrue(SourceLogStartObservation.capture(
            roles, mockedLog(Uuid.randomUuid(), 0, goodWindow()), SHARED_PARTITION
        ).isEmpty());
    }

    @Test
    void wrongKafkaPartitionCannotSupplyTheObservedOffsets() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        assertTrue(SourceLogStartObservation.capture(
            roles, mockedLog(TOPIC_ID, 1, goodWindow()), SHARED_PARTITION
        ).isEmpty());
    }

    @Test
    void incompleteNativeLogWindowIsUnknownInsteadOfAuthority() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        assertTrue(SourceLogStartObservation.capture(
            roles, mockedLog(TOPIC_ID, 0, Optional.empty()), SHARED_PARTITION
        ).isEmpty());
    }

    @Test
    void demotionDuringNativeReadRejectsThePreviouslyCapturedLeaderTicket() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        SharedUnifiedLog log = mockedLog(TOPIC_ID, 0, goodWindow());
        when(log.captureNativeSourceWindow()).thenAnswer(ignored -> {
            roles.onLeadershipChangeWithEpochs(Map.of(), Map.of(KAFKA_PARTITION, 9));
            return goodWindow();
        });

        assertTrue(SourceLogStartObservation.capture(roles, log, SHARED_PARTITION).isEmpty());
    }

    @Test
    void removedTopicIdRejectsDelayedSameEpochLeaderAndNativeSnapshot() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        roles.onPartitionsRemoved(List.of(KAFKA_PARTITION));
        elect(roles, 8);

        assertTrue(SourceLogStartObservation.capture(
            roles, mockedLog(TOPIC_ID, 0, goodWindow()), SHARED_PARTITION
        ).isEmpty());
    }

    @Test
    void freshReassignmentWithHigherEpochMayRecaptureLocalSourceView() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        roles.onPartitionsRemoved(List.of(KAFKA_PARTITION));
        elect(roles, 9);

        var observed = SourceLogStartObservation.capture(
            roles, mockedLog(TOPIC_ID, 0, goodWindow()), SHARED_PARTITION
        ).orElseThrow();
        assertEquals(9, observed.sourceLeaderEpoch());
    }

    @Test
    void topicIdChangedDuringNativeReadRejectsTheEarlierIdentityCheck() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        SharedUnifiedLog log = mockedLog(TOPIC_ID, 0, goodWindow());
        when(log.topicId()).thenReturn(
            Optional.of(TOPIC_ID), Optional.of(new Uuid(100L, 200L))
        );

        assertTrue(SourceLogStartObservation.capture(roles, log, SHARED_PARTITION).isEmpty());
    }

    @Test
    void partitionChangedDuringNativeReadRejectsTheEarlierIdentityCheck() throws IOException {
        SharedPartitionRoleListener roles = roles();
        elect(roles, 8);
        SharedUnifiedLog log = mockedLog(TOPIC_ID, 0, goodWindow());
        when(log.topicPartition()).thenReturn(
            new TopicPartition("shared-topic", 0), new TopicPartition("shared-topic", 1)
        );

        assertTrue(SourceLogStartObservation.capture(roles, log, SHARED_PARTITION).isEmpty());
    }

    @Test
    void nativeWindowRejectsUnknownOrContradictoryOffsetRanges() {
        assertTrue(SharedUnifiedLog.NativeSourceWindow.fromBounds(-1L, 0L, 1L).isEmpty());
        assertTrue(SharedUnifiedLog.NativeSourceWindow.fromBounds(10L, 9L, 11L).isEmpty());
        assertTrue(SharedUnifiedLog.NativeSourceWindow.fromBounds(10L, 20L, 19L).isEmpty());
        assertTrue(SharedUnifiedLog.NativeSourceWindow.fromBounds(0L, 0L, 0L).isPresent());
        assertThrows(IllegalArgumentException.class,
            () -> new SharedUnifiedLog.NativeSourceWindow(10L, 9L, 11L));
        assertFalse(SharedUnifiedLog.NativeSourceWindow.fromBounds(10L, 20L, 30L).isEmpty());
    }

    private SharedPartitionRoleListener roles() {
        SharedStorageConfiguration config = SharedStorageConfiguration.from(new StorageExtensionContext(
            Map.of(), List.of(tempDir.toFile()), 1, new MockTime()
        ));
        return new SharedPartitionRoleListener(config, new SharedCommitProgress());
    }

    private static void elect(SharedPartitionRoleListener roles, int epoch) {
        roles.onLeadershipChangeWithEpochs(Map.of(KAFKA_PARTITION, epoch), Map.of());
    }

    private static Optional<SharedUnifiedLog.NativeSourceWindow> goodWindow() {
        return Optional.of(new SharedUnifiedLog.NativeSourceWindow(20L, 50L, 80L));
    }

    private static SharedUnifiedLog mockedLog(
        Uuid topicId, int partition, Optional<SharedUnifiedLog.NativeSourceWindow> window
    ) throws IOException {
        SharedUnifiedLog log = mock(SharedUnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.of(topicId));
        when(log.topicPartition()).thenReturn(new TopicPartition("shared-topic", partition));
        when(log.captureNativeSourceWindow()).thenReturn(window);
        return log;
    }
}
