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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedCommitProgressTest {
    @Test
    void followsKafkaHighWatermarkExactlyRatherThanTakingMaximum() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 3);

        progress.onHighWatermarkUpdated(partition, 100L);
        assertEquals(100L, progress.highWatermark(partition).orElseThrow());

        // Recovery or leadership changes can restore a lower HW. Kafka remains the authority.
        progress.onHighWatermarkUpdated(partition, 80L);
        assertEquals(80L, progress.highWatermark(partition).orElseThrow());
    }

    @Test
    void snapshotsKafkaLogStartHighWatermarkAndRoleAsOneCommitWindow() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 6);

        progress.onLogLoaded(partition, 20L);
        SharedCommitProgress.PartitionProgress loaded = progress.partitionProgress(partition).orElseThrow();
        assertEquals(20L, loaded.logStartOffset());
        assertEquals(20L, loaded.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.UNKNOWN, loaded.role());

        progress.onHighWatermarkUpdated(partition, 50L);
        progress.onLeader(partition);
        SharedCommitProgress.PartitionProgress snapshot = progress.snapshot().get(partition);
        assertEquals(20L, snapshot.logStartOffset());
        assertEquals(50L, snapshot.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.LEADER, snapshot.role());
    }

    @Test
    void roleTransitionsDoNotChangeKafkaOffsets() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 8);

        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 40L);
        progress.onLeader(partition);
        progress.onFollower(partition);

        SharedCommitProgress.PartitionProgress snapshot = progress.partitionProgress(partition).orElseThrow();
        assertEquals(10L, snapshot.logStartOffset());
        assertEquals(40L, snapshot.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.FOLLOWER, snapshot.role());
    }

    @Test
    void logReloadCanReplaceLogStartWithoutInventingAHighWatermarkOrRole() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 7);

        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 40L);
        progress.onLeader(partition);
        progress.onLogLoaded(partition, 25L);

        SharedCommitProgress.PartitionProgress snapshot = progress.partitionProgress(partition).orElseThrow();
        assertEquals(25L, snapshot.logStartOffset());
        assertEquals(40L, snapshot.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.LEADER, snapshot.role());
    }

    @Test
    void removesPartitionProgress() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 4);
        progress.onHighWatermarkUpdated(partition, 10L);

        progress.remove(partition);

        assertFalse(progress.highWatermark(partition).isPresent());
    }

    @Test
    void kafkaDeleteRecordsUpdatesLiveUploadStartWithoutChangingCommittedHighWatermark() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(2L, 3L, 0);
        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 80L);
        progress.onLeader(partition);

        progress.onLogStartOffsetAdvanced(partition, 40L);

        SharedCommitProgress.PartitionProgress observed = progress.partitionProgress(partition).orElseThrow();
        assertEquals(40L, observed.logStartOffset());
        assertEquals(80L, observed.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.LEADER, observed.role());
    }

    @Test
    void delayedLowerDeleteRecordsObservationCannotRegressLiveSourceStart() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(2L, 3L, 1);
        progress.onLogLoaded(partition, 10L);
        progress.onLogStartOffsetAdvanced(partition, 60L);
        progress.onLogStartOffsetAdvanced(partition, 40L);

        assertEquals(60L, progress.partitionProgress(partition).orElseThrow().logStartOffset());
    }

    @Test
    void nativeTruncationCanLowerStartWithoutPreservingStaleHighWatermark() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(2L, 3L, 2);
        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 80L);
        progress.onLeader(partition);
        progress.onLogStartOffsetAdvanced(partition, 60L);

        progress.onHighWatermarkUpdated(partition, 30L);
        progress.onLogRebased(partition, 15L);

        SharedCommitProgress.PartitionProgress observed = progress.partitionProgress(partition).orElseThrow();
        assertEquals(15L, observed.logStartOffset());
        assertEquals(30L, observed.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.LEADER, observed.role());
    }

    @Test
    void observationsBeforeNativeLogInitializationDoNotInventAPartition() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(2L, 3L, 3);

        progress.onLogStartOffsetAdvanced(partition, 100L);
        progress.onLogRebased(partition, 5L);

        assertFalse(progress.partitionProgress(partition).isPresent());
        progress.onLogLoaded(partition, 11L);
        assertEquals(11L, progress.partitionProgress(partition).orElseThrow().logStartOffset());
    }

    @Test
    void lateNativeCallbacksAfterReplicaRemovalCannotResurrectUploadProgress() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(2L, 3L, 4);
        progress.onLogLoaded(partition, 50L);
        progress.onLeader(partition);
        progress.remove(partition);

        progress.onLogStartOffsetAdvanced(partition, 70L);
        progress.onLogRebased(partition, 20L);

        assertFalse(progress.partitionProgress(partition).isPresent());
    }

    @Test
    void repeatedSuccessfulLogStartObservationIsIdempotent() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(2L, 3L, 5);
        progress.onLogLoaded(partition, 10L);

        progress.onLogStartOffsetAdvanced(partition, 40L);
        progress.onLogStartOffsetAdvanced(partition, 40L);

        assertEquals(40L, progress.partitionProgress(partition).orElseThrow().logStartOffset());
    }

    @Test
    void quarantineClearsPriorCommitWindowsAndAllUploadEligibility() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(10L, 11L, 0);
        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 80L);
        progress.onLeader(partition);
        assertTrue(progress.partitionProgress(partition).orElseThrow().isLeader());

        progress.disableForRetirementQuarantine();

        assertTrue(progress.isDisabledForRetirementQuarantine());
        assertTrue(progress.snapshot().isEmpty());
        assertTrue(progress.partitionProgress(partition).isEmpty());
        assertTrue(progress.highWatermark(partition).isEmpty());
    }

    @Test
    void quarantinedProgressCannotBeRestoredByLateKafkaCallbacks() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(10L, 11L, 1);
        progress.onLogLoaded(partition, 10L);
        progress.disableForRetirementQuarantine();

        progress.onLogLoaded(partition, 20L);
        progress.onHighWatermarkUpdated(partition, 80L);
        progress.onLogStartOffsetAdvanced(partition, 40L);
        progress.onLogRebased(partition, 5L);
        progress.onLeader(partition);
        progress.onFollower(partition);

        assertTrue(progress.snapshot().isEmpty());
        assertTrue(progress.partitionProgress(partition).isEmpty());
    }

    @Test
    void quarantineIsIdempotentEvenAfterAdditionalPartitionCallbacks() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId one = new SharedPartitionId(10L, 11L, 0);
        SharedPartitionId two = new SharedPartitionId(10L, 11L, 2);
        progress.onLogLoaded(one, 10L);
        progress.disableForRetirementQuarantine();
        progress.disableForRetirementQuarantine();
        progress.onLogLoaded(two, 100L);
        progress.onLeader(two);

        assertTrue(progress.isDisabledForRetirementQuarantine());
        assertTrue(progress.snapshot().isEmpty());
        assertTrue(progress.highWatermark(two).isEmpty());
    }

    @Test
    void quarantineClosesAdmissionButPreservesAlreadyAdmittedUpload() {
        SharedCommitProgress progress = new SharedCommitProgress();
        assertTrue(progress.tryAcquireUploadAdmission());
        assertEquals(1L, progress.activeUploadAdmissions());

        progress.disableForRetirementQuarantine();

        assertTrue(progress.isDisabledForRetirementQuarantine());
        assertFalse(progress.tryAcquireUploadAdmission());
        assertEquals(1L, progress.activeUploadAdmissions());
        progress.releaseUploadAdmission();
        assertEquals(0L, progress.activeUploadAdmissions());
        assertFalse(progress.tryAcquireUploadAdmission());
    }

    @Test
    void everyAdmittedUploadReleasesItsOwnCounterEvenAfterQuarantine() {
        SharedCommitProgress progress = new SharedCommitProgress();
        assertTrue(progress.tryAcquireUploadAdmission());
        assertTrue(progress.tryAcquireUploadAdmission());
        assertTrue(progress.tryAcquireUploadAdmission());
        progress.disableForRetirementQuarantine();

        progress.releaseUploadAdmission();
        progress.releaseUploadAdmission();
        assertEquals(1L, progress.activeUploadAdmissions());
        assertFalse(progress.tryAcquireUploadAdmission());
        progress.releaseUploadAdmission();
        progress.disableForRetirementQuarantine();

        assertEquals(0L, progress.activeUploadAdmissions());
        assertTrue(progress.isDisabledForRetirementQuarantine());
    }

    @Test
    void admissionReleaseWithoutAnOwnerFailsClosed() {
        SharedCommitProgress progress = new SharedCommitProgress();
        assertThrows(IllegalStateException.class, progress::releaseUploadAdmission);
        assertTrue(progress.tryAcquireUploadAdmission());
        progress.releaseUploadAdmission();
        assertThrows(IllegalStateException.class, progress::releaseUploadAdmission);
        progress.disableForRetirementQuarantine();
        assertThrows(IllegalStateException.class, progress::releaseUploadAdmission);
    }

    @Test
    void concurrentQuarantineAndAdmissionHaveOneCasOrdering() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            for (int attempt = 0; attempt < 32; attempt++) {
                SharedCommitProgress progress = new SharedCommitProgress();
                CountDownLatch start = new CountDownLatch(1);
                CompletableFuture<Boolean> admitted = CompletableFuture.supplyAsync(() -> {
                    await(start);
                    return progress.tryAcquireUploadAdmission();
                }, workers);
                CompletableFuture<Void> quarantine = CompletableFuture.runAsync(() -> {
                    await(start);
                    progress.disableForRetirementQuarantine();
                }, workers);
                start.countDown();

                boolean accepted = admitted.get(10, TimeUnit.SECONDS);
                quarantine.get(10, TimeUnit.SECONDS);
                assertTrue(progress.isDisabledForRetirementQuarantine());
                assertFalse(progress.tryAcquireUploadAdmission());
                assertEquals(accepted ? 1L : 0L, progress.activeUploadAdmissions());
                if (accepted) {
                    progress.releaseUploadAdmission();
                }
                assertEquals(0L, progress.activeUploadAdmissions());
            }
        } finally {
            workers.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while racing admission and quarantine", e);
        }
    }

    @Test
    void staleRoleAdmissionRevisionNeverSurvivesLeaderFollowerLeaderAba() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(12L, 13L, 0);
        progress.onLogLoaded(partition, 0L);
        progress.onHighWatermarkUpdated(partition, 10L);
        progress.onLeader(partition);
        long originalRevision = progress.uploadRoleRevision();
        assertTrue(progress.tryAcquireUploadAdmission(originalRevision));

        progress.onFollower(partition);
        progress.onLeader(partition);

        assertFalse(progress.tryAcquireUploadAdmission(originalRevision));
        assertTrue(progress.uploadRoleRevision() > originalRevision);
        assertTrue(progress.partitionProgress(partition).orElseThrow().isLeader());
        assertEquals(1L, progress.activeUploadAdmissions());
        assertTrue(progress.tryAcquireUploadAdmission(progress.uploadRoleRevision()));
        progress.releaseUploadAdmission();
        progress.releaseUploadAdmission();
        assertEquals(0L, progress.activeUploadAdmissions());
    }

    @Test
    void concurrentRoleDemotionAndUploadAdmissionHaveOneCasOrdering() throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            for (int attempt = 0; attempt < 32; attempt++) {
                SharedCommitProgress progress = new SharedCommitProgress();
                SharedPartitionId partition = new SharedPartitionId(12L, 13L, 1);
                progress.onLeader(partition);
                long revision = progress.uploadRoleRevision();
                CountDownLatch start = new CountDownLatch(1);
                CompletableFuture<Boolean> admission = CompletableFuture.supplyAsync(() -> {
                    await(start);
                    return progress.tryAcquireUploadAdmission(revision);
                }, workers);
                CompletableFuture<Void> demotion = CompletableFuture.runAsync(() -> {
                    await(start);
                    progress.onFollower(partition);
                }, workers);
                start.countDown();

                boolean accepted = admission.get(10, TimeUnit.SECONDS);
                demotion.get(10, TimeUnit.SECONDS);
                assertFalse(progress.tryAcquireUploadAdmission(revision));
                assertEquals(accepted ? 1L : 0L, progress.activeUploadAdmissions());
                if (accepted) {
                    progress.releaseUploadAdmission();
                }
                assertEquals(0L, progress.activeUploadAdmissions());
            }
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    void rejectsNegativeOffsets() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 5);

        assertThrows(
            IllegalArgumentException.class,
            () -> progress.onHighWatermarkUpdated(partition, -1L)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> progress.onLogLoaded(partition, -1L)
        );
        assertThrows(IllegalArgumentException.class, () -> progress.onLogStartOffsetAdvanced(partition, -1L));
        assertThrows(IllegalArgumentException.class, () -> progress.onLogRebased(partition, -1L));
    }
}
