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
package org.apache.kafka.storage.internals.shared.s3;

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.storage.internals.shared.metadata.PartitionLogStartAdvancePrecheck;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaObjectMetadataStoreTest {
    @Test
    void ordinaryMetadataProducerCannotEmitUnfencedRetirementWatermark() {
        org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId partition =
            new org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId(1L, 2L, 0);
        assertThrows(IllegalArgumentException.class, () ->
            KafkaObjectMetadataStore.rejectUnfencedRetirementWrite(
                org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.partitionLogStartKey(
                    partition
                )
            )
        );
    }

    @Test
    void ordinaryMetadataProducerStillAcceptsExistingMetadataKeys() {
        KafkaObjectMetadataStore.rejectUnfencedRetirementWrite(
            org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.objectKey(1L)
        );
        KafkaObjectMetadataStore.rejectUnfencedRetirementWrite(
            org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.objectCleanupKey(1L)
        );
        KafkaObjectMetadataStore.rejectUnfencedRetirementWrite(
            org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.brokerSequenceKey(1)
        );
    }

    @Test
    void cleanupCompletionOnlyAcknowledgesClaimedOrDurablyDeletedObjects() {
        SharedMetadataImage image = new SharedMetadataImage();
        long claimed = 301L;
        long deleted = 302L;
        long prepared = 303L;
        long unknown = 304L;
        image.apply(
            SharedMetadataRecordCodec.objectCleanupKey(claimed),
            SharedMetadataRecordCodec.cleanupClaimedValue(1_000L)
        );
        image.apply(
            SharedMetadataRecordCodec.objectCleanupKey(deleted),
            SharedMetadataRecordCodec.cleanupDeletedValue(2_000L)
        );
        image.apply(
            SharedMetadataRecordCodec.objectKey(prepared),
            SharedMetadataRecordCodec.preparedObjectValue(3_000L)
        );
        image.markReady();

        assertEquals(OptionalLong.of(1_000L), KafkaObjectMetadataStore.completionTarget(image, claimed));
        assertEquals(OptionalLong.empty(), KafkaObjectMetadataStore.completionTarget(image, deleted));
        assertThrows(
            IllegalStateException.class,
            () -> KafkaObjectMetadataStore.completionTarget(image, prepared)
        );
        assertThrows(
            IllegalStateException.class,
            () -> KafkaObjectMetadataStore.completionTarget(image, unknown)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaObjectMetadataStore.completionTarget(image, 0L)
        );
    }

    @Test
    void closeAggregationAttemptsEveryMetadataClientResource() {
        AtomicInteger closed = new AtomicInteger();
        RuntimeException first = new IllegalStateException("first");
        RuntimeException failure = null;
        failure = KafkaObjectMetadataStore.closeResource(failure, () -> {
            closed.incrementAndGet();
            throw first;
        });
        failure = KafkaObjectMetadataStore.closeResource(failure, () -> {
            closed.incrementAndGet();
            throw new IllegalArgumentException("second");
        });
        failure = KafkaObjectMetadataStore.closeResource(failure, closed::incrementAndGet);

        assertTrue(failure == first);
        assertTrue(failure.getSuppressed().length == 1);
        assertTrue(closed.get() == 3);
    }

    @Test
    void waitsForConsumerThreadToExitBeforeClosingKafkaClients() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch wakeupCalled = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Thread consumerThread = new Thread(() -> {
            started.countDown();
            while (release.getCount() > 0) {
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // Model work outside KafkaConsumer.poll which can delay shutdown after wakeup.
                }
            }
        });
        consumerThread.start();
        assertTrue(started.await(10, TimeUnit.SECONDS), "Metadata consumer test thread did not start");

        CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(() ->
            KafkaObjectMetadataStore.awaitConsumerThreadStop(
                consumerThread,
                () -> {
                    wakeupCalled.countDown();
                    consumerThread.interrupt();
                },
                100L
            )
        );
        assertTrue(wakeupCalled.await(10, TimeUnit.SECONDS), "Close path did not retry metadata consumer wakeup");
        assertFalse(waiter.isDone(), "close wait must not finish while metadata consumer thread is still alive");

        release.countDown();
        assertFalse(waiter.get(10, TimeUnit.SECONDS));
        consumerThread.join(10_000L);
        assertFalse(consumerThread.isAlive());
    }

    @Test
    void lateAppliedOffsetWaiterFailsAfterConsumerFailure() throws Exception {
        KafkaObjectMetadataStore.AppliedOffsetTracker tracker =
            new KafkaObjectMetadataStore.AppliedOffsetTracker();
        RuntimeException consumerFailure = new RuntimeException("consumer failed");

        tracker.fail(consumerFailure);
        CompletableFuture<Void> lateWaiter = tracker.awaitApplied(42L);

        ExecutionException failure = assertThrows(
            ExecutionException.class,
            () -> lateWaiter.get(10, TimeUnit.SECONDS)
        );
        assertSame(consumerFailure, failure.getCause());
    }

    @Test
    void appliedOffsetRemainsSuccessfulAfterLaterConsumerFailure() throws Exception {
        KafkaObjectMetadataStore.AppliedOffsetTracker tracker =
            new KafkaObjectMetadataStore.AppliedOffsetTracker();

        tracker.markApplied(42L);
        tracker.fail(new RuntimeException("consumer failed"));

        tracker.awaitApplied(42L).get(10, TimeUnit.SECONDS);
    }

    @Test
    void capturedReadCommittedLsoWaitsForAppliedConsumerPositionAcrossCompactionGaps() throws Exception {
        KafkaObjectMetadataStore.ReplayHorizonRequest request =
            new KafkaObjectMetadataStore.ReplayHorizonRequest();
        request.captureTarget(42L);
        assertFalse(request.completeIfApplied(37L));
        assertFalse(request.future().isDone());
        // Kafka compaction can omit offset 41: last record offset is not
        // evidence that the read-committed consumer cursor is behind LSO 42.
        assertTrue(request.completeIfApplied(42L));
        var snapshot = request.future().get(10, TimeUnit.SECONDS);
        assertEquals(42L, snapshot.capturedStableEndExclusive());
        assertEquals(42L, snapshot.appliedPositionExclusive());
    }

    @Test
    void emptyReadCommittedMetadataTopicCanCaptureZeroWithoutRecords() throws Exception {
        KafkaObjectMetadataStore.ReplayHorizonRequest request =
            new KafkaObjectMetadataStore.ReplayHorizonRequest();
        request.captureTarget(0L);
        assertTrue(request.completeIfApplied(0L));
        assertEquals(0L, request.future().get(10, TimeUnit.SECONDS).capturedStableEndExclusive());
    }

    @Test
    void replayHorizonPreservesCapturedLsoSeparatelyFromLaterConsumerProgress() throws Exception {
        KafkaObjectMetadataStore.ReplayHorizonRequest request =
            new KafkaObjectMetadataStore.ReplayHorizonRequest();
        request.captureTarget(12L);
        assertTrue(request.completeIfApplied(20L));
        var snapshot = request.future().get(10, TimeUnit.SECONDS);
        assertEquals(12L, snapshot.capturedStableEndExclusive());
        assertEquals(20L, snapshot.appliedPositionExclusive());
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(12L, 11L));
    }

    @Test
    void malformedOrUncapturedReadCommittedHorizonFailsClosed() {
        KafkaObjectMetadataStore.ReplayHorizonRequest request =
            new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertThrows(IllegalStateException.class, () -> request.completeIfApplied(0L));
        assertThrows(IllegalArgumentException.class, () -> request.captureTarget(-1L));
        request.captureTarget(7L);
        assertThrows(IllegalArgumentException.class, () -> request.captureTarget(9L));
        assertThrows(IllegalStateException.class, () -> request.completeIfApplied(-1L));
        assertFalse(request.future().isDone());
    }

    @Test
    void failedReadCommittedHorizonNeverReopensAfterLateConsumerProgress() throws Exception {
        KafkaObjectMetadataStore.ReplayHorizonRequest request =
            new KafkaObjectMetadataStore.ReplayHorizonRequest();
        request.captureTarget(20L);
        RuntimeException failure = new IllegalStateException("metadata consumer stopped");
        request.fail(failure);
        assertFalse(request.completeIfApplied(20L));
        ExecutionException observed = assertThrows(
            ExecutionException.class, () -> request.future().get(10, TimeUnit.SECONDS));
        assertSame(failure, observed.getCause());
    }

    @Test
    void pendingHorizonMailboxRejectsExcessRequestsWithoutBlockingTheConsumer() {
        KafkaObjectMetadataStore.ReplayHorizonMailbox mailbox =
            new KafkaObjectMetadataStore.ReplayHorizonMailbox(1);
        var first = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        var excess = new KafkaObjectMetadataStore.ReplayHorizonRequest();

        assertTrue(mailbox.offer(first));
        assertFalse(mailbox.offer(excess));
        assertEquals(1, mailbox.pending());
        first.future().cancel(false);
        assertEquals(0, mailbox.pending());
        assertTrue(mailbox.offer(excess));
        assertEquals(1, mailbox.pending());
        excess.fail(new IllegalStateException("consumer failed"));
        assertEquals(0, mailbox.pending());
    }

    @Test
    void closingReplayHorizonMailboxFailsAndReleasesEveryOutstandingRequest() {
        KafkaObjectMetadataStore.ReplayHorizonMailbox mailbox =
            new KafkaObjectMetadataStore.ReplayHorizonMailbox(2);
        var first = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        var second = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertTrue(mailbox.offer(first));
        assertTrue(mailbox.offer(second));
        mailbox.failAll(new IllegalStateException("consumer closed"));
        assertTrue(first.future().isCompletedExceptionally());
        assertTrue(second.future().isCompletedExceptionally());
        assertEquals(0, mailbox.pending());
        assertTrue(mailbox.offer(new KafkaObjectMetadataStore.ReplayHorizonRequest()));
    }

    @Test
    void oneReplayHorizonProbeFailureDoesNotFailOtherPendingProbes() throws Exception {
        var mailbox = new KafkaObjectMetadataStore.ReplayHorizonMailbox(2);
        var timedOut = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        var next = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertTrue(mailbox.offer(timedOut));
        assertTrue(mailbox.offer(next));

        timedOut.fail(new TimeoutException("read-committed LSO lookup timed out"));
        assertTrue(timedOut.future().isCompletedExceptionally());
        assertSame(next, mailbox.peek());
        next.captureTarget(4L);
        assertTrue(next.completeIfApplied(4L));
        assertEquals(4L, next.future().get(10, TimeUnit.SECONDS).capturedStableEndExclusive());
        assertEquals(0, mailbox.pending());
    }

    @Test
    void replayHorizonMailboxNeverAdmitsAnAlreadyFailedOrCanceledFuture() {
        var mailbox = new KafkaObjectMetadataStore.ReplayHorizonMailbox(1);
        var canceled = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        canceled.future().cancel(false);
        var failed = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        failed.fail(new IllegalStateException("consumer closed"));

        assertFalse(mailbox.offer(canceled));
        assertFalse(mailbox.offer(failed));
        assertEquals(0, mailbox.pending());
        var fresh = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertTrue(mailbox.offer(fresh));
        assertEquals(1, mailbox.pending());
        fresh.future().cancel(false);
        assertEquals(0, mailbox.pending());
    }

    @Test
    void replayHorizonMailboxDoesNotConsumeTwoPermitsForTheSameRequest() {
        var mailbox = new KafkaObjectMetadataStore.ReplayHorizonMailbox(2);
        var request = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertTrue(mailbox.offer(request));
        assertFalse(mailbox.offer(request));
        assertEquals(1, mailbox.pending());
        request.fail(new IllegalStateException("metadata unavailable"));
        assertEquals(0, mailbox.pending());
        var one = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        var two = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertTrue(mailbox.offer(one));
        assertTrue(mailbox.offer(two));
        one.future().cancel(false);
        two.future().cancel(false);
        assertEquals(0, mailbox.pending());
    }

    @Test
    void waitingReplayHorizonExpiresOnMonotonicDeadlineAndReleasesCapacity() throws Exception {
        var mailbox = new KafkaObjectMetadataStore.ReplayHorizonMailbox(1);
        var request = new KafkaObjectMetadataStore.ReplayHorizonRequest(100L);
        assertTrue(mailbox.offer(request));
        request.captureTarget(42L);
        assertFalse(request.completeIfApplied(41L));
        assertFalse(request.failIfExpired(1_099L, 1_000L));
        assertTrue(request.failIfExpired(1_100L, 1_000L));
        assertEquals(0, mailbox.pending());
        assertFalse(request.completeIfApplied(42L));
        assertTrue(request.future().isCompletedExceptionally());
        assertTrue(mailbox.offer(new KafkaObjectMetadataStore.ReplayHorizonRequest()));
    }

    @Test
    void replayHorizonDeadlineIsWrapSafeAndNeverRevokesCompletedEvidence() {
        var timedOut = new KafkaObjectMetadataStore.ReplayHorizonRequest(Long.MAX_VALUE - 4L);
        assertFalse(timedOut.failIfExpired(Long.MIN_VALUE + 4L, 10L));
        assertTrue(timedOut.failIfExpired(Long.MIN_VALUE + 5L, 10L));

        var complete = new KafkaObjectMetadataStore.ReplayHorizonRequest(10L);
        complete.captureTarget(0L);
        assertTrue(complete.completeIfApplied(0L));
        assertFalse(complete.failIfExpired(100L, 20L));
        assertFalse(complete.future().isCompletedExceptionally());
        assertThrows(IllegalArgumentException.class, () -> complete.failIfExpired(100L, 0L));
    }

    @Test
    void kafkaMetadataReplayProbeTimeoutOnlyFailsThatFuture() throws Exception {
        var request = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        TimeoutException timeout = new TimeoutException("metadata LSO unavailable");
        KafkaObjectMetadataStore.failOnlyOptionalProbeTimeout(request, timeout);
        ExecutionException observed = assertThrows(
            ExecutionException.class, () -> request.future().get(10, TimeUnit.SECONDS)
        );
        assertSame(timeout, observed.getCause());
    }

    @Test
    void fatalMetadataConsumerErrorsCannotBeIsolatedAsProbeTimeouts() {
        var request = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        RuntimeException fatal = new IllegalStateException("invalid Kafka consumer state");
        assertSame(fatal, assertThrows(
            IllegalStateException.class,
            () -> KafkaObjectMetadataStore.failOnlyOptionalProbeTimeout(request, fatal)
        ));
        assertFalse(request.future().isDone());
    }

    @Test
    void kafkaConsumerWakeupRemainsAnOuterLifecycleSignal() {
        var request = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        var wakeup = new org.apache.kafka.common.errors.WakeupException();
        assertSame(wakeup, assertThrows(
            org.apache.kafka.common.errors.WakeupException.class,
            () -> KafkaObjectMetadataStore.failOnlyOptionalProbeTimeout(request, wakeup)
        ));
        assertFalse(request.future().isDone());
    }

    @Test
    void compactedMetadataReplayPreviewUsesRealCursorDespiteMissingRecordOffsets() {
        SharedPartitionId partition = new SharedPartitionId(4L, 5L, 0);
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(partition),
            SharedMetadataRecordCodec.partitionLogStartValue(20L),
            5L
        );
        image.markReady();
        var cursor = new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(12L, 12L);

        var result = KafkaObjectMetadataStore.classifyReadCommittedPartitionLogStart(
            image, partition, 30L, 30L, cursor
        );
        assertEquals(partition, result.partition());
        assertSame(cursor, result.replayHorizon());
        assertEquals(PartitionLogStartAdvancePrecheck.Finding.VALUE_DOMAIN_CANDIDATE, result.finding());
    }

    @Test
    void replayPreviewPreservesImmutableTopicIdAndInitialZero() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        SharedPartitionId original = new SharedPartitionId(4L, 5L, 0);
        SharedPartitionId recreated = new SharedPartitionId(4L, 6L, 0);
        var cursor = new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(0L, 0L);

        var result = KafkaObjectMetadataStore.classifyReadCommittedPartitionLogStart(
            image, original, 0L, 10L, cursor
        );
        assertEquals(PartitionLogStartAdvancePrecheck.Finding.INITIAL_ZERO_CANDIDATE, result.finding());
        assertEquals(recreated, KafkaObjectMetadataStore.classifyReadCommittedPartitionLogStart(
            image, recreated, 0L, 10L, cursor
        ).partition());
        assertEquals(PartitionLogStartAdvancePrecheck.Finding.INITIAL_ZERO_REQUIRED,
            KafkaObjectMetadataStore.classifyReadCommittedPartitionLogStart(
                image, recreated, 2L, 10L, cursor
            ).finding());
    }

    @Test
    void readCommittedPreviewStillRejectsSourceBoundsAndUnavailableImage() {
        SharedPartitionId partition = new SharedPartitionId(4L, 5L, 0);
        SharedMetadataImage ready = new SharedMetadataImage();
        ready.markReady();
        var cursor = new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(2L, 3L);
        assertEquals(PartitionLogStartAdvancePrecheck.Finding.EXCEEDS_OBSERVED_KAFKA_LOG_START,
            KafkaObjectMetadataStore.classifyReadCommittedPartitionLogStart(
                ready, partition, 11L, 10L, cursor
            ).finding());
        assertThrows(IllegalStateException.class, () ->
            KafkaObjectMetadataStore.classifyReadCommittedPartitionLogStart(
                new SharedMetadataImage(), partition, 0L, 0L, cursor
            ));
        assertThrows(IllegalArgumentException.class, () ->
            new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(3L, 2L));
    }

    @Test
    void canceledReadCommittedPartitionPreviewCancelsUnderlyingHorizon() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        var upstream = new CompletableFuture<KafkaObjectMetadataStore.ReadCommittedReplayHorizon>();
        var preview = KafkaObjectMetadataStore.composeReadCommittedPartitionLogStart(
            image, new SharedPartitionId(5L, 6L, 0), 0L, 0L, upstream
        );
        assertFalse(upstream.isDone());
        assertTrue(preview.cancel(false));
        assertTrue(upstream.isCancelled());
    }

    @Test
    void failedReadCommittedHorizonCannotProduceAValuePreview() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        var upstream = new CompletableFuture<KafkaObjectMetadataStore.ReadCommittedReplayHorizon>();
        var preview = KafkaObjectMetadataStore.composeReadCommittedPartitionLogStart(
            image, new SharedPartitionId(5L, 6L, 0), 0L, 0L, upstream
        );
        RuntimeException error = new IllegalStateException("metadata replay failed");
        upstream.completeExceptionally(error);
        ExecutionException failed = assertThrows(
            ExecutionException.class, () -> preview.get(10, TimeUnit.SECONDS)
        );
        assertSame(error, failed.getCause());
    }

    @Test
    void failedMetadataImageAfterHorizonRequestNeverReturnsPreviewFinding() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        var upstream = new CompletableFuture<KafkaObjectMetadataStore.ReadCommittedReplayHorizon>();
        var preview = KafkaObjectMetadataStore.composeReadCommittedPartitionLogStart(
            image, new SharedPartitionId(5L, 6L, 0), 0L, 0L, upstream
        );
        image.markFailed(new IllegalStateException("metadata image poisoned"));
        upstream.complete(new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(0L, 0L));
        ExecutionException failed = assertThrows(
            ExecutionException.class, () -> preview.get(10, TimeUnit.SECONDS)
        );
        assertTrue(failed.getCause() instanceof IllegalStateException);
    }

    @Test
    void completedReadCommittedHorizonClassifiesWithoutMutatingMetadataImage() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        SharedPartitionId partition = new SharedPartitionId(5L, 6L, 0);
        var upstream = new CompletableFuture<KafkaObjectMetadataStore.ReadCommittedReplayHorizon>();
        var preview = KafkaObjectMetadataStore.composeReadCommittedPartitionLogStart(
            image, partition, 0L, 5L, upstream
        );
        assertTrue(upstream.complete(new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(3L, 4L)));
        var finding = preview.join();
        assertEquals(partition, finding.partition());
        assertEquals(3L, finding.replayHorizon().capturedStableEndExclusive());
        assertEquals(PartitionLogStartAdvancePrecheck.Finding.INITIAL_ZERO_CANDIDATE, finding.finding());
        assertTrue(image.partitionLogStartsSnapshot().isEmpty());
    }

    @Test
    void retriesOnlyTransientMetadataTopicCreationFailures() {
        assertTrue(KafkaObjectMetadataStore.isTransientTopicCreationFailure(
            new InvalidReplicationFactorException("not enough brokers yet")));
        assertTrue(KafkaObjectMetadataStore.isTransientTopicCreationFailure(
            new TimeoutException("controller metadata is still converging")));
        assertFalse(KafkaObjectMetadataStore.isTransientTopicCreationFailure(
            new IllegalStateException("permanent configuration failure")));
    }

    @Test
    void retriesTransientRemotePlaneBootstrapFailuresThroughIOExceptionWrapping() {
        assertTrue(S3SharedStorageExtension.isRetriableMetadataBootstrapFailure(
            new IOException(
                "Unable to initialize shared metadata store",
                new UnknownTopicOrPartitionException("metadata partition not locally visible yet")
            )
        ));
        assertTrue(S3SharedStorageExtension.isRetriableMetadataBootstrapFailure(
            new IOException("wrapped", new TimeoutException("metadata still converging"))
        ));
        assertFalse(S3SharedStorageExtension.isRetriableMetadataBootstrapFailure(
            new IOException("permanent", new IllegalStateException("invalid cleanup policy"))
        ));
    }
}
