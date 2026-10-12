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

import org.junit.jupiter.api.Test;

import java.io.IOException;
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
