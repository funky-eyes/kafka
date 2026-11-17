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

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FencedObjectLifecycleArbiterTest {
    private static SharedObjectMetadata descriptor(long id) {
        return new SharedObjectMetadata(id, 128L, 71L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 0),
                new OffsetRange(0L, 32L), 3, 0L, 32, 51L)
        ));
    }

    private static MockProducer<byte[], byte[]> producer() {
        return new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
    }

    @Test
    void sealCapturesHorizonAfterFencingAndWaitsUntilImageReplaysCommit() throws Exception {
        SharedObjectMetadata descriptor = descriptor(1L);
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        MockProducer<byte[], byte[]> producer = producer();
        CompletableFuture<KafkaObjectMetadataStore.ReadCommittedReplayHorizon> horizon = new CompletableFuture<>();
        CountDownLatch callbackEntered = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            var result = worker.submit(() -> FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                producer, "metadata", descriptor, 7L, 3L, () -> {
                    assertTrue(producer.transactionInitialized());
                    callbackEntered.countDown();
                    return KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(
                        image, descriptor, 7L, () -> horizon, 10L, TimeUnit.SECONDS);
                }));
            assertTrue(callbackEntered.await(10, TimeUnit.SECONDS));
            assertFalse(result.isDone());
            assertTrue(producer.history().isEmpty());
            image.applyFromMetadataLog(SharedMetadataRecordCodec.objectKey(descriptor.objectId()),
                SharedMetadataRecordCodec.fencedCommittedObjectValue(descriptor), 7L);
            horizon.complete(new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(8L, 8L));
            assertTrue(result.get(10, TimeUnit.SECONDS).isPresent());
            assertTrue(producer.transactionCommitted());
            assertEquals(1, producer.history().size());
            var record = producer.history().get(0);
            var key = SharedMetadataRecordCodec.decodeKey(record.key());
            assertEquals(SharedMetadataRecordCodec.KeyType.OBJECT_CATALOG_SEAL, key.type());
            var seal = (SharedMetadataRecordCodec.ObjectCatalogSealValue)
                SharedMetadataRecordCodec.decodeValue(key, record.value());
            assertEquals(7L, seal.originalCommitOffset());
            assertEquals(3L, seal.retirementEpoch());
            assertEquals(SharedMetadataRecordCodec.committedDescriptorSha256(descriptor), seal.descriptorSha256());
        } finally {
            horizon.cancel(false);
            worker.shutdownNow();
        }
    }

    @Test
    void incompleteOrFailedHorizonNeverWritesASeal() throws Exception {
        SharedObjectMetadata descriptor = descriptor(2L);
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectKey(descriptor.objectId()),
            SharedMetadataRecordCodec.fencedCommittedObjectValue(descriptor), 7L);
        image.markReady();
        MockProducer<byte[], byte[]> tooOld = producer();
        assertTrue(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
            tooOld, "metadata", descriptor, 7L, 3L, () ->
                KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(image, descriptor, 7L,
                    () -> CompletableFuture.completedFuture(
                        new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(7L, 8L)), 1L, TimeUnit.SECONDS)
        ).isEmpty());
        assertTrue(tooOld.history().isEmpty());
        assertFalse(tooOld.transactionInFlight());

        MockProducer<byte[], byte[]> timedOut = producer();
        var pending = new CompletableFuture<KafkaObjectMetadataStore.ReadCommittedReplayHorizon>();
        assertThrows(TimeoutException.class, () -> FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
            timedOut, "metadata", descriptor, 7L, 3L, () ->
                KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(image, descriptor, 7L,
                    () -> pending, 1L, TimeUnit.MILLISECONDS)));
        assertTrue(pending.isCancelled());
        assertTrue(timedOut.history().isEmpty());
        assertFalse(timedOut.transactionInFlight());

        MockProducer<byte[], byte[]> failed = producer();
        var failedHorizon = CompletableFuture.<KafkaObjectMetadataStore.ReadCommittedReplayHorizon>failedFuture(
            new IllegalStateException("replay failed"));
        assertThrows(ExecutionException.class, () -> FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
            failed, "metadata", descriptor, 7L, 3L, () ->
                KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(image, descriptor, 7L,
                    () -> failedHorizon, 1L, TimeUnit.SECONDS)));
        assertTrue(failed.history().isEmpty());
    }

    @Test
    void postFenceImageMismatchOrFailedImageNeverWritesASeal() throws Exception {
        SharedObjectMetadata descriptor = descriptor(3L);
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectKey(descriptor.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(descriptor), 7L);
        image.markReady();
        MockProducer<byte[], byte[]> legacy = producer();
        assertTrue(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
            legacy, "metadata", descriptor, 7L, 3L, () ->
                KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(image, descriptor, 7L,
                    () -> CompletableFuture.completedFuture(
                        new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(8L, 8L)), 1L, TimeUnit.SECONDS)
        ).isEmpty());
        assertTrue(legacy.history().isEmpty());
        image.markFailed(new IllegalStateException("consumer stopped"));
        MockProducer<byte[], byte[]> failed = producer();
        assertThrows(IllegalStateException.class, () -> FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
            failed, "metadata", descriptor, 7L, 3L, () ->
                KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(image, descriptor, 7L,
                    () -> CompletableFuture.completedFuture(
                        new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(8L, 8L)), 1L, TimeUnit.SECONDS)));
        assertTrue(failed.history().isEmpty());
    }

    @Test
    void postFenceWitnessRejectsWrongCommitOrExistingSeal() throws Exception {
        SharedObjectMetadata descriptor = descriptor(4L);
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectKey(descriptor.objectId()),
            SharedMetadataRecordCodec.fencedCommittedObjectValue(descriptor), 7L);
        var horizon = CompletableFuture.completedFuture(
            new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(8L, 8L));
        assertThrows(IllegalStateException.class, () -> KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(
            image, descriptor, 7L, () -> horizon, 1L, TimeUnit.SECONDS));
        image.markReady();
        assertFalse(KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(
            image, descriptor, 6L, () -> horizon, 1L, TimeUnit.SECONDS));
        SharedObjectMetadata altered = new SharedObjectMetadata(
            descriptor.objectId(), descriptor.objectSize(), descriptor.objectChecksum() + 1L, descriptor.ranges());
        assertFalse(KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(
            image, altered, 7L, () -> horizon, 1L, TimeUnit.SECONDS));
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectCatalogSealKey(descriptor.objectId()),
            SharedMetadataRecordCodec.objectCatalogSealValue(7L, 3L, descriptor), 8L);
        assertFalse(KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(
            image, descriptor, 7L, () -> horizon, 1L, TimeUnit.SECONDS));
    }

    @Test
    void closeDuringReplayWaitPreventsSealAndDoesNotBlockConsumerProgress() throws Exception {
        SharedObjectMetadata descriptor = descriptor(5L);
        MockProducer<byte[], byte[]> producer = producer();
        AtomicBoolean open = new AtomicBoolean(true);
        CountDownLatch enteredWitness = new CountDownLatch(1);
        CountDownLatch releaseWitness = new CountDownLatch(1);
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try {
            var result = worker.submit(() -> FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                producer, "metadata", descriptor, 7L, 3L, () -> {
                    enteredWitness.countDown();
                    assertTrue(releaseWitness.await(10L, TimeUnit.SECONDS));
                    return true;
                }, open::get));
            assertTrue(enteredWitness.await(10L, TimeUnit.SECONDS));
            open.set(false);
            releaseWitness.countDown();
            assertTrue(result.get(10L, TimeUnit.SECONDS).isEmpty());
            assertTrue(producer.history().isEmpty());
        } finally {
            releaseWitness.countDown();
            worker.shutdownNow();
        }
    }

    @Test
    void failureRacingAnAlreadyStartedSealIsAnUncertainOutcome() throws Exception {
        SharedObjectMetadata descriptor = descriptor(6L);
        CountDownLatch sendEntered = new CountDownLatch(1);
        MockProducer<byte[], byte[]> producer = new MockProducer<>(false, null,
            new ByteArraySerializer(), new ByteArraySerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<byte[], byte[]> record) {
                Future<RecordMetadata> pending = super.send(record);
                sendEntered.countDown();
                return pending;
            }
        };
        AtomicBoolean open = new AtomicBoolean(true);
        ExecutorService workers = Executors.newSingleThreadExecutor();
        try {
            var result = workers.submit(() -> FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                producer, "metadata", descriptor, 7L, 3L, () -> true, open::get));
            assertTrue(sendEntered.await(10L, TimeUnit.SECONDS));
            // This models close or replay failure after the last admission
            // check: it must not be represented as a proven non-commit.
            open.set(false);
            assertTrue(producer.completeNext());
            assertTrue(result.get(10L, TimeUnit.SECONDS).isPresent());
            SharedMetadataImage image = new SharedMetadataImage();
            image.markFailed(new IllegalStateException("consumer failed"));
            var replay = CompletableFuture.completedFuture(
                new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(9L, 9L));
            var confirmation = KafkaObjectMetadataStore.confirmExperimentalCatalogSeal(
                image, descriptor.objectId(), 7L, 3L, 8L, replay, 1L, TimeUnit.SECONDS);
            assertThrows(ExecutionException.class, () -> confirmation.get(1L, TimeUnit.SECONDS));
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    void timedOutConfirmationReleasesHorizonAndDoesNotClaimRetrySucceeded() throws Exception {
        SharedObjectMetadata descriptor = descriptor(7L);
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectKey(descriptor.objectId()),
            SharedMetadataRecordCodec.fencedCommittedObjectValue(descriptor), 7L);
        image.markReady();
        var mailbox = new KafkaObjectMetadataStore.ReplayHorizonMailbox(1);
        var request = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        assertTrue(mailbox.offer(request));
        var result = KafkaObjectMetadataStore.confirmExperimentalCatalogSeal(
            image, descriptor.objectId(), 7L, 3L, 8L, request.future(), 1L, TimeUnit.MILLISECONDS);
        var failure = assertThrows(ExecutionException.class, () -> result.get(10L, TimeUnit.SECONDS));
        assertTrue(failure.getCause() instanceof TimeoutException);
        var next = new KafkaObjectMetadataStore.ReplayHorizonRequest();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        boolean admitted = false;
        while (!admitted && System.nanoTime() < deadline) {
            admitted = mailbox.offer(next);
            if (!admitted) {
                Thread.sleep(1L);
            }
        }
        assertTrue(admitted, "Timed-out horizon must release its bounded mailbox slot");

        // Kafka may have committed despite the timed-out observation. A
        // subsequent witness must reject the already sealed identity.
        image.applyFromMetadataLog(SharedMetadataRecordCodec.objectCatalogSealKey(descriptor.objectId()),
            SharedMetadataRecordCodec.objectCatalogSealValue(7L, 3L, descriptor), 8L);
        assertFalse(KafkaObjectMetadataStore.awaitCatalogCandidateAfterFence(image, descriptor, 7L,
            () -> CompletableFuture.completedFuture(
                new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(9L, 9L)), 1L, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class, () -> result.get(1L, TimeUnit.SECONDS));
        var fresh = KafkaObjectMetadataStore.confirmExperimentalCatalogSeal(image, descriptor.objectId(),
            7L, 3L, 8L, CompletableFuture.completedFuture(
                new KafkaObjectMetadataStore.ReadCommittedReplayHorizon(9L, 9L)), 1L, TimeUnit.SECONDS);
        assertEquals(OptionalLong.of(8L), fresh.get(1L, TimeUnit.SECONDS));
    }
}
