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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaObjectMetadataStoreTest {
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
