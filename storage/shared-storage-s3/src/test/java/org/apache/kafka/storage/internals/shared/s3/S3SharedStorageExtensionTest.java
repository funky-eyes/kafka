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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SharedStorageExtensionTest {
    @TempDir
    Path tempDir;

    @Test
    void rejectsFencedLifecycleBeforeWalInitialization() throws Exception {
        String key = SharedMetadataClientConfiguration.FENCED_LIFECYCLE_CONFIG;
        for (Object enabled : List.of(true, " TrUe ")) {
            StorageExtensionContext context = new StorageExtensionContext(
                Map.of(key, enabled), List.of(tempDir.toFile()), 1, Time.SYSTEM);
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> new S3SharedStorageExtension().start(context));
            assertTrue(failure.getMessage().contains(key));
        }
        try (var files = Files.list(tempDir)) {
            assertEquals(0L, files.count(), "Rejection must precede WAL or marker creation");
        }
        StorageExtensionContext invalid = new StorageExtensionContext(
            Map.of(key, "invalid"), List.of(tempDir.toFile()), 1, Time.SYSTEM);
        assertThrows(IllegalArgumentException.class, () -> new S3SharedStorageExtension().start(invalid));
    }

    @Test
    void rejectsFencedLifecycleIntroducedAtBrokerReady() {
        String key = SharedMetadataClientConfiguration.FENCED_LIFECYCLE_CONFIG;
        List<Endpoint> listeners = List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092));
        StorageExtensionBrokerContext enabled = new StorageExtensionBrokerContext(
            "cluster", 1, listeners, Map.of(key, true), Time.SYSTEM);
        CompletionException rejection = assertThrows(CompletionException.class,
            () -> new S3SharedStorageExtension().onBrokerReady(enabled).join());
        assertInstanceOf(IllegalStateException.class, rejection.getCause());
        assertTrue(rejection.getCause().getMessage().contains(key));

        StorageExtensionBrokerContext disabled = new StorageExtensionBrokerContext(
            "cluster", 1, listeners, Map.of(key, false), Time.SYSTEM);
        CompletionException normal = assertThrows(CompletionException.class,
            () -> new S3SharedStorageExtension().onBrokerReady(disabled).join());
        assertInstanceOf(IllegalStateException.class, normal.getCause());
        assertEquals("S3 shared storage extension has not been started", normal.getCause().getMessage());
    }

    @Test
    void waitsForBootstrapExecutorToTerminateBeforeReturning() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger interrupts = new AtomicInteger();
        try {
            executor.submit(() -> {
                started.countDown();
                boolean released = false;
                while (!released) {
                    try {
                        release.await();
                        released = true;
                    } catch (InterruptedException ignored) {
                        interrupts.incrementAndGet();
                    }
                }
            });
            assertTrue(started.await(10, TimeUnit.SECONDS), "Bootstrap task did not start");

            executor.shutdownNow();
            CompletableFuture<Void> waiter = CompletableFuture.runAsync(
                () -> S3SharedStorageExtension.awaitExecutorStop(executor)
            );
            assertThrows(
                TimeoutException.class,
                () -> waiter.get(200, TimeUnit.MILLISECONDS),
                "Shutdown must not return while the bootstrap task can still access shared resources"
            );

            release.countDown();
            waiter.get(10, TimeUnit.SECONDS);
            assertTrue(executor.isTerminated());
            assertTrue(interrupts.get() > 0, "Bootstrap task must have observed shutdown interruption");
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "Bootstrap executor did not terminate");
        }
    }
}
