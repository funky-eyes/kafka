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

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class S3ObjectStoreTest {
    @Test
    void closeRaceReturnsFailedFutureInsteadOfThrowingSynchronously() throws Exception {
        S3ObjectStoreConfig config = S3ObjectStoreConfig.from(Map.of(
            S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data"
        ));
        CloseRaceExecutor executor = new CloseRaceExecutor();
        S3ObjectStore store = new S3ObjectStore(config, S3ObjectStore.buildClient(config), executor);
        AtomicReference<CompletableFuture<Void>> returned = new AtomicReference<>();
        AtomicReference<Throwable> synchronousFailure = new AtomicReference<>();
        Thread submitter = new Thread(() -> {
            try {
                returned.set(store.delete(1L));
            } catch (Throwable t) {
                synchronousFailure.set(t);
            }
        }, "s3-close-race-submitter");

        try {
            submitter.start();
            assertTrue(
                executor.awaitExecuteEntered(10, TimeUnit.SECONDS),
                "S3 async submission did not reach the executor"
            );

            store.close();
            submitter.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(submitter.isAlive(), "S3 async submission did not finish after close");
            assertNull(synchronousFailure.get(), "async API must not throw executor rejection synchronously");
            CompletableFuture<Void> future = returned.get();
            assertNotNull(future, "async API must return a future during close race");

            ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> future.get(10, TimeUnit.SECONDS)
            );
            IllegalStateException closed = assertInstanceOf(IllegalStateException.class, failure.getCause());
            assertTrue(closed.getMessage().contains("closed"));
            assertInstanceOf(RejectedExecutionException.class, closed.getCause());
        } finally {
            store.close();
            submitter.interrupt();
            submitter.join(TimeUnit.SECONDS.toMillis(10));
        }
    }

    @Test
    void waitsForIoExecutorToTerminateBeforeClosingClientResources() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            executor.submit(() -> {
                started.countDown();
                while (release.getCount() > 0) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        interrupted.countDown();
                    }
                }
            });
            assertTrue(started.await(10, TimeUnit.SECONDS), "S3 I/O task did not start");

            executor.shutdownNow();
            CompletableFuture<Boolean> waiter = CompletableFuture.supplyAsync(
                () -> S3ObjectStore.awaitIoExecutorStop(executor)
            );
            assertTrue(interrupted.await(10, TimeUnit.SECONDS), "S3 I/O task did not observe shutdown interruption");
            assertFalse(waiter.isDone(), "close wait must not finish while an S3 I/O task is still alive");

            release.countDown();
            assertFalse(waiter.get(10, TimeUnit.SECONDS));
            assertTrue(executor.isTerminated());
        } finally {
            release.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "S3 I/O executor did not terminate");
        }
    }

    @Test
    void parsesDefaultsAndNormalizesObjectKeys() {
        S3ObjectStoreConfig config = S3ObjectStoreConfig.from(Map.of(
            S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
            S3ObjectStoreConfig.KEY_PREFIX_CONFIG, "/objects/"
        ));

        assertEquals("shared-data", config.bucket());
        assertEquals("objects", config.keyPrefix());
        assertEquals(S3ObjectStoreConfig.DEFAULT_REGION, config.region());
        assertEquals(S3ObjectStoreConfig.DEFAULT_IO_THREADS, config.ioThreads());
        assertEquals(S3ObjectStoreConfig.DEFAULT_CONNECTION_TIMEOUT_MS, config.connectionTimeoutMs());
        assertEquals(S3ObjectStoreConfig.DEFAULT_SOCKET_TIMEOUT_MS, config.socketTimeoutMs());
        assertEquals(S3ObjectStoreConfig.DEFAULT_API_CALL_ATTEMPT_TIMEOUT_MS, config.apiCallAttemptTimeoutMs());
        assertEquals(S3ObjectStoreConfig.DEFAULT_API_CALL_TIMEOUT_MS, config.apiCallTimeoutMs());
        assertEquals(S3ObjectStoreConfig.DEFAULT_MAX_ATTEMPTS, config.maxAttempts());
        assertFalse(config.pathStyleAccess());
        assertTrue(config.endpoint().isEmpty());
        assertEquals("objects/42", config.objectKey(42L));
    }

    @Test
    void parsesExplicitBoundedRequestPolicy() {
        S3ObjectStoreConfig config = S3ObjectStoreConfig.from(Map.of(
            S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
            S3ObjectStoreConfig.CONNECTION_TIMEOUT_MS_CONFIG, 1_000L,
            S3ObjectStoreConfig.SOCKET_TIMEOUT_MS_CONFIG, "2000",
            S3ObjectStoreConfig.API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG, 3_000L,
            S3ObjectStoreConfig.API_CALL_TIMEOUT_MS_CONFIG, "7000",
            S3ObjectStoreConfig.MAX_ATTEMPTS_CONFIG, 2
        ));

        assertEquals(1_000L, config.connectionTimeoutMs());
        assertEquals(2_000L, config.socketTimeoutMs());
        assertEquals(3_000L, config.apiCallAttemptTimeoutMs());
        assertEquals(7_000L, config.apiCallTimeoutMs());
        assertEquals(2, config.maxAttempts());
    }

    @Test
    void rejectsMissingBucketInvalidEndpointAndUnboundedRequestPolicy() {
        assertThrows(IllegalArgumentException.class, () -> S3ObjectStoreConfig.from(Map.of()));
        assertThrows(
            IllegalArgumentException.class,
            () -> S3ObjectStoreConfig.from(Map.of(
                S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
                S3ObjectStoreConfig.ENDPOINT_CONFIG, "ftp://127.0.0.1:9000"
            ))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> S3ObjectStoreConfig.from(Map.of(
                S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
                S3ObjectStoreConfig.API_CALL_TIMEOUT_MS_CONFIG, 0
            ))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> S3ObjectStoreConfig.from(Map.of(
                S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
                S3ObjectStoreConfig.API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG, 10_000,
                S3ObjectStoreConfig.API_CALL_TIMEOUT_MS_CONFIG, 5_000
            ))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> S3ObjectStoreConfig.from(Map.of(
                S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
                S3ObjectStoreConfig.MAX_ATTEMPTS_CONFIG, 0
            ))
        );
    }

    @Test
    void roundTripsPutRangeReadAndDeleteAgainstConfiguredS3() throws Exception {
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "S3 integration endpoint is not configured");

        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-e2e");
        String region = environment("SHARED_STORAGE_S3_REGION", S3ObjectStoreConfig.DEFAULT_REGION);
        Map<String, Object> originals = new HashMap<>();
        originals.put(S3ObjectStoreConfig.BUCKET_CONFIG, bucket);
        originals.put(S3ObjectStoreConfig.REGION_CONFIG, region);
        originals.put(S3ObjectStoreConfig.ENDPOINT_CONFIG, endpoint);
        originals.put(S3ObjectStoreConfig.PATH_STYLE_ACCESS_CONFIG, true);
        originals.put(S3ObjectStoreConfig.KEY_PREFIX_CONFIG, "integration/objects");
        originals.put(S3ObjectStoreConfig.IO_THREADS_CONFIG, 2);
        originals.put(S3ObjectStoreConfig.API_CALL_TIMEOUT_MS_CONFIG, 10_000L);
        originals.put(S3ObjectStoreConfig.API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG, 5_000L);
        originals.put(S3ObjectStoreConfig.SOCKET_TIMEOUT_MS_CONFIG, 5_000L);
        originals.put(S3ObjectStoreConfig.CONNECTION_TIMEOUT_MS_CONFIG, 2_000L);
        originals.put(S3ObjectStoreConfig.MAX_ATTEMPTS_CONFIG, 2);
        S3ObjectStoreConfig config = S3ObjectStoreConfig.from(originals);

        ensureBucket(config);
        long objectId = 9_001L;
        byte[] payload = new byte[] {10, 11, 12, 13, 14, 15, 16};
        try (S3ObjectStore store = new S3ObjectStore(config)) {
            store.put(objectId, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);

            ByteBuffer range = store.rangeRead(objectId, 2L, 4).get(10, TimeUnit.SECONDS);
            byte[] actual = new byte[range.remaining()];
            range.get(actual);
            assertArrayEquals(new byte[] {12, 13, 14, 15}, actual);

            ByteBuffer empty = store.rangeRead(objectId, 0L, 0).get(10, TimeUnit.SECONDS);
            assertEquals(0, empty.remaining());

            store.delete(objectId).get(10, TimeUnit.SECONDS);
        }
    }

    private static void ensureBucket(S3ObjectStoreConfig config) {
        try (S3Client client = testClient(config)) {
            try {
                client.createBucket(CreateBucketRequest.builder().bucket(config.bucket()).build());
            } catch (S3Exception e) {
                if (e.statusCode() != 409) {
                    throw e;
                }
            }
        }
    }

    private static S3Client testClient(S3ObjectStoreConfig config) {
        var builder = S3Client.builder()
            .region(Region.of(config.region()))
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(config.pathStyleAccess())
                .build());
        Optional<URI> endpoint = config.endpoint();
        endpoint.ifPresent(builder::endpointOverride);
        return builder.build();
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static final class CloseRaceExecutor extends AbstractExecutorService {
        private final CountDownLatch executeEntered = new CountDownLatch(1);
        private final CountDownLatch shutdown = new CountDownLatch(1);
        private final AtomicBoolean shutdownRequested = new AtomicBoolean();

        @Override
        public void shutdown() {
            if (shutdownRequested.compareAndSet(false, true)) {
                shutdown.countDown();
            }
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown();
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdownRequested.get();
        }

        @Override
        public boolean isTerminated() {
            return shutdownRequested.get();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return shutdownRequested.get() || shutdown.await(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            executeEntered.countDown();
            try {
                if (!shutdown.await(10, TimeUnit.SECONDS)) {
                    throw new RejectedExecutionException("timed out waiting for executor shutdown");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RejectedExecutionException("interrupted while waiting for executor shutdown", e);
            }
            throw new RejectedExecutionException("executor shut down before async submission");
        }

        boolean awaitExecuteEntered(long timeout, TimeUnit unit) throws InterruptedException {
            return executeEntered.await(timeout, unit);
        }
    }
}
