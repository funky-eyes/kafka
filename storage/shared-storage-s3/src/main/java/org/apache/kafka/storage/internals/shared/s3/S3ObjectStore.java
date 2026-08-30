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

import org.apache.kafka.storage.internals.shared.object.ObjectStore;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * S3-backed immutable object store.
 *
 * <p>The AWS SDK synchronous client intentionally uses JDK URLConnection rather than its Netty client so loading this
 * plugin cannot introduce a second Netty stack into Kafka. ObjectStore's asynchronous contract is provided by a small,
 * bounded-size worker pool owned by this instance. Callers must not mutate a PUT buffer until its future completes.</p>
 *
 * <p>This client belongs exclusively to asynchronous object publication and cold reads. Producer acknowledgement
 * durability is owned by the local and replicated WAL path; an S3 request must never be placed on that ACK path.</p>
 */
public final class S3ObjectStore implements ObjectStore {
    private static final long CLOSE_TIMEOUT_SECONDS = 30L;

    private final S3ObjectStoreConfig config;
    private final S3Client client;
    private final ExecutorService ioExecutor;
    private final AtomicBoolean closed = new AtomicBoolean();

    public S3ObjectStore(S3ObjectStoreConfig config) {
        this(config, buildClient(config), newIoExecutor(config));
    }

    S3ObjectStore(S3ObjectStoreConfig config, S3Client client, ExecutorService ioExecutor) {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
        this.ioExecutor = Objects.requireNonNull(ioExecutor, "ioExecutor");
    }

    @Override
    public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
        Objects.requireNonNull(data, "data");
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        ByteBuffer payload = data.asReadOnlyBuffer();
        return runAsync(() -> client.putObject(
            PutObjectRequest.builder()
                .bucket(config.bucket())
                .key(config.objectKey(objectId))
                .contentLength((long) payload.remaining())
                .build(),
            RequestBody.fromByteBuffer(payload)
        ));
    }

    @Override
    public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        if (position < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("position must be non-negative"));
        }
        if (length < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("length must be non-negative"));
        }
        if (length == 0) {
            return CompletableFuture.completedFuture(ByteBuffer.allocate(0).asReadOnlyBuffer());
        }

        final long lastByte;
        try {
            lastByte = Math.addExact(position, length - 1L);
        } catch (ArithmeticException e) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("S3 byte range overflow", e));
        }
        String range = "bytes=" + position + "-" + lastByte;
        return supplyAsync(() -> {
            ResponseBytes<GetObjectResponse> response = client.getObjectAsBytes(
                GetObjectRequest.builder()
                    .bucket(config.bucket())
                    .key(config.objectKey(objectId))
                    .range(range)
                    .build()
            );
            return response.asByteBuffer().asReadOnlyBuffer();
        });
    }

    @Override
    public CompletableFuture<Void> delete(long objectId) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        return runAsync(() -> client.deleteObject(
            DeleteObjectRequest.builder()
                .bucket(config.bucket())
                .key(config.objectKey(objectId))
                .build()
        ));
    }

    private CompletableFuture<Void> runAsync(Runnable operation) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("S3 object store is closed"));
        }
        return CompletableFuture.runAsync(operation, ioExecutor);
    }

    private <T> CompletableFuture<T> supplyAsync(java.util.function.Supplier<T> operation) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("S3 object store is closed"));
        }
        return CompletableFuture.supplyAsync(operation, ioExecutor);
    }

    private static S3Client buildClient(S3ObjectStoreConfig config) {
        Objects.requireNonNull(config, "config");
        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(config.region()))
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(config.pathStyleAccess())
                .build());
        config.endpoint().ifPresent(builder::endpointOverride);
        return builder.build();
    }

    private static ExecutorService newIoExecutor(S3ObjectStoreConfig config) {
        AtomicInteger threadId = new AtomicInteger();
        return Executors.newFixedThreadPool(config.ioThreads(), runnable -> {
            Thread thread = new Thread(runnable, "kafka-shared-s3-io-" + threadId.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ioExecutor.shutdown();
        boolean interrupted = false;
        try {
            if (!ioExecutor.awaitTermination(CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                ioExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            interrupted = true;
            ioExecutor.shutdownNow();
        } finally {
            client.close();
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
