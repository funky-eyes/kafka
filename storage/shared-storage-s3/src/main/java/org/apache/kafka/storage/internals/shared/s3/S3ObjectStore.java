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
import software.amazon.awssdk.retries.DefaultRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
 * bounded-size worker pool owned by this instance.</p>
 *
 * <p>When the serialized object size is known, multipart publication pulls and uploads exactly one part at a time.
 * Unknown-size sources retain a two-part look-ahead fallback so S3's 5 MiB non-final-part rule is still enforced.
 * Metadata COMMIT remains outside this class and defines the remote visibility boundary.</p>
 */
public final class S3ObjectStore implements ObjectStore {
    static final int MIN_MULTIPART_PART_BYTES = 5 * 1024 * 1024;
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
        return runAsync(() -> putSingle(objectId, payload));
    }

    @Override
    public CompletableFuture<Void> put(long objectId, List<ByteBuffer> parts) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        Objects.requireNonNull(parts, "parts");
        if (parts.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("parts must not be empty"));
        }
        List<ByteBuffer> payloads = new ArrayList<>(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            ByteBuffer part = Objects.requireNonNull(parts.get(i), "part").asReadOnlyBuffer();
            if (i < parts.size() - 1 && part.remaining() < MIN_MULTIPART_PART_BYTES) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Non-final S3 multipart part must be at least " + MIN_MULTIPART_PART_BYTES + " bytes"));
            }
            payloads.add(part);
        }
        if (payloads.size() == 1) {
            return put(objectId, payloads.get(0));
        }
        return runAsync(() -> putMultipart(objectId, payloads));
    }

    @Override
    public CompletableFuture<Void> put(long objectId, long objectSize, PartSource source) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        if (objectSize <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectSize must be positive"));
        }
        Objects.requireNonNull(source, "source");
        return runAsync(() -> putKnownSizeSource(objectId, objectSize, source));
    }

    @Override
    public CompletableFuture<Void> put(long objectId, PartSource source) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        Objects.requireNonNull(source, "source");
        return runAsync(() -> putUnknownSizeSource(objectId, source));
    }

    private void putKnownSizeSource(long objectId, long objectSize, PartSource source) {
        try (source) {
            ByteBuffer first = nextPart(source);
            if (first == null) {
                throw new IllegalArgumentException("Object part source must not be empty");
            }
            if (first.remaining() > objectSize) {
                throw new IllegalArgumentException("Object part source exceeded declared object size");
            }
            if (first.remaining() == objectSize) {
                requireEndOfSource(source);
                putSingle(objectId, first);
                return;
            }
            putKnownSizeMultipart(objectId, objectSize, first, source);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to produce S3 multipart object bytes", e);
        }
    }

    private void putUnknownSizeSource(long objectId, PartSource source) {
        try (source) {
            ByteBuffer first = nextPart(source);
            if (first == null) {
                throw new IllegalArgumentException("Object part source must not be empty");
            }
            ByteBuffer second = nextPart(source);
            if (second == null) {
                putSingle(objectId, first);
                return;
            }
            putUnknownSizeMultipart(objectId, first, second, source);
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to produce S3 multipart object bytes", e);
        }
    }

    private static ByteBuffer nextPart(PartSource source) throws IOException {
        ByteBuffer part = source.nextPart();
        if (part == null) {
            return null;
        }
        ByteBuffer payload = part.asReadOnlyBuffer();
        if (!payload.hasRemaining()) {
            throw new IllegalArgumentException("Object part source returned an empty part");
        }
        return payload;
    }

    private static void requireEndOfSource(PartSource source) throws IOException {
        if (nextPart(source) != null) {
            throw new IllegalArgumentException("Object part source exceeded declared object size");
        }
    }

    private void putSingle(long objectId, ByteBuffer payload) {
        client.putObject(
            PutObjectRequest.builder()
                .bucket(config.bucket())
                .key(config.objectKey(objectId))
                .contentLength((long) payload.remaining())
                .build(),
            RequestBody.fromByteBuffer(payload)
        );
    }

    private void putMultipart(long objectId, List<ByteBuffer> payloads) {
        String key = config.objectKey(objectId);
        String uploadId = createMultipartUpload(key);
        List<CompletedPart> completed = new ArrayList<>(payloads.size());
        try {
            for (int i = 0; i < payloads.size(); i++) {
                completed.add(uploadPart(key, uploadId, i + 1, payloads.get(i)));
            }
            completeMultipartUpload(key, uploadId, completed);
        } catch (RuntimeException uploadFailure) {
            abortMultipartUpload(key, uploadId, uploadFailure);
            throw uploadFailure;
        }
    }

    private void putKnownSizeMultipart(
        long objectId,
        long objectSize,
        ByteBuffer first,
        PartSource source
    ) throws IOException {
        String key = config.objectKey(objectId);
        String uploadId = createMultipartUpload(key);
        List<CompletedPart> completed = new ArrayList<>();
        long emitted = 0L;
        int partNumber = 1;
        ByteBuffer current = first;
        try {
            while (true) {
                long remaining = objectSize - emitted;
                if (current.remaining() > remaining) {
                    throw new IllegalArgumentException("Object part source exceeded declared object size");
                }
                boolean finalPart = current.remaining() == remaining;
                if (!finalPart && current.remaining() < MIN_MULTIPART_PART_BYTES) {
                    throw new IllegalArgumentException(
                        "Non-final S3 multipart part must be at least " + MIN_MULTIPART_PART_BYTES + " bytes");
                }
                int currentBytes = current.remaining();
                completed.add(uploadPart(key, uploadId, partNumber++, current));
                emitted = Math.addExact(emitted, currentBytes);
                if (finalPart) {
                    requireEndOfSource(source);
                    break;
                }
                current = nextPart(source);
                if (current == null) {
                    throw new IllegalArgumentException(
                        "Object part source ended early: expected=" + objectSize + ", actual=" + emitted);
                }
            }
            completeMultipartUpload(key, uploadId, completed);
        } catch (IOException uploadFailure) {
            abortMultipartUpload(key, uploadId, uploadFailure);
            throw uploadFailure;
        } catch (RuntimeException uploadFailure) {
            abortMultipartUpload(key, uploadId, uploadFailure);
            throw uploadFailure;
        }
    }

    private void putUnknownSizeMultipart(
        long objectId,
        ByteBuffer first,
        ByteBuffer second,
        PartSource source
    ) throws IOException {
        String key = config.objectKey(objectId);
        String uploadId = createMultipartUpload(key);
        List<CompletedPart> completed = new ArrayList<>();
        ByteBuffer current = first;
        ByteBuffer next = second;
        int partNumber = 1;
        try {
            while (true) {
                if (next != null && current.remaining() < MIN_MULTIPART_PART_BYTES) {
                    throw new IllegalArgumentException(
                        "Non-final S3 multipart part must be at least " + MIN_MULTIPART_PART_BYTES + " bytes");
                }
                completed.add(uploadPart(key, uploadId, partNumber++, current));
                if (next == null) {
                    break;
                }
                current = next;
                next = nextPart(source);
            }
            completeMultipartUpload(key, uploadId, completed);
        } catch (IOException uploadFailure) {
            abortMultipartUpload(key, uploadId, uploadFailure);
            throw uploadFailure;
        } catch (RuntimeException uploadFailure) {
            abortMultipartUpload(key, uploadId, uploadFailure);
            throw uploadFailure;
        }
    }

    private String createMultipartUpload(String key) {
        return client.createMultipartUpload(
            CreateMultipartUploadRequest.builder().bucket(config.bucket()).key(key).build()
        ).uploadId();
    }

    private CompletedPart uploadPart(String key, String uploadId, int partNumber, ByteBuffer source) {
        ByteBuffer payload = source.duplicate();
        String eTag = client.uploadPart(
            UploadPartRequest.builder()
                .bucket(config.bucket())
                .key(key)
                .uploadId(uploadId)
                .partNumber(partNumber)
                .contentLength((long) payload.remaining())
                .build(),
            RequestBody.fromByteBuffer(payload)
        ).eTag();
        return CompletedPart.builder().partNumber(partNumber).eTag(eTag).build();
    }

    private void completeMultipartUpload(String key, String uploadId, List<CompletedPart> completed) {
        client.completeMultipartUpload(
            CompleteMultipartUploadRequest.builder()
                .bucket(config.bucket())
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder().parts(completed).build())
                .build()
        );
    }

    private void abortMultipartUpload(String key, String uploadId, Throwable uploadFailure) {
        try {
            client.abortMultipartUpload(
                AbortMultipartUploadRequest.builder()
                    .bucket(config.bucket())
                    .key(key)
                    .uploadId(uploadId)
                    .build()
            );
        } catch (RuntimeException abortFailure) {
            uploadFailure.addSuppressed(abortFailure);
        }
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

    static S3Client buildClient(S3ObjectStoreConfig config) {
        Objects.requireNonNull(config, "config");
        var retryStrategy = DefaultRetryStrategy.standardStrategyBuilder()
            .maxAttempts(config.maxAttempts())
            .build();
        S3ClientBuilder builder = S3Client.builder()
            .region(Region.of(config.region()))
            .credentialsProvider(DefaultCredentialsProvider.builder().build())
            .httpClientBuilder(UrlConnectionHttpClient.builder()
                .connectionTimeout(Duration.ofMillis(config.connectionTimeoutMs()))
                .socketTimeout(Duration.ofMillis(config.socketTimeoutMs())))
            .overrideConfiguration(override -> override
                .apiCallAttemptTimeout(Duration.ofMillis(config.apiCallAttemptTimeoutMs()))
                .apiCallTimeout(Duration.ofMillis(config.apiCallTimeoutMs()))
                .retryStrategy(retryStrategy))
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
