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
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class S3MultipartObjectStoreTest {
    @Test
    void shouldPullKnownSizeMultipartSourceInOrderAndPublishOneObject() throws Exception {
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "S3 integration endpoint is not configured");

        S3ObjectStoreConfig config = config(endpoint);
        ensureBucket(config);
        long objectId = 9_002L;
        byte[] first = new byte[S3ObjectStore.MIN_MULTIPART_PART_BYTES];
        byte[] last = new byte[1024];
        first[first.length - 2] = 11;
        first[first.length - 1] = 12;
        last[0] = 13;
        last[1] = 14;
        AtomicInteger pulls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        ObjectStore.PartSource source = new ObjectStore.PartSource() {
            @Override
            public ByteBuffer nextPart() {
                return switch (pulls.getAndIncrement()) {
                    case 0 -> ByteBuffer.wrap(first);
                    case 1 -> ByteBuffer.wrap(last);
                    default -> null;
                };
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        try (S3ObjectStore store = new S3ObjectStore(config)) {
            try {
                store.put(objectId, first.length + (long) last.length, source).get(20, TimeUnit.SECONDS);

                ByteBuffer crossingBoundary = store.rangeRead(
                    objectId,
                    first.length - 2L,
                    4
                ).get(10, TimeUnit.SECONDS);
                byte[] actual = new byte[crossingBoundary.remaining()];
                crossingBoundary.get(actual);
                assertArrayEquals(new byte[] {11, 12, 13, 14}, actual);
                assertEquals(3, pulls.get());
                assertTrue(closed.get(), "known-size multipart source must be closed after successful upload");
            } finally {
                store.delete(objectId).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void shouldAbortKnownSizeMultipartAndCloseSourceWhenSourceEndsEarly() throws Exception {
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "S3 integration endpoint is not configured");

        S3ObjectStoreConfig config = config(endpoint);
        ensureBucket(config);
        long objectId = 9_003L;
        byte[] first = new byte[S3ObjectStore.MIN_MULTIPART_PART_BYTES];
        AtomicInteger pulls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();
        ObjectStore.PartSource source = new ObjectStore.PartSource() {
            @Override
            public ByteBuffer nextPart() {
                return pulls.getAndIncrement() == 0 ? ByteBuffer.wrap(first) : null;
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        try (S3ObjectStore store = new S3ObjectStore(config)) {
            try {
                ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> store.put(objectId, first.length + 1024L, source).get(20, TimeUnit.SECONDS)
                );
                IllegalArgumentException cause = assertInstanceOf(IllegalArgumentException.class, failure.getCause());
                assertTrue(cause.getMessage().contains("ended early"));
                assertEquals(2, pulls.get());
                assertTrue(closed.get(), "failed known-size multipart source must be closed");
                assertNoMultipartUpload(config, objectId);
            } finally {
                store.delete(objectId).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertNoMultipartUpload(S3ObjectStoreConfig config, long objectId) {
        String key = config.objectKey(objectId);
        try (S3Client client = testClient(config)) {
            var uploads = client.listMultipartUploads(
                ListMultipartUploadsRequest.builder()
                    .bucket(config.bucket())
                    .prefix(key)
                    .build()
            ).uploads().stream()
                .filter(upload -> key.equals(upload.key()))
                .toList();
            try {
                assertTrue(uploads.isEmpty(), "failed multipart upload must be aborted");
            } finally {
                for (var upload : uploads) {
                    client.abortMultipartUpload(
                        AbortMultipartUploadRequest.builder()
                            .bucket(config.bucket())
                            .key(upload.key())
                            .uploadId(upload.uploadId())
                            .build()
                    );
                }
            }
        }
    }

    private static S3ObjectStoreConfig config(String endpoint) {
        Map<String, Object> originals = new HashMap<>();
        originals.put(S3ObjectStoreConfig.BUCKET_CONFIG,
            environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-e2e"));
        originals.put(S3ObjectStoreConfig.REGION_CONFIG,
            environment("SHARED_STORAGE_S3_REGION", S3ObjectStoreConfig.DEFAULT_REGION));
        originals.put(S3ObjectStoreConfig.ENDPOINT_CONFIG, endpoint);
        originals.put(S3ObjectStoreConfig.PATH_STYLE_ACCESS_CONFIG, true);
        originals.put(
            S3ObjectStoreConfig.KEY_PREFIX_CONFIG,
            "integration/multipart-objects/" + UUID.randomUUID()
        );
        originals.put(S3ObjectStoreConfig.IO_THREADS_CONFIG, 2);
        originals.put(S3ObjectStoreConfig.API_CALL_TIMEOUT_MS_CONFIG, 20_000L);
        originals.put(S3ObjectStoreConfig.API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG, 10_000L);
        originals.put(S3ObjectStoreConfig.SOCKET_TIMEOUT_MS_CONFIG, 10_000L);
        originals.put(S3ObjectStoreConfig.CONNECTION_TIMEOUT_MS_CONFIG, 2_000L);
        originals.put(S3ObjectStoreConfig.MAX_ATTEMPTS_CONFIG, 2);
        return S3ObjectStoreConfig.from(originals);
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
}
