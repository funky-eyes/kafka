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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class S3ObjectStoreTest {
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
        assertFalse(config.pathStyleAccess());
        assertTrue(config.endpoint().isEmpty());
        assertEquals("objects/42", config.objectKey(42L));
    }

    @Test
    void rejectsMissingBucketAndInvalidEndpoint() {
        assertThrows(IllegalArgumentException.class, () -> S3ObjectStoreConfig.from(Map.of()));
        assertThrows(
            IllegalArgumentException.class,
            () -> S3ObjectStoreConfig.from(Map.of(
                S3ObjectStoreConfig.BUCKET_CONFIG, "shared-data",
                S3ObjectStoreConfig.ENDPOINT_CONFIG, "ftp://127.0.0.1:9000"
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
}
