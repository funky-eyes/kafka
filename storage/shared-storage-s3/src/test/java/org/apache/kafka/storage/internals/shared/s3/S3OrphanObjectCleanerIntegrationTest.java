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

import org.apache.kafka.storage.internals.shared.metadata.InMemoryObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.ActiveObjectUploads;
import org.apache.kafka.storage.internals.shared.object.OrphanObjectCleaner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Physical MinIO evidence for orphan cleanup fences and idempotent late-write reclamation. */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class S3OrphanObjectCleanerIntegrationTest {
    @Test
    void claimedOrphanIsDeletedAgainAfterLatePut() throws Exception {
        S3ObjectStoreConfig config = config();
        long objectId = 9_101L;
        byte[] initial = new byte[] {1, 2, 3};
        byte[] late = new byte[] {4, 5, 6, 7};

        try (S3ObjectStore objects = new S3ObjectStore(config);
             S3Client inspector = testClient(config)) {
            ensureBucket(inspector, config);
            InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
            OrphanObjectCleaner cleaner = new OrphanObjectCleaner(objects, metadata);
            metadata.prepare(objectId, 100L).get(10, TimeUnit.SECONDS);
            try {
                objects.put(objectId, ByteBuffer.wrap(initial)).get(10, TimeUnit.SECONDS);
                assertPhysicalPresent(inspector, config, objectId, initial.length);

                assertEquals(1, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isCleanupClaimed(objectId));
                assertPhysicalAbsent(inspector, config, objectId);

                ExecutionException fenced = assertThrows(ExecutionException.class,
                    () -> metadata.commit(committedMetadata(objectId, initial.length)).get(10, TimeUnit.SECONDS));
                assertInstanceOf(IllegalStateException.class, fenced.getCause());

                // Models a physical PUT that completes after a crashed uploader's first DELETE.
                objects.put(objectId, ByteBuffer.wrap(late)).get(10, TimeUnit.SECONDS);
                assertPhysicalPresent(inspector, config, objectId, late.length);
                assertEquals(1, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isCleanupClaimed(objectId));
                assertPhysicalAbsent(inspector, config, objectId);
            } finally {
                objects.delete(objectId).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void committedObjectRemainsPhysicallyReadableAfterCleanup() throws Exception {
        S3ObjectStoreConfig config = config();
        long objectId = 9_102L;
        byte[] committed = new byte[] {11, 12, 13, 14};

        try (S3ObjectStore objects = new S3ObjectStore(config);
             S3Client inspector = testClient(config)) {
            ensureBucket(inspector, config);
            InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
            OrphanObjectCleaner cleaner = new OrphanObjectCleaner(objects, metadata);
            metadata.prepare(objectId, 100L).get(10, TimeUnit.SECONDS);
            try {
                objects.put(objectId, ByteBuffer.wrap(committed)).get(10, TimeUnit.SECONDS);
                metadata.commit(committedMetadata(objectId, committed.length)).get(10, TimeUnit.SECONDS);
                assertEquals(0, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isCommitted(objectId));
                assertPhysicalPresent(inspector, config, objectId, committed.length);

                ByteBuffer result = objects.rangeRead(objectId, 0L, committed.length).get(10, TimeUnit.SECONDS);
                byte[] actual = new byte[result.remaining()];
                result.get(actual);
                assertArrayEquals(committed, actual);
            } finally {
                objects.delete(objectId).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void activePreparedUploadIsProtectedUntilUploadEnds() throws Exception {
        S3ObjectStoreConfig config = config();
        long objectId = 9_103L;
        byte[] payload = new byte[] {21, 22, 23};
        ActiveObjectUploads activeUploads = new ActiveObjectUploads();

        try (S3ObjectStore objects = new S3ObjectStore(config);
             S3Client inspector = testClient(config)) {
            ensureBucket(inspector, config);
            InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
            OrphanObjectCleaner cleaner = new OrphanObjectCleaner(objects, metadata, activeUploads);
            metadata.prepare(objectId, 100L).get(10, TimeUnit.SECONDS);
            activeUploads.begin(objectId);
            try {
                objects.put(objectId, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);
                assertEquals(0, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isPrepared(objectId));
                assertPhysicalPresent(inspector, config, objectId, payload.length);

                activeUploads.end(objectId);
                assertEquals(1, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isCleanupClaimed(objectId));
                assertPhysicalAbsent(inspector, config, objectId);
            } finally {
                activeUploads.end(objectId);
                objects.delete(objectId).get(10, TimeUnit.SECONDS);
            }
        }
    }


    @Test
    void redundantCommittedPhysicalCopyIsReclaimedWithoutDeletingReadWinner() throws Exception {
        S3ObjectStoreConfig config = config();
        long retainedId = 9_104L;
        long redundantId = 9_105L;
        byte[] contents = new byte[] {31, 32, 33};

        try (S3ObjectStore objects = new S3ObjectStore(config);
             S3Client inspector = testClient(config)) {
            ensureBucket(inspector, config);
            InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
            RemoteObjectIndex index = new RemoteObjectIndex();
            OrphanObjectCleaner cleaner =
                new OrphanObjectCleaner(objects, metadata, new ActiveObjectUploads(), index);
            SharedObjectMetadata retained = committedMetadata(retainedId, contents.length);
            SharedObjectMetadata redundant = committedMetadata(redundantId, contents.length);
            try {
                objects.put(retainedId, ByteBuffer.wrap(contents)).get(10, TimeUnit.SECONDS);
                objects.put(redundantId, ByteBuffer.wrap(contents)).get(10, TimeUnit.SECONDS);
                metadata.prepare(retainedId, 100L).get(10, TimeUnit.SECONDS);
                metadata.commit(retained).get(10, TimeUnit.SECONDS);
                metadata.prepare(redundantId, 101L).get(10, TimeUnit.SECONDS);
                metadata.commit(redundant).get(10, TimeUnit.SECONDS);
                assertTrue(index.add(retained).objectReferenced());
                assertFalse(index.add(redundant).objectReferenced());

                assertEquals(1, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isCommitted(retainedId));
                assertFalse(metadata.isCommitted(redundantId));
                assertTrue(index.referencesObject(retainedId));
                assertFalse(index.referencesObject(redundantId));
                assertPhysicalPresent(inspector, config, retainedId, contents.length);
                assertPhysicalAbsent(inspector, config, redundantId);

                ByteBuffer read = objects.rangeRead(retainedId, 0L, contents.length).get(10, TimeUnit.SECONDS);
                byte[] actual = new byte[read.remaining()];
                read.get(actual);
                assertArrayEquals(contents, actual);
                assertEquals(0, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
            } finally {
                objects.delete(redundantId).get(10, TimeUnit.SECONDS);
                objects.delete(retainedId).get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void multiPartitionPhysicalObjectSurvivesWhenAnyRangeIsStillReferenced() throws Exception {
        S3ObjectStoreConfig config = config();
        long retainedId = 9_106L;
        long mixedId = 9_107L;
        byte[] retainedBytes = new byte[] {41, 42, 43};
        byte[] mixedBytes = new byte[] {41, 42, 43, 51, 52, 53};

        try (S3ObjectStore objects = new S3ObjectStore(config);
             S3Client inspector = testClient(config)) {
            ensureBucket(inspector, config);
            InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
            RemoteObjectIndex index = new RemoteObjectIndex();
            OrphanObjectCleaner cleaner =
                new OrphanObjectCleaner(objects, metadata, new ActiveObjectUploads(), index);
            SharedObjectMetadata retained = committedMetadata(retainedId, retainedBytes.length);
            SharedPartitionId otherPartition = new SharedPartitionId(3L, 4L, 1);
            SharedObjectMetadata mixed = new SharedObjectMetadata(
                mixedId, mixedBytes.length, 23L,
                List.of(
                    retained.ranges().get(0),
                    new SharedObjectRange(otherPartition, new OffsetRange(10L, 11L), 1, 3L, 3, 19L)
                )
            );
            try {
                objects.put(retainedId, ByteBuffer.wrap(retainedBytes)).get(10, TimeUnit.SECONDS);
                objects.put(mixedId, ByteBuffer.wrap(mixedBytes)).get(10, TimeUnit.SECONDS);
                metadata.prepare(retainedId, 100L).get(10, TimeUnit.SECONDS);
                metadata.commit(retained).get(10, TimeUnit.SECONDS);
                metadata.prepare(mixedId, 101L).get(10, TimeUnit.SECONDS);
                metadata.commit(mixed).get(10, TimeUnit.SECONDS);
                assertTrue(index.add(retained).objectReferenced());
                assertTrue(index.add(mixed).objectReferenced());
                assertEquals(retainedId,
                    index.find(retained.ranges().get(0).partition(), 0L).orElseThrow().objectId());
                assertEquals(mixedId, index.find(otherPartition, 10L).orElseThrow().objectId());

                assertEquals(0, cleaner.clean(1_000L).get(10, TimeUnit.SECONDS));
                assertTrue(metadata.isCommitted(mixedId));
                assertTrue(index.referencesObject(mixedId));
                assertPhysicalPresent(inspector, config, retainedId, retainedBytes.length);
                assertPhysicalPresent(inspector, config, mixedId, mixedBytes.length);

                ByteBuffer read = objects.rangeRead(mixedId, 3L, 3).get(10, TimeUnit.SECONDS);
                byte[] actual = new byte[read.remaining()];
                read.get(actual);
                assertArrayEquals(new byte[] {51, 52, 53}, actual);
            } finally {
                objects.delete(mixedId).get(10, TimeUnit.SECONDS);
                objects.delete(retainedId).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertPhysicalPresent(
        S3Client inspector, S3ObjectStoreConfig config, long objectId, long expectedLength
    ) {
        long actualLength = inspector.headObject(HeadObjectRequest.builder()
            .bucket(config.bucket())
            .key(config.objectKey(objectId))
            .build()).contentLength();
        assertEquals(expectedLength, actualLength, "Committed physical MinIO object must remain readable");
    }

    private static void assertPhysicalAbsent(S3Client inspector, S3ObjectStoreConfig config, long objectId) {
        String key = config.objectKey(objectId);
        S3Exception notFound = assertThrows(S3Exception.class, () ->
            inspector.headObject(HeadObjectRequest.builder()
                .bucket(config.bucket())
                .key(key)
                .build()));
        assertEquals(404, notFound.statusCode(), "Orphan physical object must be deleted");
        assertFalse(inspector.listObjectsV2(ListObjectsV2Request.builder()
            .bucket(config.bucket())
            .prefix(key)
            .build()).contents().stream().anyMatch(object -> key.equals(object.key())),
            "Orphan physical object must not remain in the MinIO object listing");
    }

    private static SharedObjectMetadata committedMetadata(long objectId, int length) {
        SharedObjectRange range = new SharedObjectRange(
            new SharedPartitionId(1L, 2L, 0),
            new OffsetRange(0L, 1L),
            1,
            0L,
            length,
            17L
        );
        return new SharedObjectMetadata(objectId, length, 23L, List.of(range));
    }

    private static S3ObjectStoreConfig config() {
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MinIO integration endpoint is not configured");
        Map<String, Object> originals = new HashMap<>();
        originals.put(S3ObjectStoreConfig.BUCKET_CONFIG,
            environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-e2e"));
        originals.put(S3ObjectStoreConfig.REGION_CONFIG,
            environment("SHARED_STORAGE_S3_REGION", S3ObjectStoreConfig.DEFAULT_REGION));
        originals.put(S3ObjectStoreConfig.ENDPOINT_CONFIG, endpoint);
        originals.put(S3ObjectStoreConfig.PATH_STYLE_ACCESS_CONFIG, true);
        originals.put(S3ObjectStoreConfig.KEY_PREFIX_CONFIG,
            "integration/orphan-cleanup/" + UUID.randomUUID());
        originals.put(S3ObjectStoreConfig.IO_THREADS_CONFIG, 2);
        originals.put(S3ObjectStoreConfig.API_CALL_TIMEOUT_MS_CONFIG, 20_000L);
        originals.put(S3ObjectStoreConfig.API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG, 10_000L);
        originals.put(S3ObjectStoreConfig.SOCKET_TIMEOUT_MS_CONFIG, 10_000L);
        originals.put(S3ObjectStoreConfig.CONNECTION_TIMEOUT_MS_CONFIG, 2_000L);
        originals.put(S3ObjectStoreConfig.MAX_ATTEMPTS_CONFIG, 2);
        return S3ObjectStoreConfig.from(originals);
    }

    private static void ensureBucket(S3Client client, S3ObjectStoreConfig config) {
        try {
            client.createBucket(CreateBucketRequest.builder().bucket(config.bucket()).build());
        } catch (S3Exception e) {
            if (e.statusCode() != 409) {
                throw e;
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
        config.endpoint().ifPresent(builder::endpointOverride);
        return builder.build();
    }

    private static String environment(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}
