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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
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
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exercises replay of actually compacted Kafka records after an actual S3 object delete. */
class KafkaOrphanCompactionMinioIntegrationTest {
    /**
     * Use the production metadata store and cleaner with independent broker identities.
     * This test owns the fixed metadata topic and therefore requires a dedicated cluster.
     */
    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void productionCleanerRecoversClaimAndRetriesLatePutAcrossStoreRestart() throws Exception {
        String bootstrap = System.getenv("SHARED_STORAGE_KAFKA_BOOTSTRAP_SERVERS");
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "Kafka integration brokers are not configured");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MinIO integration endpoint is not configured");
        assumeTrue("true".equals(System.getenv("SHARED_STORAGE_ISOLATED_TEST_CLUSTER")),
            "An isolated Kafka cluster is required to own the shared metadata topic");

        String topic = SharedMetadataClientConfiguration.TOPIC_NAME;
        String namespace = UUID.randomUUID().toString();
        long orphanId = 9_211L;
        long liveId = 9_212L;
        long createdTimeMs = 3_000L;
        byte[] bytes = new byte[] {1, 3, 5, 7};
        SharedObjectMetadata orphan = new SharedObjectMetadata(orphanId, bytes.length, 11L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 0),
                new OffsetRange(0L, 1L), 0, 0L, bytes.length, 11L)
        ));
        SharedObjectMetadata live = new SharedObjectMetadata(liveId, bytes.length, 12L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 1),
                new OffsetRange(0L, 1L), 0, 0L, bytes.length, 12L)
        ));
        Map<String, Object> options = new HashMap<>();
        options.put(S3ObjectStoreConfig.BUCKET_CONFIG, "kafka-orphan-compaction-test");
        options.put(S3ObjectStoreConfig.REGION_CONFIG, "us-east-1");
        options.put(S3ObjectStoreConfig.ENDPOINT_CONFIG, endpoint);
        options.put(S3ObjectStoreConfig.PATH_STYLE_ACCESS_CONFIG, true);
        options.put(S3ObjectStoreConfig.KEY_PREFIX_CONFIG, "production-recovery/" + UUID.randomUUID());
        S3ObjectStoreConfig objectConfig = S3ObjectStoreConfig.from(options);
        Properties properties = producerProperties(bootstrap);
        boolean createdTopic = false;
        try (Admin admin = Admin.create(properties);
             S3ObjectStore objects = new S3ObjectStore(objectConfig);
             S3Client inspector = S3Client.builder().region(Region.US_EAST_1)
                 .endpointOverride(objectConfig.endpoint().orElseThrow())
                 .credentialsProvider(DefaultCredentialsProvider.builder().build())
                 .httpClientBuilder(UrlConnectionHttpClient.builder())
                 .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                 .build()) {
            awaitThreeRegisteredBrokers(admin);
            try {
                inspector.createBucket(CreateBucketRequest.builder().bucket(objectConfig.bucket()).build());
            } catch (S3Exception e) {
                if (e.statusCode() != 409) {
                    throw e;
                }
            }
            // If the fixed topic already exists, fail rather than modifying or deleting it.
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3).configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048576",
                TopicConfig.SEGMENT_BYTES_CONFIG, "1048576",
                TopicConfig.SEGMENT_MS_CONFIG, "1000",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01",
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "3000"
            )))).all().get(30, TimeUnit.SECONDS);
            createdTopic = true;
            try {
                byte[] orphanKey = SharedMetadataRecordCodec.objectKey(orphanId);
                long preparedOffset;
                long claimOffset;
                try (KafkaObjectMetadataStore owner = KafkaObjectMetadataStore.open(
                         productionStoreConfiguration(bootstrap, namespace, 101));
                     KafkaObjectMetadataStore first = KafkaObjectMetadataStore.open(
                         productionStoreConfiguration(bootstrap, namespace, 102));
                     KafkaObjectMetadataStore second = KafkaObjectMetadataStore.open(
                         productionStoreConfiguration(bootstrap, namespace, 103))) {
                    owner.prepare(orphanId, createdTimeMs).get(30, TimeUnit.SECONDS);
                    List<ConsumerRecord<byte[], byte[]>> preparedRecords =
                        readTargetRecords(bootstrap, topic, orphanKey, orphanKey);
                    assertEquals(1, preparedRecords.size());
                    preparedOffset = preparedRecords.get(0).offset();
                    objects.put(orphanId, ByteBuffer.wrap(bytes)).get(30, TimeUnit.SECONDS);
                    owner.prepare(liveId, createdTimeMs + 1L).get(30, TimeUnit.SECONDS);
                    objects.put(liveId, ByteBuffer.wrap(bytes)).get(30, TimeUnit.SECONDS);
                    owner.commit(live).get(30, TimeUnit.SECONDS);
                    first.captureReadCommittedReplayHorizon().get(30, TimeUnit.SECONDS);
                    second.captureReadCommittedReplayHorizon().get(30, TimeUnit.SECONDS);

                    assertAtMostOneClaimResponse(first, second, orphanId, createdTimeMs);

                    ExecutionException fenced = assertThrows(ExecutionException.class,
                        () -> owner.commit(orphan).get(30, TimeUnit.SECONDS));
                    assertTrue(hasProducerFencedCause(fenced),
                        "The older uploader must be fenced, not merely fail for an unrelated reason");
                    owner.releaseUpload(orphanId);
                    first.captureReadCommittedReplayHorizon().get(30, TimeUnit.SECONDS);
                    assertTrue(first.cleanupClaimedObjects().stream()
                        .anyMatch(object -> object.objectId() == orphanId),
                        "At least one CLAIM must be durably visible, including after an uncertain response");
                    List<ConsumerRecord<byte[], byte[]>> claimedRecords =
                        readTargetRecords(bootstrap, topic, orphanKey, orphanKey);
                    List<ConsumerRecord<byte[], byte[]>> claims = claimedRecords.stream()
                        .filter(record -> SharedMetadataRecordCodec.decodeValue(
                            SharedMetadataRecordCodec.decodeKey(orphanKey), record.value()
                        ) instanceof SharedMetadataRecordCodec.FencedClaimedObjectValue)
                        .toList();
                    assertEquals(1, claims.size(), "The object key must retain one CLAIM");
                    claimOffset = claims.get(0).offset();
                    assertTrue(claimOffset > preparedOffset);
                    assertFalse(first.claimCleanup(liveId, createdTimeMs + 1L).get(60, TimeUnit.SECONDS));
                    assertEquals(1, new OrphanObjectCleaner(objects, first).clean(createdTimeMs + 1L)
                        .get(60, TimeUnit.SECONDS));
                    assertEquals(404, assertThrows(S3Exception.class, () -> inspector.headObject(
                        HeadObjectRequest.builder().bucket(objectConfig.bucket())
                            .key(objectConfig.objectKey(orphanId)).build())).statusCode());

                    // A deliberately late physical PUT models an upload whose
                    // completion was delayed beyond the first successful DELETE.
                    objects.put(orphanId, ByteBuffer.wrap(bytes)).get(30, TimeUnit.SECONDS);
                }

                // Roll segments with valid metadata records: a store restart
                // must be able to replay the compacted topic, including filler.
                writeCompactionFiller(properties, topic);
                awaitSingleKeyCompaction(bootstrap, topic, orphanKey, preparedOffset, claimOffset);

                List<SharedObjectMetadata> replayed = new ArrayList<>();
                try (KafkaObjectMetadataStore restarted = KafkaObjectMetadataStore.open(
                    productionStoreConfiguration(bootstrap, namespace, 102), replayed::add)) {
                    assertTrue(restarted.cleanupClaimedObjects().stream()
                        .anyMatch(object -> object.objectId() == orphanId));
                    assertTrue(restarted.committedObject(orphanId).isEmpty());
                    assertEquals(live, restarted.committedObject(liveId).orElseThrow());
                    assertFalse(replayed.contains(orphan), "A claimed object must never be republished on replay");
                    assertEquals(1, new OrphanObjectCleaner(objects, restarted).clean(createdTimeMs + 1L)
                        .get(60, TimeUnit.SECONDS));
                    assertEquals(404, assertThrows(S3Exception.class, () -> inspector.headObject(
                        HeadObjectRequest.builder().bucket(objectConfig.bucket())
                            .key(objectConfig.objectKey(orphanId)).build())).statusCode());
                    inspector.headObject(HeadObjectRequest.builder().bucket(objectConfig.bucket())
                        .key(objectConfig.objectKey(liveId)).build());
                }
            } finally {
                objects.delete(orphanId).get(30, TimeUnit.SECONDS);
                objects.delete(liveId).get(30, TimeUnit.SECONDS);
                if (createdTopic) {
                    admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static void awaitThreeRegisteredBrokers(Admin admin) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int count;
        do {
            count = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size();
            if (count < 3) {
                Thread.sleep(250L);
            }
        } while (count < 3 && System.nanoTime() < deadline);
        assertTrue(count >= 3, "The recovery test requires three registered Kafka brokers");
    }

    private static void assertAtMostOneClaimResponse(
        KafkaObjectMetadataStore first, KafkaObjectMetadataStore second, long objectId, long createdTimeMs
    ) throws Exception {
        ExecutorService claimants = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<Boolean> firstClaim = claimants.submit(() -> claimAfterStart(first, objectId, createdTimeMs,
                ready, start));
            Future<Boolean> secondClaim = claimants.submit(() -> claimAfterStart(second, objectId, createdTimeMs,
                ready, start));
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            int winners = (claimWonOrWasFenced(firstClaim) ? 1 : 0)
                + (claimWonOrWasFenced(secondClaim) ? 1 : 0);
            assertTrue(winners <= 1, "Concurrent claimants must not both report a winning CLAIM");
        } finally {
            start.countDown();
            claimants.shutdownNow();
            assertTrue(claimants.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static boolean claimAfterStart(KafkaObjectMetadataStore store, long objectId, long createdTimeMs,
                                           CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Claim start timed out");
        }
        return store.claimCleanup(objectId, createdTimeMs).get(60, TimeUnit.SECONDS);
    }

    private static void writeCompactionFiller(Properties properties, String topic) throws Exception {
        try (KafkaProducer<byte[], byte[]> filler = new KafkaProducer<>(properties)) {
            byte[] fillerKey = SharedMetadataRecordCodec.brokerSequenceKey(999);
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (long i = 1; i <= 100_000L; i++) {
                sends.add(filler.send(new ProducerRecord<>(topic, 0, fillerKey,
                    SharedMetadataRecordCodec.brokerSequenceValue(i))));
            }
            filler.flush();
            for (Future<RecordMetadata> send : sends) {
                send.get(30, TimeUnit.SECONDS);
            }
            // Segment age alone does not roll the active segment until the
            // next append. Ensure the original object records are cleanable.
            Thread.sleep(1_200L);
            filler.send(new ProducerRecord<>(topic, 0, fillerKey,
                SharedMetadataRecordCodec.brokerSequenceValue(100_001L))).get(30, TimeUnit.SECONDS);
        }
    }

    private static SharedMetadataClientConfiguration productionStoreConfiguration(
        String bootstrap, String clusterId, int brokerId
    ) {
        return SharedMetadataClientConfiguration.from(new StorageExtensionBrokerContext(
            clusterId, brokerId,
            List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 1)),
            Map.of(
                SharedMetadataClientConfiguration.CLIENT_PREFIX + "bootstrap.servers", bootstrap,
                SharedMetadataClientConfiguration.FENCED_LIFECYCLE_CONFIG, true
            ), Time.SYSTEM
        ));
    }

    private static boolean claimWonOrWasFenced(Future<Boolean> result) throws Exception {
        try {
            return result.get(75, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (hasProducerFencedCause(e)) {
                return false;
            }
            throw e;
        }
    }

    private static boolean hasProducerFencedCause(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ProducerFencedException) {
                return true;
            }
        }
        return false;
    }

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void ambiguousOrphanHistoryFailsClosedAfterKafkaCompaction() throws Exception {
        String bootstrap = System.getenv("SHARED_STORAGE_KAFKA_BOOTSTRAP_SERVERS");
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "Kafka integration brokers are not configured");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MinIO integration endpoint is not configured");

        String topic = "orphan-compact-" + UUID.randomUUID();
        long objectId = 9_201L;
        long createdTimeMs = 1_000L;
        byte[] bytes = new byte[] {1, 2, 3, 4};
        byte[] objectKey = SharedMetadataRecordCodec.objectKey(objectId);
        byte[] cleanupKey = SharedMetadataRecordCodec.objectCleanupKey(objectId);
        byte[] prepared = SharedMetadataRecordCodec.preparedObjectValue(createdTimeMs);
        byte[] claim = SharedMetadataRecordCodec.cleanupClaimedValue(createdTimeMs);
        SharedObjectMetadata descriptor = new SharedObjectMetadata(objectId, bytes.length, 1L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 0), new OffsetRange(0L, 1L), 0, 0L, bytes.length, 1L)
        ));
        byte[] commit = SharedMetadataRecordCodec.committedObjectValue(descriptor);
        Map<String, Object> s3Options = new HashMap<>();
        s3Options.put(S3ObjectStoreConfig.BUCKET_CONFIG, "kafka-orphan-compaction-test");
        s3Options.put(S3ObjectStoreConfig.REGION_CONFIG, "us-east-1");
        s3Options.put(S3ObjectStoreConfig.ENDPOINT_CONFIG, endpoint);
        s3Options.put(S3ObjectStoreConfig.PATH_STYLE_ACCESS_CONFIG, true);
        s3Options.put(S3ObjectStoreConfig.KEY_PREFIX_CONFIG, "orphan-compaction/" + UUID.randomUUID());
        S3ObjectStoreConfig config = S3ObjectStoreConfig.from(s3Options);
        Properties adminProperties = new Properties();
        adminProperties.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(adminProperties);
             S3ObjectStore objects = new S3ObjectStore(config);
             S3Client inspector = S3Client.builder().region(Region.US_EAST_1)
                 .endpointOverride(config.endpoint().orElseThrow())
                 .credentialsProvider(DefaultCredentialsProvider.builder().build())
                 .httpClientBuilder(UrlConnectionHttpClient.builder())
                 .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                 .build()) {
            assertTrue(admin.describeCluster().nodes().get(30, TimeUnit.SECONDS).size() >= 3,
                "The compaction test requires at least three real Kafka brokers");
            try {
                inspector.createBucket(CreateBucketRequest.builder().bucket(config.bucket()).build());
            } catch (S3Exception e) {
                if (e.statusCode() != 409) {
                    throw e;
                }
            }
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3).configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                TopicConfig.SEGMENT_BYTES_CONFIG, "1048576",
                TopicConfig.SEGMENT_MS_CONFIG, "1000",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01",
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "3000"
            )))).all().get(30, TimeUnit.SECONDS);
            try {
                Properties properties = producerProperties(bootstrap);
                try (KafkaProducer<byte[], byte[]> uploader = new KafkaProducer<>(properties);
                     KafkaProducer<byte[], byte[]> cleaner = new KafkaProducer<>(properties);
                     KafkaProducer<byte[], byte[]> secondCleaner = new KafkaProducer<>(properties)) {
                    SharedMetadataImage live = new SharedMetadataImage();
                    live.markReady();
                    applySent(live, uploader, topic, objectKey, prepared);
                    objects.put(objectId, ByteBuffer.wrap(bytes)).get(30, TimeUnit.SECONDS);
                    long firstClaim = applySent(live, cleaner, topic, cleanupKey, claim);
                    objects.delete(objectId).get(30, TimeUnit.SECONDS);
                    S3Exception missing = assertThrows(S3Exception.class, () -> inspector.headObject(
                        HeadObjectRequest.builder().bucket(config.bucket()).key(config.objectKey(objectId)).build()));
                    assertEquals(404, missing.statusCode());
                    long commitOffset = applySent(live, uploader, topic, objectKey, commit);
                    long lastClaim = applySent(live, secondCleaner, topic, cleanupKey, claim);
                    assertTrue(live.committedObject(objectId).isEmpty());
                    assertTrue(live.cleanupClaimed(objectId, createdTimeMs));

                    // Roll several segments so Kafka's real log cleaner can compact the first CLAIM.
                    byte[] filler = new byte[4_096];
                    for (int i = 0; i < 1024; i++) {
                        uploader.send(new ProducerRecord<>(topic, 0, new byte[] {99}, filler))
                            .get(30, TimeUnit.SECONDS);
                    }
                    List<ConsumerRecord<byte[], byte[]>> compacted = awaitCompacted(
                        bootstrap, topic, objectKey, cleanupKey, firstClaim, commitOffset, lastClaim
                    );
                    List<SharedObjectMetadata> published = new ArrayList<>();
                    SharedMetadataImage recovered = new SharedMetadataImage(published::add);
                    IllegalStateException error = assertThrows(IllegalStateException.class, () -> {
                        for (ConsumerRecord<byte[], byte[]> record : compacted) {
                            recovered.applyFromMetadataLog(record.key(), record.value(), record.offset());
                        }
                        recovered.markReady();
                    });
                    assertTrue(error.getMessage().contains("Ambiguous compacted COMMIT and cleanup fence"));
                    assertEquals(SharedMetadataImage.State.FAILED, recovered.state());
                    assertTrue(published.isEmpty(), "A physically deleted object must not be republished");
                }
            } finally {
                objects.delete(objectId).get(30, TimeUnit.SECONDS);
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }


    /**
     * Exercises the proposed single-key lifecycle through real transactional Kafka brokers,
     * the real log cleaner, and real MinIO. This does not enable the production cleaner.
     */
    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void fencedSingleKeyClaimSurvivesRealKafkaCompactionAndRejectsLateCommit() throws Exception {
        String bootstrap = System.getenv("SHARED_STORAGE_KAFKA_BOOTSTRAP_SERVERS");
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "Kafka integration brokers are not configured");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "MinIO integration endpoint is not configured");

        String topic = "fenced-compact-" + UUID.randomUUID();
        String namespace = UUID.randomUUID().toString();
        long objectId = 9_202L;
        long committedObjectId = 9_203L;
        long createdTimeMs = 2_000L;
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);
        byte[] bytes = new byte[] {5, 6, 7, 8};
        SharedObjectMetadata descriptor = new SharedObjectMetadata(objectId, bytes.length, 2L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 0), new OffsetRange(0L, 1L),
                0, 0L, bytes.length, 2L)
        ));
        SharedObjectMetadata surviving = new SharedObjectMetadata(committedObjectId, bytes.length, 3L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 1), new OffsetRange(0L, 1L),
                0, 0L, bytes.length, 3L)
        ));
        Map<String, Object> options = new HashMap<>();
        options.put(S3ObjectStoreConfig.BUCKET_CONFIG, "kafka-orphan-compaction-test");
        options.put(S3ObjectStoreConfig.REGION_CONFIG, "us-east-1");
        options.put(S3ObjectStoreConfig.ENDPOINT_CONFIG, endpoint);
        options.put(S3ObjectStoreConfig.PATH_STYLE_ACCESS_CONFIG, true);
        options.put(S3ObjectStoreConfig.KEY_PREFIX_CONFIG, "fenced-compaction/" + UUID.randomUUID());
        S3ObjectStoreConfig objectConfig = S3ObjectStoreConfig.from(options);
        Properties properties = producerProperties(bootstrap);
        try (Admin admin = Admin.create(properties);
             S3ObjectStore objects = new S3ObjectStore(objectConfig);
             S3Client inspector = S3Client.builder().region(Region.US_EAST_1)
                 .endpointOverride(objectConfig.endpoint().orElseThrow())
                 .credentialsProvider(DefaultCredentialsProvider.builder().build())
                 .httpClientBuilder(UrlConnectionHttpClient.builder())
                 .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                 .build()) {
            assertTrue(admin.describeCluster().nodes().get(30, TimeUnit.SECONDS).size() >= 3);
            try {
                inspector.createBucket(CreateBucketRequest.builder().bucket(objectConfig.bucket()).build());
            } catch (S3Exception e) {
                if (e.statusCode() != 409) {
                    throw e;
                }
            }
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3).configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                TopicConfig.SEGMENT_BYTES_CONFIG, "1048576",
                TopicConfig.SEGMENT_MS_CONFIG, "1000",
                TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01",
                TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "3000"
            )))).all().get(30, TimeUnit.SECONDS);
            try {
                final long preparedOffset;
                final long claimOffset;
                try (FencedObjectLifecycleArbiter.Uploader uploader =
                         new FencedObjectLifecycleArbiter.Uploader(properties, topic, namespace, objectId)) {
                    preparedOffset = uploader.prepare(createdTimeMs);
                    objects.put(objectId, ByteBuffer.wrap(bytes)).get(30, TimeUnit.SECONDS);
                    OptionalLong claim = FencedObjectLifecycleArbiter.claimIfPrepared(
                        properties, topic, namespace, objectId, createdTimeMs
                    );
                    assertTrue(claim.isPresent(), "Fenced cleaner must win over an unfinished upload");
                    claimOffset = claim.getAsLong();
                    objects.delete(objectId).get(30, TimeUnit.SECONDS);
                    S3Exception missing = assertThrows(S3Exception.class, () -> inspector.headObject(
                        HeadObjectRequest.builder().bucket(objectConfig.bucket())
                            .key(objectConfig.objectKey(objectId)).build()));
                    assertEquals(404, missing.statusCode());

                    // Model a PUT that was already in flight when the owner was fenced.
                    // The durable CLAIM permits idempotent re-DELETE, never a late COMMIT.
                    objects.put(objectId, ByteBuffer.wrap(bytes)).get(30, TimeUnit.SECONDS);
                    objects.delete(objectId).get(30, TimeUnit.SECONDS);
                    assertEquals(404, assertThrows(S3Exception.class, () -> inspector.headObject(
                        HeadObjectRequest.builder().bucket(objectConfig.bucket())
                            .key(objectConfig.objectKey(objectId)).build())).statusCode());

                    // The uploader's producer was initialized before PREPARE.
                    // Fencing must forbid the delayed COMMIT, not just hide it on replay.
                    assertThrows(Exception.class, () -> uploader.commit(descriptor));
                }
                assertFalse(FencedObjectLifecycleArbiter.claimIfPrepared(
                    properties, topic, namespace, objectId, createdTimeMs
                ).isPresent(), "Duplicate cleaner must not mint a second CLAIM");

                try (FencedObjectLifecycleArbiter.Uploader uploader =
                         new FencedObjectLifecycleArbiter.Uploader(
                             properties, topic, namespace, committedObjectId)) {
                    uploader.prepare(createdTimeMs + 1L);
                    uploader.commit(surviving);
                }
                assertFalse(FencedObjectLifecycleArbiter.claimIfPrepared(
                    properties, topic, namespace, committedObjectId, createdTimeMs + 1L
                ).isPresent(), "A committed winner must never be claimed for deletion");

                try (KafkaProducer<byte[], byte[]> fillerProducer = new KafkaProducer<>(properties)) {
                    byte[] filler = new byte[4_096];
                    for (int i = 0; i < 1024; i++) {
                        fillerProducer.send(new ProducerRecord<>(topic, 0, new byte[] {99}, filler))
                            .get(30, TimeUnit.SECONDS);
                    }
                }
                awaitSingleKeyCompaction(bootstrap, topic, key, preparedOffset, claimOffset);
                List<ConsumerRecord<byte[], byte[]>> compacted = readTargetRecords(
                    bootstrap, topic, key, SharedMetadataRecordCodec.objectKey(committedObjectId)
                );
                List<SharedObjectMetadata> published = new ArrayList<>();
                SharedMetadataImage recovered = new SharedMetadataImage(published::add);
                for (ConsumerRecord<byte[], byte[]> record : compacted) {
                    recovered.applyFromMetadataLog(record.key(), record.value(), record.offset());
                }
                recovered.markReady();
                assertTrue(recovered.cleanupClaimed(objectId, createdTimeMs));
                assertTrue(recovered.committedObject(objectId).isEmpty());
                assertEquals(List.of(surviving), published,
                    "Only the object whose COMMIT won may publish a remote read reference");

                // initTransactions alone is not a permanent object-ID fence.
                // A new producer generation must inspect the post-fence
                // catalog and refuse to re-PREPARE either terminal object.
                assertThrows(IllegalStateException.class, () ->
                    new FencedObjectLifecycleArbiter.Uploader(
                        properties, topic, namespace, objectId));
                assertThrows(IllegalStateException.class, () ->
                    new FencedObjectLifecycleArbiter.Uploader(
                        properties, topic, namespace, committedObjectId));
            } finally {
                objects.delete(objectId).get(30, TimeUnit.SECONDS);
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * A real Kafka transaction fences the previous v2 uploader before the
     * independent compacted seal key is written. This is intentionally
     * limited to catalog facts: no Controller approval or S3 DELETE occurs.
     */
    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void fencedCommittedCatalogSealPreservesOriginalObjectAndRejectsStaleWriter() throws Exception {
        String bootstrap = System.getenv("SHARED_STORAGE_KAFKA_BOOTSTRAP_SERVERS");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "Kafka integration brokers are not configured");

        String topic = "committed-seal-" + UUID.randomUUID();
        String namespace = UUID.randomUUID().toString();
        long objectId = 9_204L;
        long epoch = 8L;
        Properties properties = producerProperties(bootstrap);
        SharedObjectMetadata descriptor = new SharedObjectMetadata(objectId, 128L, 71L, List.of(
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 0),
                new OffsetRange(0L, 32L), 3, 0L, 32, 51L),
            new SharedObjectRange(new SharedPartitionId(1L, 2L, 1),
                new OffsetRange(0L, 32L), 4, 64L, 32, 52L)
        ));
        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3).configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"
            )))).all().get(30, TimeUnit.SECONDS);
            try (FencedObjectLifecycleArbiter.Uploader uploader =
                     new FencedObjectLifecycleArbiter.Uploader(properties, topic, namespace, objectId)) {
                uploader.prepare(1_000L);
                long commitOffset = uploader.commit(descriptor);
                SharedObjectMetadata altered = new SharedObjectMetadata(objectId, 128L, 72L, descriptor.ranges());
                assertFalse(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                    properties, topic, namespace, altered, commitOffset, epoch
                ).isPresent(), "Conflicting full object descriptor cannot borrow the COMMIT");
                // The failed attempt already fenced the original producer.
                assertThrows(Exception.class, () -> uploader.commit(descriptor));

                long sealOffset = FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                    properties, topic, namespace, descriptor, commitOffset, epoch
                ).orElseThrow();
                assertTrue(sealOffset > commitOffset);
                assertFalse(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                    properties, topic, namespace, descriptor, commitOffset, epoch
                ).isPresent(), "A previously sealed physical identity cannot be sealed twice");
                assertFalse(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                    properties, topic, namespace, descriptor, commitOffset + 1, epoch
                ).isPresent(), "A forged COMMIT offset cannot be accepted");

                List<ConsumerRecord<byte[], byte[]>> persisted = readTargetRecords(
                    bootstrap, topic, SharedMetadataRecordCodec.objectKey(objectId),
                    SharedMetadataRecordCodec.objectCatalogSealKey(objectId)
                );
                SharedMetadataImage image = new SharedMetadataImage();
                for (ConsumerRecord<byte[], byte[]> record : persisted) {
                    image.applyFromMetadataLog(record.key(), record.value(), record.offset());
                }
                image.markReady();
                assertEquals(descriptor, image.committedObject(objectId).orElseThrow());
                SharedMetadataImage.CatalogSealEvidence evidence = image.catalogSeal(objectId).orElseThrow();
                assertEquals(sealOffset, evidence.sealRecordOffset());
                assertEquals(commitOffset, evidence.seal().originalCommitOffset());
                assertEquals(epoch, evidence.seal().retirementEpoch());
                assertEquals(SharedMetadataRecordCodec.committedDescriptorSha256(descriptor),
                    evidence.seal().descriptorSha256());

                // A matching legacy COMMIT is NOT an object-scoped
                // transactional v2 COMMIT, even with the right Kafka offset.
                long legacyObjectId = objectId + 2L;
                SharedObjectMetadata legacy = new SharedObjectMetadata(
                    legacyObjectId, 128L, 71L, descriptor.ranges());
                try (KafkaProducer<byte[], byte[]> legacyProducer = new KafkaProducer<>(properties)) {
                    long legacyOffset = legacyProducer.send(new ProducerRecord<>(
                        topic, 0, SharedMetadataRecordCodec.objectKey(legacyObjectId),
                        SharedMetadataRecordCodec.committedObjectValue(legacy)
                    )).get(30, TimeUnit.SECONDS).offset();
                    assertFalse(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                        properties, topic, namespace, legacy, legacyOffset, epoch
                    ).isPresent(), "Legacy COMMIT must never be promoted to fenced catalog seal");
                }

                // A compacted snapshot may retain only the independent
                // catalog-seal key after losing the original COMMIT key. A
                // new transactional producer must not reuse that identity.
                long sealOnlyObjectId = objectId + 3L;
                SharedObjectMetadata sealOnly = new SharedObjectMetadata(
                    sealOnlyObjectId, 128L, 71L, descriptor.ranges());
                try (KafkaProducer<byte[], byte[]> fixtureWriter = new KafkaProducer<>(properties)) {
                    fixtureWriter.send(new ProducerRecord<>(
                        topic, 0, SharedMetadataRecordCodec.objectCatalogSealKey(sealOnlyObjectId),
                        SharedMetadataRecordCodec.objectCatalogSealValue(1L, epoch, sealOnly)
                    )).get(30, TimeUnit.SECONDS);
                }
                assertThrows(IllegalStateException.class, () ->
                    new FencedObjectLifecycleArbiter.Uploader(
                        properties, topic, namespace, sealOnlyObjectId),
                    "An independent surviving catalog seal must forbid object ID reuse");

                // A PREPARED-only uploader must not turn a sampled catalog
                // into a fake COMMITTED seal. Initializing the sealer still
                // fences that uploader's producer before its late COMMIT.
                long pendingObjectId = objectId + 1L;
                SharedObjectMetadata pending = new SharedObjectMetadata(
                    pendingObjectId, 128L, 71L, descriptor.ranges());
                try (FencedObjectLifecycleArbiter.Uploader pendingUploader =
                         new FencedObjectLifecycleArbiter.Uploader(
                             properties, topic, namespace, pendingObjectId)) {
                    long preparedOffset = pendingUploader.prepare(2_000L);
                    assertFalse(FencedObjectLifecycleArbiter.sealCommittedAfterFencing(
                        properties, topic, namespace, pending, preparedOffset, epoch
                    ).isPresent(), "PREPARED is never a COMMITTED catalog seal");
                    assertThrows(Exception.class, () -> pendingUploader.commit(pending),
                        "The old PREPARED uploader must be fenced before a late COMMIT");
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static List<ConsumerRecord<byte[], byte[]>> awaitSingleKeyCompaction(
        String bootstrap, String topic, byte[] key, long preparedOffset, long claimedOffset
    ) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
        while (System.nanoTime() < deadline) {
            List<ConsumerRecord<byte[], byte[]>> records = readTargetRecords(bootstrap, topic, key, key);
            boolean preparedSurvives = records.stream().anyMatch(record -> record.offset() == preparedOffset);
            boolean claimSurvives = records.stream().anyMatch(record -> record.offset() == claimedOffset);
            if (!preparedSurvives && claimSurvives) {
                return records;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError("Kafka did not compact the fenced PREPARE while retaining its CLAIM");
    }

    private static long applySent(SharedMetadataImage image, KafkaProducer<byte[], byte[]> producer,
                                  String topic, byte[] key, byte[] value) throws Exception {
        long offset = producer.send(new ProducerRecord<>(topic, 0, key, value)).get(30, TimeUnit.SECONDS).offset();
        image.applyFromMetadataLog(key, value, offset);
        return offset;
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    private static List<ConsumerRecord<byte[], byte[]>> awaitCompacted(
        String bootstrap, String topic, byte[] objectKey, byte[] cleanupKey,
        long firstClaim, long commitOffset, long lastClaim
    ) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
        while (System.nanoTime() < deadline) {
            List<ConsumerRecord<byte[], byte[]>> records = readTargetRecords(bootstrap, topic, objectKey, cleanupKey);
            boolean firstClaimPresent = records.stream().anyMatch(record -> record.offset() == firstClaim);
            boolean latestPresent = records.stream().anyMatch(record -> record.offset() == commitOffset)
                && records.stream().anyMatch(record -> record.offset() == lastClaim);
            if (!firstClaimPresent && latestPresent) {
                return records;
            }
            Thread.sleep(500L);
        }
        throw new AssertionError("Kafka did not compact the original CLAIM within the test deadline");
    }

    private static List<ConsumerRecord<byte[], byte[]>> readTargetRecords(
        String bootstrap, String topic, byte[] objectKey, byte[] cleanupKey
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        TopicPartition partition = new TopicPartition(topic, 0);
        List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(250))) {
                    if (Arrays.equals(record.key(), objectKey) || Arrays.equals(record.key(), cleanupKey)) {
                        result.add(record);
                    }
                }
            }
            assertFalse(consumer.position(partition) < end, "Kafka replay did not reach its end offset");
        }
        return result;
    }
}
