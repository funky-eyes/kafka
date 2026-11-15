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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exercises replay of actually compacted Kafka records after an actual S3 object delete. */
class KafkaOrphanCompactionMinioIntegrationTest {
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
