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
package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRemoteCoverage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Forces a real Kafka producer workload through a KSO2 object larger than the 16 MiB upload-part target.
 *
 * <p>This is intentionally above the packer's multipart boundary rather than an ObjectStore-only fixture. The test
 * proves the full path from Kafka Produce -> replicated Ring WAL -> lazy DataBlock serialization -> S3 multipart
 * completion -> metadata COMMIT. It then removes the producing leader and verifies that the complete acknowledged
 * history remains readable and that a new leader continues the Kafka offset sequence without a gap.</p>
 */
@Tag("integration")
@Timeout(value = 8, unit = TimeUnit.MINUTES)
public class SharedStorageMultipartKafkaE2ETest {
    private static final String TOPIC = "shared-multipart-kafka-e2e";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int INITIAL_RECORDS = 80;
    private static final int POST_FAILOVER_RECORDS = 8;
    private static final int VALUE_BYTES = 256 * 1024;
    private static final long MULTIPART_BOUNDARY_BYTES = 16L * 1024 * 1024;
    private static final long OBJECT_TARGET_BYTES = 18L * 1024 * 1024;
    private static final long WAL_CAPACITY_BYTES = 64L * 1024 * 1024;

    @Test
    public void kafkaProducerCreatesMultipartObjectAndSurvivesLeaderFailover() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-multipart");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topics", TOPIC)
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", WAL_CAPACITY_BYTES)
            .setConfigProp("shared.storage.object.target.bytes", OBJECT_TARGET_BYTES)
            .setConfigProp("shared.storage.upload.interval.ms", 100L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 10_000L)
            .setConfigProp("shared.storage.upload.wal.pressure.percent", 90)
            .setConfigProp("shared.storage.upload.max.inflight", 1)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "multipart/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .build()) {

            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            String bootstrapServers = cluster.bootstrapServers();

            try (Admin admin = cluster.admin();
                 KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3)
                    .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                    .all().get(30, TimeUnit.SECONDS);
                TopicDescription description = waitForTopicReady(admin, 3);
                SharedPartitionId partition = sharedPartitionId(description.topicId(), 0);
                int oldLeader = description.partitions().get(0).leader().id();

                produce(producer, 0, INITIAL_RECORDS);
                assertEquals(INITIAL_RECORDS, consumeAndAssert(bootstrapServers, INITIAL_RECORDS));

                SharedObjectMetadata multipartObject = waitForMultipartObject(bootstrapServers, partition);
                assertTrue(multipartObject.objectSize() > MULTIPART_BOUNDARY_BYTES,
                    "The committed Kafka object must cross the 16 MiB streaming part boundary");
                assertTrue(multipartObject.ranges().size() > 1,
                    "The multipart object must contain more than one Kafka batch range");

                waitForRemoteCoverage(bootstrapServers, partition, INITIAL_RECORDS);

                cluster.brokers().get(oldLeader).shutdown();
                cluster.brokers().get(oldLeader).awaitShutdown();
                int newLeader = waitForNewLeader(admin, oldLeader);
                assertNotEquals(oldLeader, newLeader);

                assertEquals(INITIAL_RECORDS, consumeAndAssert(bootstrapServers, INITIAL_RECORDS));
                produce(producer, INITIAL_RECORDS, POST_FAILOVER_RECORDS);
                int finalRecords = INITIAL_RECORDS + POST_FAILOVER_RECORDS;
                assertEquals(finalRecords, consumeAndAssert(bootstrapServers, finalRecords));

                cluster.brokers().get(oldLeader).startup();
                cluster.waitForReadyBrokers();
                waitForTopicReady(admin, 3);
                waitForRemoteCoverage(bootstrapServers, partition, finalRecords);
                assertEquals(finalRecords, consumeAndAssert(bootstrapServers, finalRecords));
            }
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 512 * 1024);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static void produce(
        KafkaProducer<String, String> producer,
        int startSequence,
        int count
    ) throws Exception {
        List<RecordMetadata> metadata = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int sequence = startSequence + i;
            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>(
                TOPIC,
                0,
                key(sequence),
                value(sequence)
            )).get(30, TimeUnit.SECONDS);
            metadata.add(recordMetadata);
        }
        producer.flush();
        for (int i = 0; i < metadata.size(); i++) {
            assertEquals(startSequence + i, metadata.get(i).offset(),
                "Acknowledged Kafka offsets must remain contiguous across multipart object formation");
        }
    }

    private static int consumeAndAssert(String bootstrapServers, int expectedRecords) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-multipart-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        TopicPartition partition = new TopicPartition(TOPIC, 0);
        int received = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (received < expectedRecords && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    assertEquals(received, record.offset(),
                        "Remote/local reads must not lose, duplicate or reorder Kafka records");
                    assertEquals(key(received), record.key());
                    assertEquals(value(received), record.value());
                    received++;
                }
            }
        }
        assertEquals(expectedRecords, received, "Timed out reading all acknowledged multipart-path records");
        return received;
    }

    private static SharedObjectMetadata waitForMultipartObject(
        String bootstrapServers,
        SharedPartitionId partition
    ) throws Exception {
        SharedObjectMetadata[] result = new SharedObjectMetadata[1];
        TestUtils.waitForCondition(() -> {
            for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
                boolean containsPartition = metadata.ranges().stream()
                    .anyMatch(range -> range.partition().equals(partition));
                if (containsPartition && metadata.objectSize() > MULTIPART_BOUNDARY_BYTES) {
                    result[0] = metadata;
                    return true;
                }
            }
            return false;
        }, 120_000L, () -> "No committed Kafka-produced object crossed the 16 MiB multipart boundary");
        return result[0];
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        long endOffset
    ) throws Exception {
        TestUtils.waitForCondition(() -> {
            PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
            for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
                metadata.ranges().stream()
                    .filter(range -> range.partition().equals(partition))
                    .forEach(range -> coverage.add(range.offsets()));
            }
            return coverage.covers(new OffsetRange(0L, endOffset));
        }, 120_000L, () -> "Authoritative remote coverage did not reach [0," + endOffset + ")");
    }

    private static int waitForNewLeader(Admin admin, int oldLeader) throws Exception {
        int[] result = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describe(admin);
                int candidate = description.partitions().get(0).leader().id();
                if (candidate >= 0 && candidate != oldLeader && description.partitions().get(0).isr().size() == 2) {
                    result[0] = candidate;
                    return true;
                }
                return false;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "No healthy replacement leader after stopping broker " + oldLeader);
        return result[0];
    }

    private static TopicDescription waitForTopicReady(Admin admin, int expectedIsr) throws Exception {
        TopicDescription[] result = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describe(admin);
                if (description.partitions().size() != 1) {
                    return false;
                }
                var partition = description.partitions().get(0);
                boolean ready = partition.leader() != null && partition.leader().id() >= 0 &&
                    partition.replicas().size() == 3 && partition.isr().size() == expectedIsr;
                if (ready) {
                    result[0] = description;
                }
                return ready;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Topic did not converge to RF=3 and ISR=" + expectedIsr);
        return result[0];
    }

    private static TopicDescription describe(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(TOPIC);
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-multipart-metadata-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        TopicPartition metadataPartition = new TopicPartition(METADATA_TOPIC, 0);
        Map<Long, SharedObjectMetadata> latestCommitted = new LinkedHashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(metadataPartition));
            consumer.seekToBeginning(List.of(metadataPartition));
            long endOffset = consumer.endOffsets(List.of(metadataPartition)).get(metadataPartition);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (consumer.position(metadataPartition) < endOffset && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.key() == null) {
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(record.key());
                    if (key.type() != SharedMetadataRecordCodec.KeyType.OBJECT) {
                        continue;
                    }
                    if (record.value() == null) {
                        latestCommitted.remove(key.id());
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataValue value =
                        SharedMetadataRecordCodec.decodeValue(key, record.value());
                    if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committed) {
                        latestCommitted.put(key.id(), committed.metadata());
                    }
                }
            }
        }
        return List.copyOf(latestCommitted.values());
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition
        );
    }

    private static String key(int sequence) {
        return "key-" + sequence;
    }

    private static String value(int sequence) {
        String prefix = "value-" + sequence + "-";
        return prefix + "M".repeat(VALUE_BYTES - prefix.length());
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
