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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end high-availability proof for the replicated broker-wide Ring WAL plus S3 architecture.
 *
 * <p>The workload deliberately exceeds the physical Ring WAL capacity several times while waiting for authoritative
 * remote coverage between bounded chunks. This proves that acknowledged history survives actual WAL reuse. The test
 * then removes each broker in turn while continuing acks=all writes and full-history reads, restores full ISR, and
 * finally restarts all brokers before reading from offset zero again. Every phase verifies gap-free partition offsets,
 * deterministic keys and values, and therefore detects message loss, duplication, reordering or stale remote reads.</p>
 */
@Tag("integration")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class SharedStorageHighAvailabilityE2ETest {
    private static final String TOPIC = "shared-ha-lifecycle";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int PARTITIONS = 3;
    private static final int BASE_CHUNKS = 7;
    private static final int RECORDS_PER_CHUNK_PER_PARTITION = 24;
    private static final int ROLLING_RECORDS_PER_PARTITION = 8;
    private static final int FINAL_RECORDS_PER_PARTITION = 8;
    private static final int VALUE_BYTES = 4 * 1024;
    private static final long WAL_CAPACITY_BYTES = 512L * 1024;
    private static final long OBJECT_TARGET_BYTES = 64L * 1024;
    private static final long MAX_SEQUENTIAL_BROKER_STARTUP_MS = 30_000L;

    @Test
    public void rollingAndFullBrokerRestartPreserveRecycledWalHistory() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-ha");
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
            .setConfigProp("shared.storage.upload.interval.ms", 50L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 100L)
            .setConfigProp("shared.storage.upload.wal.pressure.percent", 60)
            .setConfigProp("shared.storage.upload.max.inflight", 4)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "ha/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .build()) {

            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            String bootstrapServers = cluster.bootstrapServers();
            int[] nextSequence = new int[PARTITIONS];

            TopicDescription description;
            try (Admin admin = cluster.admin();
                 KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) 3)
                    .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                    .all().get(30, TimeUnit.SECONDS);
                description = waitForTopicState(admin, 3, null);

                for (int chunk = 0; chunk < BASE_CHUNKS; chunk++) {
                    produce(producer, nextSequence, RECORDS_PER_CHUNK_PER_PARTITION);
                    waitForRemoteCoverage(bootstrapServers, description, nextSequence);
                }

                long acknowledgedPayloadBytes = (long) PARTITIONS * BASE_CHUNKS *
                    RECORDS_PER_CHUNK_PER_PARTITION * VALUE_BYTES;
                assertTrue(acknowledgedPayloadBytes > WAL_CAPACITY_BYTES * 3L,
                    "The pre-restart workload must force multiple logical Ring WAL reuse cycles");
                consumeAndAssert(bootstrapServers, nextSequence);

                for (int brokerId : List.copyOf(cluster.brokers().keySet())) {
                    BrokerServer broker = cluster.brokers().get(brokerId);
                    broker.shutdown();
                    broker.awaitShutdown();
                    waitForTopicState(admin, 2, brokerId);

                    produce(producer, nextSequence, ROLLING_RECORDS_PER_PARTITION);
                    consumeAndAssert(bootstrapServers, nextSequence);
                    waitForRemoteCoverage(bootstrapServers, description, nextSequence);

                    broker.startup();
                    cluster.waitForReadyBrokers();
                    waitForTopicState(admin, 3, null);
                    consumeAndAssert(bootstrapServers, nextSequence);
                }
            }

            restartAllBrokersSequentially(cluster);

            try (Admin restartedAdmin = cluster.admin()) {
                description = waitForTopicState(restartedAdmin, 3, null);
                consumeAndAssert(bootstrapServers, nextSequence);

                try (KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                    produce(producer, nextSequence, FINAL_RECORDS_PER_PARTITION);
                }
                waitForRemoteCoverage(bootstrapServers, description, nextSequence);
                consumeAndAssert(bootstrapServers, nextSequence);
            }

            for (int partition = 0; partition < PARTITIONS; partition++) {
                assertEquals(200, nextSequence[partition],
                    "Every partition must finish with the same deterministic acknowledged record count");
            }
        }
    }

    private static void restartAllBrokersSequentially(KafkaClusterTestKit cluster) throws Exception {
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.shutdown();
        }
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.awaitShutdown();
        }
        for (BrokerServer broker : cluster.brokers().values()) {
            long startNanos = System.nanoTime();
            broker.startup();
            long startupMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            assertTrue(startupMs < MAX_SEQUENTIAL_BROKER_STARTUP_MS,
                "Each broker must finish sequential cold startup without waiting for the next broker; took " +
                    startupMs + " ms");
        }
        cluster.waitForReadyBrokers();
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static void produce(
        KafkaProducer<String, String> producer,
        int[] nextSequence,
        int recordsPerPartition
    ) throws Exception {
        List<PendingSend> sends = new ArrayList<>(PARTITIONS * recordsPerPartition);
        for (int partition = 0; partition < PARTITIONS; partition++) {
            for (int i = 0; i < recordsPerPartition; i++) {
                int sequence = nextSequence[partition]++;
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
                    TOPIC,
                    partition,
                    key(partition, sequence),
                    value(partition, sequence)
                ));
                sends.add(new PendingSend(partition, sequence, future));
            }
        }
        producer.flush();
        for (PendingSend send : sends) {
            RecordMetadata metadata = send.future().get(30, TimeUnit.SECONDS);
            assertEquals(send.partition(), metadata.partition());
            assertEquals(send.sequence(), metadata.offset(),
                "No failed or phantom append may create a gap in the acknowledged Kafka offset sequence");
        }
    }

    private static void consumeAndAssert(String bootstrapServers, int[] expectedEndOffsets) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-ha-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<TopicPartition> assignments = new ArrayList<>();
        int expectedTotal = 0;
        for (int partition = 0; partition < PARTITIONS; partition++) {
            assignments.add(new TopicPartition(TOPIC, partition));
            expectedTotal += expectedEndOffsets[partition];
        }

        int[] nextExpected = new int[PARTITIONS];
        int received = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(assignments);
            consumer.seekToBeginning(assignments);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (received < expectedTotal && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    int partition = record.partition();
                    int sequence = nextExpected[partition];
                    assertTrue(sequence < expectedEndOffsets[partition],
                        "Consumer returned a duplicate or unexpected record past the acknowledged partition end");
                    assertEquals(sequence, record.offset(),
                        "Partition offsets must remain gap-free and ordered through failover and remote reads");
                    assertEquals(key(partition, sequence), record.key());
                    assertEquals(value(partition, sequence), record.value());
                    nextExpected[partition]++;
                    received++;
                }
            }
        }
        assertEquals(expectedTotal, received, "Timed out reading the complete acknowledged history");
        for (int partition = 0; partition < PARTITIONS; partition++) {
            assertEquals(expectedEndOffsets[partition], nextExpected[partition],
                "Partition history must contain every acknowledged record exactly once");
        }
    }

    private static TopicDescription waitForTopicState(
        Admin admin,
        int expectedIsrSize,
        Integer unavailableBrokerId
    ) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = admin.describeTopics(List.of(TOPIC))
                    .allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
                if (description == null || description.partitions().size() != PARTITIONS) {
                    return false;
                }
                boolean valid = description.partitions().stream().allMatch(partition -> {
                    if (partition.leader() == null || partition.leader().id() < 0 ||
                        partition.replicas().size() != 3 || partition.isr().size() != expectedIsrSize) {
                        return false;
                    }
                    return unavailableBrokerId == null || partition.leader().id() != unavailableBrokerId;
                });
                if (valid) {
                    ready[0] = description;
                }
                return valid;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Topic did not converge to expected ISR=" + expectedIsrSize +
            " unavailableBroker=" + unavailableBrokerId);
        return ready[0];
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        TopicDescription description,
        int[] expectedEndOffsets
    ) throws Exception {
        TestUtils.waitForCondition(() -> {
            List<SharedObjectMetadata> objects = committedObjects(bootstrapServers);
            for (int partition = 0; partition < PARTITIONS; partition++) {
                SharedPartitionId sharedPartition = sharedPartitionId(description.topicId(), partition);
                PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
                for (SharedObjectMetadata object : objects) {
                    object.ranges().stream()
                        .filter(range -> range.partition().equals(sharedPartition))
                        .forEach(range -> coverage.add(range.offsets()));
                }
                if (!coverage.covers(new OffsetRange(0L, expectedEndOffsets[partition]))) {
                    return false;
                }
            }
            return true;
        }, 120_000L, () -> "Authoritative S3 coverage did not reach all acknowledged HA offsets");
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-ha-metadata-" + UUID.randomUUID());
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

    private static String key(int partition, int sequence) {
        return "key-" + partition + "-" + sequence;
    }

    private static String value(int partition, int sequence) {
        String prefix = "value-" + partition + "-" + sequence + "-";
        return prefix + "R".repeat(VALUE_BYTES - prefix.length());
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record PendingSend(int partition, int sequence, Future<RecordMetadata> future) {
    }
}
