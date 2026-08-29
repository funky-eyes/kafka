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
import org.apache.kafka.storage.internals.shared.metadata.BrokerObjectId;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRemoteCoverage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end proof that Kafka's native RF/ISR/HW semantics protect the broker-wide WAL tail independently from S3.
 *
 * <p>The test intentionally waits for one old-leader upload, then writes a second {@code acks=all} tranche immediately
 * after that scheduler tick and shuts the leader down before the next tick. It verifies that the tranche was not remote
 * before failover and later becomes remotely committed by the newly elected leader, whose broker ID is encoded in the
 * object ID. Consumers must then read every acknowledged record exactly once.</p>
 */
@Tag("integration")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
public class SharedStorageKRaftIntegrationTest {
    private static final String TOPIC = "shared-wal-failover";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int WARMUP_RECORDS = 20;
    private static final int FAILOVER_RECORDS = 40;
    private static final int POST_FAILOVER_RECORDS = 20;
    private static final long UPLOAD_INTERVAL_MS = 30_000L;

    @Test
    public void acksAllCommittedWalTailIsUploadedByNewLeaderAfterFailover() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-e2e");
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
            .setConfigProp("shared.storage.wal.capacity.bytes", 64L * 1024 * 1024)
            .setConfigProp("shared.storage.wal.segment.bytes", 4L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", 1024L * 1024)
            .setConfigProp("shared.storage.upload.interval.ms", UPLOAD_INTERVAL_MS)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
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

                TopicDescription topic = waitForTopicReady(admin, TOPIC);
                SharedPartitionId partition = sharedPartitionId(topic.topicId(), 0);
                int oldLeader = topic.partitions().get(0).leader().id();

                OffsetRange warmupRange = produceRange(producer, 0, WARMUP_RECORDS);
                assertEquals(new OffsetRange(0L, WARMUP_RECORDS), warmupRange);
                waitForReplicatedWalOnEveryBroker(cluster);

                // Establish a known scheduler tick by waiting until the original leader has remotely committed warmup.
                TestUtils.waitForCondition(
                    () -> brokerCoverage(bootstrapServers, partition, oldLeader).covers(warmupRange),
                    90_000L,
                    () -> "Old leader " + oldLeader + " never remotely committed warmup range " + warmupRange
                );

                OffsetRange failoverRange = produceRange(producer, WARMUP_RECORDS, FAILOVER_RECORDS);
                assertEquals(
                    new OffsetRange(WARMUP_RECORDS, WARMUP_RECORDS + FAILOVER_RECORDS),
                    failoverRange
                );

                // We are immediately after the previous scheduler tick. The failover tranche must still be WAL-only.
                PartitionRemoteCoverage beforeCrash = allCoverage(bootstrapServers, partition);
                assertFalse(
                    beforeCrash.covers(failoverRange),
                    "Failover tranche unexpectedly reached S3 before the leader was stopped"
                );

                cluster.brokers().get(oldLeader).shutdown();
                int newLeader = waitForNewLeader(admin, oldLeader);
                assertNotEquals(oldLeader, newLeader);

                OffsetRange postFailoverRange = produceRange(
                    producer,
                    WARMUP_RECORDS + FAILOVER_RECORDS,
                    POST_FAILOVER_RECORDS
                );
                assertEquals(
                    new OffsetRange(
                        WARMUP_RECORDS + FAILOVER_RECORDS,
                        WARMUP_RECORDS + FAILOVER_RECORDS + POST_FAILOVER_RECORDS
                    ),
                    postFailoverRange
                );

                // This is the key recovery assertion: data acknowledged before the old leader stopped is uploaded from
                // the new leader's replicated WAL, proven by the broker ID embedded in the committed object IDs.
                TestUtils.waitForCondition(
                    () -> brokerCoverage(bootstrapServers, partition, newLeader).covers(failoverRange),
                    90_000L,
                    () -> "New leader " + newLeader + " never uploaded replicated failover range " + failoverRange
                );

                List<String> consumed = consumeAll(
                    bootstrapServers,
                    WARMUP_RECORDS + FAILOVER_RECORDS + POST_FAILOVER_RECORDS
                );
                List<String> expected = new ArrayList<>(consumed.size());
                for (int i = 0; i < consumed.size(); i++) {
                    expected.add(value(i));
                }
                assertEquals(expected, consumed, "Acknowledged records must be visible exactly once and in offset order");
            }
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static OffsetRange produceRange(
        KafkaProducer<String, String> producer,
        int valueStart,
        int count
    ) throws Exception {
        long firstOffset = -1L;
        long lastOffset = -1L;
        for (int i = 0; i < count; i++) {
            int sequence = valueStart + i;
            RecordMetadata metadata = producer.send(new ProducerRecord<>(TOPIC, 0, Integer.toString(sequence), value(sequence)))
                .get(30, TimeUnit.SECONDS);
            if (firstOffset < 0) {
                firstOffset = metadata.offset();
            }
            lastOffset = metadata.offset();
        }
        producer.flush();
        return new OffsetRange(firstOffset, Math.addExact(lastOffset, 1L));
    }

    private static String value(int sequence) {
        return "value-" + sequence;
    }

    private static TopicDescription describeTopic(Admin admin, String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS).get(topic);
    }

    private static TopicDescription waitForTopicReady(Admin admin, String topic) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describeTopic(admin, topic);
                if (description == null || description.partitions().size() != 1) {
                    return false;
                }
                var partition = description.partitions().get(0);
                if (partition.leader() == null || partition.leader().id() < 0 ||
                    partition.replicas().size() != 3 || partition.isr().size() != 3) {
                    return false;
                }
                ready[0] = description;
                return true;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "Topic " + topic + " did not converge to one leader with all three replicas in ISR");
        return ready[0];
    }

    private static int waitForNewLeader(Admin admin, int oldLeader) throws Exception {
        final int[] leader = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describeTopic(admin, TOPIC);
                if (description == null || description.partitions().isEmpty()) {
                    return false;
                }
                int candidate = description.partitions().get(0).leader().id();
                if (candidate >= 0 && candidate != oldLeader) {
                    leader[0] = candidate;
                    return true;
                }
                return false;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "No new leader elected after stopping broker " + oldLeader);
        return leader[0];
    }

    private static void waitForReplicatedWalOnEveryBroker(KafkaClusterTestKit cluster) throws Exception {
        for (Map.Entry<Integer, org.apache.kafka.common.test.TestKitNode> broker :
            cluster.nodes().brokerNodes().entrySet()) {
            int brokerId = broker.getKey();
            Path dataDir = Path.of(broker.getValue().logDataDirectories().iterator().next())
                .toAbsolutePath()
                .normalize();
            Path walDir = dataDir
                .resolveSibling(dataDir.getFileName() + ".shared-storage")
                .resolve("broker-" + brokerId)
                .resolve("wal");
            TestUtils.waitForCondition(
                () -> walPayloadBytes(walDir) > 0L,
                30_000L,
                () -> "Broker " + brokerId + " never received replicated shared WAL data in " + walDir
            );
        }
    }

    private static long walPayloadBytes(Path walDir) {
        if (!Files.isDirectory(walDir)) {
            return 0L;
        }
        try (Stream<Path> files = Files.list(walDir)) {
            return files
                .filter(path -> path.getFileName().toString().startsWith("wal-"))
                .filter(path -> path.getFileName().toString().endsWith(".log"))
                .mapToLong(path -> {
                    try {
                        return Files.size(path);
                    } catch (Exception ignored) {
                        return 0L;
                    }
                })
                .sum();
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static PartitionRemoteCoverage allCoverage(String bootstrapServers, SharedPartitionId partition) {
        return coverage(bootstrapServers, partition, null);
    }

    private static PartitionRemoteCoverage brokerCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        int brokerId
    ) {
        return coverage(bootstrapServers, partition, brokerId);
    }

    private static PartitionRemoteCoverage coverage(
        String bootstrapServers,
        SharedPartitionId partition,
        Integer objectBrokerId
    ) {
        PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
        for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
            if (objectBrokerId != null && BrokerObjectId.brokerId(metadata.objectId()) != objectBrokerId) {
                continue;
            }
            metadata.ranges().stream()
                .filter(range -> range.partition().equals(partition))
                .forEach(range -> coverage.add(range.offsets()));
        }
        return coverage;
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-storage-e2e-metadata-" + UUID.randomUUID());
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

    private static List<String> consumeAll(String bootstrapServers, int expectedRecords) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-storage-e2e-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "shared-storage-e2e-group-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        TopicPartition partition = new TopicPartition(TOPIC, 0);
        List<String> values = new ArrayList<>(expectedRecords);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (values.size() < expectedRecords && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    assertEquals(values.size(), record.offset(), "Consumer offsets must remain contiguous");
                    values.add(record.value());
                }
            }
        }
        assertEquals(expectedRecords, values.size(), "Timed out waiting for all acknowledged records");
        return values;
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition
        );
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
