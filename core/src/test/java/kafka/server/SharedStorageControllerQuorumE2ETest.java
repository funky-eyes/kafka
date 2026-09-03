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
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Proves shared WAL/S3 correctness while a three-node KRaft controller quorum changes leaders. */
@Tag("integration")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class SharedStorageControllerQuorumE2ETest {
    private static final String TOPIC = "shared-controller-ha-primary";
    private static final String SECONDARY_TOPIC = "shared-controller-ha-secondary";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int BROKERS = 3;
    private static final int CONTROLLERS = 3;

    @Test
    public void controllerFailoverPreservesDataAndPostFailoverMetadataMutations() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-controller-ha");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        try (KafkaClusterTestKit cluster = startCluster(s3Endpoint, region, bucket);
             Admin admin = cluster.admin()) {
            String bootstrapServers = cluster.bootstrapServers();
            int[] primaryOffsets = new int[] {0, 0, 0};

            createTopic(admin, TOPIC, 3);
            TopicDescription primary = waitForTopicReady(admin, TOPIC, 3);
            produce(bootstrapServers, TOPIC, primaryOffsets, 20);
            assertHistory(bootstrapServers, TOPIC, primaryOffsets);
            waitForCoverage(bootstrapServers, primary, primaryOffsets);

            int firstController = activeControllerId(cluster, -1);
            stopController(cluster, firstController);
            int secondController = waitForDifferentActiveController(cluster, firstController);
            assertNotEquals(firstController, secondController);

            produce(bootstrapServers, TOPIC, primaryOffsets, 10);
            assertHistory(bootstrapServers, TOPIC, primaryOffsets);
            waitForCoverage(bootstrapServers, primary, primaryOffsets);

            createTopic(admin, SECONDARY_TOPIC, 2);
            TopicDescription secondary = waitForTopicReady(admin, SECONDARY_TOPIC, 2);
            int[] secondaryOffsets = new int[] {0, 0};
            produce(bootstrapServers, SECONDARY_TOPIC, secondaryOffsets, 12);
            assertHistory(bootstrapServers, SECONDARY_TOPIC, secondaryOffsets);
            waitForCoverage(bootstrapServers, secondary, secondaryOffsets);

            admin.createPartitions(Map.of(TOPIC, NewPartitions.increaseTo(4)))
                .all().get(30, TimeUnit.SECONDS);
            primary = waitForTopicReady(admin, TOPIC, 4);
            primaryOffsets = extendOffsets(primaryOffsets, 4);
            produce(bootstrapServers, TOPIC, primaryOffsets, 8);
            assertHistory(bootstrapServers, TOPIC, primaryOffsets);
            waitForCoverage(bootstrapServers, primary, primaryOffsets);

            cluster.controllers().get(firstController).startup();
            int activeBeforeSecondFailure = activeControllerId(cluster, -1);
            stopController(cluster, activeBeforeSecondFailure);
            int thirdController = waitForDifferentActiveController(cluster, activeBeforeSecondFailure);
            assertNotEquals(activeBeforeSecondFailure, thirdController);

            produce(bootstrapServers, TOPIC, primaryOffsets, 8);
            produce(bootstrapServers, SECONDARY_TOPIC, secondaryOffsets, 8);
            assertHistory(bootstrapServers, TOPIC, primaryOffsets);
            assertHistory(bootstrapServers, SECONDARY_TOPIC, secondaryOffsets);
            waitForCoverage(bootstrapServers, primary, primaryOffsets);
            waitForCoverage(bootstrapServers, secondary, secondaryOffsets);

            cluster.controllers().get(activeBeforeSecondFailure).startup();
            waitForTopicReady(admin, TOPIC, 4);
        }
    }

    private static KafkaClusterTestKit startCluster(
        String s3Endpoint,
        String region,
        String bucket
    ) throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(BROKERS)
            .setNumControllerNodes(CONTROLLERS)
            .setNumDisksPerBroker(1)
            .build();
        KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topic.pattern", "shared-controller-ha-.*")
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", 16L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", 128L * 1024)
            .setConfigProp("shared.storage.upload.interval.ms", 100L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 200L)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "controller-ha/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        cluster.waitForActiveController();
        return cluster;
    }

    private static void createTopic(Admin admin, String topic, int partitions) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 3)
            .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
            .all().get(30, TimeUnit.SECONDS);
    }

    private static TopicDescription waitForTopicReady(Admin admin, String topic, int partitions) throws Exception {
        TopicDescription[] result = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = admin.describeTopics(List.of(topic))
                    .allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
                boolean ready = description != null && description.partitions().size() == partitions &&
                    description.partitions().stream().allMatch(partition ->
                        partition.leader() != null && partition.leader().id() >= 0 &&
                            partition.replicas().size() == BROKERS && partition.isr().size() == BROKERS
                    );
                if (ready) {
                    result[0] = description;
                }
                return ready;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Topic " + topic + " did not converge to " + partitions + " RF=3 partitions");
        return result[0];
    }

    private static int activeControllerId(KafkaClusterTestKit cluster, int excludedControllerId) {
        return cluster.controllers().entrySet().stream()
            .filter(entry -> entry.getKey() != excludedControllerId)
            .filter(entry -> entry.getValue().controller().isActive())
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No active KRaft controller"));
    }

    private static int waitForDifferentActiveController(
        KafkaClusterTestKit cluster,
        int oldControllerId
    ) throws Exception {
        int[] result = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                result[0] = activeControllerId(cluster, oldControllerId);
                return true;
            } catch (IllegalStateException ignored) {
                return false;
            }
        }, 60_000L, () -> "Controller quorum did not elect a replacement for " + oldControllerId);
        return result[0];
    }

    private static void stopController(KafkaClusterTestKit cluster, int controllerId) {
        ControllerServer controller = cluster.controllers().get(controllerId);
        controller.shutdown();
        controller.awaitShutdown();
    }

    private static void produce(
        String bootstrapServers,
        String topic,
        int[] endOffsets,
        int recordsPerPartition
    ) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int partition = 0; partition < endOffsets.length; partition++) {
                for (int i = 0; i < recordsPerPartition; i++) {
                    int sequence = endOffsets[partition];
                    producer.send(new ProducerRecord<>(
                        topic,
                        partition,
                        key(partition, sequence),
                        value(partition, sequence)
                    )).get(30, TimeUnit.SECONDS);
                    endOffsets[partition]++;
                }
            }
        }
    }

    private static void assertHistory(String bootstrapServers, String topic, int[] endOffsets) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-controller-ha-read-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<TopicPartition> assignments = new ArrayList<>();
        int expectedTotal = 0;
        for (int partition = 0; partition < endOffsets.length; partition++) {
            assignments.add(new TopicPartition(topic, partition));
            expectedTotal += endOffsets[partition];
        }
        int[] nextOffset = new int[endOffsets.length];
        int received = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(assignments);
            consumer.seekToBeginning(assignments);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (received < expectedTotal && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    int partition = record.partition();
                    int sequence = nextOffset[partition];
                    assertEquals(sequence, record.offset(),
                        "Controller failover must not create a gap or duplicate Kafka offset");
                    assertEquals(key(partition, sequence), record.key());
                    assertEquals(value(partition, sequence), record.value());
                    nextOffset[partition]++;
                    received++;
                }
            }
        }
        assertEquals(expectedTotal, received, "Timed out reading complete history after controller failover");
        for (int partition = 0; partition < endOffsets.length; partition++) {
            assertEquals(endOffsets[partition], nextOffset[partition]);
        }
    }

    private static void waitForCoverage(
        String bootstrapServers,
        TopicDescription description,
        int[] endOffsets
    ) throws Exception {
        TestUtils.waitForCondition(() -> {
            List<SharedObjectMetadata> objects = committedObjects(bootstrapServers);
            for (int partition = 0; partition < endOffsets.length; partition++) {
                PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
                SharedPartitionId id = sharedPartitionId(description.topicId(), partition);
                for (SharedObjectMetadata object : objects) {
                    object.ranges().stream()
                        .filter(range -> range.partition().equals(id))
                        .forEach(range -> coverage.add(range.offsets()));
                }
                if (!coverage.covers(new OffsetRange(0L, endOffsets[partition]))) {
                    return false;
                }
            }
            return true;
        }, 120_000L, () -> "Remote coverage did not catch up after KRaft controller failover");
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-controller-ha-metadata-" + UUID.randomUUID());
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
                    SharedMetadataRecordCodec.MetadataKey metadataKey =
                        SharedMetadataRecordCodec.decodeKey(record.key());
                    if (metadataKey.type() != SharedMetadataRecordCodec.KeyType.OBJECT) {
                        continue;
                    }
                    if (record.value() == null) {
                        latestCommitted.remove(metadataKey.id());
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataValue value =
                        SharedMetadataRecordCodec.decodeValue(metadataKey, record.value());
                    if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committed) {
                        latestCommitted.put(metadataKey.id(), committed.metadata());
                    }
                }
            }
        }
        return List.copyOf(latestCommitted.values());
    }

    private static int[] extendOffsets(int[] current, int size) {
        int[] extended = new int[size];
        System.arraycopy(current, 0, extended, 0, current.length);
        return extended;
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), partition);
    }

    private static String key(int partition, int sequence) {
        return "controller-key-" + partition + "-" + sequence;
    }

    private static String value(int partition, int sequence) {
        return "controller-value-" + partition + "-" + sequence + "-" + "C".repeat(1024);
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
