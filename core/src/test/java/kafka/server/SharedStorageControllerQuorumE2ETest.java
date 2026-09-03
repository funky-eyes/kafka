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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves that the shared WAL/S3 data plane stays correct while the three-node KRaft controller quorum changes leaders.
 */
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

        try (KafkaClusterTestKit cluster = startCluster(s3Endpoint, region, bucket)) {
            String bootstrapServers = cluster.bootstrapServers();
            int[] primaryEndOffsets = new int[] {0, 0, 0};

            try (Admin admin = cluster.admin()) {
                createTopic(admin, TOPIC, 3);
                TopicDescription primary = waitForTopicReady(admin, TOPIC, 3);
                produceChunk(bootstrapServers, TOPIC, primaryEndOffsets, 20, "before-controller-failover");
                waitForCoverage(bootstrapServers, primary, primaryEndOffsets);
                assertHistory(bootstrapServers, TOPIC, primaryEndOffsets);

                int firstActiveController = activeControllerId(cluster, -1);
                stopController(cluster, firstActiveController);
                int secondActiveController = waitForDifferentActiveController(cluster, firstActiveController);
                assertNotEquals(firstActiveController, secondActiveController);

                produceChunk(bootstrapServers, TOPIC, primaryEndOffsets, 10, "after-first-controller-failover");
                assertHistory(bootstrapServers, TOPIC, primaryEndOffsets);
                waitForCoverage(bootstrapServers, primary, primaryEndOffsets);

                createTopic(admin, SECONDARY_TOPIC, 2);
                TopicDescription secondary = waitForTopicReady(admin, SECONDARY_TOPIC, 2);
                int[] secondaryEndOffsets = new int[] {0, 0};
                produceChunk(bootstrapServers, SECONDARY_TOPIC, secondaryEndOffsets, 12, "created-after-failover");
                assertHistory(bootstrapServers, SECONDARY_TOPIC, secondaryEndOffsets);
                waitForCoverage(bootstrapServers, secondary, secondaryEndOffsets);

                admin.createPartitions(Map.of(TOPIC, NewPartitions.increaseTo(4)))
                    .all().get(30, TimeUnit.SECONDS);
                primary = waitForTopicReady(admin, TOPIC, 4);
                primaryEndOffsets = extendOffsets(primaryEndOffsets, 4);
                produceChunk(bootstrapServers, TOPIC, primaryEndOffsets, 8, "expanded-after-failover");
                assertHistory(bootstrapServers, TOPIC, primaryEndOffsets);
                waitForCoverage(bootstrapServers, primary, primaryEndOffsets);

                cluster.controllers().get(firstActiveController).startup();
                int controllerBeforeSecondFailure = activeControllerId(cluster, -1);
                stopController(cluster, controllerBeforeSecondFailure);
                int thirdActiveController = waitForDifferentActiveController(cluster, controllerBeforeSecondFailure);
                assertNotEquals(controllerBeforeSecondFailure, thirdActiveController);

                produceChunk(bootstrapServers, TOPIC, primaryEndOffsets, 8, "after-second-controller-failover");
                produceChunk(bootstrapServers, SECONDARY_TOPIC, secondaryEndOffsets, 8, "after-second-controller-failover");
                assertHistory(bootstrapServers, TOPIC, primaryEndOffsets);
                assertHistory(bootstrapServers, SECONDARY_TOPIC, secondaryEndOffsets);
                waitForCoverage(bootstrapServers, primary, primaryEndOffsets);
                waitForCoverage(bootstrapServers, secondary, secondaryEndOffsets);

                cluster.controllers().get(controllerBeforeSecondFailure).startup();
                assertEquals(BROKERS, waitForTopicReady(admin, TOPIC, 4).partitions().get(0).isr().size());
            }
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
                int candidate = activeControllerId(cluster, oldControllerId);
                result[0] = candidate;
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

    private static void produceChunk(
        String bootstrapServers,
        String topic,
        int[] endOffsets,
        int recordsPerPartition,
        String phase
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
                        value(phase, partition, sequence)
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
                    assertEquals(nextOffset[partition], record.offset(),
                        "Controller failover must not create a gap or duplicate Kafka offset");
                    assertEquals(key(partition, nextOffset[partition]), record.key());
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

    private static int[] extendOffsets(int[] current, int size) {
        int[] extended = new int[size];
        System.arraycopy(current, 0, extended, 0, current.length);
        return extended;
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

    private static String value(String phase, int partition, int sequence) {
        return phase + "-value-" + partition + "-" + sequence + "-" + "C".repeat(1024);
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
