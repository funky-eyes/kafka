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
import org.apache.kafka.clients.admin.RecordsToDelete;
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
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

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

/** Dynamic topic and log-start lifecycle correctness for shared WAL/S3 storage. */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class SharedStorageTopicLifecycleE2ETest {
    private static final String METADATA_TOPIC = "__shared_storage_metadata";

    @Test
    @Timeout(value = 7, unit = TimeUnit.MINUTES)
    public void deleteAndRecreateSameNameNeverExposesOldTopicIdData() throws Exception {
        String topic = "shared-topic-lifecycle-recreate";
        try (KafkaClusterTestKit cluster = startSharedCluster()) {
            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin()) {
                createTopic(admin, topic, 1);
                TopicDescription oldDescription = waitForTopicReady(admin, topic, 1);
                SharedPartitionId oldPartition = sharedPartitionId(oldDescription.topicId(), 0);
                produceRange(bootstrapServers, topic, 0, 0, 24, "old");
                waitForRemoteCoverage(bootstrapServers, oldPartition, 24);
                assertPartitionValues(bootstrapServers, topic, 0, 0, 24, "old");

                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
                waitForTopicAbsent(admin, topic);
                createTopic(admin, topic, 1);
                TopicDescription newDescription = waitForTopicReady(admin, topic, 1);
                assertNotEquals(oldDescription.topicId(), newDescription.topicId(),
                    "Recreated Kafka topic must receive a new topic ID");

                SharedPartitionId newPartition = sharedPartitionId(newDescription.topicId(), 0);
                produceRange(bootstrapServers, topic, 0, 0, 12, "new");
                assertPartitionValues(bootstrapServers, topic, 0, 0, 12, "new");
                waitForRemoteCoverage(bootstrapServers, newPartition, 12);

                PartitionRemoteCoverage oldCoverage = coverage(bootstrapServers, oldPartition);
                PartitionRemoteCoverage newCoverage = coverage(bootstrapServers, newPartition);
                assertTrue(oldCoverage.covers(new OffsetRange(0L, 24L)),
                    "Old immutable object metadata should remain attributable only to the old topic ID");
                assertTrue(newCoverage.covers(new OffsetRange(0L, 12L)),
                    "New topic ID must build independent authoritative remote coverage from offset zero");
            }
        }
    }

    @Test
    @Timeout(value = 7, unit = TimeUnit.MINUTES)
    public void onlinePartitionExpansionCreatesIndependentWalAndRemoteStreams() throws Exception {
        String topic = "shared-topic-lifecycle-expand";
        try (KafkaClusterTestKit cluster = startSharedCluster()) {
            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin()) {
                createTopic(admin, topic, 1);
                TopicDescription initial = waitForTopicReady(admin, topic, 1);
                produceRange(bootstrapServers, topic, 0, 0, 20, "p0-before-expand");
                waitForRemoteCoverage(
                    bootstrapServers,
                    sharedPartitionId(initial.topicId(), 0),
                    20
                );

                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(4)))
                    .all().get(30, TimeUnit.SECONDS);
                TopicDescription expanded = waitForTopicReady(admin, topic, 4);
                assertEquals(initial.topicId(), expanded.topicId(),
                    "Partition expansion must retain the Kafka topic ID");

                produceRange(bootstrapServers, topic, 0, 20, 10, "p0-after-expand");
                for (int partition = 1; partition < 4; partition++) {
                    produceRange(bootstrapServers, topic, partition, 0, 10, "p" + partition);
                }

                assertPartitionValues(bootstrapServers, topic, 0, 0, 20, "p0-before-expand");
                assertPartitionValues(bootstrapServers, topic, 0, 20, 10, "p0-after-expand");
                for (int partition = 1; partition < 4; partition++) {
                    assertPartitionValues(bootstrapServers, topic, partition, 0, 10, "p" + partition);
                }

                waitForRemoteCoverage(
                    bootstrapServers,
                    sharedPartitionId(expanded.topicId(), 0),
                    30
                );
                for (int partition = 1; partition < 4; partition++) {
                    waitForRemoteCoverage(
                        bootstrapServers,
                        sharedPartitionId(expanded.topicId(), partition),
                        10
                    );
                }
            }
        }
    }

    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    public void deleteRecordsLogStartOffsetSurvivesRemoteCoverageAndFullRestart() throws Exception {
        String topic = "shared-topic-lifecycle-delete-records";
        int deleteBeforeOffset = 20;
        int totalRecords = 40;
        try (KafkaClusterTestKit cluster = startSharedCluster()) {
            String bootstrapServers = cluster.bootstrapServers();
            TopicDescription description;
            try (Admin admin = cluster.admin()) {
                createTopic(admin, topic, 1);
                description = waitForTopicReady(admin, topic, 1);
                produceRange(bootstrapServers, topic, 0, 0, totalRecords, "retained");
                waitForRemoteCoverage(
                    bootstrapServers,
                    sharedPartitionId(description.topicId(), 0),
                    totalRecords
                );

                TopicPartition partition = new TopicPartition(topic, 0);
                admin.deleteRecords(Map.of(partition, RecordsToDelete.beforeOffset(deleteBeforeOffset)))
                    .all().get(30, TimeUnit.SECONDS);
                waitForBeginningOffset(bootstrapServers, partition, deleteBeforeOffset);
                assertRetainedTail(
                    bootstrapServers,
                    topic,
                    deleteBeforeOffset,
                    totalRecords,
                    "retained"
                );
            }

            for (BrokerServer broker : cluster.brokers().values()) {
                broker.shutdown();
            }
            for (BrokerServer broker : cluster.brokers().values()) {
                broker.awaitShutdown();
            }
            for (BrokerServer broker : cluster.brokers().values()) {
                broker.startup();
            }
            cluster.waitForReadyBrokers();

            try (Admin restartedAdmin = cluster.admin()) {
                waitForTopicReady(restartedAdmin, topic, 1);
            }
            TopicPartition partition = new TopicPartition(topic, 0);
            waitForBeginningOffset(bootstrapServers, partition, deleteBeforeOffset);
            assertRetainedTail(
                bootstrapServers,
                topic,
                deleteBeforeOffset,
                totalRecords,
                "retained"
            );
        }
    }

    private static KafkaClusterTestKit startSharedCluster() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-topic-lifecycle");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();
        KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topic.pattern", "shared-topic-lifecycle-.*")
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", 64L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", 128L * 1024)
            .setConfigProp("shared.storage.upload.interval.ms", 100L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 200L)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "topic-lifecycle/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
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
                            partition.replicas().size() == 3 && partition.isr().size() == 3
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

    private static void waitForTopicAbsent(Admin admin, String topic) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                return !admin.listTopics().names().get(10, TimeUnit.SECONDS).contains(topic);
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "Deleted topic " + topic + " remained visible in metadata");
    }

    private static void produceRange(
        String bootstrapServers,
        String topic,
        int partition,
        int startSequence,
        int count,
        String generation
    ) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < count; i++) {
                int sequence = startSequence + i;
                RecordMetadata metadata = producer.send(new ProducerRecord<>(
                    topic,
                    partition,
                    key(generation, partition, sequence),
                    value(generation, partition, sequence)
                )).get(30, TimeUnit.SECONDS);
                assertEquals(sequence, metadata.offset());
            }
        }
    }

    private static void assertPartitionValues(
        String bootstrapServers,
        String topic,
        int partitionId,
        int startSequence,
        int count,
        String generation
    ) {
        List<ConsumerRecord<String, String>> records = consumeFromOffset(
            bootstrapServers,
            topic,
            partitionId,
            startSequence,
            count
        );
        for (int i = 0; i < count; i++) {
            int sequence = startSequence + i;
            ConsumerRecord<String, String> record = records.get(i);
            assertEquals(sequence, record.offset());
            assertEquals(key(generation, partitionId, sequence), record.key());
            assertEquals(value(generation, partitionId, sequence), record.value());
        }
    }

    private static void assertRetainedTail(
        String bootstrapServers,
        String topic,
        int startOffset,
        int endOffset,
        String generation
    ) {
        List<ConsumerRecord<String, String>> records = consumeFromBeginning(
            bootstrapServers,
            topic,
            0,
            endOffset - startOffset
        );
        for (int sequence = startOffset; sequence < endOffset; sequence++) {
            ConsumerRecord<String, String> record = records.get(sequence - startOffset);
            assertEquals(sequence, record.offset());
            assertEquals(key(generation, 0, sequence), record.key());
            assertEquals(value(generation, 0, sequence), record.value());
        }
    }

    private static List<ConsumerRecord<String, String>> consumeFromOffset(
        String bootstrapServers,
        String topic,
        int partitionId,
        long startOffset,
        int count
    ) {
        Properties properties = consumerProperties(bootstrapServers);
        TopicPartition partition = new TopicPartition(topic, partitionId);
        List<ConsumerRecord<String, String>> records = new ArrayList<>(count);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seek(partition, startOffset);
            collect(consumer, records, count);
        }
        assertEquals(count, records.size());
        return List.copyOf(records);
    }

    private static List<ConsumerRecord<String, String>> consumeFromBeginning(
        String bootstrapServers,
        String topic,
        int partitionId,
        int count
    ) {
        Properties properties = consumerProperties(bootstrapServers);
        TopicPartition partition = new TopicPartition(topic, partitionId);
        List<ConsumerRecord<String, String>> records = new ArrayList<>(count);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            collect(consumer, records, count);
        }
        assertEquals(count, records.size());
        return List.copyOf(records);
    }

    private static Properties consumerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-topic-lifecycle-read-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private static void collect(
        KafkaConsumer<String, String> consumer,
        List<ConsumerRecord<String, String>> records,
        int expectedRecords
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (records.size() < expectedRecords && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                records.add(record);
            }
        }
    }

    private static void waitForBeginningOffset(
        String bootstrapServers,
        TopicPartition partition,
        long expectedOffset
    ) throws Exception {
        TestUtils.waitForCondition(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties(bootstrapServers))) {
                return consumer.beginningOffsets(List.of(partition)).get(partition) == expectedOffset;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Kafka beginning offset did not advance to " + expectedOffset + " for " + partition);
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        long endOffset
    ) throws Exception {
        TestUtils.waitForCondition(() -> coverage(bootstrapServers, partition).covers(new OffsetRange(0L, endOffset)),
            120_000L,
            () -> "Remote coverage did not reach [0," + endOffset + ") for " + partition);
    }

    private static PartitionRemoteCoverage coverage(String bootstrapServers, SharedPartitionId partition) {
        PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
        for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
            metadata.ranges().stream()
                .filter(range -> range.partition().equals(partition))
                .forEach(range -> coverage.add(range.offsets()));
        }
        return coverage;
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-topic-lifecycle-metadata-" + UUID.randomUUID());
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

    private static String key(String generation, int partition, int sequence) {
        return generation + "-key-" + partition + "-" + sequence;
    }

    private static String value(String generation, int partition, int sequence) {
        return generation + "-value-" + partition + "-" + sequence;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
