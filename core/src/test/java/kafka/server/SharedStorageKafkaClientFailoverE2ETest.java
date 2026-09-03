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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Kafka client correctness under broker and leader changes on the shared WAL/S3 storage path. */
@Tag("integration")
@Execution(ExecutionMode.SAME_THREAD)
public class SharedStorageKafkaClientFailoverE2ETest {
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int BROKERS = 3;

    @Test
    @Timeout(value = 7, unit = TimeUnit.MINUTES)
    public void concurrentIdempotentProducersRemainUniqueAcrossLeaderEpochChange() throws Exception {
        String topic = "shared-idempotent-producer-failover";
        try (KafkaClusterTestKit cluster = startSharedCluster(topic, 8L * 1024 * 1024)) {
            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin()) {
                createTopic(admin, topic, 1);
                TopicDescription description = waitForTopicReady(admin, topic, 1, 3);
                int oldLeader = description.partitions().get(0).leader().id();
                SharedPartitionId sharedPartition = sharedPartitionId(description.topicId(), 0);

                int producers = 2;
                int recordsPerProducer = 60;
                int beforeFailover = 20;
                CountDownLatch atFailoverBoundary = new CountDownLatch(producers);
                CountDownLatch resumeAfterFailover = new CountDownLatch(1);
                ExecutorService executor = Executors.newFixedThreadPool(producers);
                List<Future<Void>> tasks = startIdempotentProducerTasks(
                    executor,
                    bootstrapServers,
                    topic,
                    producers,
                    recordsPerProducer,
                    beforeFailover,
                    atFailoverBoundary,
                    resumeAfterFailover
                );
                try {
                    assertTrue(atFailoverBoundary.await(90, TimeUnit.SECONDS),
                        "Both idempotent producers must establish state before leader failover");
                    stopBroker(cluster, oldLeader);
                    int newLeader = waitForNewLeader(admin, topic, oldLeader, 2);
                    assertNotEquals(oldLeader, newLeader);
                    resumeAfterFailover.countDown();
                    awaitTasks(tasks);
                } finally {
                    resumeAfterFailover.countDown();
                    executor.shutdownNow();
                }

                int expectedRecords = producers * recordsPerProducer;
                assertUniqueProducerHistory(
                    bootstrapServers,
                    topic,
                    producers,
                    recordsPerProducer,
                    expectedRecords
                );
                waitForRemoteCoverage(bootstrapServers, sharedPartition, expectedRecords);

                cluster.brokers().get(oldLeader).startup();
                cluster.waitForReadyBrokers();
                waitForTopicReady(admin, topic, 1, 3);
            }
        }
    }

    @Test
    @Timeout(value = 7, unit = TimeUnit.MINUTES)
    public void transactionCommitAndAbortRemainCorrectAcrossLeaderFailover() throws Exception {
        String topic = "shared-transaction-leader-failover";
        try (KafkaClusterTestKit cluster = startSharedCluster(topic, 4L * 1024 * 1024)) {
            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin();
                 KafkaProducer<String, String> producer = transactionalProducer(bootstrapServers)) {
                createTopic(admin, topic, 1);
                TopicDescription description = waitForTopicReady(admin, topic, 1, 3);
                int oldLeader = description.partitions().get(0).leader().id();

                producer.initTransactions();
                producer.beginTransaction();
                sendTransactionRecords(producer, topic, "commit", 12);
                stopBroker(cluster, oldLeader);
                int newLeader = waitForNewLeader(admin, topic, oldLeader, 2);
                assertNotEquals(oldLeader, newLeader);
                producer.commitTransaction();

                assertTransactionRecords(
                    consumeAssigned(bootstrapServers, topic, 0, 12, "read_committed"),
                    "commit",
                    12
                );

                producer.beginTransaction();
                sendTransactionRecords(producer, topic, "abort", 6);
                producer.abortTransaction();

                List<ConsumerRecord<String, String>> afterAbort = consumeAssigned(
                    bootstrapServers,
                    topic,
                    0,
                    12,
                    "read_committed"
                );
                assertEquals(12, afterAbort.size(), "Aborted records must remain invisible to read_committed");

                List<ConsumerRecord<String, String>> uncommitted = consumeAssigned(
                    bootstrapServers,
                    topic,
                    0,
                    18,
                    "read_uncommitted"
                );
                assertEquals(6L, uncommitted.stream()
                    .filter(record -> record.key().startsWith("abort-"))
                    .count());

                cluster.brokers().get(oldLeader).startup();
                cluster.waitForReadyBrokers();
                waitForTopicReady(admin, topic, 1, 3);
                assertEquals(12, consumeAssigned(
                    bootstrapServers,
                    topic,
                    0,
                    12,
                    "read_committed"
                ).size());
            }
        }
    }

    @Test
    @Timeout(value = 8, unit = TimeUnit.MINUTES)
    public void consumerGroupCommittedOffsetsSurviveFullBrokerRestart() throws Exception {
        String topic = "shared-consumer-group-restart";
        String groupId = "shared-consumer-group-" + UUID.randomUUID();
        int partitions = 3;
        int recordsPerPartition = 30;
        int committedOffset = 10;

        try (KafkaClusterTestKit cluster = startSharedCluster(topic, 8L * 1024 * 1024)) {
            String bootstrapServers = cluster.bootstrapServers();
            TopicDescription description = createReadyTopic(cluster, topic, partitions);
            producePartitioned(bootstrapServers, topic, partitions, recordsPerPartition);
            waitForAllPartitionCoverage(
                bootstrapServers,
                description,
                partitions,
                recordsPerPartition
            );
            readAndCommitGroupOffsets(
                bootstrapServers,
                topic,
                groupId,
                partitions,
                committedOffset
            );

            restartAllBrokersSequentially(cluster);
            try (Admin restartedAdmin = cluster.admin()) {
                waitForTopicReady(restartedAdmin, topic, partitions, 3);
            }

            assertGroupResume(
                bootstrapServers,
                topic,
                groupId,
                partitions,
                committedOffset,
                recordsPerPartition
            );
        }
    }

    private static List<Future<Void>> startIdempotentProducerTasks(
        ExecutorService executor,
        String bootstrapServers,
        String topic,
        int producers,
        int recordsPerProducer,
        int beforeFailover,
        CountDownLatch atFailoverBoundary,
        CountDownLatch resumeAfterFailover
    ) {
        List<Future<Void>> tasks = new ArrayList<>();
        for (int producerId = 0; producerId < producers; producerId++) {
            int id = producerId;
            tasks.add(executor.submit(() -> {
                runIdempotentProducer(
                    bootstrapServers,
                    topic,
                    id,
                    recordsPerProducer,
                    beforeFailover,
                    atFailoverBoundary,
                    resumeAfterFailover
                );
                return null;
            }));
        }
        return tasks;
    }

    private static void runIdempotentProducer(
        String bootstrapServers,
        String topic,
        int producerId,
        int recordsPerProducer,
        int beforeFailover,
        CountDownLatch atFailoverBoundary,
        CountDownLatch resumeAfterFailover
    ) throws Exception {
        try (KafkaProducer<String, String> producer = idempotentProducer(
            bootstrapServers,
            "shared-idempotent-" + producerId
        )) {
            for (int sequence = 0; sequence < recordsPerProducer; sequence++) {
                if (sequence == beforeFailover) {
                    atFailoverBoundary.countDown();
                    if (!resumeAfterFailover.await(90, TimeUnit.SECONDS)) {
                        throw new AssertionError("Timed out waiting for leader failover");
                    }
                }
                producer.send(new ProducerRecord<>(
                    topic,
                    0,
                    producerKey(producerId, sequence),
                    producerValue(producerId, sequence)
                )).get(30, TimeUnit.SECONDS);
            }
            producer.flush();
        }
    }

    private static void awaitTasks(List<Future<Void>> tasks) throws Exception {
        for (Future<Void> task : tasks) {
            task.get(120, TimeUnit.SECONDS);
        }
    }

    private static void assertUniqueProducerHistory(
        String bootstrapServers,
        String topic,
        int producers,
        int recordsPerProducer,
        int expectedRecords
    ) {
        List<ConsumerRecord<String, String>> consumed = consumeAssigned(
            bootstrapServers,
            topic,
            0,
            expectedRecords,
            "read_committed"
        );
        Set<String> expectedKeys = new HashSet<>();
        for (int producerId = 0; producerId < producers; producerId++) {
            for (int sequence = 0; sequence < recordsPerProducer; sequence++) {
                expectedKeys.add(producerKey(producerId, sequence));
            }
        }
        Set<String> actualKeys = new HashSet<>();
        long expectedOffset = 0L;
        for (ConsumerRecord<String, String> record : consumed) {
            assertEquals(expectedOffset++, record.offset(),
                "Concurrent producer failover must not create Kafka offset gaps or duplicate offsets");
            assertTrue(actualKeys.add(record.key()), "Duplicate acknowledged producer key " + record.key());
            assertTrue(expectedKeys.contains(record.key()), "Unexpected producer key " + record.key());
            assertEquals(expectedValueForKey(record.key()), record.value());
        }
        assertEquals(expectedKeys, actualKeys,
            "Every acknowledged idempotent producer record must be visible exactly once");
    }

    private static void sendTransactionRecords(
        KafkaProducer<String, String> producer,
        String topic,
        String phase,
        int count
    ) throws Exception {
        for (int sequence = 0; sequence < count; sequence++) {
            producer.send(new ProducerRecord<>(
                topic,
                0,
                phase + "-" + sequence,
                phase + "-" + sequence
            )).get(30, TimeUnit.SECONDS);
        }
    }

    private static void assertTransactionRecords(
        List<ConsumerRecord<String, String>> records,
        String phase,
        int count
    ) {
        assertEquals(count, records.size());
        for (int sequence = 0; sequence < count; sequence++) {
            assertEquals(phase + "-" + sequence, records.get(sequence).key());
            assertEquals(phase + "-" + sequence, records.get(sequence).value());
        }
    }

    private static TopicDescription createReadyTopic(
        KafkaClusterTestKit cluster,
        String topic,
        int partitions
    ) throws Exception {
        try (Admin admin = cluster.admin()) {
            createTopic(admin, topic, partitions);
            return waitForTopicReady(admin, topic, partitions, 3);
        }
    }

    private static void waitForAllPartitionCoverage(
        String bootstrapServers,
        TopicDescription description,
        int partitions,
        int endOffset
    ) throws Exception {
        for (int partition = 0; partition < partitions; partition++) {
            waitForRemoteCoverage(
                bootstrapServers,
                sharedPartitionId(description.topicId(), partition),
                endOffset
            );
        }
    }

    private static void readAndCommitGroupOffsets(
        String bootstrapServers,
        String topic,
        String groupId,
        int partitions,
        int committedOffset
    ) {
        Properties properties = groupConsumerProperties(bootstrapServers, groupId);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            awaitEveryPartitionOffset(consumer, partitions, committedOffset - 1L);
            Map<TopicPartition, OffsetAndMetadata> commits = new LinkedHashMap<>();
            for (int partition = 0; partition < partitions; partition++) {
                commits.put(new TopicPartition(topic, partition), new OffsetAndMetadata(committedOffset));
            }
            consumer.commitSync(commits);
        }
    }

    private static void awaitEveryPartitionOffset(
        KafkaConsumer<String, String> consumer,
        int partitions,
        long requiredOffset
    ) {
        Map<Integer, Long> maxSeen = new HashMap<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!allPartitionsReached(maxSeen, partitions, requiredOffset) && System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                maxSeen.merge(record.partition(), record.offset(), Math::max);
            }
        }
        assertTrue(allPartitionsReached(maxSeen, partitions, requiredOffset),
            "Consumer group must reach the commit boundary on every partition");
    }

    private static boolean allPartitionsReached(
        Map<Integer, Long> maxSeen,
        int partitions,
        long requiredOffset
    ) {
        return maxSeen.size() == partitions &&
            maxSeen.values().stream().allMatch(offset -> offset >= requiredOffset);
    }

    private static void assertGroupResume(
        String bootstrapServers,
        String topic,
        String groupId,
        int partitions,
        int committedOffset,
        int recordsPerPartition
    ) {
        Properties properties = groupConsumerProperties(bootstrapServers, groupId);
        int[] nextExpected = new int[partitions];
        java.util.Arrays.fill(nextExpected, committedOffset);
        int expectedRemaining = partitions * (recordsPerPartition - committedOffset);
        Set<Integer> seenPartitions = new HashSet<>();
        int received = consumeGroupRemainder(
            properties,
            topic,
            nextExpected,
            seenPartitions,
            expectedRemaining,
            committedOffset
        );
        assertEquals(expectedRemaining, received,
            "Consumer group must read every uncommitted record after full broker restart");
        assertEquals(partitions, seenPartitions.size());
        for (int partition = 0; partition < partitions; partition++) {
            assertEquals(recordsPerPartition, nextExpected[partition]);
        }
    }

    private static int consumeGroupRemainder(
        Properties properties,
        String topic,
        int[] nextExpected,
        Set<Integer> seenPartitions,
        int expectedRemaining,
        int committedOffset
    ) {
        int received = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (received < expectedRemaining && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    assertResumedGroupRecord(record, nextExpected, seenPartitions, committedOffset);
                    received++;
                }
            }
        }
        return received;
    }

    private static void assertResumedGroupRecord(
        ConsumerRecord<String, String> record,
        int[] nextExpected,
        Set<Integer> seenPartitions,
        int committedOffset
    ) {
        int partition = record.partition();
        int expectedSequence = nextExpected[partition];
        if (seenPartitions.add(partition)) {
            assertEquals(committedOffset, record.offset(),
                "First record after full restart must start at the committed group offset");
        }
        assertEquals(expectedSequence, record.offset(),
            "Consumer group resume must not skip or replay committed offsets");
        assertEquals(groupKey(partition, expectedSequence), record.key());
        assertEquals(groupValue(partition, expectedSequence), record.value());
        nextExpected[partition]++;
    }

    private static void restartAllBrokersSequentially(KafkaClusterTestKit cluster) throws Exception {
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.shutdown();
        }
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.awaitShutdown();
        }
        // Deliberately sequential. A cold cluster must not require a second broker process to be started in parallel
        // merely so the first broker can finish its StorageExtension onBrokerReady hook.
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.startup();
        }
        cluster.waitForReadyBrokers();
    }

    private static void stopBroker(KafkaClusterTestKit cluster, int brokerId) throws Exception {
        BrokerServer broker = cluster.brokers().get(brokerId);
        broker.shutdown();
        broker.awaitShutdown();
    }

    private static KafkaClusterTestKit startSharedCluster(String topic, long objectTargetBytes) throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-client-failover");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(BROKERS)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();
        KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topics", topic)
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", 64L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", objectTargetBytes)
            .setConfigProp("shared.storage.upload.interval.ms", 100L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 250L)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "client-failover/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .setConfigProp("group.initial.rebalance.delay.ms", 0)
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

    private static KafkaProducer<String, String> idempotentProducer(String bootstrapServers, String clientId) {
        Properties properties = baseProducerProperties(bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(properties);
    }

    private static KafkaProducer<String, String> transactionalProducer(String bootstrapServers) {
        Properties properties = baseProducerProperties(bootstrapServers);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "shared-tx-failover-" + UUID.randomUUID());
        properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 120_000);
        return new KafkaProducer<>(properties);
    }

    private static Properties baseProducerProperties(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    private static void producePartitioned(
        String bootstrapServers,
        String topic,
        int partitions,
        int recordsPerPartition
    ) throws Exception {
        try (KafkaProducer<String, String> producer = idempotentProducer(
            bootstrapServers,
            "shared-group-seed"
        )) {
            List<Future<RecordMetadata>> sends = new ArrayList<>();
            for (int partition = 0; partition < partitions; partition++) {
                for (int sequence = 0; sequence < recordsPerPartition; sequence++) {
                    sends.add(producer.send(new ProducerRecord<>(
                        topic,
                        partition,
                        groupKey(partition, sequence),
                        groupValue(partition, sequence)
                    )));
                }
            }
            producer.flush();
            for (Future<RecordMetadata> send : sends) {
                send.get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static Properties groupConsumerProperties(String bootstrapServers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    private static List<ConsumerRecord<String, String>> consumeAssigned(
        String bootstrapServers,
        String topic,
        int partitionId,
        int expectedRecords,
        String isolationLevel
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-client-failover-read-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        TopicPartition partition = new TopicPartition(topic, partitionId);
        List<ConsumerRecord<String, String>> records = new ArrayList<>(expectedRecords);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (records.size() < expectedRecords && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    records.add(record);
                }
            }
        }
        assertEquals(expectedRecords, records.size(),
            "Timed out consuming expected client-failover records with isolation=" + isolationLevel);
        return List.copyOf(records);
    }

    private static TopicDescription waitForTopicReady(
        Admin admin,
        String topic,
        int partitions,
        int expectedIsr
    ) throws Exception {
        TopicDescription[] result = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describe(admin, topic);
                boolean ready = description != null && description.partitions().size() == partitions &&
                    description.partitions().stream().allMatch(partition ->
                        partition.leader() != null && partition.leader().id() >= 0 &&
                            partition.replicas().size() == 3 && partition.isr().size() == expectedIsr
                    );
                if (ready) {
                    result[0] = description;
                }
                return ready;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Topic " + topic + " did not converge to ISR=" + expectedIsr);
        return result[0];
    }

    private static int waitForNewLeader(
        Admin admin,
        String topic,
        int oldLeader,
        int expectedIsr
    ) throws Exception {
        int[] result = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describe(admin, topic);
                var partition = description.partitions().get(0);
                int candidate = partition.leader().id();
                if (candidate >= 0 && candidate != oldLeader && partition.isr().size() == expectedIsr) {
                    result[0] = candidate;
                    return true;
                }
                return false;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "No new leader elected for " + topic + " after broker " + oldLeader + " stopped");
        return result[0];
    }

    private static TopicDescription describe(Admin admin, String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(topic);
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
        }, 120_000L, () -> "Remote coverage did not reach [0," + endOffset + ") for " + partition);
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-client-failover-metadata-" + UUID.randomUUID());
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

    private static String producerKey(int producerId, int sequence) {
        return "producer-" + producerId + "-" + sequence;
    }

    private static String producerValue(int producerId, int sequence) {
        return "value-" + producerId + "-" + sequence + "-" + "P".repeat(8 * 1024);
    }

    private static String expectedValueForKey(String key) {
        String[] parts = key.split("-");
        return producerValue(Integer.parseInt(parts[1]), Integer.parseInt(parts[2]));
    }

    private static String groupKey(int partition, int sequence) {
        return "group-key-" + partition + "-" + sequence;
    }

    private static String groupValue(int partition, int sequence) {
        return "group-value-" + partition + "-" + sequence;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
