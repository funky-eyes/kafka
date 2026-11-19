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
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
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
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.common.SharedStorageVersion;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.shared.kafka.SharedUnifiedLog;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRemoteCoverage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Compares Kafka client-visible semantics between the unmodified local log and the shared WAL/S3 log implementation.
 *
 * <p>The same deterministic workload is executed on independent classic and shared-storage clusters. The proof covers
 * explicit multi-partition ordering, all Kafka producer compression codecs, keys, values, timestamps and headers. It
 * also compares transactional commit/abort visibility under both read_committed and read_uncommitted isolation.</p>
 */
@Tag("integration")
@Timeout(value = 8, unit = TimeUnit.MINUTES)
public class SharedStorageKafkaSemanticsParityIntegrationTest {
    private static final String RECORD_TOPIC = "shared-parity-records";
    private static final String TRANSACTION_TOPIC = "shared-parity-transactions";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int PARTITIONS = 3;
    private static final int RECORDS_PER_CODEC = 12;
    private static final int TX_FIRST_COMMITTED = 6;
    private static final int TX_ABORTED = 4;
    private static final int TX_SECOND_COMMITTED = 6;
    private static final long BASE_TIMESTAMP = 1_780_000_000_000L;
    private static final List<String> COMPRESSION_TYPES = List.of("none", "gzip", "snappy", "lz4", "zstd");

    @Test
    public void sharedStorageMatchesClassicKafkaProducerConsumerAndTransactionSemantics() throws Exception {
        runParity(false);
    }

    @Test
    public void topicOptInMatchesClassicKafkaProducerConsumerAndTransactionSemantics() throws Exception {
        runParity(true);
    }

    private static void runParity(boolean topicMode) throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-parity");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        ClusterSnapshot classic = runWorkload(false, topicMode, s3Endpoint, region, bucket);
        ClusterSnapshot shared = runWorkload(true, topicMode, s3Endpoint, region, bucket);

        assertEquals(classic.regularRecords(), shared.regularRecords(),
            "Shared storage must preserve classic Kafka record offsets, timestamps, keys, values and headers");
        assertEquals(classic.readCommittedTransactions(), shared.readCommittedTransactions(),
            "read_committed visibility must match classic Kafka exactly");
        assertEquals(classic.readUncommittedTransactions(), shared.readUncommittedTransactions(),
            "read_uncommitted visibility and data-record offsets must match classic Kafka exactly");

        assertEquals(PARTITIONS * COMPRESSION_TYPES.size() * RECORDS_PER_CODEC, shared.regularRecords().size());
        assertEquals(PARTITIONS * (TX_FIRST_COMMITTED + TX_SECOND_COMMITTED),
            shared.readCommittedTransactions().size());
        assertEquals(PARTITIONS * (TX_FIRST_COMMITTED + TX_ABORTED + TX_SECOND_COMMITTED),
            shared.readUncommittedTransactions().size());
    }

    private static ClusterSnapshot runWorkload(
        boolean sharedStorage,
        boolean topicMode,
        String s3Endpoint,
        String region,
        String bucket
    ) throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();

        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
        if (sharedStorage) {
            builder
                .setConfigProp("storage.extension.class",
                    "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
                .setConfigProp("shared.storage.wal.engine", "ring")
                .setConfigProp("shared.storage.wal.capacity.bytes", 8L * 1024 * 1024)
                .setConfigProp("shared.storage.object.target.bytes", 128L * 1024)
                .setConfigProp("shared.storage.upload.interval.ms", 100L)
                .setConfigProp("shared.storage.upload.max.linger.ms", 100L)
                .setConfigProp("shared.storage.metadata.replication.factor", 3)
                .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
                .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
                .setConfigProp("shared.storage.s3.region", region)
                .setConfigProp("shared.storage.s3.bucket", bucket)
                .setConfigProp("shared.storage.s3.key.prefix", "parity/" + UUID.randomUUID() + "/objects")
                .setConfigProp("shared.storage.s3.path.style", true)
                .setConfigProp("shared.storage.s3.io.threads", 2);
            if (!topicMode) {
                builder.setConfigProp("shared.storage.legacy.selectors.enabled", true)
                    .setConfigProp("shared.storage.topic.pattern", "shared-parity-.*");
            }
        }

        try (KafkaClusterTestKit cluster = builder.build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            String bootstrapServers = cluster.bootstrapServers();

            try (Admin admin = cluster.admin()) {
                if (sharedStorage && topicMode) {
                    admin.updateFeatures(Map.of(SharedStorageVersion.FEATURE_NAME,
                        new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE)),
                        new UpdateFeaturesOptions()).all().get(30, TimeUnit.SECONDS);
                }
                createTopics(admin, sharedStorage && topicMode);
                TopicDescription recordDescription = waitForTopicReady(admin, RECORD_TOPIC);
                waitForTopicReady(admin, TRANSACTION_TOPIC);
                if (sharedStorage && topicMode) {
                    assertLogMode(cluster, RECORD_TOPIC, true);
                    assertLogMode(cluster, TRANSACTION_TOPIC, true);
                    String ordinaryTopic = "ordinary-parity";
                    admin.createTopics(List.of(new NewTopic(ordinaryTopic, PARTITIONS, (short) 3)))
                        .all().get(30, TimeUnit.SECONDS);
                    waitForTopicReady(admin, ordinaryTopic);
                    assertLogMode(cluster, ordinaryTopic, false);
                    try (KafkaProducer<String, String> producer = producer(bootstrapServers, "none", null)) {
                        producer.send(new ProducerRecord<>(ordinaryTopic, 0, "ordinary", "native"))
                            .get(30, TimeUnit.SECONDS);
                    }
                    assertEquals("native", consume(bootstrapServers, ordinaryTopic, 1, "read_uncommitted")
                        .get(0).value());
                    System.out.println("TOPIC_PARITY_MIXED_MODE_READY");
                    assertConsumerGroupParity(cluster, admin);
                }

                produceCompressedRecords(bootstrapServers);
                produceTransactions(bootstrapServers);

                List<RecordSnapshot> regular = consume(
                    bootstrapServers,
                    RECORD_TOPIC,
                    PARTITIONS * COMPRESSION_TYPES.size() * RECORDS_PER_CODEC,
                    "read_uncommitted"
                );
                List<RecordSnapshot> readCommitted = consume(
                    bootstrapServers,
                    TRANSACTION_TOPIC,
                    PARTITIONS * (TX_FIRST_COMMITTED + TX_SECOND_COMMITTED),
                    "read_committed"
                );
                List<RecordSnapshot> readUncommitted = consume(
                    bootstrapServers,
                    TRANSACTION_TOPIC,
                    PARTITIONS * (TX_FIRST_COMMITTED + TX_ABORTED + TX_SECOND_COMMITTED),
                    "read_uncommitted"
                );

                assertContinuousRegularOffsets(regular);
                if (sharedStorage) {
                    waitForRemoteCoverage(
                        bootstrapServers,
                        recordDescription,
                        COMPRESSION_TYPES.size() * RECORDS_PER_CODEC
                    );
                }
                return new ClusterSnapshot(regular, readCommitted, readUncommitted);
            }
        }
    }

    private static void assertConsumerGroupParity(KafkaClusterTestKit cluster, Admin admin) throws Exception {
        String ordinary = "ordinary-parity-group";
        String shared = "shared-parity-group";
        List<String> topics = List.of(ordinary, shared);
        admin.createTopics(List.of(
            new NewTopic(ordinary, PARTITIONS, (short) 3)
                .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")),
            new NewTopic(shared, PARTITIONS, (short) 3)
                .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2",
                    TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, "true"))
        )).all().get(30, TimeUnit.SECONDS);

        waitForTopicReady(admin, ordinary);
        TopicDescription sharedDescription = waitForTopicReady(admin, shared);
        assertLogMode(cluster, ordinary, false);
        assertLogMode(cluster, shared, true);
        Set<TopicPartition> expectedPartitions = Set.of(
            new TopicPartition(ordinary, 0), new TopicPartition(ordinary, 1), new TopicPartition(ordinary, 2),
            new TopicPartition(shared, 0), new TopicPartition(shared, 1), new TopicPartition(shared, 2));

        String bootstrapServers = cluster.bootstrapServers();
        for (String protocol : List.of("classic", "consumer")) {
            assertGroupRebalance(admin, bootstrapServers, protocol, topics, expectedPartitions);
        }
        try (KafkaProducer<String, String> producer = producer(bootstrapServers, "none", null)) {
            for (int offset = 0; offset < 2; offset++) {
                for (TopicPartition partition : expectedPartitions) {
                    RecordMetadata metadata = producer.send(new ProducerRecord<>(partition.topic(), partition.partition(),
                        "group-key-" + partition.partition(), "group-value-" + offset)).get(30, TimeUnit.SECONDS);
                    assertEquals(offset, metadata.offset());
                }
            }
        }
        waitForRemoteCoverage(bootstrapServers, sharedDescription, 2);
        for (String protocol : List.of("classic", "consumer")) {
            String group = "parity-" + protocol;
            String firstMember = consumeAndCommitGroupBatch(
                bootstrapServers, group, protocol, topics, expectedPartitions, 0);
            assertGroupOffsets(admin, group, expectedPartitions, 1);

            String replacement = consumeAndCommitGroupBatch(
                bootstrapServers, group, protocol, topics, expectedPartitions, 1);
            assertNotEquals(firstMember, replacement, "Replacement consumer must join as a new member");
            assertGroupOffsets(admin, group, expectedPartitions, 2);
            System.out.println("TOPIC_PARITY_GROUP_PROTOCOL_PASSED=" + protocol);
        }
    }

    private static void assertGroupRebalance(
        Admin admin,
        String bootstrapServers,
        String protocol,
        List<String> topics,
        Set<TopicPartition> expectedPartitions
    ) throws Exception {
        String group = "parity-rebalance-" + protocol;
        try (KafkaConsumer<String, String> remaining = groupConsumer(bootstrapServers, group, protocol, "earliest")) {
            remaining.subscribe(topics);
            waitForGroupAssignment(admin, group, expectedPartitions, List.of(remaining));

            try (KafkaConsumer<String, String> joining = groupConsumer(bootstrapServers, group, protocol, "earliest")) {
                joining.subscribe(topics);
                waitForGroupAssignment(admin, group, expectedPartitions, List.of(remaining, joining));
            }

            waitForGroupAssignment(admin, group, expectedPartitions, List.of(remaining));
        }
        System.out.println("TOPIC_PARITY_GROUP_REBALANCE_PASSED=" + protocol);
    }

    private static void waitForGroupAssignment(
        Admin admin,
        String group,
        Set<TopicPartition> expectedPartitions,
        List<KafkaConsumer<String, String>> consumers
    ) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline) {
            for (KafkaConsumer<String, String> consumer : consumers) {
                consumer.poll(Duration.ofMillis(200));
            }
            if (hasExactGroupAssignment(expectedPartitions, consumers)
                    && coordinatorAgreesWithGroupAssignment(admin, group, consumers)) {
                return;
            }
        }
        throw new AssertionError("Group did not converge to an exact " + consumers.size()
            + "-member assignment for " + group);
    }

    private static boolean hasExactGroupAssignment(
        Set<TopicPartition> expectedPartitions,
        List<KafkaConsumer<String, String>> consumers
    ) {
        Set<TopicPartition> assigned = new HashSet<>();
        int assignmentCount = 0;
        for (KafkaConsumer<String, String> consumer : consumers) {
            Set<TopicPartition> assignment = consumer.assignment();
            if (assignment.isEmpty()) {
                return false;
            }
            assignmentCount += assignment.size();
            assigned.addAll(assignment);
        }
        return assigned.equals(expectedPartitions) && assignmentCount == expectedPartitions.size();
    }

    private static boolean coordinatorAgreesWithGroupAssignment(
        Admin admin,
        String group,
        List<KafkaConsumer<String, String>> consumers
    ) throws Exception {
        ConsumerGroupDescription description = admin.describeConsumerGroups(List.of(group))
            .describedGroups().get(group).get(10, TimeUnit.SECONDS);
        Map<String, Set<TopicPartition>> expected = new LinkedHashMap<>();
        for (KafkaConsumer<String, String> consumer : consumers) {
            expected.put(consumer.groupMetadata().memberId(), consumer.assignment());
        }
        return description.groupState() == GroupState.STABLE
            && description.members().size() == consumers.size()
            && description.members().stream().allMatch(member ->
                member.assignment().topicPartitions().equals(expected.get(member.consumerId())));
    }

    private static KafkaConsumer<String, String> groupConsumer(
        String bootstrapServers,
        String group,
        String protocol,
        String offsetReset
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    private static String consumeAndCommitGroupBatch(
        String bootstrapServers,
        String group,
        String protocol,
        List<String> topics,
        Set<TopicPartition> expectedPartitions,
        long expectedOffset
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new LinkedHashMap<>();
        try (KafkaConsumer<String, String> consumer = groupConsumer(
            bootstrapServers, group, protocol, expectedOffset == 0 ? "earliest" : "none")) {
            consumer.subscribe(topics);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (offsets.size() < expectedPartitions.size() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                    assertTrue(expectedPartitions.contains(partition), () -> "Unexpected group partition " + partition);
                    assertEquals("group-key-" + partition.partition(), record.key());
                    assertTrue(record.offset() >= expectedOffset, () -> "Group replayed " + record);
                    assertEquals("group-value-" + record.offset(), record.value());
                    // Kafka may fetch two records for one partition before delivering any from another.
                    // Commit only the first record per partition so the replacement must replay the tail.
                    offsets.putIfAbsent(partition, new OffsetAndMetadata(record.offset() + 1));
                }
            }
            assertEquals(expectedPartitions, consumer.assignment());
            assertEquals(expectedPartitions, offsets.keySet());
            offsets.values().forEach(offset -> assertEquals(expectedOffset + 1, offset.offset()));
            consumer.commitSync(offsets, Duration.ofSeconds(15));
            String member = consumer.groupMetadata().memberId();
            assertTrue(!member.isEmpty(), "Subscribed consumer must have an active group membership");
            return member;
        }
    }

    private static void assertGroupOffsets(
        Admin admin,
        String group,
        Set<TopicPartition> expectedPartitions,
        long expectedOffset
    ) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(group)
            .partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
        assertEquals(expectedPartitions, offsets.keySet());
        offsets.values().forEach(offset -> assertEquals(expectedOffset, offset.offset()));
    }

    private static void createTopics(Admin admin, boolean topicMode) throws Exception {
        Map<String, String> configs = new LinkedHashMap<>();
        configs.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        if (topicMode) configs.put(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, "true");
        admin.createTopics(List.of(
            new NewTopic(RECORD_TOPIC, PARTITIONS, (short) 3).configs(configs),
            new NewTopic(TRANSACTION_TOPIC, PARTITIONS, (short) 3).configs(configs)
        )).all().get(30, TimeUnit.SECONDS);
    }

    private static void assertLogMode(KafkaClusterTestKit cluster, String topic, boolean shared) throws Exception {
        TestUtils.waitForCondition(() -> cluster.brokers().values().stream().allMatch(broker -> {
            for (int partition = 0; partition < PARTITIONS; partition++) {
                scala.Option<UnifiedLog> log = broker.logManager().getLog(new TopicPartition(topic, partition), false);
                if (log.isEmpty() || (log.get() instanceof SharedUnifiedLog) != shared) return false;
            }
            return true;
        }), 30_000L, "Unexpected physical log mode for " + topic);
    }

    private static TopicDescription waitForTopicReady(Admin admin, String topic) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = admin.describeTopics(List.of(topic))
                    .allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
                if (description == null || description.partitions().size() != PARTITIONS) {
                    return false;
                }
                boolean allReady = description.partitions().stream().allMatch(partition ->
                    partition.leader() != null && partition.leader().id() >= 0 &&
                        partition.replicas().size() == 3 && partition.isr().size() == 3
                );
                if (allReady) {
                    ready[0] = description;
                }
                return allReady;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "Topic " + topic + " did not converge to three RF=3 partitions with full ISR");
        return ready[0];
    }

    private static void produceCompressedRecords(String bootstrapServers) throws Exception {
        for (int codecIndex = 0; codecIndex < COMPRESSION_TYPES.size(); codecIndex++) {
            String codec = COMPRESSION_TYPES.get(codecIndex);
            try (KafkaProducer<String, String> producer = producer(bootstrapServers, codec, null)) {
                List<Future<RecordMetadata>> sends = new ArrayList<>();
                for (int partition = 0; partition < PARTITIONS; partition++) {
                    for (int sequence = 0; sequence < RECORDS_PER_CODEC; sequence++) {
                        int logicalSequence = codecIndex * RECORDS_PER_CODEC + sequence;
                        long timestamp = BASE_TIMESTAMP + codecIndex * 100_000L + partition * 1_000L + sequence;
                        List<Header> headers = List.of(
                            new RecordHeader("codec", bytes(codec)),
                            new RecordHeader("sequence", bytes(Integer.toString(logicalSequence)))
                        );
                        sends.add(producer.send(new ProducerRecord<>(
                            RECORD_TOPIC,
                            partition,
                            timestamp,
                            "key-" + partition + "-" + logicalSequence,
                            regularValue(codec, partition, logicalSequence),
                            headers
                        )));
                    }
                }
                producer.flush();
                for (Future<RecordMetadata> send : sends) {
                    RecordMetadata metadata = send.get(30, TimeUnit.SECONDS);
                    assertEquals(RECORD_TOPIC, metadata.topic());
                    assertTrue(metadata.offset() >= 0L);
                }
            }
        }
    }

    private static void produceTransactions(String bootstrapServers) throws Exception {
        try (KafkaProducer<String, String> producer = producer(
            bootstrapServers,
            "zstd",
            "shared-parity-tx-" + UUID.randomUUID()
        )) {
            producer.initTransactions();
            writeTransaction(producer, "commit-a", 0, TX_FIRST_COMMITTED, true);
            writeTransaction(producer, "abort", TX_FIRST_COMMITTED, TX_ABORTED, false);
            writeTransaction(
                producer,
                "commit-b",
                TX_FIRST_COMMITTED + TX_ABORTED,
                TX_SECOND_COMMITTED,
                true
            );
        }
    }

    private static void writeTransaction(
        KafkaProducer<String, String> producer,
        String phase,
        int sequenceStart,
        int count,
        boolean commit
    ) throws Exception {
        producer.beginTransaction();
        List<Future<RecordMetadata>> sends = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            for (int i = 0; i < count; i++) {
                int sequence = sequenceStart + i;
                List<Header> headers = List.of(
                    new RecordHeader("phase", bytes(phase)),
                    new RecordHeader("sequence", bytes(Integer.toString(sequence)))
                );
                sends.add(producer.send(new ProducerRecord<>(
                    TRANSACTION_TOPIC,
                    partition,
                    BASE_TIMESTAMP + 1_000_000L + partition * 10_000L + sequence,
                    "tx-key-" + partition + "-" + sequence,
                    "tx-value-" + phase + "-" + partition + "-" + sequence,
                    headers
                )));
            }
        }
        for (Future<RecordMetadata> send : sends) {
            send.get(30, TimeUnit.SECONDS);
        }
        if (commit) {
            producer.commitTransaction();
        } else {
            producer.abortTransaction();
        }
    }

    private static KafkaProducer<String, String> producer(
        String bootstrapServers,
        String compressionType,
        String transactionalId
    ) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (transactionalId != null) {
            properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        }
        return new KafkaProducer<>(properties);
    }

    private static List<RecordSnapshot> consume(
        String bootstrapServers,
        String topic,
        int expectedRecords,
        String isolationLevel
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-parity-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<TopicPartition> assignments = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            assignments.add(new TopicPartition(topic, partition));
        }

        List<RecordSnapshot> records = new ArrayList<>(expectedRecords);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(assignments);
            consumer.seekToBeginning(assignments);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (records.size() < expectedRecords && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    records.add(snapshot(record));
                }
            }
        }
        assertEquals(expectedRecords, records.size(),
            "Timed out consuming " + topic + " with isolation.level=" + isolationLevel);
        records.sort(Comparator.comparingInt(RecordSnapshot::partition).thenComparingLong(RecordSnapshot::offset));
        return List.copyOf(records);
    }

    private static RecordSnapshot snapshot(ConsumerRecord<String, String> record) {
        return new RecordSnapshot(
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.key(),
            record.value(),
            header(record, "codec"),
            header(record, "phase"),
            header(record, "sequence")
        );
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void assertContinuousRegularOffsets(List<RecordSnapshot> records) {
        Map<Integer, Long> nextOffset = new LinkedHashMap<>();
        for (RecordSnapshot record : records) {
            long expected = nextOffset.getOrDefault(record.partition(), 0L);
            assertEquals(expected, record.offset(),
                "Regular Kafka data offsets must be gap-free within partition " + record.partition());
            nextOffset.put(record.partition(), expected + 1L);
        }
        for (int partition = 0; partition < PARTITIONS; partition++) {
            assertEquals((long) COMPRESSION_TYPES.size() * RECORDS_PER_CODEC, nextOffset.get(partition));
        }
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        TopicDescription description,
        long expectedEndOffset
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
                if (!coverage.covers(new OffsetRange(0L, expectedEndOffset))) {
                    return false;
                }
            }
            return true;
        }, 120_000L, () -> "Shared parity records never reached complete authoritative S3 coverage");
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-parity-metadata-" + UUID.randomUUID());
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

    private static String regularValue(String codec, int partition, int sequence) {
        return "value-" + codec + "-" + partition + "-" + sequence + "-" + "payload".repeat(64);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record RecordSnapshot(
        int partition,
        long offset,
        long timestamp,
        String key,
        String value,
        String codec,
        String phase,
        String sequence
    ) {
    }

    private record ClusterSnapshot(
        List<RecordSnapshot> regularRecords,
        List<RecordSnapshot> readCommittedTransactions,
        List<RecordSnapshot> readUncommittedTransactions
    ) {
    }
}
