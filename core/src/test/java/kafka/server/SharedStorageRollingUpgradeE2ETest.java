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
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRemoteCoverage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs a true binary mixed-version rolling upgrade with three external broker/controller JVMs.
 *
 * <p>The old and new brokers are started from independent Kafka distributions and independent shared-storage plugin
 * runtimes. The test performs an OLD -> NEW -> OLD rollback probe after the new broker has durably published remote
 * metadata, then upgrades all brokers one at a time while forcing each upgraded broker to lead its preferred partition.
 * Client-visible records are verified after every transition.</p>
 */
@Tag("integration")
@Timeout(value = 20, unit = TimeUnit.MINUTES)
public class SharedStorageRollingUpgradeE2ETest {
    private static final String TOPIC = "shared-rolling-upgrade";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int PARTITIONS = 3;
    private static final int RECORDS_PER_PARTITION_PER_STAGE = 24;
    private static final int VALUE_BYTES = 4 * 1024;
    private static final long WAL_CAPACITY_BYTES = 512L * 1024;
    private static final long OBJECT_TARGET_BYTES = 64L * 1024;
    private static final int[] BROKER_PORTS = {29092, 29192, 29292};
    private static final int[] CONTROLLER_PORTS = {29093, 29193, 29293};

    @TempDir
    Path tempDir;

    @Test
    public void oldAndNewBinariesRemainCompatibleAcrossRollingUpgradeAndRollback() throws Exception {
        KafkaRuntime oldRuntime = runtime("OLD", "SHARED_STORAGE_OLD_KAFKA_HOME",
            "SHARED_STORAGE_OLD_PROCESS_RUNTIME");
        KafkaRuntime newRuntime = runtime("NEW", "SHARED_STORAGE_NEW_KAFKA_HOME",
            "SHARED_STORAGE_NEW_PROCESS_RUNTIME");
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");

        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-rolling-upgrade");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");
        String clusterId = Uuid.randomUuid().toString();
        String bootstrapServers = bootstrapServers();
        Map<Integer, BrokerProcess> brokers = new LinkedHashMap<>();
        LinkedHashSet<String> expected = new LinkedHashSet<>();

        try {
            startInitialOldCluster(oldRuntime, clusterId, s3Endpoint, bucket, region, brokers);
            waitForCluster(brokers, bootstrapServers);

            try (Admin admin = admin(bootstrapServers)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, assignments())
                    .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                    .all().get(30, TimeUnit.SECONDS);

                TopicDescription description = waitForTopicFullyReplicated(admin);
                Map<Integer, SharedPartitionId> sharedPartitions = sharedPartitions(description);

                produceStage(bootstrapServers, 0, expected);
                assertReadableExactlyOnce(bootstrapServers, expected);

                replaceBroker(brokers, 1, newRuntime);
                waitForCluster(brokers, bootstrapServers);
                waitForTopicFullyReplicated(admin);
                electPreferredLeader(admin, 0, 1);
                Map<Integer, OffsetRange> newRanges = produceStage(bootstrapServers, 1, expected);
                assertReadableExactlyOnce(bootstrapServers, expected);

                long rollbackEndOffset = 2L * RECORDS_PER_PARTITION_PER_STAGE;
                long acknowledgedPayloadBytes =
                    rollbackEndOffset * PARTITIONS * VALUE_BYTES;
                assertTrue(
                    acknowledgedPayloadBytes > WAL_CAPACITY_BYTES,
                    "The pre-rollback workload must exceed one broker-wide Ring WAL capacity"
                );
                for (int partition = 0; partition < PARTITIONS; partition++) {
                    waitForRemoteCoverage(
                        bootstrapServers,
                        sharedPartitions.get(partition),
                        new OffsetRange(0L, rollbackEndOffset)
                    );
                }
                System.out.println(
                    "ROLLING_UPGRADE_NEW_REMOTE_COMMIT brokerId=1 range=" + newRanges.get(0) +
                        " acknowledgedPayloadBytes=" + acknowledgedPayloadBytes +
                        " walCapacityBytes=" + WAL_CAPACITY_BYTES
                );

                replaceBroker(brokers, 1, oldRuntime);
                waitForCluster(brokers, bootstrapServers);
                waitForTopicFullyReplicated(admin);
                electPreferredLeader(admin, 0, 1);
                produceStage(bootstrapServers, 2, expected);
                assertReadableExactlyOnce(bootstrapServers, expected);
                System.out.println("ROLLING_UPGRADE_ROLLBACK_OK brokerId=1 records=" + expected.size());

                replaceBroker(brokers, 1, newRuntime);
                waitForCluster(brokers, bootstrapServers);
                waitForTopicFullyReplicated(admin);
                electPreferredLeader(admin, 0, 1);
                produceStage(bootstrapServers, 3, expected);
                assertReadableExactlyOnce(bootstrapServers, expected);

                replaceBroker(brokers, 2, newRuntime);
                waitForCluster(brokers, bootstrapServers);
                waitForTopicFullyReplicated(admin);
                electPreferredLeader(admin, 1, 2);
                produceStage(bootstrapServers, 4, expected);
                assertReadableExactlyOnce(bootstrapServers, expected);

                replaceBroker(brokers, 3, newRuntime);
                waitForCluster(brokers, bootstrapServers);
                waitForTopicFullyReplicated(admin);
                electPreferredLeader(admin, 2, 3);
                produceStage(bootstrapServers, 5, expected);
                assertReadableExactlyOnce(bootstrapServers, expected);

                long finalEndOffset = 6L * RECORDS_PER_PARTITION_PER_STAGE;
                for (int partition = 0; partition < PARTITIONS; partition++) {
                    waitForRemoteCoverage(
                        bootstrapServers,
                        sharedPartitions.get(partition),
                        new OffsetRange(0L, finalEndOffset)
                    );
                }
                System.out.println("ROLLING_UPGRADE_SUCCESS old=" + oldRuntime.kafkaHome() +
                    " new=" + newRuntime.kafkaHome() + " records=" + expected.size());
            }
        } finally {
            stopProcesses(brokers);
            copyDiagnostics();
        }
    }

    private void startInitialOldCluster(
        KafkaRuntime runtime,
        String clusterId,
        String s3Endpoint,
        String bucket,
        String region,
        Map<Integer, BrokerProcess> brokers
    ) throws Exception {
        for (int nodeId = 1; nodeId <= 3; nodeId++) {
            Path config = writeBrokerConfig(nodeId, clusterId, s3Endpoint, bucket, region);
            formatStorage(runtime, clusterId, config, nodeId);
            brokers.put(nodeId, startBroker(runtime, nodeId, config));
        }
    }

    private Path writeBrokerConfig(
        int nodeId,
        String clusterId,
        String s3Endpoint,
        String bucket,
        String region
    ) throws IOException {
        Path nodeDir = tempDir.resolve("node-" + nodeId);
        Path dataDir = nodeDir.resolve("data");
        Path walDir = nodeDir.resolve("wal");
        Files.createDirectories(nodeDir);
        int index = nodeId - 1;
        String voters = "1@127.0.0.1:" + CONTROLLER_PORTS[0] +
            ",2@127.0.0.1:" + CONTROLLER_PORTS[1] +
            ",3@127.0.0.1:" + CONTROLLER_PORTS[2];
        String config = String.join("\n",
            "process.roles=broker,controller",
            "node.id=" + nodeId,
            "controller.quorum.voters=" + voters,
            "listeners=PLAINTEXT://127.0.0.1:" + BROKER_PORTS[index] +
                ",CONTROLLER://127.0.0.1:" + CONTROLLER_PORTS[index],
            "advertised.listeners=PLAINTEXT://127.0.0.1:" + BROKER_PORTS[index],
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "controller.listener.names=CONTROLLER",
            "inter.broker.listener.name=PLAINTEXT",
            "log.dirs=" + dataDir.toAbsolutePath(),
            "num.partitions=3",
            "default.replication.factor=3",
            "min.insync.replicas=2",
            "offsets.topic.replication.factor=3",
            "transaction.state.log.replication.factor=3",
            "transaction.state.log.min.isr=2",
            "group.initial.rebalance.delay.ms=0",
            "auto.create.topics.enable=false",
            "unclean.leader.election.enable=false",
            "storage.extension.class=org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension",
            "shared.storage.topics=" + TOPIC,
            "shared.storage.wal.dir=" + walDir.toAbsolutePath(),
            "shared.storage.wal.engine=ring",
            "shared.storage.wal.capacity.bytes=" + WAL_CAPACITY_BYTES,
            "shared.storage.object.target.bytes=" + OBJECT_TARGET_BYTES,
            "shared.storage.upload.interval.ms=50",
            "shared.storage.upload.max.linger.ms=100",
            "shared.storage.upload.wal.pressure.percent=60",
            "shared.storage.upload.max.inflight=4",
            "shared.storage.orphan.cleanup.interval.ms=60000",
            "shared.storage.orphan.grace.ms=600000",
            "shared.storage.metadata.listener.name=PLAINTEXT",
            "shared.storage.metadata.replication.factor=3",
            "shared.storage.metadata.min.insync.replicas=2",
            "shared.storage.s3.endpoint=" + s3Endpoint,
            "shared.storage.s3.region=" + region,
            "shared.storage.s3.bucket=" + bucket,
            "shared.storage.s3.key.prefix=rolling/" + clusterId + "/objects",
            "shared.storage.s3.path.style=true",
            "shared.storage.s3.io.threads=2",
            ""
        );
        Path configFile = nodeDir.resolve("server.properties");
        Files.writeString(configFile, config);
        return configFile;
    }

    private void formatStorage(
        KafkaRuntime runtime,
        String clusterId,
        Path config,
        int nodeId
    ) throws Exception {
        Path log = tempDir.resolve("node-" + nodeId).resolve("format-" + runtime.label() + ".log");
        ProcessBuilder builder = new ProcessBuilder(
            runtime.kafkaHome().resolve("bin/kafka-storage.sh").toString(),
            "format",
            "-t",
            clusterId,
            "-c",
            config.toString()
        );
        configureEnvironment(builder, runtime, config);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Storage format timed out for node " + nodeId);
        assertEquals(0, process.exitValue(), () -> "Storage format failed for node " + nodeId + ":\n" + readLog(log));
    }

    private BrokerProcess startBroker(KafkaRuntime runtime, int nodeId, Path config) throws IOException {
        Path log = tempDir.resolve("node-" + nodeId)
            .resolve("broker-" + runtime.label() + "-" + System.nanoTime() + ".log");
        ProcessBuilder builder = new ProcessBuilder(
            runtime.kafkaHome().resolve("bin/kafka-server-start.sh").toString(),
            config.toString()
        );
        configureEnvironment(builder, runtime, config);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        System.out.println("ROLLING_BROKER_STARTED brokerId=" + nodeId + " version=" + runtime.label() +
            " pid=" + process.pid());
        return new BrokerProcess(nodeId, runtime, process, config, log);
    }

    private void replaceBroker(
        Map<Integer, BrokerProcess> brokers,
        int brokerId,
        KafkaRuntime runtime
    ) throws Exception {
        BrokerProcess current = brokers.get(brokerId);
        stopBroker(current);
        BrokerProcess replacement = startBroker(runtime, brokerId, current.configFile());
        brokers.put(brokerId, replacement);
    }

    private static void stopBroker(BrokerProcess broker) throws Exception {
        if (broker == null || !broker.process().isAlive()) {
            return;
        }
        broker.process().destroy();
        if (!broker.process().waitFor(30, TimeUnit.SECONDS)) {
            broker.process().destroyForcibly();
            assertTrue(broker.process().waitFor(30, TimeUnit.SECONDS),
                "Broker " + broker.nodeId() + " did not terminate");
        }
        assertFalse(broker.process().isAlive(), "Broker " + broker.nodeId() + " is still alive");
    }

    private static void configureEnvironment(
        ProcessBuilder builder,
        KafkaRuntime runtime,
        Path config
    ) {
        Map<String, String> environment = builder.environment();
        String existingClasspath = environment.getOrDefault("CLASSPATH", "");
        String pluginClasspath = runtime.processRuntime().toAbsolutePath() + "/*";
        environment.put(
            "CLASSPATH",
            existingClasspath.isBlank() ? pluginClasspath : pluginClasspath + File.pathSeparator + existingClasspath
        );
        environment.put("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m");
        environment.put("KAFKA_JVM_PERFORMANCE_OPTS", "-server -XX:+UseG1GC");
        environment.put("LOG_DIR", config.getParent().resolve("runtime-logs").toString());
        environment.putIfAbsent("AWS_ACCESS_KEY_ID", "minioadmin");
        environment.putIfAbsent("AWS_SECRET_ACCESS_KEY", "minioadmin123");
    }

    private static void waitForCluster(
        Map<Integer, BrokerProcess> brokers,
        String bootstrapServers
    ) throws Exception {
        try (Admin admin = admin(bootstrapServers)) {
            TestUtils.waitForCondition(() -> {
                for (BrokerProcess broker : brokers.values()) {
                    if (!broker.process().isAlive()) {
                        throw new AssertionError(
                            "Broker " + broker.nodeId() + " exited:\n" + readLog(broker.logFile()));
                    }
                }
                try {
                    return admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).size() == 3;
                } catch (Exception ignored) {
                    return false;
                }
            }, 120_000L, () -> "Mixed-version brokers did not form a three-node cluster");
        }
    }

    private static TopicDescription waitForTopicFullyReplicated(Admin admin) throws Exception {
        TopicDescription[] result = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin);
                if (topic == null || topic.partitions().size() != PARTITIONS) {
                    return false;
                }
                boolean ready = topic.partitions().stream().allMatch(partition ->
                    partition.leader() != null &&
                        partition.leader().id() >= 0 &&
                        partition.replicas().size() == 3 &&
                        partition.isr().size() == 3
                );
                if (ready) {
                    result[0] = topic;
                }
                return ready;
            } catch (Exception ignored) {
                return false;
            }
        }, 120_000L, () -> "Rolling-upgrade topic did not converge to RF=3/ISR=3");
        return result[0];
    }

    private static void electPreferredLeader(Admin admin, int partition, int brokerId) throws Exception {
        TopicPartition topicPartition = new TopicPartition(TOPIC, partition);
        admin.electLeaders(ElectionType.PREFERRED, Set.of(topicPartition)).all().get(30, TimeUnit.SECONDS);
        TestUtils.waitForCondition(() -> {
            try {
                return describeTopic(admin).partitions().get(partition).leader().id() == brokerId;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "Broker " + brokerId + " did not become preferred leader for " + topicPartition);
    }

    private static TopicDescription describeTopic(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(TOPIC);
    }

    private static Map<Integer, OffsetRange> produceStage(
        String bootstrapServers,
        int stage,
        Set<String> expected
    ) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Map<Integer, List<Future<RecordMetadata>>> sends = new LinkedHashMap<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int partition = 0; partition < PARTITIONS; partition++) {
                List<Future<RecordMetadata>> partitionSends = new ArrayList<>();
                for (int index = 0; index < RECORDS_PER_PARTITION_PER_STAGE; index++) {
                    String value = value(stage, partition, index);
                    expected.add(value);
                    partitionSends.add(producer.send(new ProducerRecord<>(
                        TOPIC,
                        partition,
                        "key-" + value,
                        value
                    )));
                }
                sends.put(partition, partitionSends);
            }
            producer.flush();

            Map<Integer, OffsetRange> ranges = new LinkedHashMap<>();
            for (Map.Entry<Integer, List<Future<RecordMetadata>>> entry : sends.entrySet()) {
                long first = -1L;
                long last = -1L;
                for (Future<RecordMetadata> send : entry.getValue()) {
                    long offset = send.get(60, TimeUnit.SECONDS).offset();
                    if (first < 0L) {
                        first = offset;
                    }
                    last = offset;
                }
                ranges.put(entry.getKey(), new OffsetRange(first, Math.addExact(last, 1L)));
            }
            return Map.copyOf(ranges);
        }
    }

    private static void assertReadableExactlyOnce(String bootstrapServers, Set<String> expected) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<TopicPartition> partitions = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            partitions.add(new TopicPartition(TOPIC, partition));
        }

        List<String> actual = new ArrayList<>(expected.size());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (actual.size() < expected.size() && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    actual.add(record.value());
                }
            }
        }

        assertEquals(expected.size(), actual.size(), "Unexpected record count during rolling upgrade");
        assertEquals(expected, new LinkedHashSet<>(actual),
            "Rolling upgrade must preserve every acknowledged record exactly once");
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        OffsetRange expected
    ) throws Exception {
        TestUtils.waitForCondition(() -> {
            PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
            for (SharedObjectMetadata object : committedObjects(bootstrapServers)) {
                object.ranges().stream()
                    .filter(range -> range.partition().equals(partition))
                    .forEach(range -> coverage.add(range.offsets()));
            }
            return coverage.covers(expected);
        }, 120_000L, () -> "Remote coverage did not reach " + expected + " for " + partition);
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "rolling-metadata-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        TopicPartition metadataPartition = new TopicPartition(METADATA_TOPIC, 0);
        Map<Long, SharedObjectMetadata> committed = new LinkedHashMap<>();
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
                        committed.remove(key.id());
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataValue value =
                        SharedMetadataRecordCodec.decodeValue(key, record.value());
                    if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committedValue) {
                        committed.put(key.id(), committedValue.metadata());
                    }
                }
            }
        }
        return List.copyOf(committed.values());
    }

    private static Map<Integer, List<Integer>> assignments() {
        Map<Integer, List<Integer>> assignments = new LinkedHashMap<>();
        assignments.put(0, List.of(1, 2, 3));
        assignments.put(1, List.of(2, 3, 1));
        assignments.put(2, List.of(3, 1, 2));
        return assignments;
    }

    private static Map<Integer, SharedPartitionId> sharedPartitions(TopicDescription topic) {
        Map<Integer, SharedPartitionId> partitions = new LinkedHashMap<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            partitions.put(partition, sharedPartitionId(topic.topicId(), partition));
        }
        return Map.copyOf(partitions);
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition
        );
    }

    private static Admin admin(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Admin.create(properties);
    }

    private static String bootstrapServers() {
        return "127.0.0.1:" + BROKER_PORTS[0] +
            ",127.0.0.1:" + BROKER_PORTS[1] +
            ",127.0.0.1:" + BROKER_PORTS[2];
    }

    private static KafkaRuntime runtime(String label, String homeEnv, String pluginEnv) {
        String home = System.getenv(homeEnv);
        String plugin = System.getenv(pluginEnv);
        assumeTrue(home != null && !home.isBlank(), homeEnv + " is not configured");
        assumeTrue(plugin != null && !plugin.isBlank(), pluginEnv + " is not configured");

        Path kafkaHome = Path.of(home).toAbsolutePath().normalize();
        Path processRuntime = Path.of(plugin).toAbsolutePath().normalize();
        assumeTrue(Files.isRegularFile(kafkaHome.resolve("bin/kafka-server-start.sh")),
            "Kafka runtime is missing bin/kafka-server-start.sh: " + kafkaHome);
        assumeTrue(Files.isDirectory(processRuntime), "Shared storage process runtime is missing: " + processRuntime);
        return new KafkaRuntime(label, kafkaHome, processRuntime);
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private void stopProcesses(Map<Integer, BrokerProcess> brokers) {
        for (BrokerProcess broker : brokers.values()) {
            try {
                stopBroker(broker);
            } catch (Exception e) {
                System.err.println("Unable to stop rolling-upgrade broker " + broker.nodeId() + ": " + e);
            }
        }
    }

    private void copyDiagnostics() {
        Path output = Path.of("core/build/shared-storage-rolling-upgrade-diagnostics");
        try {
            Files.createDirectories(output);
            try (Stream<Path> paths = Files.walk(tempDir)) {
                paths.filter(Files::isRegularFile).forEach(path -> {
                    try {
                        String relative = tempDir.relativize(path).toString()
                            .replace('/', '_')
                            .replace('\\', '_');
                        Files.copy(path, output.resolve(relative), StandardCopyOption.REPLACE_EXISTING);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (RuntimeException | IOException e) {
            System.err.println("Unable to copy rolling-upgrade diagnostics: " + e);
        }
    }

    private static String readLog(Path path) {
        try {
            return Files.exists(path) ? Files.readString(path) : "<missing>";
        } catch (IOException e) {
            return "<unable to read: " + e + ">";
        }
    }

    private static String value(int stage, int partition, int index) {
        String prefix = "stage-" + stage + "-partition-" + partition + "-record-" + index + "-";
        if (prefix.length() >= VALUE_BYTES) {
            throw new IllegalStateException("Rolling-upgrade record prefix exceeds configured payload size");
        }
        return prefix + "x".repeat(VALUE_BYTES - prefix.length());
    }

    private record KafkaRuntime(String label, Path kafkaHome, Path processRuntime) {
    }

    private record BrokerProcess(
        int nodeId,
        KafkaRuntime runtime,
        Process process,
        Path configFile,
        Path logFile
    ) {
    }
}
