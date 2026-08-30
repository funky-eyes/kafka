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
import org.apache.kafka.clients.admin.AdminClientConfig;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves the positive {@code acks=1} recovery boundary after asynchronous replica WAL propagation has completed.
 *
 * <p>The producer acknowledgement itself still depends only on the leader WAL. Before crashing the leader, this test
 * independently waits for every assigned replica WAL to advance and for the record to become consumer-visible through
 * Kafka's high watermark. RF=2 and RF=3 must then perform a clean automatic failover. RF=1 has no alternate replica and
 * therefore requires the original broker and disk to restart.</p>
 */
@Tag("integration")
@Timeout(value = 7, unit = TimeUnit.MINUTES)
public class SharedStorageAcksOneReplicatedSigkillTest {
    private static final String REPLICATION_FACTOR_ENV = "SHARED_STORAGE_ACKS_ONE_REPLICATION_FACTOR";
    private static final String TOPIC = "shared-wal-acks-one-replicated";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final long UPLOAD_INTERVAL_MS = 10L * 60L * 1_000L;
    private static final int[] BROKER_PORTS = {20092, 20192, 20292};
    private static final int[] CONTROLLER_PORTS = {20093, 20193, 20293};

    @TempDir
    Path tempDir;

    @Test
    public void replicatedAcksOneRecordSurvivesLeaderSigkill() throws Exception {
        short replicationFactor = replicationFactor();
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        Path repositoryRoot = repositoryRoot();
        Path processRuntime = repositoryRoot.resolve("storage/shared-storage-s3/build/process-runtime");
        assumeTrue(Files.isDirectory(processRuntime), "S3 process runtime was not staged: " + processRuntime);

        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-acks-one");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");
        String clusterId = Uuid.randomUuid().toString();
        String bootstrapServers = bootstrapServers();
        Map<Integer, BrokerProcess> brokers = new LinkedHashMap<>();

        System.out.println("ACKS1_REPLICATED_SCENARIO rf=" + replicationFactor +
            " minIsr=" + replicationFactor);
        try {
            startInitialBrokers(
                repositoryRoot,
                processRuntime,
                clusterId,
                s3Endpoint,
                bucket,
                region,
                brokers
            );
            waitForCluster(brokers, bootstrapServers);

            try (Admin admin = admin(bootstrapServers)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, replicationFactor)
                    .configs(Map.of(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        Short.toString(replicationFactor)
                    )))).all().get(30, TimeUnit.SECONDS);

                TopicDescription topic = waitForTopicState(
                    admin,
                    replicationFactor,
                    replicationFactor,
                    -1
                );
                SharedPartitionId partition = sharedPartitionId(topic.topicId(), 0);
                int oldLeader = topic.partitions().get(0).leader().id();
                List<Integer> replicaIds = topic.partitions().get(0).replicas().stream()
                    .map(node -> node.id())
                    .toList();
                Map<Integer, Long> walBefore = walBytesByBroker(brokers, replicaIds);

                RecordMetadata acknowledged = produceOne(bootstrapServers);
                assertEquals(0L, acknowledged.offset());
                for (int replicaId : replicaIds) {
                    TestUtils.waitForCondition(
                        () -> walBytes(brokers.get(replicaId).walDir()) > walBefore.get(replicaId),
                        30_000L,
                        () -> "Replica " + replicaId + " did not copy the acks=1 record into its WAL"
                    );
                }
                assertExpectedValues(
                    consumeAll(bootstrapServers, 1),
                    1,
                    "The replicated acks=1 record must advance Kafka's high watermark before SIGKILL"
                );
                assertFalse(
                    hasCommittedCoverage(bootstrapServers, partition, new OffsetRange(0, 1)),
                    "The positive failover proof must remain independent from S3 publication"
                );
                System.out.println("ACKS1_REPLICATED_COMMITTED rf=" + replicationFactor +
                    " leader=" + oldLeader + " durableCopies=" + replicaIds.size() +
                    " highWatermarkVisible=true remoteCommitted=false");

                BrokerProcess victim = brokers.get(oldLeader);
                long victimPid = victim.process().pid();
                sigkill(victimPid);
                assertTrue(victim.process().waitFor(30, TimeUnit.SECONDS), "SIGKILLed broker JVM did not exit");
                assertFalse(victim.process().isAlive());
                System.out.println("ACKS1_REPLICATED_SIGKILL rf=" + replicationFactor +
                    " oldLeader=" + oldLeader + " pid=" + victimPid);

                if (replicationFactor == 1) {
                    assertProduceUnavailable(bootstrapServers);
                    restartBroker(repositoryRoot, processRuntime, brokers, oldLeader);
                    waitForTopicState(admin, (short) 1, (short) 1, oldLeader);
                    assertExpectedValues(
                        consumeAll(bootstrapServers, 1),
                        1,
                        "RF=1 must recover the replicated acks=1 record from the original disk"
                    );
                    System.out.println("ACKS1_REPLICATED_RF1_RESTART leader=" + oldLeader + " records=1");
                } else {
                    int newLeader = waitForNewLeader(admin, oldLeader);
                    assertNotEquals(oldLeader, newLeader);
                    assertExpectedValues(
                        consumeAll(bootstrapServers, 1),
                        1,
                        "A replica-WAL-propagated acks=1 record must survive clean leader failover"
                    );
                    System.out.println("ACKS1_REPLICATED_FAILOVER rf=" + replicationFactor +
                        " oldLeader=" + oldLeader + " newLeader=" + newLeader + " records=1");
                    restartBroker(repositoryRoot, processRuntime, brokers, oldLeader);
                    waitForTopicState(admin, replicationFactor, replicationFactor, -1);
                }
            }
            System.out.println("ACKS1_REPLICATED_MATRIX_SUCCESS rf=" + replicationFactor + " records=1");
        } finally {
            stopProcesses(brokers);
            copyDiagnostics(repositoryRoot);
        }
    }

    private void startInitialBrokers(
        Path repositoryRoot,
        Path processRuntime,
        String clusterId,
        String s3Endpoint,
        String bucket,
        String region,
        Map<Integer, BrokerProcess> brokers
    ) throws Exception {
        for (int nodeId = 1; nodeId <= 3; nodeId++) {
            Path config = writeBrokerConfig(nodeId, clusterId, s3Endpoint, bucket, region);
            formatStorage(repositoryRoot, processRuntime, clusterId, config, nodeId);
            brokers.put(nodeId, startBroker(repositoryRoot, processRuntime, nodeId, config));
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
            "num.partitions=1",
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
            "shared.storage.wal.capacity.bytes=" + (64L * 1024 * 1024),
            "shared.storage.wal.segment.bytes=" + (4L * 1024 * 1024),
            "shared.storage.object.target.bytes=" + (1024L * 1024),
            "shared.storage.upload.interval.ms=" + UPLOAD_INTERVAL_MS,
            "shared.storage.orphan.cleanup.interval.ms=60000",
            "shared.storage.orphan.grace.ms=600000",
            "shared.storage.metadata.listener.name=PLAINTEXT",
            "shared.storage.metadata.replication.factor=3",
            "shared.storage.metadata.min.insync.replicas=2",
            "shared.storage.s3.endpoint=" + s3Endpoint,
            "shared.storage.s3.region=" + region,
            "shared.storage.s3.bucket=" + bucket,
            "shared.storage.s3.key.prefix=acks-one-replicated/" + clusterId + "/objects",
            "shared.storage.s3.path.style=true",
            "shared.storage.s3.io.threads=2",
            ""
        );
        Path configFile = nodeDir.resolve("server.properties");
        Files.writeString(configFile, config);
        return configFile;
    }

    private void formatStorage(
        Path repositoryRoot,
        Path processRuntime,
        String clusterId,
        Path config,
        int nodeId
    ) throws Exception {
        Path log = tempDir.resolve("node-" + nodeId).resolve("format.log");
        ProcessBuilder builder = new ProcessBuilder(
            repositoryRoot.resolve("bin/kafka-storage.sh").toString(),
            "format",
            "-t",
            clusterId,
            "-c",
            config.toString()
        );
        configureEnvironment(builder, processRuntime, nodeId);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Storage format timed out for node " + nodeId);
        assertEquals(0, process.exitValue(), () -> "Storage format failed for node " + nodeId + ":\n" + readLog(log));
    }

    private BrokerProcess startBroker(
        Path repositoryRoot,
        Path processRuntime,
        int nodeId,
        Path config
    ) throws IOException {
        Path log = tempDir.resolve("node-" + nodeId)
            .resolve("broker-" + System.nanoTime() + ".log");
        ProcessBuilder builder = new ProcessBuilder(
            repositoryRoot.resolve("bin/kafka-server-start.sh").toString(),
            config.toString()
        );
        configureEnvironment(builder, processRuntime, nodeId);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        System.out.println("ACKS1_REPLICATED_BROKER_STARTED brokerId=" + nodeId + " pid=" + process.pid());
        return new BrokerProcess(
            nodeId,
            process,
            log,
            tempDir.resolve("node-" + nodeId).resolve("wal"),
            config
        );
    }

    private void restartBroker(
        Path repositoryRoot,
        Path processRuntime,
        Map<Integer, BrokerProcess> brokers,
        int brokerId
    ) throws IOException {
        BrokerProcess stopped = brokers.get(brokerId);
        assertFalse(stopped.process().isAlive(), "Broker " + brokerId + " must be stopped before restart");
        brokers.put(
            brokerId,
            startBroker(repositoryRoot, processRuntime, brokerId, stopped.configFile())
        );
    }

    private static void configureEnvironment(ProcessBuilder builder, Path processRuntime, int nodeId) {
        Map<String, String> environment = builder.environment();
        String existingClasspath = environment.getOrDefault("CLASSPATH", "");
        String pluginClasspath = processRuntime.toAbsolutePath() + "/*";
        environment.put(
            "CLASSPATH",
            existingClasspath.isBlank() ? pluginClasspath : pluginClasspath + ":" + existingClasspath
        );
        environment.put("KAFKA_HEAP_OPTS", "-Xms256m -Xmx256m");
        environment.put("KAFKA_JVM_PERFORMANCE_OPTS", "-server -XX:+UseG1GC");
        environment.put(
            "LOG_DIR",
            processRuntime.resolve("../acks-one-replicated-broker-" + nodeId + "-logs").normalize().toString()
        );
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
                            "Broker " + broker.nodeId() + " exited during startup:\n" + readLog(broker.logFile()));
                    }
                }
                try {
                    return admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).size() == 3;
                } catch (Exception ignored) {
                    return false;
                }
            }, 90_000L, () -> "Independent broker JVMs did not form a three-node cluster");
        }
    }

    private static TopicDescription waitForTopicState(
        Admin admin,
        short expectedReplicas,
        short expectedIsr,
        int expectedLeader
    ) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        String[] observed = {"<unavailable>"};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin);
                if (topic == null || topic.partitions().size() != 1) {
                    observed[0] = String.valueOf(topic);
                    return false;
                }
                var partition = topic.partitions().get(0);
                observed[0] = partition.toString();
                if (partition.leader() == null || partition.leader().id() < 0 ||
                    partition.replicas().size() != expectedReplicas || partition.isr().size() != expectedIsr) {
                    return false;
                }
                if (expectedLeader >= 0 && partition.leader().id() != expectedLeader) {
                    return false;
                }
                ready[0] = topic;
                return true;
            } catch (Exception e) {
                observed[0] = e.toString();
                return false;
            }
        }, 90_000L, () -> "Topic did not converge to RF=" + expectedReplicas +
            ", ISR=" + expectedIsr + ", expectedLeader=" + expectedLeader +
            ", lastObserved=" + observed[0]);
        return ready[0];
    }

    private static int waitForNewLeader(Admin admin, int oldLeader) throws Exception {
        int[] result = {-1};
        String[] observed = {"<unavailable>"};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin);
                if (topic == null || topic.partitions().isEmpty()) {
                    observed[0] = String.valueOf(topic);
                    return false;
                }
                var partition = topic.partitions().get(0);
                observed[0] = partition.toString();
                if (partition.leader() == null) {
                    return false;
                }
                int leader = partition.leader().id();
                if (leader >= 0 && leader != oldLeader) {
                    result[0] = leader;
                    return true;
                }
                return false;
            } catch (Exception e) {
                observed[0] = e.toString();
                return false;
            }
        }, 60_000L, () -> "No new leader was elected after SIGKILL of broker " + oldLeader +
            ", lastObserved=" + observed[0]);
        return result[0];
    }

    private static TopicDescription describeTopic(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(TOPIC);
    }

    private static RecordMetadata produceOne(String bootstrapServers) throws Exception {
        KafkaProducer<String, String> producer = producer(bootstrapServers);
        try {
            RecordMetadata metadata = producer.send(
                new ProducerRecord<>(TOPIC, 0, "0", value(0))
            ).get(30, TimeUnit.SECONDS);
            producer.flush();
            return metadata;
        } finally {
            producer.close(Duration.ofSeconds(5));
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static void assertProduceUnavailable(String bootstrapServers) {
        KafkaProducer<String, String> producer = producer(bootstrapServers);
        boolean completed = false;
        try {
            producer.send(new ProducerRecord<>(TOPIC, 0, "unavailable", "unavailable"))
                .get(15, TimeUnit.SECONDS);
            completed = true;
        } catch (Exception expected) {
            System.out.println("ACKS1_REPLICATED_EXPECTED_UNAVAILABLE error=" +
                expected.getClass().getSimpleName());
        } finally {
            producer.close(Duration.ZERO);
        }
        assertFalse(completed, "A partition without a live assigned replica must reject writes");
    }

    private static Admin admin(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 15_000);
        return Admin.create(properties);
    }

    private static List<String> consumeAll(String bootstrapServers, int expectedCount) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "acks-one-replicated-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
        List<String> values = new ArrayList<>(expectedCount);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (values.size() < expectedCount && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
            }
        }
        return values;
    }

    private static void assertExpectedValues(List<String> actual, int expectedCount, String message) {
        assertEquals(expectedCount, actual.size(), message);
        List<String> expected = new ArrayList<>(expectedCount);
        for (int i = 0; i < expectedCount; i++) {
            expected.add(value(i));
        }
        assertEquals(expected, actual, message);
    }

    private static Map<Integer, Long> walBytesByBroker(
        Map<Integer, BrokerProcess> brokers,
        List<Integer> replicaIds
    ) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        for (int replicaId : replicaIds) {
            result.put(replicaId, walBytes(brokers.get(replicaId).walDir()));
        }
        return result;
    }

    private static long walBytes(Path walDir) {
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
                    } catch (IOException ignored) {
                        return 0L;
                    }
                })
                .sum();
        } catch (IOException ignored) {
            return 0L;
        }
    }

    private static boolean hasCommittedCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        OffsetRange expected
    ) {
        PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
        for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
            metadata.ranges().stream()
                .filter(range -> range.partition().equals(partition))
                .forEach(range -> coverage.add(range.offsets()));
        }
        return coverage.covers(expected);
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "acks-one-replicated-metadata-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        TopicPartition metadataPartition = new TopicPartition(METADATA_TOPIC, 0);
        Map<Long, SharedObjectMetadata> latest = new LinkedHashMap<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(metadataPartition));
            consumer.seekToBeginning(List.of(metadataPartition));
            long end = consumer.endOffsets(List.of(metadataPartition)).get(metadataPartition);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (consumer.position(metadataPartition) < end && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.key() == null) {
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(record.key());
                    if (key.type() != SharedMetadataRecordCodec.KeyType.OBJECT) {
                        continue;
                    }
                    if (record.value() == null) {
                        latest.remove(key.id());
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataValue value =
                        SharedMetadataRecordCodec.decodeValue(key, record.value());
                    if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committed) {
                        latest.put(key.id(), committed.metadata());
                    }
                }
            }
        }
        return List.copyOf(latest.values());
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), partition);
    }

    private static short replicationFactor() {
        String configured = environment(REPLICATION_FACTOR_ENV, "3");
        final short replicationFactor;
        try {
            replicationFactor = Short.parseShort(configured);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(REPLICATION_FACTOR_ENV + " must be an integer", e);
        }
        if (replicationFactor < 1 || replicationFactor > 3) {
            throw new IllegalArgumentException(REPLICATION_FACTOR_ENV + " must be between 1 and 3");
        }
        return replicationFactor;
    }

    private static void sigkill(long pid) throws Exception {
        Process kill = new ProcessBuilder("/bin/kill", "-9", Long.toString(pid)).start();
        assertTrue(kill.waitFor(10, TimeUnit.SECONDS), "kill -9 command timed out for pid " + pid);
        assertEquals(0, kill.exitValue(), "kill -9 failed for pid " + pid);
    }

    private static void stopProcesses(Map<Integer, BrokerProcess> brokers) {
        for (BrokerProcess broker : brokers.values()) {
            if (broker.process().isAlive()) {
                broker.process().destroy();
            }
        }
        for (BrokerProcess broker : brokers.values()) {
            try {
                if (broker.process().isAlive() && !broker.process().waitFor(10, TimeUnit.SECONDS)) {
                    broker.process().destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                broker.process().destroyForcibly();
            }
        }
    }

    private void copyDiagnostics(Path repositoryRoot) {
        Path destination = repositoryRoot.resolve("core/build/shared-storage-acks-one-replicated-diagnostics");
        try (Stream<Path> paths = Files.walk(tempDir)) {
            for (Path source : paths.filter(Files::isRegularFile).toList()) {
                String name = source.getFileName().toString();
                if (!(name.endsWith(".log") || name.endsWith(".properties"))) {
                    continue;
                }
                Path target = destination.resolve(tempDir.relativize(source).toString());
                Files.createDirectories(target.getParent());
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            System.out.println("Unable to copy replicated acks=1 diagnostics: " + e);
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (Path candidate = current; candidate != null; candidate = candidate.getParent()) {
            if (Files.isDirectory(candidate.resolve("bin")) && Files.isDirectory(candidate.resolve("core"))) {
                return candidate;
            }
        }
        throw new IllegalStateException("Unable to locate Kafka repository root from " + current);
    }

    private static String bootstrapServers() {
        return "127.0.0.1:" + BROKER_PORTS[0] +
            ",127.0.0.1:" + BROKER_PORTS[1] +
            ",127.0.0.1:" + BROKER_PORTS[2];
    }

    private static String value(int sequence) {
        return "value-" + sequence;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String readLog(Path log) {
        try {
            if (!Files.exists(log)) {
                return "<missing " + log + ">";
            }
            List<String> lines = Files.readAllLines(log);
            int start = Math.max(0, lines.size() - 100);
            return String.join("\n", lines.subList(start, lines.size()));
        } catch (IOException e) {
            return "<unable to read " + log + ": " + e + ">";
        }
    }

    private record BrokerProcess(
        int nodeId,
        Process process,
        Path logFile,
        Path walDir,
        Path configFile
    ) {
    }
}
