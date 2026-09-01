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
 * Proves the exact {@code acks=1} durability contract with an independent controller and three external broker JVMs.
 *
 * <p>A leader-only acknowledgement is durable on the leader WAL but is neither quorum durable nor remotely durable.
 * Killing every assigned replica therefore makes the partition unavailable until the original leader and disk return.
 * After all assigned replicas have asynchronously copied a later record into their own WALs, RF=2 and RF=3 can elect
 * a replacement leader and retain that record. RF=1 still requires the original broker and disk.</p>
 */
@Tag("integration")
@Timeout(value = 8, unit = TimeUnit.MINUTES)
public class SharedStorageAcksOneIndependentProcessTest {
    private static final String REPLICATION_FACTOR_ENV = "SHARED_STORAGE_ACKS_ONE_REPLICATION_FACTOR";
    private static final String TOPIC = "shared-wal-acks-one-matrix";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final long UPLOAD_INTERVAL_MS = 10L * 60L * 1_000L;
    private static final int CONTROLLER_ID = 100;
    private static final int CONTROLLER_PORT = 19693;
    private static final int[] BROKER_PORTS = {19692, 19792, 19892};

    @TempDir
    Path tempDir;

    @Test
    public void acksOneSeparatesSingleCopyRiskFromReplicatedFailover() throws Exception {
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
        String keyPrefix = "acks-one/" + clusterId + "/objects";
        Map<Integer, BrokerProcess> brokers = new LinkedHashMap<>();
        ControllerProcess controller = null;

        System.out.println("ACKS1_SCENARIO rf=" + replicationFactor +
            " minIsr=" + replicationFactor + " brokers=3 controller=dedicated");
        try {
            Path controllerConfig = writeControllerConfig(clusterId);
            formatStorage(repositoryRoot, processRuntime, clusterId, controllerConfig, "controller");
            for (int brokerId = 1; brokerId <= 3; brokerId++) {
                Path brokerConfig = writeBrokerConfig(
                    brokerId,
                    clusterId,
                    s3Endpoint,
                    bucket,
                    region,
                    keyPrefix
                );
                formatStorage(
                    repositoryRoot,
                    processRuntime,
                    clusterId,
                    brokerConfig,
                    "broker-" + brokerId
                );
                brokers.put(brokerId, new BrokerProcess(
                    brokerId,
                    null,
                    null,
                    brokerConfig
                ));
            }

            controller = startController(repositoryRoot, processRuntime, controllerConfig);
            for (int brokerId = 1; brokerId <= 3; brokerId++) {
                BrokerProcess configured = brokers.get(brokerId);
                brokers.put(
                    brokerId,
                    startBroker(repositoryRoot, processRuntime, configured)
                );
            }
            waitForCluster(controller, brokers, bootstrapServers);

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
                int firstLeader = topic.partitions().get(0).leader().id();
                List<Integer> replicas = replicaIds(topic);
                List<Integer> followers = replicas.stream()
                    .filter(id -> id != firstLeader)
                    .toList();

                proveLeaderOnlyAck(
                    bootstrapServers,
                    admin,
                    brokers,
                    partition,
                    replicationFactor,
                    firstLeader,
                    followers
                );
                recoverOriginalLeader(
                    repositoryRoot,
                    processRuntime,
                    bootstrapServers,
                    admin,
                    brokers,
                    replicationFactor,
                    firstLeader,
                    followers,
                    1
                );

                proveReplicatedAckBehavior(
                    repositoryRoot,
                    processRuntime,
                    bootstrapServers,
                    admin,
                    brokers,
                    partition,
                    replicationFactor
                );
            }
            System.out.println("ACKS1_MATRIX_SUCCESS rf=" + replicationFactor + " records=2");
        } finally {
            stopProcesses(brokers, controller);
            copyDiagnostics(repositoryRoot);
        }
    }

    private static void proveLeaderOnlyAck(
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        SharedPartitionId partition,
        short replicationFactor,
        int leaderId,
        List<Integer> followers
    ) throws Exception {
        for (int followerId : followers) {
            stopBroker(brokers.get(followerId));
        }
        waitForTopicState(admin, replicationFactor, (short) 1, leaderId);

        RecordMetadata acknowledged = produceOne(bootstrapServers, 0);
        assertEquals(0L, acknowledged.offset());
        assertFalse(
            hasCommittedCoverage(bootstrapServers, partition, new OffsetRange(0, 1)),
            "Leader-only acks=1 record must not depend on S3 publication"
        );
        System.out.println("ACKS1_LEADER_ONLY_ACK rf=" + replicationFactor +
            " leader=" + leaderId + " offset=0 followerCopies=0 remoteCommitted=false");
        System.out.println("ACKS1_SINGLE_COPY_RISK rf=" + replicationFactor +
            " leader=" + leaderId + " durableCopies=1");

        stopBroker(brokers.get(leaderId));
        assertProduceUnavailable(bootstrapServers);
        System.out.println("ACKS1_NO_AUTOMATIC_FAILOVER rf=" + replicationFactor +
            " reason=no-live-replica-with-acknowledged-record");
    }

    private static void recoverOriginalLeader(
        Path repositoryRoot,
        Path processRuntime,
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        short replicationFactor,
        int leaderId,
        List<Integer> followers,
        int expectedRecords
    ) throws Exception {
        restartBroker(repositoryRoot, processRuntime, brokers, leaderId);
        restartBrokers(repositoryRoot, processRuntime, brokers, followers);
        waitForTopicState(admin, replicationFactor, replicationFactor, leaderId);
        assertExpectedValues(
            consumeAll(bootstrapServers, expectedRecords),
            expectedRecords,
            "Original leader WAL must recover and replicate the leader-only acks=1 record"
        );
        System.out.println("ACKS1_ORIGINAL_DISK_RECOVERED rf=" + replicationFactor +
            " leader=" + leaderId + " records=" + expectedRecords +
            " recoveredWithFullMetadataQuorum=true");
    }

    private static void proveReplicatedAckBehavior(
        Path repositoryRoot,
        Path processRuntime,
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        SharedPartitionId partition,
        short replicationFactor
    ) throws Exception {
        TopicDescription topic = waitForTopicState(
            admin,
            replicationFactor,
            replicationFactor,
            -1
        );
        int leaderId = topic.partitions().get(0).leader().id();
        List<Integer> replicas = replicaIds(topic);

        RecordMetadata acknowledged = produceOne(bootstrapServers, 1);
        assertEquals(1L, acknowledged.offset());
        waitForReplicasAtHighWatermark(admin, replicas);
        assertFalse(
            hasCommittedCoverage(bootstrapServers, partition, new OffsetRange(1, 2)),
            "Replicated acks=1 test record must still be WAL-only before the leader crash"
        );
        System.out.println("ACKS1_ASYNC_REPLICATION_COMPLETE rf=" + replicationFactor +
            " leader=" + leaderId + " durableCopies=" + replicas.size());

        stopBroker(brokers.get(leaderId));
        if (replicationFactor == 1) {
            assertProduceUnavailable(bootstrapServers);
            restartBroker(repositoryRoot, processRuntime, brokers, leaderId);
            waitForTopicState(admin, (short) 1, (short) 1, leaderId);
            assertExpectedValues(
                consumeAll(bootstrapServers, 2),
                2,
                "RF=1 still requires the original disk even after the acks=1 replication wait"
            );
            System.out.println("ACKS1_RF1_RESTART_RECOVERED leader=" + leaderId + " records=2");
            return;
        }

        int newLeader = waitForNewLeader(admin, leaderId);
        assertNotEquals(leaderId, newLeader);
        assertExpectedValues(
            consumeAll(bootstrapServers, 2),
            2,
            "Asynchronously replicated acks=1 record must survive leader SIGKILL"
        );
        System.out.println("ACKS1_REPLICATED_FAILOVER rf=" + replicationFactor +
            " oldLeader=" + leaderId + " newLeader=" + newLeader + " records=2");

        restartBroker(repositoryRoot, processRuntime, brokers, leaderId);
        waitForTopicState(admin, replicationFactor, replicationFactor, -1);
    }

    private Path writeControllerConfig(String clusterId) throws IOException {
        Path controllerDir = tempDir.resolve("controller");
        Path dataDir = controllerDir.resolve("data");
        Files.createDirectories(controllerDir);
        String config = String.join("\n",
            "process.roles=controller",
            "node.id=" + CONTROLLER_ID,
            "controller.quorum.voters=" + CONTROLLER_ID + "@127.0.0.1:" + CONTROLLER_PORT,
            "listeners=CONTROLLER://127.0.0.1:" + CONTROLLER_PORT,
            "listener.security.protocol.map=CONTROLLER:PLAINTEXT",
            "controller.listener.names=CONTROLLER",
            "log.dirs=" + dataDir.toAbsolutePath(),
            ""
        );
        Path configFile = controllerDir.resolve("server.properties");
        Files.writeString(configFile, config);
        return configFile;
    }

    private Path writeBrokerConfig(
        int brokerId,
        String clusterId,
        String s3Endpoint,
        String bucket,
        String region,
        String keyPrefix
    ) throws IOException {
        Path brokerDir = tempDir.resolve("broker-" + brokerId);
        Path dataDir = brokerDir.resolve("data");
        Path walDir = brokerDir.resolve("wal");
        Files.createDirectories(brokerDir);
        int port = BROKER_PORTS[brokerId - 1];
        String config = String.join("\n",
            "process.roles=broker",
            "node.id=" + brokerId,
            "controller.quorum.voters=" + CONTROLLER_ID + "@127.0.0.1:" + CONTROLLER_PORT,
            "listeners=PLAINTEXT://127.0.0.1:" + port,
            "advertised.listeners=PLAINTEXT://127.0.0.1:" + port,
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
            "shared.storage.wal.engine=ring",
            "shared.storage.wal.capacity.bytes=" + (64L * 1024 * 1024),
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
            "shared.storage.s3.key.prefix=" + keyPrefix,
            "shared.storage.s3.path.style=true",
            "shared.storage.s3.io.threads=2",
            ""
        );
        Path configFile = brokerDir.resolve("server.properties");
        Files.writeString(configFile, config);
        return configFile;
    }

    private static void formatStorage(
        Path repositoryRoot,
        Path processRuntime,
        String clusterId,
        Path config,
        String processName
    ) throws Exception {
        Path log = config.getParent().resolve("format.log");
        ProcessBuilder builder = new ProcessBuilder(
            repositoryRoot.resolve("bin/kafka-storage.sh").toString(),
            "format",
            "-t",
            clusterId,
            "-c",
            config.toString()
        );
        configureEnvironment(builder, processRuntime, processName);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Storage format timed out for " + processName);
        assertEquals(0, process.exitValue(), () -> "Storage format failed for " + processName + ":\n" + readLog(log));
    }

    private static ControllerProcess startController(
        Path repositoryRoot,
        Path processRuntime,
        Path config
    ) throws IOException {
        Path log = config.getParent().resolve("controller-" + System.nanoTime() + ".log");
        ProcessBuilder builder = new ProcessBuilder(
            repositoryRoot.resolve("bin/kafka-server-start.sh").toString(),
            config.toString()
        );
        configureEnvironment(builder, processRuntime, "controller");
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        System.out.println("ACKS1_CONTROLLER_STARTED nodeId=" + CONTROLLER_ID + " pid=" + process.pid());
        return new ControllerProcess(process, log);
    }

    private static BrokerProcess startBroker(
        Path repositoryRoot,
        Path processRuntime,
        BrokerProcess configured
    ) throws IOException {
        Path log = configured.configFile().getParent()
            .resolve("broker-" + System.nanoTime() + ".log");
        ProcessBuilder builder = new ProcessBuilder(
            repositoryRoot.resolve("bin/kafka-server-start.sh").toString(),
            configured.configFile().toString()
        );
        configureEnvironment(builder, processRuntime, "broker-" + configured.brokerId());
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        System.out.println("ACKS1_BROKER_STARTED brokerId=" + configured.brokerId() + " pid=" + process.pid());
        return new BrokerProcess(
            configured.brokerId(),
            process,
            log,
            configured.configFile()
        );
    }

    private static void restartBroker(
        Path repositoryRoot,
        Path processRuntime,
        Map<Integer, BrokerProcess> brokers,
        int brokerId
    ) throws IOException {
        BrokerProcess stopped = brokers.get(brokerId);
        assertFalse(stopped.process().isAlive(), "Broker " + brokerId + " must be stopped before restart");
        brokers.put(brokerId, startBroker(repositoryRoot, processRuntime, stopped));
    }

    private static void restartBrokers(
        Path repositoryRoot,
        Path processRuntime,
        Map<Integer, BrokerProcess> brokers,
        List<Integer> brokerIds
    ) throws IOException {
        for (int brokerId : brokerIds) {
            restartBroker(repositoryRoot, processRuntime, brokers, brokerId);
        }
    }

    private static void configureEnvironment(
        ProcessBuilder builder,
        Path processRuntime,
        String processName
    ) {
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
            processRuntime.resolve("../acks-one-" + processName + "-logs").normalize().toString()
        );
        environment.putIfAbsent("AWS_ACCESS_KEY_ID", "minioadmin");
        environment.putIfAbsent("AWS_SECRET_ACCESS_KEY", "minioadmin123");
    }

    private static void waitForCluster(
        ControllerProcess controller,
        Map<Integer, BrokerProcess> brokers,
        String bootstrapServers
    ) throws Exception {
        try (Admin admin = admin(bootstrapServers)) {
            TestUtils.waitForCondition(() -> {
                if (!controller.process().isAlive()) {
                    throw new AssertionError("Dedicated controller exited during startup:\n" + readLog(controller.logFile()));
                }
                for (BrokerProcess broker : brokers.values()) {
                    if (!broker.process().isAlive()) {
                        throw new AssertionError(
                            "Broker " + broker.brokerId() + " exited during startup:\n" + readLog(broker.logFile()));
                    }
                }
                try {
                    return admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).size() == 3;
                } catch (Exception ignored) {
                    return false;
                }
            }, 90_000L, () -> "Independent brokers did not register with the dedicated controller");
        }
    }

    private static TopicDescription waitForTopicState(
        Admin admin,
        short expectedReplicas,
        short expectedIsr,
        int expectedLeader
    ) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin);
                if (topic == null || topic.partitions().size() != 1) {
                    return false;
                }
                var partition = topic.partitions().get(0);
                if (partition.leader() == null || partition.leader().id() < 0 ||
                    partition.replicas().size() != expectedReplicas || partition.isr().size() != expectedIsr) {
                    return false;
                }
                if (expectedLeader >= 0 && partition.leader().id() != expectedLeader) {
                    return false;
                }
                ready[0] = topic;
                return true;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Topic did not converge to RF=" + expectedReplicas +
            ", ISR=" + expectedIsr + ", expectedLeader=" + expectedLeader);
        return ready[0];
    }

    private static int waitForNewLeader(Admin admin, int oldLeader) throws Exception {
        int[] result = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin);
                if (topic == null || topic.partitions().isEmpty() || topic.partitions().get(0).leader() == null) {
                    return false;
                }
                int leader = topic.partitions().get(0).leader().id();
                if (leader >= 0 && leader != oldLeader) {
                    result[0] = leader;
                    return true;
                }
                return false;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "No new leader was elected after SIGKILL of broker " + oldLeader);
        return result[0];
    }

    private static TopicDescription describeTopic(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(TOPIC);
    }

    private static List<Integer> replicaIds(TopicDescription topic) {
        return topic.partitions().get(0).replicas().stream()
            .map(node -> node.id())
            .toList();
    }

    private static void waitForReplicasAtHighWatermark(Admin admin, List<Integer> replicas) throws Exception {
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        TestUtils.waitForCondition(() -> {
            try {
                var descriptions = admin.describeLogDirs(replicas).allDescriptions().get(10, TimeUnit.SECONDS);
                for (int replicaId : replicas) {
                    var logDirs = descriptions.get(replicaId);
                    if (logDirs == null) {
                        return false;
                    }
                    boolean foundCurrentReplica = false;
                    for (var logDir : logDirs.values()) {
                        if (logDir.error() != null) {
                            return false;
                        }
                        var replica = logDir.replicaInfos().get(partition);
                        if (replica == null || replica.isFuture()) {
                            continue;
                        }
                        foundCurrentReplica = true;
                        if (replica.offsetLag() != 0L) {
                            return false;
                        }
                    }
                    if (!foundCurrentReplica) {
                        return false;
                    }
                }
                return true;
            } catch (Exception ignored) {
                return false;
            }
        }, 30_000L, () -> "Assigned replicas did not advance to the partition high watermark: " + replicas);
    }

    private static RecordMetadata produceOne(String bootstrapServers, int sequence) throws Exception {
        KafkaProducer<String, String> producer = producer(bootstrapServers);
        try {
            RecordMetadata metadata = producer.send(
                new ProducerRecord<>(TOPIC, 0, Integer.toString(sequence), value(sequence))
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
            System.out.println("ACKS1_EXPECTED_UNAVAILABLE error=" + expected.getClass().getSimpleName());
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
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "acks-one-matrix-consumer-" + UUID.randomUUID());
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
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "acks-one-matrix-metadata-" + UUID.randomUUID());
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

    private static void stopBroker(BrokerProcess broker) throws Exception {
        if (!broker.process().isAlive()) {
            return;
        }
        sigkill(broker.process().pid());
        assertTrue(
            broker.process().waitFor(30, TimeUnit.SECONDS),
            "SIGKILLed broker " + broker.brokerId() + " did not exit"
        );
        assertFalse(broker.process().isAlive());
    }

    private static void sigkill(long pid) throws Exception {
        Process kill = new ProcessBuilder("/bin/kill", "-9", Long.toString(pid)).start();
        assertTrue(kill.waitFor(10, TimeUnit.SECONDS), "kill -9 command timed out for pid " + pid);
        assertEquals(0, kill.exitValue(), "kill -9 failed for pid " + pid);
    }

    private static void stopProcesses(
        Map<Integer, BrokerProcess> brokers,
        ControllerProcess controller
    ) {
        for (BrokerProcess broker : brokers.values()) {
            if (broker.process() != null && broker.process().isAlive()) {
                broker.process().destroy();
            }
        }
        if (controller != null && controller.process().isAlive()) {
            controller.process().destroy();
        }
        for (BrokerProcess broker : brokers.values()) {
            waitForExit(broker.process());
        }
        if (controller != null) {
            waitForExit(controller.process());
        }
    }

    private static void waitForExit(Process process) {
        if (process == null) {
            return;
        }
        try {
            if (process.isAlive() && !process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private void copyDiagnostics(Path repositoryRoot) {
        Path destination = repositoryRoot.resolve("core/build/shared-storage-acks-one-diagnostics");
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
            System.out.println("Unable to copy acks=1 matrix diagnostics: " + e);
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
            if (log == null || !Files.exists(log)) {
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
        int brokerId,
        Process process,
        Path logFile,
        Path configFile
    ) {
    }

    private record ControllerProcess(Process process, Path logFile) {
    }
}
