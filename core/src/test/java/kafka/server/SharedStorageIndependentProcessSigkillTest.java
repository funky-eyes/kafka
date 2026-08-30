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
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
 * Proves the production {@code acks=all} durability and availability boundary with three real Kafka JVMs.
 *
 * <p>The configured topic replication factor is supplied through {@code SHARED_STORAGE_REPLICATION_FACTOR}. The
 * current leader is terminated with an OS-level {@code SIGKILL} while an acknowledged range is still WAL-only. RF=3
 * must remain writable with {@code min.insync.replicas=2}; RF=2 must preserve acknowledged data but reject new strict
 * quorum writes until the failed replica returns; RF=1 must require the original node and disk to restart.</p>
 */
@Tag("integration")
@Timeout(value = 8, unit = TimeUnit.MINUTES)
public class SharedStorageIndependentProcessSigkillTest {
    private static final String REPLICATION_FACTOR_ENV = "SHARED_STORAGE_REPLICATION_FACTOR";
    private static final String TOPIC = "shared-wal-sigkill";
    private static final String WRITE_PROBE_TOPIC = "shared-wal-write-probe";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int WARMUP_RECORDS = 20;
    private static final int CRASH_RECORDS = 40;
    private static final int POST_CRASH_RECORDS = 20;
    private static final int PRE_RECOVERY_RECORDS = WARMUP_RECORDS + CRASH_RECORDS;
    private static final int TOTAL_RECORDS = PRE_RECOVERY_RECORDS + POST_CRASH_RECORDS;
    private static final long UPLOAD_INTERVAL_MS = 30_000L;
    private static final int[] BROKER_PORTS = {19092, 19192, 19292};
    private static final int[] CONTROLLER_PORTS = {19093, 19193, 19293};

    @TempDir
    Path tempDir;

    @Test
    public void acksAllWalTailObeysReplicationFactorAvailabilityContract() throws Exception {
        Scenario scenario = scenario();
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        Path repositoryRoot = repositoryRoot();
        Path processRuntime = repositoryRoot.resolve("storage/shared-storage-s3/build/process-runtime");
        assumeTrue(Files.isDirectory(processRuntime), "S3 process runtime was not staged: " + processRuntime);

        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-sigkill");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");
        String clusterId = Uuid.randomUuid().toString();
        String bootstrapServers = bootstrapServers();
        Map<Integer, BrokerProcess> brokers = new LinkedHashMap<>();

        System.out.println("MATRIX_SCENARIO rf=" + scenario.replicationFactor() +
            " acks=all minIsr=" + scenario.minIsr());
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
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, scenario.replicationFactor())
                    .configs(Map.of(
                        TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,
                        Short.toString(scenario.minIsr())
                    )))).all().get(30, TimeUnit.SECONDS);

                TopicDescription topic = waitForTopicState(
                    admin,
                    TOPIC,
                    scenario.replicationFactor(),
                    scenario.replicationFactor(),
                    -1
                );
                SharedPartitionId partition = sharedPartitionId(topic.topicId(), 0);
                int oldLeader = topic.partitions().get(0).leader().id();
                List<Integer> replicaIds = topic.partitions().get(0).replicas().stream()
                    .map(node -> node.id())
                    .toList();

                if (scenario.replicationFactor() == 2) {
                    createWriteProbeTopic(admin, oldLeader, replicaIds, scenario.minIsr());
                }

                OffsetRange warmup = produceRange(bootstrapServers, 0, WARMUP_RECORDS);
                waitForReplicatedWal(brokers, replicaIds);
                TestUtils.waitForCondition(
                    () -> brokerCoverage(bootstrapServers, partition, oldLeader).covers(warmup),
                    90_000L,
                    () -> "Old leader " + oldLeader + " never remotely committed warmup range " + warmup
                );

                OffsetRange crashRange = produceRange(bootstrapServers, WARMUP_RECORDS, CRASH_RECORDS);
                assertEquals(new OffsetRange(WARMUP_RECORDS, PRE_RECOVERY_RECORDS), crashRange);
                assertFalse(
                    allCoverage(bootstrapServers, partition).covers(crashRange),
                    "Crash tranche unexpectedly reached remote storage before SIGKILL"
                );

                BrokerProcess victim = brokers.get(oldLeader);
                assertTrue(victim.process().isAlive(), "Leader process was not alive before SIGKILL");
                long victimPid = victim.process().pid();
                sigkill(victimPid);
                assertTrue(victim.process().waitFor(30, TimeUnit.SECONDS), "SIGKILLed broker JVM did not exit");
                assertFalse(victim.process().isAlive());
                System.out.println("MATRIX_SIGKILL rf=" + scenario.replicationFactor() +
                    " brokerId=" + oldLeader + " pid=" + victimPid);

                if (scenario.replicationFactor() == 1) {
                    runSingleReplicaRecovery(
                        repositoryRoot,
                        processRuntime,
                        bootstrapServers,
                        admin,
                        brokers,
                        partition,
                        crashRange,
                        oldLeader,
                        scenario
                    );
                } else {
                    runReplicatedFailover(
                        repositoryRoot,
                        processRuntime,
                        bootstrapServers,
                        admin,
                        brokers,
                        partition,
                        crashRange,
                        oldLeader,
                        scenario
                    );
                }
            }
        } finally {
            stopProcesses(brokers);
            copyDiagnostics(repositoryRoot);
        }
    }

    private void runReplicatedFailover(
        Path repositoryRoot,
        Path processRuntime,
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        SharedPartitionId partition,
        OffsetRange crashRange,
        int oldLeader,
        Scenario scenario
    ) throws Exception {
        int newLeader = waitForNewLeader(admin, TOPIC, oldLeader);
        assertNotEquals(oldLeader, newLeader);
        assertTrue(brokers.get(newLeader).process().isAlive(), "Replacement leader is not an independent live JVM");

        TestUtils.waitForCondition(
            () -> brokerCoverage(bootstrapServers, partition, newLeader).covers(crashRange),
            90_000L,
            () -> "Replacement leader " + newLeader +
                " never uploaded the pre-SIGKILL acknowledged range " + crashRange
        );
        assertExpectedValues(
            consumeAll(bootstrapServers, PRE_RECOVERY_RECORDS),
            PRE_RECOVERY_RECORDS,
            "Acknowledged data must be readable from the replacement leader"
        );
        System.out.println("MATRIX_FAILOVER_READABLE rf=" + scenario.replicationFactor() +
            " oldLeader=" + oldLeader + " newLeader=" + newLeader + " records=" + PRE_RECOVERY_RECORDS);

        if (scenario.replicationFactor() == 3) {
            OffsetRange postCrash = produceRange(
                bootstrapServers,
                PRE_RECOVERY_RECORDS,
                POST_CRASH_RECORDS
            );
            assertEquals(new OffsetRange(PRE_RECOVERY_RECORDS, TOTAL_RECORDS), postCrash);
            System.out.println("MATRIX_FAILOVER_WRITABLE rf=3 newLeader=" + newLeader +
                " postRange=" + postCrash);
            restartBroker(repositoryRoot, processRuntime, brokers, oldLeader);
            waitForTopicState(admin, TOPIC, (short) 3, (short) 3, newLeader);
        } else {
            waitForTopicState(admin, WRITE_PROBE_TOPIC, (short) 2, (short) 1, newLeader);
            assertAcksAllRejectedByMinIsr(bootstrapServers);
            System.out.println("MATRIX_WRITE_REJECTED_MIN_ISR rf=2 leader=" + newLeader + " minIsr=2");
            restartBroker(repositoryRoot, processRuntime, brokers, oldLeader);
            waitForTopicState(admin, TOPIC, (short) 2, (short) 2, newLeader);
            OffsetRange postCrash = produceRange(
                bootstrapServers,
                PRE_RECOVERY_RECORDS,
                POST_CRASH_RECORDS
            );
            assertEquals(new OffsetRange(PRE_RECOVERY_RECORDS, TOTAL_RECORDS), postCrash);
            System.out.println("MATRIX_WRITE_RESUMED rf=2 leader=" + newLeader + " postRange=" + postCrash);
        }

        waitForFullRemoteCoverage(bootstrapServers, partition);
        assertExpectedValues(
            consumeAll(bootstrapServers, TOTAL_RECORDS),
            TOTAL_RECORDS,
            "All acknowledged records must survive replicated failover exactly once and in order"
        );
        System.out.println("MATRIX_RECOVERED rf=" + scenario.replicationFactor() +
            " mode=automatic-failover oldLeader=" + oldLeader + " activeLeader=" + newLeader +
            " records=" + TOTAL_RECORDS);
    }

    private void runSingleReplicaRecovery(
        Path repositoryRoot,
        Path processRuntime,
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        SharedPartitionId partition,
        OffsetRange crashRange,
        int oldLeader,
        Scenario scenario
    ) throws Exception {
        assertProduceUnavailable(bootstrapServers, TOPIC);
        System.out.println("MATRIX_RESTART_REQUIRED rf=1 leader=" + oldLeader +
            " reason=no-replica-for-election");

        restartBroker(repositoryRoot, processRuntime, brokers, oldLeader);
        waitForTopicState(admin, TOPIC, scenario.replicationFactor(), scenario.replicationFactor(), oldLeader);
        TestUtils.waitForCondition(
            () -> brokerCoverage(bootstrapServers, partition, oldLeader).covers(crashRange),
            90_000L,
            () -> "Restarted RF=1 leader never uploaded recovered WAL range " + crashRange
        );

        OffsetRange postCrash = produceRange(
            bootstrapServers,
            PRE_RECOVERY_RECORDS,
            POST_CRASH_RECORDS
        );
        assertEquals(new OffsetRange(PRE_RECOVERY_RECORDS, TOTAL_RECORDS), postCrash);
        waitForFullRemoteCoverage(bootstrapServers, partition);
        assertExpectedValues(
            consumeAll(bootstrapServers, TOTAL_RECORDS),
            TOTAL_RECORDS,
            "RF=1 must recover acknowledged records from the original disk after process restart"
        );
        System.out.println("MATRIX_RECOVERED rf=1 mode=original-node-restart leader=" + oldLeader +
            " records=" + TOTAL_RECORDS);
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

    private static void createWriteProbeTopic(
        Admin admin,
        int oldLeader,
        List<Integer> replicaIds,
        short minIsr
    ) throws Exception {
        List<Integer> orderedReplicas = new ArrayList<>(replicaIds.size());
        orderedReplicas.add(oldLeader);
        replicaIds.stream().filter(id -> id != oldLeader).forEach(orderedReplicas::add);
        NewTopic probe = new NewTopic(WRITE_PROBE_TOPIC, Map.of(0, orderedReplicas))
            .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Short.toString(minIsr)));
        admin.createTopics(List.of(probe)).all().get(30, TimeUnit.SECONDS);
        waitForTopicState(admin, WRITE_PROBE_TOPIC, (short) 2, (short) 2, oldLeader);
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
            "shared.storage.s3.key.prefix=sigkill/" + clusterId + "/objects",
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
        System.out.println("MATRIX_BROKER_STARTED brokerId=" + nodeId + " pid=" + process.pid());
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
            processRuntime.resolve("../matrix-broker-" + nodeId + "-logs").normalize().toString()
        );
        environment.putIfAbsent("AWS_ACCESS_KEY_ID", "minioadmin");
        environment.putIfAbsent("AWS_SECRET_ACCESS_KEY", "minioadmin123");
    }

    private static void waitForCluster(Map<Integer, BrokerProcess> brokers, String bootstrapServers) throws Exception {
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
        String topicName,
        short expectedReplicas,
        short expectedIsr,
        int expectedLeader
    ) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin, topicName);
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
        }, 90_000L, () -> "Topic " + topicName + " did not converge to RF=" + expectedReplicas +
            ", ISR=" + expectedIsr + ", expectedLeader=" + expectedLeader);
        return ready[0];
    }

    private static int waitForNewLeader(Admin admin, String topicName, int oldLeader) throws Exception {
        int[] result = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin, topicName);
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
        }, 60_000L, () -> "No new leader was elected for " + topicName +
            " after SIGKILL of broker " + oldLeader);
        return result[0];
    }

    private static TopicDescription describeTopic(Admin admin, String topicName) throws Exception {
        return admin.describeTopics(List.of(topicName)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(topicName);
    }

    private static OffsetRange produceRange(
        String bootstrapServers,
        int start,
        int count
    ) throws Exception {
        try (KafkaProducer<String, String> producer = producer(bootstrapServers, true)) {
            long first = -1L;
            long last = -1L;
            for (int i = 0; i < count; i++) {
                int sequence = start + i;
                RecordMetadata metadata = producer.send(
                    new ProducerRecord<>(TOPIC, 0, Integer.toString(sequence), value(sequence))
                ).get(30, TimeUnit.SECONDS);
                if (first < 0) {
                    first = metadata.offset();
                }
                last = metadata.offset();
            }
            producer.flush();
            return new OffsetRange(first, Math.addExact(last, 1L));
        }
    }

    private static KafkaProducer<String, String> producer(
        String bootstrapServers,
        boolean idempotent
    ) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotent);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (!idempotent) {
            properties.put(ProducerConfig.RETRIES_CONFIG, 0);
            properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
            properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        }
        return new KafkaProducer<>(properties);
    }

    private static void assertAcksAllRejectedByMinIsr(String bootstrapServers) throws Exception {
        Throwable failure = null;
        try (KafkaProducer<String, String> producer = producer(bootstrapServers, false)) {
            producer.send(new ProducerRecord<>(WRITE_PROBE_TOPIC, 0, "probe", "probe"))
                .get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
            failure = e;
        }
        assertTrue(failure != null, "RF=2/minISR=2 must reject acks=all while only one replica is in ISR");
        assertTrue(
            hasCause(failure, NotEnoughReplicasException.class) ||
                hasCause(failure, NotEnoughReplicasAfterAppendException.class),
            () -> "Expected a minISR rejection but received: " + failure
        );
    }

    private static void assertProduceUnavailable(String bootstrapServers, String topicName) {
        boolean completed = false;
        try (KafkaProducer<String, String> producer = producer(bootstrapServers, false)) {
            producer.send(new ProducerRecord<>(topicName, 0, "unavailable", "unavailable"))
                .get(10, TimeUnit.SECONDS);
            completed = true;
        } catch (Exception expected) {
            System.out.println("MATRIX_EXPECTED_UNAVAILABLE topic=" + topicName +
                " error=" + expected.getClass().getSimpleName());
        }
        assertFalse(completed, "A partition without a live replica must not accept acks=all writes");
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> expectedType) {
        Throwable current = error;
        while (current != null) {
            if (expectedType.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Admin admin(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Admin.create(properties);
    }

    private static List<String> consumeAll(String bootstrapServers, int expectedCount) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "matrix-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<String> values = new ArrayList<>(expectedCount);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TOPIC));
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

    private static void waitForReplicatedWal(
        Map<Integer, BrokerProcess> brokers,
        List<Integer> replicaIds
    ) throws Exception {
        for (int replicaId : replicaIds) {
            BrokerProcess broker = brokers.get(replicaId);
            TestUtils.waitForCondition(
                () -> walBytes(broker.walDir()) > 0L,
                30_000L,
                () -> "Replica broker " + replicaId +
                    " never received shared WAL data in " + broker.walDir()
            );
        }
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

    private static void waitForFullRemoteCoverage(
        String bootstrapServers,
        SharedPartitionId partition
    ) throws Exception {
        OffsetRange fullRange = new OffsetRange(0, TOTAL_RECORDS);
        TestUtils.waitForCondition(
            () -> allCoverage(bootstrapServers, partition).covers(fullRange),
            90_000L,
            () -> "Remote coverage never reached the full acknowledged range " + fullRange
        );
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
        Integer brokerId
    ) {
        PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
        for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
            if (brokerId != null && BrokerObjectId.brokerId(metadata.objectId()) != brokerId) {
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
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "matrix-metadata-" + UUID.randomUUID());
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

    private static Scenario scenario() {
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
        short minIsr = replicationFactor == 3 ? (short) 2 : replicationFactor;
        return new Scenario(replicationFactor, minIsr);
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
        Path destination = repositoryRoot.resolve("core/build/shared-storage-sigkill-diagnostics");
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
            System.out.println("Unable to copy durability-matrix diagnostics: " + e);
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

    private record Scenario(short replicationFactor, short minIsr) {
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
