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
 * Gate 3: proves failover with three real Kafka JVMs and an OS-level SIGKILL of the current partition leader.
 *
 * <p>This test deliberately does not use {@code KafkaClusterTestKit} and never calls {@code BrokerServer.shutdown()} for
 * the crash transition. {@code kafka-server-start.sh} execs the Java process, so the {@link Process#pid()} tracked here
 * is the actual broker JVM PID. The leader is terminated with {@code /bin/kill -9}; only surviving independent JVMs may
 * elect the replacement leader and upload the acknowledged WAL tail.</p>
 */
@Tag("integration")
@Timeout(value = 6, unit = TimeUnit.MINUTES)
public class SharedStorageIndependentProcessSigkillTest {
    private static final String TOPIC = "shared-wal-sigkill";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int WARMUP_RECORDS = 20;
    private static final int CRASH_RECORDS = 40;
    private static final int POST_CRASH_RECORDS = 20;
    private static final int TOTAL_RECORDS = WARMUP_RECORDS + CRASH_RECORDS + POST_CRASH_RECORDS;
    private static final long UPLOAD_INTERVAL_MS = 30_000L;
    private static final int[] BROKER_PORTS = {19092, 19192, 19292};
    private static final int[] CONTROLLER_PORTS = {19093, 19193, 19293};

    @TempDir
    Path tempDir;

    @Test
    public void acksAllWalTailSurvivesRealLeaderSigkill() throws Exception {
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

        try {
            for (int nodeId = 1; nodeId <= 3; nodeId++) {
                Path config = writeBrokerConfig(
                    repositoryRoot,
                    nodeId,
                    clusterId,
                    s3Endpoint,
                    bucket,
                    region
                );
                formatStorage(repositoryRoot, processRuntime, clusterId, config, nodeId);
                brokers.put(nodeId, startBroker(repositoryRoot, processRuntime, nodeId, config));
            }

            waitForCluster(brokers, bootstrapServers);
            try (Admin admin = admin(bootstrapServers);
                 KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3)
                    .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                    .all().get(30, TimeUnit.SECONDS);

                TopicDescription topic = waitForTopicReady(admin);
                SharedPartitionId partition = sharedPartitionId(topic.topicId(), 0);
                int oldLeader = topic.partitions().get(0).leader().id();

                OffsetRange warmup = produceRange(producer, 0, WARMUP_RECORDS);
                waitForReplicatedWal(brokers);
                TestUtils.waitForCondition(
                    () -> brokerCoverage(bootstrapServers, partition, oldLeader).covers(warmup),
                    90_000L,
                    () -> "Old leader " + oldLeader + " never remotely committed warmup range " + warmup
                );

                OffsetRange crashRange = produceRange(producer, WARMUP_RECORDS, CRASH_RECORDS);
                PartitionRemoteCoverage beforeKill = allCoverage(bootstrapServers, partition);
                assertFalse(
                    beforeKill.covers(crashRange),
                    "Crash tranche unexpectedly reached remote storage before SIGKILL"
                );

                BrokerProcess victim = brokers.get(oldLeader);
                assertTrue(victim.process().isAlive(), "Leader process was not alive before SIGKILL");
                long victimPid = victim.process().pid();
                sigkill(victimPid);
                assertTrue(victim.process().waitFor(30, TimeUnit.SECONDS), "SIGKILLed broker JVM did not exit");
                assertFalse(victim.process().isAlive());
                System.out.println("GATE3_SIGKILL brokerId=" + oldLeader + " pid=" + victimPid +
                    " exitCode=" + victim.process().exitValue());

                int newLeader = waitForNewLeader(admin, oldLeader);
                assertNotEquals(oldLeader, newLeader);
                assertTrue(brokers.get(newLeader).process().isAlive(), "Replacement leader is not an independent live JVM");

                OffsetRange postCrash = produceRange(
                    producer,
                    WARMUP_RECORDS + CRASH_RECORDS,
                    POST_CRASH_RECORDS
                );
                assertEquals(
                    new OffsetRange(
                        WARMUP_RECORDS + CRASH_RECORDS,
                        TOTAL_RECORDS
                    ),
                    postCrash
                );

                TestUtils.waitForCondition(
                    () -> brokerCoverage(bootstrapServers, partition, newLeader).covers(crashRange),
                    90_000L,
                    () -> "Replacement leader " + newLeader +
                        " never uploaded the pre-SIGKILL acknowledged range " + crashRange
                );

                List<String> consumed = consumeAll(bootstrapServers, TOTAL_RECORDS);
                assertEquals(
                    TOTAL_RECORDS,
                    consumed.size(),
                    "Every acknowledged record must remain consumable after SIGKILL"
                );
                List<String> expected = new ArrayList<>(TOTAL_RECORDS);
                for (int i = 0; i < TOTAL_RECORDS; i++) {
                    expected.add(value(i));
                }
                assertEquals(expected, consumed, "All acknowledged records must survive SIGKILL exactly once and in order");
                System.out.println("GATE3_RECOVERED oldLeader=" + oldLeader + " newLeader=" + newLeader +
                    " crashRange=" + crashRange + " records=" + consumed.size());
            }
        } finally {
            stopProcesses(brokers);
        }
    }

    private Path writeBrokerConfig(
        Path repositoryRoot,
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
        Path log = tempDir.resolve("node-" + nodeId).resolve("broker.log");
        ProcessBuilder builder = new ProcessBuilder(
            repositoryRoot.resolve("bin/kafka-server-start.sh").toString(),
            config.toString()
        );
        configureEnvironment(builder, processRuntime, nodeId);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        Process process = builder.start();
        System.out.println("GATE3_STARTED brokerId=" + nodeId + " pid=" + process.pid());
        return new BrokerProcess(nodeId, process, log, tempDir.resolve("node-" + nodeId).resolve("wal"));
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
        environment.put("LOG_DIR", processRuntime.resolve("../gate3-broker-" + nodeId + "-logs").normalize().toString());
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

    private static TopicDescription waitForTopicReady(Admin admin) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = describeTopic(admin);
                if (topic == null || topic.partitions().size() != 1) {
                    return false;
                }
                var partition = topic.partitions().get(0);
                if (partition.leader() == null || partition.leader().id() < 0 ||
                    partition.replicas().size() != 3 || partition.isr().size() != 3) {
                    return false;
                }
                ready[0] = topic;
                return true;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "SIGKILL topic never reached leader + RF3/ISR3 readiness");
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
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
    }

    private static OffsetRange produceRange(
        KafkaProducer<String, String> producer,
        int start,
        int count
    ) throws Exception {
        long first = -1L;
        long last = -1L;
        for (int i = 0; i < count; i++) {
            int value = start + i;
            RecordMetadata metadata = producer.send(
                new ProducerRecord<>(TOPIC, 0, Integer.toString(value), value(value))
            ).get(30, TimeUnit.SECONDS);
            if (first < 0) {
                first = metadata.offset();
            }
            last = metadata.offset();
        }
        producer.flush();
        return new OffsetRange(first, Math.addExact(last, 1L));
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

    private static Admin admin(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return Admin.create(properties);
    }

    private static List<String> consumeAll(String bootstrapServers, int expectedCount) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "gate3-" + UUID.randomUUID());
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

    private static void waitForReplicatedWal(Map<Integer, BrokerProcess> brokers) throws Exception {
        for (BrokerProcess broker : brokers.values()) {
            TestUtils.waitForCondition(
                () -> walBytes(broker.walDir()) > 0L,
                30_000L,
                () -> "Broker " + broker.nodeId() + " never received shared WAL data in " + broker.walDir()
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
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "gate3-metadata-" + UUID.randomUUID());
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

    private static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("bin")) && Files.isDirectory(current.resolve("core"))) {
            return current;
        }
        Path parent = current.getParent();
        if (parent != null && Files.isDirectory(parent.resolve("bin")) && Files.isDirectory(parent.resolve("core"))) {
            return parent;
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
            int start = Math.max(0, lines.size() - 80);
            return String.join("\n", lines.subList(start, lines.size()));
        } catch (IOException e) {
            return "<unable to read " + log + ": " + e + ">";
        }
    }

    private record BrokerProcess(int nodeId, Process process, Path logFile, Path walDir) {
    }
}
