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
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNode;
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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves safe producer backpressure when an S3 outage exhausts the configured broker-wide shared WAL window.
 *
 * <p>The test stops MinIO before filling a deliberately small WAL. Every successful {@code acks=all} response must
 * remain readable from Kafka's replicated WAL. The first non-admitted append must fail without becoming visible and,
 * critically, without taking Kafka's physical log directory or broker offline. A classic topic on the same brokers
 * must remain writable. After MinIO returns, every acknowledged shared record must become authoritatively remote.</p>
 *
 * <p>This gate intentionally does not claim that remote publication reclaims physical WAL bytes. Safe reclamation
 * requires an independently crash-safe checkpoint and cold-read path. Until that exists, reaching the configured
 * capacity is a fail-closed admission boundary rather than permission to overwrite acknowledged data.</p>
 */
@Tag("integration")
@Timeout(value = 7, unit = TimeUnit.MINUTES)
public class SharedStorageWalCapacityPressureKRaftIntegrationTest {
    private static final String SHARED_TOPIC = "shared-wal-capacity-pressure";
    private static final String CLASSIC_TOPIC = "classic-capacity-probe";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final String MINIO_CONTAINER_ENV = "SHARED_STORAGE_S3_CONTAINER";
    private static final int MAX_FILL_RECORDS = 128;
    private static final int VALUE_BYTES = 8 * 1024;
    private static final int CLASSIC_PROBE_RECORDS = 5;
    private static final long WAL_CAPACITY_BYTES = 256L * 1024;
    private static final long WAL_SEGMENT_BYTES = 64L * 1024;
    private static final long OBJECT_TARGET_BYTES = 32L * 1024;
    private static final long UPLOAD_INTERVAL_MS = 500L;

    @Test
    public void walCapacityRejectsSafelyWithoutOffliningBroker() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        String minioContainer = System.getenv(MINIO_CONTAINER_ENV);
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        assumeTrue(minioContainer != null && !minioContainer.isBlank(), "MinIO container control is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-wal-capacity");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");
        String keyPrefix = "wal-capacity/" + UUID.randomUUID() + "/objects";
        boolean minioStopped = false;

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topics", SHARED_TOPIC)
            .setConfigProp("shared.storage.wal.capacity.bytes", WAL_CAPACITY_BYTES)
            .setConfigProp("shared.storage.wal.segment.bytes", WAL_SEGMENT_BYTES)
            .setConfigProp("shared.storage.object.target.bytes", OBJECT_TARGET_BYTES)
            .setConfigProp("shared.storage.upload.interval.ms", UPLOAD_INTERVAL_MS)
            .setConfigProp("shared.storage.orphan.cleanup.interval.ms", 1_000L)
            .setConfigProp("shared.storage.orphan.grace.ms", 10 * 60 * 1_000L)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", keyPrefix)
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .setConfigProp("shared.storage.s3.connection.timeout.ms", 500L)
            .setConfigProp("shared.storage.s3.socket.timeout.ms", 1_000L)
            .setConfigProp("shared.storage.s3.api.call.attempt.timeout.ms", 1_500L)
            .setConfigProp("shared.storage.s3.api.call.timeout.ms", 3_000L)
            .setConfigProp("shared.storage.s3.max.attempts", 1)
            .build()) {

            try {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                String bootstrapServers = cluster.bootstrapServers();

                try (Admin admin = cluster.admin()) {
                    createTopics(admin);
                    TopicDescription sharedDescription = waitForTopicReady(admin, SHARED_TOPIC);
                    waitForTopicReady(admin, CLASSIC_TOPIC);
                    SharedPartitionId partition = sharedPartitionId(sharedDescription.topicId(), 0);

                    stopContainer(minioContainer);
                    minioStopped = true;
                    waitForMinioState(s3Endpoint, false);
                    System.out.println("WAL_CAPACITY_OUTAGE_STARTED capacityBytes=" + WAL_CAPACITY_BYTES +
                        " valueBytes=" + VALUE_BYTES);

                    FillResult fill = fillSharedWal(bootstrapServers);
                    assertTrue(fill.acknowledgedValues().size() > 0,
                        "The WAL must admit records before reaching its capacity boundary");
                    assertNotNull(fill.rejection(), "The bounded fill must reach WAL capacity");
                    assertTrue(
                        hasCause(fill.rejection(), KafkaStorageException.class),
                        "Expected KafkaStorageException admission backpressure, but received " +
                            failureDescription(fill.rejection())
                    );
                    assertEquals(0L, fill.acknowledgedRange().startOffset());
                    assertEquals(fill.acknowledgedValues().size(), fill.acknowledgedRange().endOffset());
                    System.out.println("WAL_CAPACITY_BACKPRESSURE acknowledged=" +
                        fill.acknowledgedValues().size() + " rejectedSequence=" + fill.rejectedSequence() +
                        " error=" + rootCause(fill.rejection()).getClass().getSimpleName());

                    Thread.sleep(2L * UPLOAD_INTERVAL_MS);
                    assertFalse(
                        allCoverage(bootstrapServers, partition).covers(fill.acknowledgedRange()),
                        "Records acknowledged while S3 is unavailable must not have fabricated remote coverage"
                    );
                    assertEquals(
                        fill.acknowledgedValues(),
                        consumeAssigned(bootstrapServers, SHARED_TOPIC, fill.acknowledgedValues().size(), 60),
                        "Only successfully acknowledged shared records may remain visible"
                    );

                    Map<Integer, Long> walBytes = assertBrokerAndClassicTopicHealthy(
                        cluster,
                        admin,
                        bootstrapServers
                    );
                    System.out.println("WAL_CAPACITY_BROKER_ONLINE brokers=3 isr=3 classicRecords=" +
                        CLASSIC_PROBE_RECORDS + " walBytes=" + walBytes);

                    startContainer(minioContainer);
                    minioStopped = false;
                    waitForMinioState(s3Endpoint, true);

                    TestUtils.waitForCondition(
                        () -> allCoverage(bootstrapServers, partition).covers(fill.acknowledgedRange()),
                        120_000L,
                        () -> "Remote coverage never reached acknowledged WAL range " + fill.acknowledgedRange()
                    );
                    assertEquals(
                        fill.acknowledgedValues(),
                        consumeAssigned(bootstrapServers, SHARED_TOPIC, fill.acknowledgedValues().size(), 60),
                        "Every acknowledged shared record must survive capacity backpressure and S3 recovery"
                    );
                    assertEquals(3, waitForTopicReady(admin, SHARED_TOPIC).partitions().get(0).isr().size());
                    System.out.println("WAL_CAPACITY_REMOTE_RECOVERED range=" + fill.acknowledgedRange() +
                        " records=" + fill.acknowledgedValues().size() + " overwriteAllowed=false");
                }
            } finally {
                if (minioStopped) {
                    startContainerIgnoringFailure(minioContainer);
                    waitForMinioStateIgnoringFailure(s3Endpoint, true);
                }
            }
        }
    }

    private static void createTopics(Admin admin) throws Exception {
        Map<String, String> configs = Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        admin.createTopics(List.of(
            new NewTopic(SHARED_TOPIC, 1, (short) 3).configs(configs),
            new NewTopic(CLASSIC_TOPIC, 1, (short) 3).configs(configs)
        )).all().get(30, TimeUnit.SECONDS);
    }

    private static FillResult fillSharedWal(String bootstrapServers) throws Exception {
        List<String> acknowledged = new ArrayList<>();
        long firstOffset = -1L;
        long lastOffset = -1L;
        Throwable rejection = null;
        int rejectedSequence = -1;

        try (KafkaProducer<String, String> producer = producer(bootstrapServers)) {
            for (int sequence = 0; sequence < MAX_FILL_RECORDS; sequence++) {
                String value = sharedValue(sequence);
                try {
                    RecordMetadata metadata = producer.send(
                        new ProducerRecord<>(SHARED_TOPIC, 0, Integer.toString(sequence), value)
                    ).get(20, TimeUnit.SECONDS);
                    if (firstOffset < 0) {
                        firstOffset = metadata.offset();
                    }
                    lastOffset = metadata.offset();
                    acknowledged.add(value);
                } catch (ExecutionException e) {
                    rejection = e;
                    rejectedSequence = sequence;
                    break;
                }
            }
        }

        if (acknowledged.isEmpty()) {
            return new FillResult(List.of(), new OffsetRange(0L, 0L), rejection, rejectedSequence);
        }
        return new FillResult(
            List.copyOf(acknowledged),
            new OffsetRange(firstOffset, Math.addExact(lastOffset, 1L)),
            rejection,
            rejectedSequence
        );
    }

    private static Map<Integer, Long> assertBrokerAndClassicTopicHealthy(
        KafkaClusterTestKit cluster,
        Admin admin,
        String bootstrapServers
    ) throws Exception {
        assertEquals(3, admin.describeCluster().nodes().get(30, TimeUnit.SECONDS).size(),
            "WAL capacity pressure must not terminate or unregister brokers");
        TopicDescription shared = waitForTopicReady(admin, SHARED_TOPIC);
        assertEquals(3, shared.partitions().get(0).isr().size(),
            "WAL capacity rejection must not remove healthy replicas from ISR");

        Map<Integer, Long> walBytes = walBytesByBroker(cluster);
        for (Map.Entry<Integer, Long> entry : walBytes.entrySet()) {
            assertTrue(
                entry.getValue() <= WAL_CAPACITY_BYTES,
                "Broker " + entry.getKey() + " exceeded WAL capacity: " + entry.getValue()
            );
        }

        List<String> expected = new ArrayList<>(CLASSIC_PROBE_RECORDS);
        try (KafkaProducer<String, String> producer = producer(bootstrapServers)) {
            for (int sequence = 0; sequence < CLASSIC_PROBE_RECORDS; sequence++) {
                String value = "classic-" + sequence;
                producer.send(new ProducerRecord<>(CLASSIC_TOPIC, 0, Integer.toString(sequence), value))
                    .get(30, TimeUnit.SECONDS);
                expected.add(value);
            }
        }
        assertEquals(
            expected,
            consumeAssigned(bootstrapServers, CLASSIC_TOPIC, CLASSIC_PROBE_RECORDS, 30),
            "A classic topic on the same brokers must remain writable after shared WAL rejection"
        );
        return walBytes;
    }

    private static Map<Integer, Long> walBytesByBroker(KafkaClusterTestKit cluster) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        for (Map.Entry<Integer, TestKitNode> broker : cluster.nodes().brokerNodes().entrySet()) {
            result.put(broker.getKey(), walBytes(walDir(broker.getValue(), broker.getKey())));
        }
        return Map.copyOf(result);
    }

    private static Path walDir(TestKitNode broker, int brokerId) {
        Path dataDir = Path.of(broker.logDataDirectories().iterator().next())
            .toAbsolutePath()
            .normalize();
        return dataDir
            .resolveSibling(dataDir.getFileName() + ".shared-storage")
            .resolve("broker-" + brokerId)
            .resolve("wal");
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
                    } catch (Exception ignored) {
                        return 0L;
                    }
                })
                .sum();
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static TopicDescription waitForTopicReady(Admin admin, String topicName) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topic = admin.describeTopics(List.of(topicName)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topicName);
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
        }, 60_000L, () -> "Topic " + topicName + " never reached leader + RF3/ISR3 readiness");
        return ready[0];
    }

    private static List<String> consumeAssigned(
        String bootstrapServers,
        String topicName,
        int expectedCount,
        int timeoutSeconds
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        TopicPartition partition = new TopicPartition(topicName, 0);
        List<String> values = new ArrayList<>(expectedCount);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
            while (values.size() < expectedCount && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> values.add(record.value()));
            }
        }
        return values;
    }

    private static PartitionRemoteCoverage allCoverage(
        String bootstrapServers,
        SharedPartitionId partition
    ) {
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
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "wal-capacity-metadata-" + UUID.randomUUID());
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
                    } else {
                        latestCommitted.remove(key.id());
                    }
                }
            }
        }
        return List.copyOf(latestCommitted.values());
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static String failureDescription(Throwable error) {
        Throwable root = rootCause(error);
        return root.getClass().getName() + ": " + root.getMessage();
    }

    private static void stopContainer(String containerName) throws Exception {
        docker("stop", "--time", "0", containerName);
    }

    private static void startContainer(String containerName) throws Exception {
        docker("start", containerName);
    }

    private static void startContainerIgnoringFailure(String containerName) {
        try {
            startContainer(containerName);
        } catch (Exception e) {
            System.out.println("Unable to restart MinIO container " + containerName + ": " + e);
        }
    }

    private static void docker(String... arguments) throws Exception {
        List<String> command = new ArrayList<>(arguments.length + 1);
        command.add("docker");
        command.addAll(List.of(arguments));
        Process process = new ProcessBuilder(command)
            .redirectErrorStream(true)
            .start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Docker command timed out: " + command);
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), () -> "Docker command failed: " + command + "\n" + output);
    }

    private static void waitForMinioState(String endpoint, boolean expectedReady) throws Exception {
        TestUtils.waitForCondition(
            () -> minioReady(endpoint) == expectedReady,
            60_000L,
            () -> "MinIO endpoint " + endpoint + " did not reach ready=" + expectedReady
        );
    }

    private static void waitForMinioStateIgnoringFailure(String endpoint, boolean expectedReady) {
        try {
            waitForMinioState(endpoint, expectedReady);
        } catch (Exception e) {
            System.out.println("Unable to verify MinIO recovery at " + endpoint + ": " + e);
        }
    }

    private static boolean minioReady(String endpoint) {
        String normalized = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        HttpRequest request = HttpRequest.newBuilder(URI.create(normalized + "/minio/health/ready"))
            .timeout(Duration.ofSeconds(2))
            .GET()
            .build();
        try {
            HttpResponse<Void> response = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build()
                .send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() >= 200 && response.statusCode() < 300;
        } catch (IOException e) {
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition
        );
    }

    private static String sharedValue(int sequence) {
        String prefix = String.format("%08d:", sequence);
        return prefix + "x".repeat(VALUE_BYTES - prefix.length());
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record FillResult(
        List<String> acknowledgedValues,
        OffsetRange acknowledgedRange,
        Throwable rejection,
        int rejectedSequence
    ) {
    }
}
