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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves that exhausting the configured WAL durability window applies safe backpressure instead of overwriting data or
 * taking a healthy Kafka log directory offline.
 *
 * <p>MinIO is stopped after one remotely committed record. An RF=3/minISR=2 topic is then filled with sequential
 * {@code acks=all} records until WAL admission rejects the next logical append group. All previously acknowledged data
 * must remain readable, every broker must remain registered with the partition fully in ISR, and physical WAL usage
 * must remain within the configured capacity. After MinIO returns, the complete acknowledged range must become
 * authoritative remote coverage. WAL-space reclamation is intentionally a separate gate; this test locks down the
 * safety boundary that must hold before reclamation is introduced.</p>
 */
@Tag("integration")
@Timeout(value = 6, unit = TimeUnit.MINUTES)
public class SharedStorageWalCapacityBackpressureKRaftIntegrationTest {
    private static final String TOPIC = "shared-wal-capacity-backpressure";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final String MINIO_CONTAINER_ENV = "SHARED_STORAGE_S3_CONTAINER";
    private static final long WAL_CAPACITY_BYTES = 96L * 1024L;
    private static final long WAL_SEGMENT_BYTES = 24L * 1024L;
    private static final int VALUE_BYTES = 4 * 1024;
    private static final int MAX_PRODUCE_ATTEMPTS = 200;
    private static final long UPLOAD_INTERVAL_MS = 250L;

    @Test
    public void walCapacityAppliesNonFatalBackpressureWithoutOverwritingAcknowledgedData() throws Exception {
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
            .setConfigProp("shared.storage.topics", TOPIC)
            .setConfigProp("shared.storage.wal.capacity.bytes", WAL_CAPACITY_BYTES)
            .setConfigProp("shared.storage.wal.segment.bytes", WAL_SEGMENT_BYTES)
            .setConfigProp("shared.storage.object.target.bytes", 16L * 1024L)
            .setConfigProp("shared.storage.upload.interval.ms", UPLOAD_INTERVAL_MS)
            .setConfigProp("shared.storage.orphan.cleanup.interval.ms", 1_000L)
            .setConfigProp("shared.storage.orphan.grace.ms", 10 * 60 * 1000L)
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
                    createTopic(admin);
                    TopicDescription topic = waitForTopicHealthy(admin);
                    SharedPartitionId partition = sharedPartitionId(topic.topicId(), 0);

                    List<String> acknowledged = new ArrayList<>();
                    try (KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                        acknowledged.add(produceOne(producer, 0));
                    }
                    OffsetRange warmup = new OffsetRange(0, 1);
                    TestUtils.waitForCondition(
                        () -> allCoverage(bootstrapServers, partition).covers(warmup),
                        90_000L,
                        () -> "Warmup record never became authoritative remote coverage"
                    );

                    stopContainer(minioContainer);
                    minioStopped = true;
                    waitForMinioState(s3Endpoint, false);
                    System.out.println("WAL_CAPACITY_OUTAGE_STARTED capacityBytes=" + WAL_CAPACITY_BYTES);

                    Throwable rejection;
                    try (KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                        rejection = produceUntilRejected(producer, acknowledged);
                    }
                    assertTrue(rejection != null, "WAL admission never reached the configured capacity boundary");
                    assertTrue(
                        hasCause(rejection, KafkaStorageException.class),
                        () -> "Expected KafkaStorageException capacity backpressure but received " + rejection
                    );
                    assertTrue(acknowledged.size() > 1, "The test must acknowledge data while S3 is unavailable");
                    System.out.println("WAL_CAPACITY_BACKPRESSURE acknowledged=" + acknowledged.size() +
                        " error=" + rootCause(rejection).getClass().getSimpleName());

                    TopicDescription healthy = waitForTopicHealthy(admin);
                    assertEquals(3, healthy.partitions().get(0).isr().size());
                    assertEquals(3, admin.describeCluster().nodes().get(30, TimeUnit.SECONDS).size());
                    assertExpectedValues(consumeAssigned(bootstrapServers, acknowledged.size()), acknowledged);
                    Map<Integer, Long> walBytes = walBytesByBroker(cluster);
                    for (Map.Entry<Integer, Long> entry : walBytes.entrySet()) {
                        assertTrue(
                            entry.getValue() <= WAL_CAPACITY_BYTES,
                            "Broker " + entry.getKey() + " exceeded configured WAL capacity: " + entry.getValue()
                        );
                    }
                    System.out.println("WAL_CAPACITY_BROKERS_HEALTHY isr=3 walBytes=" + walBytes);

                    startContainer(minioContainer);
                    minioStopped = false;
                    waitForMinioState(s3Endpoint, true);
                    OffsetRange acknowledgedRange = new OffsetRange(0, acknowledged.size());
                    TestUtils.waitForCondition(
                        () -> allCoverage(bootstrapServers, partition).covers(acknowledgedRange),
                        120_000L,
                        () -> "Acknowledged WAL range never became remote after S3 recovery: " + acknowledgedRange
                    );
                    assertExpectedValues(consumeAssigned(bootstrapServers, acknowledged.size()), acknowledged);
                    assertEquals(3, waitForTopicHealthy(admin).partitions().get(0).isr().size());
                    System.out.println("WAL_CAPACITY_REMOTE_RECOVERED range=" + acknowledgedRange +
                        " records=" + acknowledged.size() + " overwrite=false");
                }
            } finally {
                if (minioStopped) {
                    startContainerIgnoringFailure(minioContainer);
                    waitForMinioStateIgnoringFailure(s3Endpoint, true);
                }
            }
        }
    }

    private static void createTopic(Admin admin) throws Exception {
        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3)
            .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
            .all().get(30, TimeUnit.SECONDS);
    }

    private static Throwable produceUntilRejected(
        KafkaProducer<String, String> producer,
        List<String> acknowledged
    ) {
        for (int sequence = 1; sequence < MAX_PRODUCE_ATTEMPTS; sequence++) {
            try {
                acknowledged.add(produceOne(producer, sequence));
            } catch (Exception e) {
                return e;
            }
        }
        return null;
    }

    private static String produceOne(KafkaProducer<String, String> producer, int sequence) throws Exception {
        String value = value(sequence);
        RecordMetadata metadata = producer.send(
            new ProducerRecord<>(TOPIC, 0, Integer.toString(sequence), value)
        ).get(15, TimeUnit.SECONDS);
        assertEquals(sequence, metadata.offset(), "sequential rejected appends must not create offset holes");
        return value;
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static TopicDescription waitForTopicHealthy(Admin admin) throws Exception {
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
        }, 60_000L, () -> "Capacity-test topic did not retain leader + RF3/ISR3 health");
        return ready[0];
    }

    private static TopicDescription describeTopic(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
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
                    applyMetadataRecord(latestCommitted, record);
                }
            }
        }
        return List.copyOf(latestCommitted.values());
    }

    private static void applyMetadataRecord(
        Map<Long, SharedObjectMetadata> latestCommitted,
        ConsumerRecord<byte[], byte[]> record
    ) {
        if (record.key() == null) {
            return;
        }
        SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(record.key());
        if (key.type() != SharedMetadataRecordCodec.KeyType.OBJECT) {
            return;
        }
        if (record.value() == null) {
            latestCommitted.remove(key.id());
            return;
        }
        SharedMetadataRecordCodec.MetadataValue value = SharedMetadataRecordCodec.decodeValue(key, record.value());
        if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committed) {
            latestCommitted.put(key.id(), committed.metadata());
        } else {
            latestCommitted.remove(key.id());
        }
    }

    private static List<String> consumeAssigned(String bootstrapServers, int expectedCount) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "wal-capacity-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> values = new ArrayList<>(expectedCount);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            TopicPartition partition = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (values.size() < expectedCount && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
            }
        }
        return values;
    }

    private static void assertExpectedValues(List<String> actual, List<String> expected) {
        assertEquals(expected.size(), actual.size(), "Every acknowledged record must remain readable");
        assertEquals(expected, actual, "Capacity rejection must not overwrite or reorder acknowledged data");
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), partition);
    }

    private static String value(int sequence) {
        String prefix = "value-" + sequence + "-";
        return prefix + "x".repeat(Math.max(0, VALUE_BYTES - prefix.length()));
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> expected) {
        Throwable current = failure;
        while (current != null) {
            if (expected.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static void stopContainer(String container) throws Exception {
        runDocker("stop", container);
    }

    private static void startContainer(String container) throws Exception {
        runDocker("start", container);
    }

    private static void runDocker(String action, String container) throws Exception {
        Process process = new ProcessBuilder("docker", action, container)
            .redirectErrorStream(true)
            .start();
        assertTrue(process.waitFor(30, TimeUnit.SECONDS), "docker " + action + " timed out");
        assertEquals(0, process.exitValue(), "docker " + action + " failed for " + container);
    }

    private static void startContainerIgnoringFailure(String container) {
        try {
            startContainer(container);
        } catch (Exception ignored) {
            // Best effort cleanup. The workflow also starts MinIO before collecting diagnostics.
        }
    }

    private static void waitForMinioState(String endpoint, boolean expectedReady) throws Exception {
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(500))
            .build();
        URI readiness = URI.create(endpoint + "/minio/health/ready");
        TestUtils.waitForCondition(
            () -> isReady(client, readiness) == expectedReady,
            60_000L,
            () -> "MinIO readiness did not become " + expectedReady + " at " + readiness
        );
    }

    private static boolean isReady(HttpClient client, URI readiness) {
        try {
            HttpRequest request = HttpRequest.newBuilder(readiness)
                .timeout(Duration.ofSeconds(1))
                .GET()
                .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (Exception ignored) {
            return false;
        }
    }

    private static void waitForMinioStateIgnoringFailure(String endpoint, boolean expectedReady) {
        try {
            waitForMinioState(endpoint, expectedReady);
        } catch (Exception ignored) {
            // Best effort cleanup.
        }
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
