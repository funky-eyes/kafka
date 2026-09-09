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
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.test.api.TestKitDefaults;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves recovery when the authoritative remote prefix must be rediscovered from Kafka metadata while a newer
 * acknowledged WAL tail survives locally.
 *
 * <p>The prefix is deliberately larger than the fixed ring WAL, and every prefix chunk is remotely committed before
 * the next chunk is produced. Successful production beyond ring capacity therefore proves that old physical slots have
 * been reclaimed/reused. MinIO is then stopped and an {@code acks=all} tail is appended, making that tail WAL-only.
 * After every broker is stopped, only {@code remote-object-ranges.checkpoint} is deleted; {@code shared-ring.wal} is
 * preserved byte-for-byte. On restart, metadata replay must rematerialize the remote prefix even though the surviving
 * WAL tail already raises the local LEO beyond the remote committed end.</p>
 */
@Tag("integration")
@Timeout(value = 12, unit = TimeUnit.MINUTES)
public class SharedStorageRemoteCheckpointLossE2ETest {
    private static final String TOPIC = "shared-remote-checkpoint-loss";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final String REMOTE_CHECKPOINT_FILE = "remote-object-ranges.checkpoint";
    private static final String RING_WAL_FILE = "shared-ring.wal";
    private static final String MINIO_CONTAINER_ENV = "SHARED_STORAGE_S3_CONTAINER";
    private static final int BROKERS = 3;
    private static final int PARTITIONS = 1;
    private static final int PREFIX_CHUNKS = 6;
    private static final int RECORDS_PER_PREFIX_CHUNK = 32;
    private static final int WAL_ONLY_TAIL_RECORDS = 16;
    private static final int VALUE_BYTES = 4 * 1024;
    private static final long WAL_CAPACITY_BYTES = 512L * 1024;
    private static final long OBJECT_TARGET_BYTES = 64L * 1024;
    private static final long MAX_SEQUENTIAL_BROKER_STARTUP_MS = 30_000L;

    @Test
    public void clusterRecoversRemotePrefixWhenCheckpointIsLostButWalTailSurvives() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        String minioContainer = System.getenv(MINIO_CONTAINER_ENV);
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        assumeTrue(minioContainer != null && !minioContainer.isBlank(), "MinIO container control is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-local-state-loss");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        assertTrue((long) PREFIX_CHUNKS * RECORDS_PER_PREFIX_CHUNK * VALUE_BYTES > WAL_CAPACITY_BYTES,
            "Remote prefix must exceed ring capacity so old physical WAL slots are necessarily reclaimed/reused");

        Path baseDirectory = TestUtils.tempDirectory().toPath();
        Map<Integer, Path> walDirs = new LinkedHashMap<>();
        Map<Integer, Map<String, String>> perServerProperties = new LinkedHashMap<>();
        for (int index = 0; index < BROKERS; index++) {
            int brokerId = TestKitDefaults.BROKER_ID_OFFSET + index;
            Path walDir = baseDirectory.resolve("checkpoint-loss-shared-state-broker-" + brokerId);
            walDirs.put(brokerId, walDir);
            perServerProperties.put(brokerId, Map.of("shared.storage.wal.dir", walDir.toString()));
        }

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setBaseDirectory(baseDirectory)
            .setNumBrokerNodes(BROKERS)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .setPerServerProperties(perServerProperties)
            .build();

        boolean minioStopped = false;
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topics", TOPIC)
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", WAL_CAPACITY_BYTES)
            .setConfigProp("shared.storage.object.target.bytes", OBJECT_TARGET_BYTES)
            .setConfigProp("shared.storage.upload.interval.ms", 50L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 100L)
            .setConfigProp("shared.storage.upload.wal.pressure.percent", 60)
            .setConfigProp("shared.storage.upload.max.inflight", 4)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "remote-checkpoint-loss/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .build()) {

            try {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                String bootstrapServers = cluster.bootstrapServers();
                int[] nextSequence = new int[PARTITIONS];
                TopicDescription description;

                try (Admin admin = cluster.admin();
                     KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                    admin.createTopics(List.of(new NewTopic(TOPIC, PARTITIONS, (short) BROKERS)
                        .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                        .all().get(30, TimeUnit.SECONDS);
                    description = waitForTopicState(admin, BROKERS);

                    for (int chunk = 0; chunk < PREFIX_CHUNKS; chunk++) {
                        produce(producer, nextSequence, RECORDS_PER_PREFIX_CHUNK);
                        waitForRemoteCoverage(bootstrapServers, description, nextSequence[0]);
                    }
                    waitForLocalCheckpoints(walDirs);
                    consumeAndAssert(bootstrapServers, nextSequence[0]);

                    int remotePrefixEnd = nextSequence[0];
                    stopContainer(minioContainer);
                    minioStopped = true;
                    waitForMinioState(s3Endpoint, false);

                    produce(producer, nextSequence, WAL_ONLY_TAIL_RECORDS);
                    int acknowledgedEnd = nextSequence[0];
                    Thread.sleep(1_000L);
                    assertFalse(remoteCoverageCovers(bootstrapServers, description, acknowledgedEnd),
                        "The tail acknowledged while S3 is down must remain outside authoritative remote coverage");
                    System.out.println("REMOTE_CHECKPOINT_LOSS_WAL_TAIL_ACKED prefixEnd=" + remotePrefixEnd +
                        " acknowledgedEnd=" + acknowledgedEnd);
                }

                shutdownAllBrokers(cluster);
                for (Map.Entry<Integer, Path> entry : walDirs.entrySet()) {
                    Path walDir = entry.getValue();
                    Path checkpoint = walDir.resolve(REMOTE_CHECKPOINT_FILE);
                    Path ringWal = walDir.resolve(RING_WAL_FILE);
                    assertTrue(Files.isRegularFile(checkpoint) && Files.size(checkpoint) > 0L,
                        "Broker " + entry.getKey() + " must have a durable remote checkpoint before deletion");
                    assertTrue(Files.isRegularFile(ringWal),
                        "Broker " + entry.getKey() + " must retain its ring WAL");
                    long ringWalSize = Files.size(ringWal);
                    assertTrue(ringWalSize > 0L, "Ring WAL must be non-empty before checkpoint-only loss");

                    Files.delete(checkpoint);
                    assertFalse(Files.exists(checkpoint),
                        "Only the remote checkpoint should be removed for broker " + entry.getKey());
                    assertTrue(Files.isRegularFile(ringWal),
                        "Checkpoint loss must not remove the surviving ring WAL for broker " + entry.getKey());
                    assertEquals(ringWalSize, Files.size(ringWal),
                        "Checkpoint deletion must preserve the ring WAL byte-for-byte for broker " + entry.getKey());
                }
                System.out.println("REMOTE_CHECKPOINT_LOSS_WAL_PRESERVED brokers=" + BROKERS);

                startContainer(minioContainer);
                minioStopped = false;
                waitForMinioState(s3Endpoint, true);

                restartAllBrokersSequentially(cluster);
                waitForLocalCheckpoints(walDirs);

                try (Admin admin = cluster.admin()) {
                    description = waitForTopicState(admin, BROKERS);
                    consumeAndAssert(bootstrapServers, nextSequence[0]);
                    waitForRemoteCoverage(bootstrapServers, description, nextSequence[0]);
                    consumeAndAssert(bootstrapServers, nextSequence[0]);
                }
                System.out.println("REMOTE_CHECKPOINT_LOSS_RECOVERED records=" + nextSequence[0]);
            } finally {
                if (minioStopped) {
                    startContainerIgnoringFailure(minioContainer);
                    waitForMinioStateIgnoringFailure(s3Endpoint, true);
                }
            }
        }
    }

    private static void shutdownAllBrokers(KafkaClusterTestKit cluster) throws Exception {
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.shutdown();
        }
        for (BrokerServer broker : cluster.brokers().values()) {
            broker.awaitShutdown();
        }
    }

    private static void restartAllBrokersSequentially(KafkaClusterTestKit cluster) throws Exception {
        for (BrokerServer broker : cluster.brokers().values()) {
            long startNanos = System.nanoTime();
            broker.startup();
            long startupMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            assertTrue(startupMs < MAX_SEQUENTIAL_BROKER_STARTUP_MS,
                "A broker recovering shared state must not block on the next broker; took " + startupMs + " ms");
        }
        cluster.waitForReadyBrokers();
    }

    private static void waitForLocalCheckpoints(Map<Integer, Path> walDirs) throws Exception {
        TestUtils.waitForCondition(() -> walDirs.values().stream().allMatch(walDir -> {
            Path checkpoint = walDir.resolve(REMOTE_CHECKPOINT_FILE);
            try {
                return Files.isRegularFile(checkpoint) && Files.size(checkpoint) > 0L;
            } catch (IOException ignored) {
                return false;
            }
        }), 90_000L, () -> "All brokers must have a non-empty remote checkpoint");
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static void produce(
        KafkaProducer<String, String> producer,
        int[] nextSequence,
        int records
    ) throws Exception {
        List<PendingSend> sends = new ArrayList<>(records);
        for (int index = 0; index < records; index++) {
            int sequence = nextSequence[0]++;
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(
                TOPIC,
                0,
                key(sequence),
                value(sequence)
            ));
            sends.add(new PendingSend(sequence, future));
        }
        producer.flush();
        for (PendingSend send : sends) {
            RecordMetadata metadata = send.future().get(30, TimeUnit.SECONDS);
            assertEquals(0, metadata.partition());
            assertEquals(send.sequence(), metadata.offset(),
                "Acknowledged offsets must remain gap-free across checkpoint-only loss");
        }
    }

    private static void consumeAndAssert(String bootstrapServers, int expectedEndOffset) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-remote-checkpoint-loss-consumer-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        TopicPartition partition = new TopicPartition(TOPIC, 0);
        int nextExpected = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (nextExpected < expectedEndOffset && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    assertTrue(nextExpected < expectedEndOffset,
                        "Consumer returned duplicate or unexpected history after checkpoint-only loss");
                    assertEquals(nextExpected, record.offset(),
                        "Remote prefix and surviving WAL tail must form one gap-free logical log");
                    assertEquals(key(nextExpected), record.key());
                    assertEquals(value(nextExpected), record.value());
                    nextExpected++;
                }
            }
        }
        assertEquals(expectedEndOffset, nextExpected,
            "Timed out reading the complete remote prefix plus surviving WAL tail");
    }

    private static TopicDescription waitForTopicState(Admin admin, int expectedIsrSize) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = admin.describeTopics(List.of(TOPIC))
                    .allTopicNames().get(10, TimeUnit.SECONDS).get(TOPIC);
                if (description == null || description.partitions().size() != PARTITIONS) {
                    return false;
                }
                var partition = description.partitions().get(0);
                boolean valid = partition.leader() != null && partition.leader().id() >= 0 &&
                    partition.replicas().size() == BROKERS && partition.isr().size() == expectedIsrSize;
                if (valid) {
                    ready[0] = description;
                }
                return valid;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Checkpoint-loss topic did not converge to ISR=" + expectedIsrSize);
        return ready[0];
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        TopicDescription description,
        long expectedEndOffset
    ) throws Exception {
        TestUtils.waitForCondition(
            () -> remoteCoverageCovers(bootstrapServers, description, expectedEndOffset),
            120_000L,
            () -> "Authoritative S3 coverage did not reach offset " + expectedEndOffset
        );
    }

    private static boolean remoteCoverageCovers(
        String bootstrapServers,
        TopicDescription description,
        long expectedEndOffset
    ) {
        try {
            SharedPartitionId partition = sharedPartitionId(description.topicId(), 0);
            PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
            for (SharedObjectMetadata object : committedObjects(bootstrapServers)) {
                object.ranges().stream()
                    .filter(range -> range.partition().equals(partition))
                    .forEach(range -> coverage.add(range.offsets()));
            }
            return coverage.covers(new OffsetRange(0L, expectedEndOffset));
        } catch (RuntimeException ignored) {
            return false;
        }
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-remote-checkpoint-loss-metadata-" + UUID.randomUUID());
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
                    SharedMetadataRecordCodec.MetadataKey metadataKey =
                        SharedMetadataRecordCodec.decodeKey(record.key());
                    if (metadataKey.type() != SharedMetadataRecordCodec.KeyType.OBJECT) {
                        continue;
                    }
                    if (record.value() == null) {
                        latestCommitted.remove(metadataKey.id());
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataValue metadataValue =
                        SharedMetadataRecordCodec.decodeValue(metadataKey, record.value());
                    if (metadataValue instanceof SharedMetadataRecordCodec.CommittedObjectValue committed) {
                        latestCommitted.put(metadataKey.id(), committed.metadata());
                    } else {
                        latestCommitted.remove(metadataKey.id());
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

    private static String key(int sequence) {
        return "checkpoint-loss-key-" + sequence;
    }

    private static String value(int sequence) {
        String prefix = "checkpoint-loss-value-" + sequence + "-";
        return prefix + "R".repeat(VALUE_BYTES - prefix.length());
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record PendingSend(int sequence, Future<RecordMetadata> future) {
    }
}
