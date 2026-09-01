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

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves that object-store availability is outside the producer durability and broker failover paths.
 *
 * <p>After warmup reaches S3, MinIO is stopped. An RF=3/minISR=2 partition must continue accepting
 * {@code acks=all} writes into the replicated WAL. The current leader is then stopped while S3 is still unavailable;
 * the surviving replicas must elect a new leader, retain all acknowledged records, and continue accepting writes.
 * Once MinIO returns, the new leader must publish the complete outage-period WAL tail.</p>
 */
@Tag("integration")
@Timeout(value = 6, unit = TimeUnit.MINUTES)
public class SharedStorageS3OutageKRaftIntegrationTest {
    private static final String TOPIC = "shared-wal-s3-outage";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final String MINIO_CONTAINER_ENV = "SHARED_STORAGE_S3_CONTAINER";
    private static final int WARMUP_RECORDS = 20;
    private static final int OUTAGE_RECORDS = 40;
    private static final int POST_FAILOVER_RECORDS = 20;
    private static final int TOTAL_RECORDS = WARMUP_RECORDS + OUTAGE_RECORDS + POST_FAILOVER_RECORDS;
    private static final long UPLOAD_INTERVAL_MS = 1_000L;

    @Test
    public void replicatedWalStaysAvailableThroughS3OutageAndLeaderFailover() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        String minioContainer = System.getenv(MINIO_CONTAINER_ENV);
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        assumeTrue(minioContainer != null && !minioContainer.isBlank(), "MinIO container control is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-s3-outage");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");
        String keyPrefix = "s3-outage/" + UUID.randomUUID() + "/objects";
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
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", 64L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", 1024L * 1024)
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
            .build()) {

            try {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                String bootstrapServers = cluster.bootstrapServers();

                try (Admin admin = cluster.admin();
                     KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                    admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3)
                        .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                        .all().get(30, TimeUnit.SECONDS);

                    TopicDescription topic = waitForTopicReady(admin);
                    SharedPartitionId partition = sharedPartitionId(topic.topicId(), 0);
                    int oldLeader = topic.partitions().get(0).leader().id();

                    OffsetRange warmup = produceRange(producer, 0, WARMUP_RECORDS);
                    TestUtils.waitForCondition(
                        () -> brokerCoverage(bootstrapServers, partition, oldLeader).covers(warmup),
                        90_000L,
                        () -> "Old leader " + oldLeader + " never remotely committed warmup " + warmup
                    );

                    stopContainer(minioContainer);
                    minioStopped = true;
                    waitForMinioState(s3Endpoint, false);
                    System.out.println("S3_OUTAGE_STARTED oldLeader=" + oldLeader + " endpoint=" + s3Endpoint);

                    OffsetRange outageRange = produceRange(producer, WARMUP_RECORDS, OUTAGE_RECORDS);
                    assertEquals(new OffsetRange(WARMUP_RECORDS, WARMUP_RECORDS + OUTAGE_RECORDS), outageRange);
                    Thread.sleep(2L * UPLOAD_INTERVAL_MS);
                    assertFalse(
                        allCoverage(bootstrapServers, partition).covers(outageRange),
                        "S3-unavailable WAL tail must not be represented as authoritative remote coverage"
                    );
                    System.out.println("S3_OUTAGE_ACKED range=" + outageRange + " remoteRequired=false");

                    cluster.brokers().get(oldLeader).shutdown();
                    int newLeader = waitForNewLeader(admin, oldLeader);
                    assertNotEquals(oldLeader, newLeader);

                    OffsetRange postFailover = produceRange(
                        producer,
                        WARMUP_RECORDS + OUTAGE_RECORDS,
                        POST_FAILOVER_RECORDS
                    );
                    assertEquals(
                        new OffsetRange(WARMUP_RECORDS + OUTAGE_RECORDS, TOTAL_RECORDS),
                        postFailover
                    );
                    assertFalse(
                        allCoverage(bootstrapServers, partition).covers(postFailover),
                        "Writes acknowledged while S3 is down must remain WAL-only until recovery"
                    );
                    assertExpectedValues(consumeAssigned(bootstrapServers, TOTAL_RECORDS));
                    System.out.println("S3_OUTAGE_FAILOVER oldLeader=" + oldLeader +
                        " newLeader=" + newLeader + " postRange=" + postFailover);

                    startContainer(minioContainer);
                    minioStopped = false;
                    waitForMinioState(s3Endpoint, true);

                    OffsetRange outageTail = new OffsetRange(WARMUP_RECORDS, TOTAL_RECORDS);
                    TestUtils.waitForCondition(
                        () -> brokerCoverage(bootstrapServers, partition, newLeader).covers(outageTail),
                        120_000L,
                        () -> "New leader " + newLeader +
                            " never published the S3-outage WAL tail " + outageTail
                    );
                    assertExpectedValues(consumeAssigned(bootstrapServers, TOTAL_RECORDS));
                    System.out.println("S3_OUTAGE_RECOVERED newLeader=" + newLeader +
                        " remoteRange=" + outageTail + " records=" + TOTAL_RECORDS);
                }
            } finally {
                if (minioStopped) {
                    startContainerIgnoringFailure(minioContainer);
                    waitForMinioStateIgnoringFailure(s3Endpoint, true);
                }
            }
        }
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static OffsetRange produceRange(
        KafkaProducer<String, String> producer,
        int valueStart,
        int count
    ) throws Exception {
        long firstOffset = -1L;
        long lastOffset = -1L;
        for (int i = 0; i < count; i++) {
            int sequence = valueStart + i;
            RecordMetadata metadata = producer.send(
                new ProducerRecord<>(TOPIC, 0, Integer.toString(sequence), value(sequence))
            ).get(30, TimeUnit.SECONDS);
            if (firstOffset < 0) {
                firstOffset = metadata.offset();
            }
            lastOffset = metadata.offset();
        }
        producer.flush();
        return new OffsetRange(firstOffset, Math.addExact(lastOffset, 1L));
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
        }, 60_000L, () -> "S3-outage topic never reached leader + RF3/ISR3 readiness");
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
                var partition = topic.partitions().get(0);
                int leader = partition.leader().id();
                if (leader >= 0 && leader != oldLeader && partition.isr().size() >= 2) {
                    result[0] = leader;
                    return true;
                }
                return false;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "No writable replacement leader was elected after stopping broker " + oldLeader);
        return result[0];
    }

    private static TopicDescription describeTopic(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
    }

    private static PartitionRemoteCoverage allCoverage(
        String bootstrapServers,
        SharedPartitionId partition
    ) {
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
        Integer objectBrokerId
    ) {
        PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
        for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
            if (objectBrokerId != null && BrokerObjectId.brokerId(metadata.objectId()) != objectBrokerId) {
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

    private static List<String> consumeAssigned(String bootstrapServers, int expectedCount) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        List<String> values = new ArrayList<>(expectedCount);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (values.size() < expectedCount && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(250)).forEach(record -> values.add(record.value()));
            }
        }
        return values;
    }

    private static void assertExpectedValues(List<String> actual) {
        List<String> expected = new ArrayList<>(TOTAL_RECORDS);
        for (int i = 0; i < TOTAL_RECORDS; i++) {
            expected.add(value(i));
        }
        assertEquals(expected, actual, "All acknowledged records must survive S3 outage and leader failover");
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

    private static String value(int sequence) {
        return "value-" + sequence;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
