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
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRemoteCoverage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Exercises the Kafka idempotent producer when leader loss happens with acknowledged-result ambiguity.
 *
 * <p>A large first wave is submitted asynchronously without waiting for per-record futures. The test requires at
 * least one callback to still be outstanding when the current leader is stopped, then keeps the same producer
 * instance alive across leader election. Kafka retries must resolve every future exactly once, preserve partition
 * ordering, and expose a gap-free unique history through both local and remote shared-storage reads.</p>
 */
@Tag("integration")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class SharedStorageInflightIdempotentProduceE2ETest {
    private static final String TOPIC = "shared-inflight-idempotent-produce";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int INFLIGHT_WAVE_RECORDS = 400;
    private static final int POST_FAILOVER_RECORDS = 200;
    private static final int VALUE_BYTES = 64 * 1024;

    @Test
    public void pendingProduceFuturesResolveWithoutDuplicatesAcrossLeaderLoss() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-inflight-produce");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        try (KafkaClusterTestKit cluster = startCluster(s3Endpoint, region, bucket)) {
            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin();
                 KafkaProducer<String, String> producer = producer(bootstrapServers)) {
                createTopic(admin);
                TopicDescription description = waitForTopicState(admin, 3, null);
                int oldLeader = description.partitions().get(0).leader().id();
                SharedPartitionId sharedPartition = sharedPartitionId(description.topicId(), 0);

                AtomicInteger callbacks = new AtomicInteger();
                AtomicInteger callbackFailures = new AtomicInteger();
                List<Future<RecordMetadata>> futures = sendWave(
                    producer,
                    0,
                    INFLIGHT_WAVE_RECORDS,
                    callbacks,
                    callbackFailures
                );
                int completedBeforeLeaderLoss = callbacks.get();
                assertTrue(completedBeforeLeaderLoss < INFLIGHT_WAVE_RECORDS,
                    "Leader must be removed while at least one produce result is still unresolved; completed=" +
                        completedBeforeLeaderLoss);

                stopBroker(cluster, oldLeader);
                int newLeader = waitForNewLeader(admin, oldLeader);
                assertNotEquals(oldLeader, newLeader);

                futures.addAll(sendWave(
                    producer,
                    INFLIGHT_WAVE_RECORDS,
                    POST_FAILOVER_RECORDS,
                    callbacks,
                    callbackFailures
                ));
                producer.flush();
                awaitOrderedMetadata(futures);
                assertEquals(0, callbackFailures.get(),
                    "Idempotent producer retries must not surface a final delivery failure after leader loss");
                assertEquals(INFLIGHT_WAVE_RECORDS + POST_FAILOVER_RECORDS, callbacks.get());

                int totalRecords = INFLIGHT_WAVE_RECORDS + POST_FAILOVER_RECORDS;
                assertExactHistory(bootstrapServers, totalRecords);
                waitForRemoteCoverage(bootstrapServers, sharedPartition, totalRecords);

                cluster.brokers().get(oldLeader).startup();
                cluster.waitForReadyBrokers();
                waitForTopicState(admin, 3, null);
                assertExactHistory(bootstrapServers, totalRecords);
            }
        }
    }

    private static KafkaClusterTestKit startCluster(
        String s3Endpoint,
        String region,
        String bucket
    ) throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();
        KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topics", TOPIC)
            .setConfigProp("shared.storage.wal.engine", "ring")
            .setConfigProp("shared.storage.wal.capacity.bytes", 128L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", 4L * 1024 * 1024)
            .setConfigProp("shared.storage.upload.interval.ms", 100L)
            .setConfigProp("shared.storage.upload.max.linger.ms", 500L)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.key.prefix", "inflight/" + UUID.randomUUID() + "/objects")
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .setConfigProp("replica.fetch.min.bytes", 1024 * 1024)
            .setConfigProp("replica.fetch.wait.max.ms", 500)
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        return cluster;
    }

    private static KafkaProducer<String, String> producer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "shared-inflight-idempotent");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 256 * 1024);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 128L * 1024 * 1024);
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024);
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 15_000);
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static void createTopic(Admin admin) throws Exception {
        admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3)
            .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
            .all().get(30, TimeUnit.SECONDS);
    }

    private static List<Future<RecordMetadata>> sendWave(
        KafkaProducer<String, String> producer,
        int startSequence,
        int count,
        AtomicInteger callbacks,
        AtomicInteger callbackFailures
    ) {
        List<Future<RecordMetadata>> futures = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int sequence = startSequence + i;
            futures.add(producer.send(
                new ProducerRecord<>(TOPIC, 0, key(sequence), value(sequence)),
                (metadata, exception) -> {
                    if (exception != null) {
                        callbackFailures.incrementAndGet();
                    }
                    callbacks.incrementAndGet();
                }
            ));
        }
        return futures;
    }

    private static void awaitOrderedMetadata(List<Future<RecordMetadata>> futures) throws Exception {
        for (int sequence = 0; sequence < futures.size(); sequence++) {
            RecordMetadata metadata = futures.get(sequence).get(120, TimeUnit.SECONDS);
            assertEquals(0, metadata.partition());
            assertEquals(sequence, metadata.offset(),
                "One idempotent producer must retain strict partition order across ambiguous retries");
        }
    }

    private static void stopBroker(KafkaClusterTestKit cluster, int brokerId) throws Exception {
        BrokerServer broker = cluster.brokers().get(brokerId);
        broker.shutdown();
        broker.awaitShutdown();
    }

    private static int waitForNewLeader(Admin admin, int oldLeader) throws Exception {
        int[] result = {-1};
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describe(admin);
                var partition = description.partitions().get(0);
                int candidate = partition.leader().id();
                if (candidate >= 0 && candidate != oldLeader && partition.isr().size() == 2) {
                    result[0] = candidate;
                    return true;
                }
                return false;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "No replacement leader after stopping broker " + oldLeader);
        return result[0];
    }

    private static TopicDescription waitForTopicState(
        Admin admin,
        int expectedIsr,
        Integer excludedLeader
    ) throws Exception {
        TopicDescription[] result = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describe(admin);
                var partition = description.partitions().get(0);
                boolean ready = partition.leader() != null && partition.leader().id() >= 0 &&
                    partition.replicas().size() == 3 && partition.isr().size() == expectedIsr &&
                    (excludedLeader == null || partition.leader().id() != excludedLeader);
                if (ready) {
                    result[0] = description;
                }
                return ready;
            } catch (Exception ignored) {
                return false;
            }
        }, 90_000L, () -> "Topic did not converge to expected ISR=" + expectedIsr);
        return result[0];
    }

    private static TopicDescription describe(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames()
            .get(10, TimeUnit.SECONDS).get(TOPIC);
    }

    private static void assertExactHistory(String bootstrapServers, int expectedRecords) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-inflight-read-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        TopicPartition partition = new TopicPartition(TOPIC, 0);
        int received = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (received < expectedRecords && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    assertEquals(received, record.offset(), "Ambiguous retry must not create offset gaps or duplicates");
                    assertEquals(key(received), record.key());
                    assertEquals(value(received), record.value());
                    received++;
                }
            }
        }
        assertEquals(expectedRecords, received, "Timed out reading complete idempotent-producer history");
    }

    private static void waitForRemoteCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        long endOffset
    ) throws Exception {
        TestUtils.waitForCondition(() -> {
            PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
            for (SharedObjectMetadata metadata : committedObjects(bootstrapServers)) {
                metadata.ranges().stream()
                    .filter(range -> range.partition().equals(partition))
                    .forEach(range -> coverage.add(range.offsets()));
            }
            return coverage.covers(new OffsetRange(0L, endOffset));
        }, 180_000L, () -> "Remote coverage did not catch up after in-flight producer failover");
    }

    private static List<SharedObjectMetadata> committedObjects(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-inflight-metadata-" + UUID.randomUUID());
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
                    SharedMetadataRecordCodec.MetadataValue value =
                        SharedMetadataRecordCodec.decodeValue(metadataKey, record.value());
                    if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committed) {
                        latestCommitted.put(metadataKey.id(), committed.metadata());
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

    private static String key(int sequence) {
        return "inflight-key-" + sequence;
    }

    private static String value(int sequence) {
        String prefix = "inflight-value-" + sequence + "-";
        return prefix + "I".repeat(VALUE_BYTES - prefix.length());
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
