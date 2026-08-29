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
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNode;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
 * End-to-end proof of the shared-storage {@code acks=1} durability boundary.
 *
 * <p>After a RF=3 partition is fully in-sync, both followers are stopped and the test waits until Kafka reports an
 * ISR containing only the leader. The topic uses {@code min.insync.replicas=3}, so replica quorum is deliberately not
 * available. A producer configured with {@code acks=1} must still complete after the leader's broker-wide WAL becomes
 * durable. The stopped follower WALs must not advance and no authoritative S3 object may be published before the ACK.
 * This preserves Kafka's native {@code acks=1} semantics rather than silently upgrading it to quorum durability.</p>
 */
@Tag("integration")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
public class SharedStorageAcksOneKRaftIntegrationTest {
    private static final String TOPIC = "shared-wal-acks-one";
    private static final String METADATA_TOPIC = "__shared_storage_metadata";
    private static final int RECORDS = 8;
    private static final long UPLOAD_INTERVAL_MS = 10 * 60 * 1000L;

    @Test
    public void acksOneCompletesWithOnlyLeaderWalDurable() throws Exception {
        String s3Endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(s3Endpoint != null && !s3Endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-e2e");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();

        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp("storage.extension.class",
                "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
            .setConfigProp("shared.storage.topics", TOPIC)
            .setConfigProp("shared.storage.wal.capacity.bytes", 64L * 1024 * 1024)
            .setConfigProp("shared.storage.wal.segment.bytes", 4L * 1024 * 1024)
            .setConfigProp("shared.storage.object.target.bytes", 1024L * 1024)
            .setConfigProp("shared.storage.upload.interval.ms", UPLOAD_INTERVAL_MS)
            .setConfigProp("shared.storage.metadata.replication.factor", 3)
            .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
            .setConfigProp("shared.storage.s3.endpoint", s3Endpoint)
            .setConfigProp("shared.storage.s3.region", region)
            .setConfigProp("shared.storage.s3.bucket", bucket)
            .setConfigProp("shared.storage.s3.path.style", true)
            .setConfigProp("shared.storage.s3.io.threads", 2)
            .build()) {

            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();

            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin()) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 3)
                    .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"))))
                    .all().get(30, TimeUnit.SECONDS);

                TopicDescription readyTopic = waitForIsr(admin, 3, -1);
                var partitionInfo = readyTopic.partitions().get(0);
                int leaderId = partitionInfo.leader().id();
                SharedPartitionId partition = sharedPartitionId(readyTopic.topicId(), 0);
                List<Integer> followerIds = partitionInfo.replicas().stream()
                    .map(node -> node.id())
                    .filter(id -> id != leaderId)
                    .toList();
                assertEquals(2, followerIds.size());

                for (int followerId : followerIds) {
                    cluster.brokers().get(followerId).shutdown();
                    cluster.brokers().get(followerId).awaitShutdown();
                }
                waitForIsr(admin, 1, leaderId);

                Path leaderWal = walDir(cluster, leaderId);
                Map<Integer, Long> stoppedFollowerWalBytes = Map.of(
                    followerIds.get(0), walPayloadBytes(walDir(cluster, followerIds.get(0))),
                    followerIds.get(1), walPayloadBytes(walDir(cluster, followerIds.get(1)))
                );
                long leaderWalBefore = walPayloadBytes(leaderWal);

                OffsetRange acknowledged;
                try (KafkaProducer<String, String> producer = acksOneProducer(bootstrapServers)) {
                    acknowledged = produceRange(producer);
                }
                assertEquals(new OffsetRange(0L, RECORDS), acknowledged);

                TestUtils.waitForCondition(
                    () -> walPayloadBytes(leaderWal) > leaderWalBefore,
                    30_000L,
                    () -> "Leader broker " + leaderId + " did not durably append the acks=1 records to shared WAL"
                );
                for (int followerId : followerIds) {
                    assertEquals(
                        stoppedFollowerWalBytes.get(followerId).longValue(),
                        walPayloadBytes(walDir(cluster, followerId)),
                        "Stopped follower WAL must not advance for an acks=1 acknowledgement"
                    );
                }

                assertFalse(
                    hasCommittedCoverage(bootstrapServers, partition, acknowledged),
                    "acks=1 must not depend on authoritative S3 publication"
                );
                assertTrue(
                    describeTopic(admin).partitions().get(0).isr().size() == 1,
                    "The acknowledgement must be observed while Kafka still has only one ISR replica"
                );
            }
        }
    }

    private static KafkaProducer<String, String> acksOneProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(properties);
    }

    private static OffsetRange produceRange(KafkaProducer<String, String> producer) throws Exception {
        long firstOffset = -1L;
        long lastOffset = -1L;
        for (int i = 0; i < RECORDS; i++) {
            RecordMetadata metadata = producer.send(
                new ProducerRecord<>(TOPIC, 0, Integer.toString(i), "value-" + i)
            ).get(30, TimeUnit.SECONDS);
            if (firstOffset < 0) {
                firstOffset = metadata.offset();
            }
            lastOffset = metadata.offset();
        }
        producer.flush();
        return new OffsetRange(firstOffset, Math.addExact(lastOffset, 1L));
    }

    private static TopicDescription waitForIsr(Admin admin, int expectedIsr, int expectedLeader) throws Exception {
        TopicDescription[] ready = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription description = describeTopic(admin);
                if (description == null || description.partitions().size() != 1) {
                    return false;
                }
                var partition = description.partitions().get(0);
                if (partition.leader() == null || partition.leader().id() < 0 ||
                    partition.replicas().size() != 3 || partition.isr().size() != expectedIsr) {
                    return false;
                }
                if (expectedLeader >= 0 && partition.leader().id() != expectedLeader) {
                    return false;
                }
                ready[0] = description;
                return true;
            } catch (Exception ignored) {
                return false;
            }
        }, 60_000L, () -> "Topic " + TOPIC + " did not converge to ISR size " + expectedIsr);
        return ready[0];
    }

    private static TopicDescription describeTopic(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
    }

    private static Path walDir(KafkaClusterTestKit cluster, int brokerId) {
        TestKitNode broker = cluster.nodes().brokerNodes().get(brokerId);
        Path dataDir = Path.of(broker.logDataDirectories().iterator().next())
            .toAbsolutePath()
            .normalize();
        return dataDir
            .resolveSibling(dataDir.getFileName() + ".shared-storage")
            .resolve("broker-" + brokerId)
            .resolve("wal");
    }

    private static long walPayloadBytes(Path walDir) {
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

    private static boolean hasCommittedCoverage(
        String bootstrapServers,
        SharedPartitionId partition,
        OffsetRange expected
    ) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "shared-storage-acks-one-metadata-" + UUID.randomUUID());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        TopicPartition metadataPartition = new TopicPartition(METADATA_TOPIC, 0);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(List.of(metadataPartition));
            consumer.seekToBeginning(List.of(metadataPartition));
            long endOffset = consumer.endOffsets(List.of(metadataPartition)).get(metadataPartition);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (consumer.position(metadataPartition) < endOffset && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.key() == null || record.value() == null) {
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataKey key = SharedMetadataRecordCodec.decodeKey(record.key());
                    if (key.type() != SharedMetadataRecordCodec.KeyType.OBJECT) {
                        continue;
                    }
                    SharedMetadataRecordCodec.MetadataValue value =
                        SharedMetadataRecordCodec.decodeValue(key, record.value());
                    if (value instanceof SharedMetadataRecordCodec.CommittedObjectValue committed &&
                        committed.metadata().ranges().stream().anyMatch(range ->
                            range.partition().equals(partition) && range.offsets().equals(expected))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition
        );
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
