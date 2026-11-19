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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.common.SharedStorageVersion;
import org.apache.kafka.storage.internals.shared.kafka.SharedStorageExtension;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.OutputStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@Tag("integration")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
public class DiskPressureGuardIntegrationTest {
    @Test
    public void brokerConfigChangesBlockProducesButAllowReadsAndOffsetCommits() throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder().setNumBrokerNodes(1).setNumControllerNodes(1).build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp("offsets.topic.replication.factor", (short) 1).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            try (Admin admin = cluster.admin()) {
                String topic = "disk-pressure";
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
                Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                producerProps.put(ProducerConfig.RETRIES_CONFIG, "0");
                Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "disk-pressure-group");
                consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
                     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    producer.send(new ProducerRecord<>(topic, "before")).get(30, TimeUnit.SECONDS);
                    int brokerId = cluster.brokers().keySet().iterator().next();
                    setThreshold(admin, brokerId, "0.01");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(brokerId).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 0.01,
                        30_000, "Dynamic disk threshold was not applied");
                    ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> producer.send(new ProducerRecord<>(topic, "blocked")).get(30, TimeUnit.SECONDS));
                    assertInstanceOf(KafkaStorageException.class, failure.getCause());
                    TopicPartition partition = new TopicPartition(topic, 0);
                    consumer.assign(List.of(partition));
                    consumer.seekToBeginning(List.of(partition));
                    TestUtils.waitForCondition(() -> !consumer.poll(Duration.ofMillis(100)).isEmpty(),
                        30_000, "Previously acknowledged records were not readable");
                    consumer.commitSync(Duration.ofSeconds(30));
                    cluster.brokers().get(brokerId).shutdown();
                    cluster.brokers().get(brokerId).startup();
                    cluster.waitForReadyBrokers();
                    TestUtils.waitForCondition(() -> cluster.brokers().get(brokerId).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 0.01,
                        30_000, "Dynamic disk threshold did not survive broker restart");
                    ExecutionException afterRestart = assertThrows(ExecutionException.class,
                        () -> producer.send(new ProducerRecord<>(topic, "blocked-after-restart")).get(30, TimeUnit.SECONDS));
                    assertInstanceOf(KafkaStorageException.class, afterRestart.getCause());
                    setThreshold(admin, brokerId, "100");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(brokerId).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 100.0,
                        30_000, "Disabling disk protection was not applied");
                    assertEquals(1, producer.send(new ProducerRecord<>(topic, "after")).get(30, TimeUnit.SECONDS).offset());
                }
            }
        }
    }

    @Test
    public void sharedTopicRejectsAndRecoversAfterDynamicThresholdChange() throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder().setNumBrokerNodes(1).setNumControllerNodes(1).build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp("storage.extension.class", SharedStorageExtension.class.getName())
                .setConfigProp("shared.storage.wal.capacity.bytes", 8L * 1024 * 1024).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            try (Admin admin = cluster.admin()) {
                admin.updateFeatures(Map.of(SharedStorageVersion.FEATURE_NAME,
                    new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE))).all().get(30, TimeUnit.SECONDS);
                String topic = "shared-disk-pressure";
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)
                    .configs(Map.of("shared.storage.enable", "true")))).all().get(30, TimeUnit.SECONDS);
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                props.put(ProducerConfig.RETRIES_CONFIG, "0");
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    producer.send(new ProducerRecord<>(topic, "before")).get(30, TimeUnit.SECONDS);
                    int brokerId = cluster.brokers().keySet().iterator().next();
                    setThreshold(admin, brokerId, "0.01");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(brokerId).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 0.01,
                        30_000, "Dynamic disk threshold was not applied");
                    ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> producer.send(new ProducerRecord<>(topic, "blocked")).get(30, TimeUnit.SECONDS));
                    assertInstanceOf(KafkaStorageException.class, failure.getCause());
                    setThreshold(admin, brokerId, "100");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(brokerId).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 100.0,
                        30_000, "Disabling disk protection was not applied");
                    assertEquals(1, producer.send(new ProducerRecord<>(topic, "after")).get(30, TimeUnit.SECONDS).offset());
                }
            }
        }
    }

    @Test
    public void followerPressureDoesNotBlockReplicationButLeaderPressureBlocksProduce() throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder().setNumBrokerNodes(3).setNumControllerNodes(1).build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            try (Admin admin = cluster.admin()) {
                String topic = "replicated-disk-pressure";
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)
                    .configs(Map.of("min.insync.replicas", "2")))).all().get(30, TimeUnit.SECONDS);
                int leader = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS)
                    .get(topic).partitions().get(0).leader().id();
                int follower = cluster.brokers().keySet().stream().filter(id -> id != leader).findFirst().orElseThrow();
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                props.put(ProducerConfig.RETRIES_CONFIG, "0");
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    setThreshold(admin, follower, "0.01");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(follower).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 0.01,
                        30_000, "Follower disk threshold was not applied");
                    assertEquals(0, producer.send(new ProducerRecord<>(topic, "follower-pressure"))
                        .get(30, TimeUnit.SECONDS).offset());
                    assertEquals(3, admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS)
                        .get(topic).partitions().get(0).isr().size());
                    setThreshold(admin, leader, "0.01");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(leader).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 0.01,
                        30_000, "Leader disk threshold was not applied");
                    ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> producer.send(new ProducerRecord<>(topic, "leader-pressure")).get(30, TimeUnit.SECONDS));
                    assertInstanceOf(KafkaStorageException.class, failure.getCause());
                    setThreshold(admin, leader, "100");
                    TestUtils.waitForCondition(() -> cluster.brokers().get(leader).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == 100.0,
                        30_000, "Leader disk threshold was not disabled");
                    assertEquals(1, producer.send(new ProducerRecord<>(topic, "recovered"))
                        .get(30, TimeUnit.SECONDS).offset());
                    System.out.println("REPLICA_DISK_PRESSURE_OK leader=" + leader + " follower=" + follower);
                }
            }
        }
    }

    @Test
    public void sharedWalFilesystemPressureRejectsAndRecovers() throws Exception {
        String root = System.getenv("KAFKA_TEST_WAL_PRESSURE_ROOT");
        assumeTrue(root != null && !root.isBlank(), "Dedicated WAL test filesystem not configured");
        Path walDir = Files.createTempDirectory(Path.of(root), "wal-");
        Path filler = walDir.resolve("pressure-filler");
        TestKitNodes nodes = new TestKitNodes.Builder().setNumBrokerNodes(1).setNumControllerNodes(1).build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp("storage.extension.class", SharedStorageExtension.class.getName())
                .setConfigProp("shared.storage.wal.dir", walDir.toString())
                .setConfigProp("shared.storage.wal.capacity.bytes", 8L * 1024 * 1024).build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            try (Admin admin = cluster.admin()) {
                admin.updateFeatures(Map.of(SharedStorageVersion.FEATURE_NAME,
                    new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE))).all().get(30, TimeUnit.SECONDS);
                String topic = "separate-wal-pressure";
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)
                    .configs(Map.of("shared.storage.enable", "true")))).all().get(30, TimeUnit.SECONDS);
                Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
                props.put(ProducerConfig.RETRIES_CONFIG, "0");
                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                    producer.send(new ProducerRecord<>(topic, "before")).get(30, TimeUnit.SECONDS);
                    int brokerId = cluster.brokers().keySet().iterator().next();
                    Path logPath = cluster.brokers().get(brokerId).logManager().liveLogDirs().head().toPath();
                    FileStore logStore = Files.getFileStore(logPath);
                    FileStore walStore = Files.getFileStore(walDir);
                    assertTrue(!walStore.equals(logStore), "WAL and log must use distinct filesystems");
                    double logUsage = usagePercent(logStore);
                    byte[] megabyte = new byte[1024 * 1024];
                    try (OutputStream out = Files.newOutputStream(filler)) {
                        for (int i = 0; i < 70 && usagePercent(walStore) < logUsage + 15.0; i++) {
                            out.write(megabyte);
                            out.flush();
                        }
                    }
                    double walUsage = usagePercent(walStore);
                    assertTrue(walUsage > logUsage + 5.0, "Dedicated WAL pressure did not exceed log usage");
                    double threshold = (walUsage + logUsage) / 2.0;
                    setThreshold(admin, brokerId, Double.toString(threshold));
                    TestUtils.waitForCondition(() -> cluster.brokers().get(brokerId).config()
                        .getDouble(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG) == threshold,
                        30_000, "Dynamic disk threshold was not applied");
                    ExecutionException failure = assertThrows(ExecutionException.class,
                        () -> producer.send(new ProducerRecord<>(topic, "blocked")).get(30, TimeUnit.SECONDS));
                    assertInstanceOf(KafkaStorageException.class, failure.getCause());
                    Files.delete(filler);
                    assertTrue(usagePercent(walStore) < threshold, "WAL filesystem did not recover below threshold");
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                    boolean recovered = false;
                    while (!recovered && System.nanoTime() < deadline) {
                        try {
                            assertEquals(1, producer.send(new ProducerRecord<>(topic, "recovered"))
                                .get(5, TimeUnit.SECONDS).offset());
                            recovered = true;
                        } catch (ExecutionException blocked) {
                            assertInstanceOf(KafkaStorageException.class, blocked.getCause());
                            Thread.sleep(250);
                        }
                    }
                    assertTrue(recovered, "Writes did not resume after WAL filesystem recovered");
                    System.out.println("SEPARATE_WAL_PRESSURE_OK log=" + logUsage + " wal=" + walUsage
                        + " threshold=" + threshold);
                }
            }
        } finally {
            Files.deleteIfExists(filler);
        }
    }

    private static double usagePercent(FileStore store) throws Exception {
        return 100.0 * (store.getTotalSpace() - store.getUsableSpace()) / store.getTotalSpace();
    }

    private static void setThreshold(Admin admin, int brokerId, String value) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, Integer.toString(brokerId));
        ConfigEntry entry = new ConfigEntry(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG, value);
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(entry, AlterConfigOp.OpType.SET))))
            .all().get(30, TimeUnit.SECONDS);
    }
}
