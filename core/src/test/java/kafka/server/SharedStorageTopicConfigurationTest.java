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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidUpdateVersionException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.BrokerLifecycleManager;
import org.apache.kafka.server.common.SharedStorageVersion;
import org.apache.kafka.storage.internals.log.KafkaStorageExtension;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.kafka.SharedStorageExtension;
import org.apache.kafka.storage.internals.shared.kafka.SharedUnifiedLog;
import org.apache.kafka.test.TestUtils;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the real Admin/controller/broker boundary without S3 or a legacy topic selector. */
@Tag("integration")
@Timeout(value = 4, unit = TimeUnit.MINUTES)
public class SharedStorageTopicConfigurationTest {
    private static final String MODE = TopicConfig.SHARED_STORAGE_ENABLE_CONFIG;

    @Test
    public void ordinaryKafkaRemainsClassicAndCannotActivateWithoutExtension() throws Exception {
        try (KafkaClusterTestKit cluster = startCluster(false);
             Admin admin = cluster.admin()) {
            createTopic(admin, "ordinary-default", Map.of());
            assertLogMode(cluster, "ordinary-default", false);
            roundTrip(cluster.bootstrapServers(), "ordinary-default", "classic");

            ExecutionException createFailure = assertThrows(ExecutionException.class,
                () -> createTopic(admin, "unavailable-shared", Map.of(MODE, "true")));
            assertInstanceOf(InvalidConfigurationException.class, createFailure.getCause());
            ExecutionException explicitFalse = assertThrows(ExecutionException.class,
                () -> createTopic(admin, "unavailable-explicit", Map.of(MODE, "false")));
            assertInstanceOf(InvalidConfigurationException.class, explicitFalse.getCause());
            ExecutionException activationFailure = assertThrows(ExecutionException.class,
                () -> activateTopicStorage(admin));
            assertInstanceOf(InvalidUpdateVersionException.class, activationFailure.getCause());
            assertFalse(admin.listTopics().names().get(30, TimeUnit.SECONDS).contains("unavailable-shared"));
        }
    }

    @Test
    public void legacyDefaultAllRoutingCannotActivateTopicMode() throws Exception {
        try (KafkaClusterTestKit cluster = startCluster(true, true);
             Admin admin = cluster.admin()) {
            createTopic(admin, "legacy-default-all", Map.of());
            assertLogMode(cluster, "legacy-default-all", true);
            roundTrip(cluster.bootstrapServers(), "legacy-default-all", "legacy");
            ExecutionException activationFailure = assertThrows(ExecutionException.class,
                () -> activateTopicStorage(admin));
            assertInstanceOf(InvalidUpdateVersionException.class, activationFailure.getCause());
            assertModeChangeRejected(admin, "legacy-default-all", AlterConfigOp.OpType.SET, "true");
            assertModeChangeRejected(admin, "legacy-default-all", AlterConfigOp.OpType.SET, "false");
        }
    }

    @Test
    public void missingExtensionBrokerCannotJoinActivatedTopicMode() throws Exception {
        assertIncompatibleBrokerCannotJoin(Map.of());
    }

    @Test
    public void legacySelectorBrokerCannotJoinActivatedTopicMode() throws Exception {
        assertIncompatibleBrokerCannotJoin(Map.of(
            "storage.extension.class", SharedStorageExtension.class.getName(),
            "shared.storage.wal.capacity.bytes", "8388608",
            "shared.storage.legacy.selectors.enabled", "true"));
    }

    @Test
    public void incompatibleExtensionBrokerCannotJoinActivatedTopicMode() throws Exception {
        // This exercises an extension without the new capability, not a historical broker binary.
        assertIncompatibleBrokerCannotJoin(Map.of(
            "storage.extension.class", UnsupportedTopicStorageExtension.class.getName()));
    }

    private static void assertIncompatibleBrokerCannotJoin(Map<String, String> extensionConfigs) throws Exception {
        Map<String, String> joiningConfigs = new HashMap<>(extensionConfigs);
        joiningConfigs.put(KRaftConfigs.INITIAL_BROKER_REGISTRATION_TIMEOUT_MS_CONFIG, "10000");
        joiningConfigs.put(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, "60000");
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(2).setNumControllerNodes(1)
            .setPerServerProperties(Map.of(
                0, Map.of("storage.extension.class", SharedStorageExtension.class.getName(),
                    "shared.storage.wal.capacity.bytes", "8388608"),
                1, joiningConfigs))
            .build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).build()) {
            cluster.format();
            for (ControllerServer controller : cluster.controllers().values()) {
                controller.startup();
            }
            BrokerServer ready = cluster.brokers().get(0);
            BrokerServer joining = cluster.brokers().get(1);
            ready.startup();
            cluster.waitForActiveController().waitForReadyBrokers(1).get(30, TimeUnit.SECONDS);
            String bootstrapServers = "localhost:" + ready.boundPort(nodes.brokerListenerName());
            // The second broker has never registered. It is intentionally absent from activation's support check.
            try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
                activateTopicStorage(admin);
                String topic = "shared-before-incompatible-join";
                TopicPartition partition = new TopicPartition(topic, 0);
                createTopic(admin, topic, Map.of(MODE, "true"));
                roundTrip(bootstrapServers, topic, "shared");
                assertInstanceOf(SharedUnifiedLog.class, ready.logManager().getLog(partition, false).get());

                try (LogCaptureAppender appender = LogCaptureAppender.createAndRegister(BrokerLifecycleManager.class)) {
                    appender.setClassLogger(BrokerLifecycleManager.class, Level.INFO);
                    RuntimeException failure = assertThrows(RuntimeException.class, joining::startup);
                    assertInstanceOf(CancellationException.class, failure.getCause());
                    assertTrue(appender.getMessages().stream().anyMatch(message -> message.contains(
                        "Unable to register broker 1 because the controller returned error UNSUPPORTED_VERSION")),
                        "Startup must observe a real incompatible-version registration response, not only a timeout");
                    assertTrue(appender.getMessages().stream().anyMatch(message -> message.contains(
                        "Shutting down because we were unable to register with the controller quorum.")),
                        "The broker must reach its initial registration deadline and terminate startup");
                }

                // Check final failure, not a broker that is merely still waiting to register.
                assertTrue(joining.isShutdown());
                assertEquals(-1L, joining.lifecycleManager().brokerEpoch());
                assertTrue(joining.lifecycleManager().initialCatchUpFuture().isCompletedExceptionally());
                assertFalse(joining.brokerFeatures().supportedFeatures().features().containsKey(SharedStorageVersion.FEATURE_NAME));
                assertFalse(joining.logManager().getLog(partition, false).isDefined());
                for (String directory : nodes.brokerNodes().get(1).logDataDirectories()) {
                    assertFalse(Files.exists(Path.of(directory, topic + "-0")), "Rejected broker created a Topic log");
                }
                assertEquals(List.of(0), admin.describeCluster().nodes().get(30, TimeUnit.SECONDS)
                    .stream().map(node -> node.id()).sorted().toList());

                ExecutionException reassignment = assertThrows(ExecutionException.class,
                    () -> admin.alterPartitionReassignments(Map.of(partition, Optional.of(
                        new NewPartitionReassignment(List.of(0, 1)))))
                        .all().get(30, TimeUnit.SECONDS));
                assertInstanceOf(InvalidReplicaAssignmentException.class, reassignment.getCause());
                var description = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(30, TimeUnit.SECONDS).get(topic).partitions().get(0);
                assertEquals(List.of(0), description.replicas().stream().map(node -> node.id()).toList());
                assertEquals(List.of(0), description.isr().stream().map(node -> node.id()).toList());
                assertValues(bootstrapServers, topic, List.of("shared-0", "shared-1", "shared-2"));
                assertInstanceOf(SharedUnifiedLog.class, ready.logManager().getLog(partition, false).get());
            }
        }
    }

    /** A loadable pre-capability extension fixture; it deliberately inherits capability=false. */
    public static final class UnsupportedTopicStorageExtension implements KafkaStorageExtension {
        @Override
        public void start(StorageExtensionContext context) {
        }

        @Override
        public UnifiedLogFactory unifiedLogFactory() {
            return UnifiedLogFactory.DEFAULT;
        }

        @Override
        public void close() {
        }
    }

    @Test
    public void offlineReplicaDiscardsOldModeBeforeRecreatedTopicAssignment() throws Exception {
        String topic = "offline-recreated-topic";
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaClusterTestKit cluster = startCluster(true, false, 3);
             Admin admin = cluster.admin()) {
            activateTopicStorage(admin);
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)
                .configs(Map.of(MODE, "true", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                .all().get(30, TimeUnit.SECONDS);
            assertLogMode(cluster, topic, true);
            roundTrip(cluster.bootstrapServers(), topic, "old-shared");
            Uuid oldId = admin.describeTopics(List.of(topic)).allTopicNames()
                .get(30, TimeUnit.SECONDS).get(topic).topicId();
            Map.Entry<Integer, BrokerServer> stopped = cluster.brokers().entrySet().iterator().next();
            stopped.getValue().shutdown();
            TestUtils.waitForCondition(() -> {
                var description = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topic).partitions().get(0);
                return description.leader() != null && description.leader().id() != stopped.getKey()
                    && description.isr().size() == 2;
            }, 60_000L, "Old Topic did not converge after stopping one replica");

            admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            TestUtils.waitForCondition(() -> !admin.listTopics().names().get(10, TimeUnit.SECONDS)
                .contains(topic), 30_000L, "Deleted Topic remained visible");
            List<Integer> activeReplicas = cluster.brokers().keySet().stream()
                .filter(id -> !id.equals(stopped.getKey())).toList();
            var recreated = admin.createTopics(List.of(new NewTopic(topic, Map.of(0, activeReplicas))));
            recreated.all().get(30, TimeUnit.SECONDS);
            Uuid newId = recreated.topicId(topic).get(30, TimeUnit.SECONDS);
            assertNotEquals(oldId, newId);
            awaitTopicId(admin, topic, newId);
            roundTrip(cluster.bootstrapServers(), topic, "new-classic");

            // This broker still has the old shared directory and missed both metadata operations.
            // Its startup must identify that stale Topic ID before applying the new name's classic mode.
            stopped.getValue().startup();
            cluster.waitForReadyBrokers();
            admin.alterPartitionReassignments(Map.of(partition, Optional.of(
                new NewPartitionReassignment(new ArrayList<>(cluster.brokers().keySet())))))
                .all().get(30, TimeUnit.SECONDS);
            assertLogMode(cluster, topic, false);
            TestUtils.waitForCondition(() -> admin.describeTopics(List.of(topic)).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(topic).partitions().get(0).isr().size() == 3,
                60_000L, "Recreated classic Topic did not regain all three replicas");
            assertValues(cluster.bootstrapServers(), topic,
                List.of("new-classic-0", "new-classic-1", "new-classic-2"));
        }
    }

    @Test
    public void explicitTopicModeSurvivesRestartAndDoesNotLeakAcrossRecreation() throws Exception {
        try (KafkaClusterTestKit cluster = startCluster(true);
             Admin admin = cluster.admin()) {
            createTopic(admin, "before-activation", Map.of());
            assertLogMode(cluster, "before-activation", false);
            ExecutionException beforeActivation = assertThrows(ExecutionException.class,
                () -> createTopic(admin, "too-early", Map.of(MODE, "true")));
            assertInstanceOf(InvalidConfigurationException.class, beforeActivation.getCause());

            activateTopicStorage(admin);
            createTopic(admin, "ordinary-after-activation", Map.of());
            createTopic(admin, "selected-topic", Map.of(MODE, "true"));
            assertLogMode(cluster, "ordinary-after-activation", false);
            assertLogMode(cluster, "selected-topic", true);
            assertEquals("false", describeMode(admin, "ordinary-after-activation").value());
            assertEquals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
                describeMode(admin, "ordinary-after-activation").source());
            assertEquals("true", describeMode(admin, "selected-topic").value());

            assertModeChangeRejected(admin, "selected-topic", AlterConfigOp.OpType.SET, "false");
            assertModeChangeRejected(admin, "selected-topic", AlterConfigOp.OpType.DELETE, null);
            assertModeChangeRejected(admin, "ordinary-after-activation", AlterConfigOp.OpType.SET, "true");
            assertModeChangeRejected(admin, "before-activation", AlterConfigOp.OpType.SET, "true");
            alter(admin, "ordinary-after-activation", MODE, AlterConfigOp.OpType.DELETE, null);
            assertEquals("false", describeMode(admin, "ordinary-after-activation").value());
            alter(admin, "ordinary-after-activation", MODE, AlterConfigOp.OpType.SET, "false");
            alter(admin, "selected-topic", TopicConfig.RETENTION_MS_CONFIG, AlterConfigOp.OpType.SET, "7200000");
            assertEquals("true", describeMode(admin, "selected-topic").value());
            roundTrip(cluster.bootstrapServers(), "selected-topic", "shared");
            roundTrip(cluster.bootstrapServers(), "ordinary-after-activation", "ordinary");

            BrokerServer broker = cluster.brokers().values().iterator().next();
            broker.shutdown();
            broker.startup();
            cluster.waitForReadyBrokers();
            assertLogMode(cluster, "selected-topic", true);
            assertLogMode(cluster, "ordinary-after-activation", false);
            assertValues(cluster.bootstrapServers(), "selected-topic", List.of("shared-0", "shared-1", "shared-2"));

            ExecutionException downgrade = assertThrows(ExecutionException.class,
                () -> admin.updateFeatures(Map.of(SharedStorageVersion.FEATURE_NAME,
                    new FeatureUpdate((short) 0, FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE)),
                    new UpdateFeaturesOptions()).all().get(30, TimeUnit.SECONDS));
            assertInstanceOf(InvalidUpdateVersionException.class, downgrade.getCause());

            Uuid oldId = admin.describeTopics(List.of("selected-topic")).allTopicNames()
                .get(30, TimeUnit.SECONDS).get("selected-topic").topicId();
            admin.deleteTopics(List.of("selected-topic")).all().get(30, TimeUnit.SECONDS);
            TestUtils.waitForCondition(() -> !admin.listTopics().names().get(10, TimeUnit.SECONDS)
                .contains("selected-topic"), 30_000L, "Deleted topic remained visible");
            Uuid newId = createTopic(admin, "selected-topic", Map.of());
            assertNotEquals(oldId, newId);
            awaitTopicId(admin, "selected-topic", newId);
            assertLogMode(cluster, "selected-topic", false);
            roundTrip(cluster.bootstrapServers(), "selected-topic", "recreated-classic");
        }
    }

    private static KafkaClusterTestKit startCluster(boolean withExtension) throws Exception {
        return startCluster(withExtension, false);
    }

    private static KafkaClusterTestKit startCluster(boolean withExtension, boolean legacySelectors) throws Exception {
        return startCluster(withExtension, legacySelectors, 1);
    }

    private static KafkaClusterTestKit startCluster(boolean withExtension, boolean legacySelectors, int brokers)
        throws Exception {
        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
            .setNumBrokerNodes(brokers).setNumControllerNodes(1).build());
        if (withExtension) {
            builder.setConfigProp("storage.extension.class", SharedStorageExtension.class.getName())
                .setConfigProp("shared.storage.wal.capacity.bytes", 8L * 1024 * 1024)
                .setConfigProp("shared.storage.legacy.selectors.enabled", legacySelectors);
        }
        KafkaClusterTestKit cluster = builder.build();
        try {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            return cluster;
        } catch (Exception failure) {
            try {
                cluster.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static void activateTopicStorage(Admin admin) throws Exception {
        admin.updateFeatures(Map.of(SharedStorageVersion.FEATURE_NAME,
            new FeatureUpdate((short) 1, FeatureUpdate.UpgradeType.UPGRADE)),
            new UpdateFeaturesOptions()).all().get(30, TimeUnit.SECONDS);
    }

    private static Uuid createTopic(Admin admin, String topic, Map<String, String> configs) throws Exception {
        var creation = admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1).configs(configs)));
        creation.all().get(30, TimeUnit.SECONDS);
        return creation.topicId(topic).get(30, TimeUnit.SECONDS);
    }

    private static void awaitTopicId(Admin admin, String topic, Uuid expectedId) throws Exception {
        long startNanos = System.nanoTime();
        Uuid observedId = null;
        // CreateTopics acknowledges controller persistence before every broker has
        // published the new metadata. Only absence is retryable; an old ID cannot
        // satisfy this wait and any other Admin failure must remain a test failure.
        while (System.nanoTime() - startNanos < TimeUnit.SECONDS.toNanos(30)) {
            try {
                observedId = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topic).topicId();
                if (expectedId.equals(observedId)) {
                    return;
                }
            } catch (ExecutionException failure) {
                if (!(failure.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw failure;
                }
            }
            Thread.sleep(100);
        }
        assertEquals(expectedId, observedId, "Created Topic ID did not become visible for " + topic);
    }

    private static ConfigEntry describeMode(Admin admin, String topic) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        return admin.describeConfigs(List.of(resource)).all().get(30, TimeUnit.SECONDS).get(resource).get(MODE);
    }

    private static void assertModeChangeRejected(Admin admin, String topic, AlterConfigOp.OpType op, String value) {
        ExecutionException failure = assertThrows(ExecutionException.class, () -> alter(admin, topic, MODE, op, value));
        assertInstanceOf(InvalidConfigurationException.class, failure.getCause());
    }

    private static void alter(Admin admin, String topic, String key, AlterConfigOp.OpType op, String value)
        throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        admin.incrementalAlterConfigs(Map.of(resource, List.of(new AlterConfigOp(new ConfigEntry(key, value), op))))
            .all().get(30, TimeUnit.SECONDS);
    }

    private static void assertLogMode(KafkaClusterTestKit cluster, String topic, boolean shared) throws Exception {
        TopicPartition partition = new TopicPartition(topic, 0);
        TestUtils.waitForCondition(() -> cluster.brokers().values().stream().allMatch(broker -> {
            scala.Option<UnifiedLog> log = broker.logManager().getLog(partition, false);
            return log.isDefined() && (log.get() instanceof SharedUnifiedLog) == shared;
        }), 30_000L, "Unexpected physical log implementation for " + topic);
    }

    private static void roundTrip(String bootstrapServers, String topic, String prefix) throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        List<String> expected = new ArrayList<>();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < 3; i++) {
                String value = prefix + "-" + i;
                expected.add(value);
                assertEquals(i, producer.send(new ProducerRecord<>(topic, 0, value, value))
                    .get(30, TimeUnit.SECONDS).offset());
            }
        }
        assertValues(bootstrapServers, topic, expected);
    }

    private static void assertValues(String bootstrapServers, String topic, List<String> expected) throws Exception {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            List<String> actual = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(record -> actual.add(record.value()));
                return actual.size() >= expected.size();
            }, 30_000L, "Unable to read acknowledged records from " + topic);
            assertEquals(expected, actual);
            assertEquals(expected.size(), consumer.endOffsets(List.of(partition)).get(partition).longValue());
        }
    }
}
