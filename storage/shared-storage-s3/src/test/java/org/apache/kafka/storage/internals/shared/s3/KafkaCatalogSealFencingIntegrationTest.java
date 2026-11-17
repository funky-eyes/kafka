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
package org.apache.kafka.storage.internals.shared.s3;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests the experimental metadata-store entry point against a dedicated real Kafka cluster.
 * A successful local seal is not an authenticated Controller handoff or a GC grant.
 */
class KafkaCatalogSealFencingIntegrationTest {
    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void realStoreSealsReplayedCommitAfterFencing() throws Exception {
        String bootstrap = System.getenv("SHARED_STORAGE_KAFKA_BOOTSTRAP_SERVERS");
        assumeTrue(bootstrap != null && !bootstrap.isBlank(), "Kafka brokers are not configured");
        assumeTrue("true".equals(System.getenv("SHARED_STORAGE_ISOLATED_SEAL_TEST_CLUSTER")),
            "This test requires its own isolated Kafka cluster");

        String namespace = Uuid.randomUuid().toString();
        SharedMetadataClientConfiguration config = configuration(bootstrap, namespace);
        String topic = SharedMetadataClientConfiguration.TOPIC_NAME;
        long objectId = 9_501L;
        long epoch = 9L;
        SharedObjectMetadata object = new SharedObjectMetadata(objectId, 32L, 71L, List.of(
            new SharedObjectRange(new SharedPartitionId(11L, 12L, 0),
                new OffsetRange(0L, 1L), 1, 0L, 32, 71L)
        ));
        try (Admin admin = Admin.create(config.adminProperties())) {
            awaitThreeRegisteredBrokers(admin);
            // createTopics must succeed: an existing topic is never reused or deleted.
            admin.createTopics(List.of(config.newMetadataTopic())).all().get(30, TimeUnit.SECONDS);
            try {
                Uuid topicId = admin.describeTopics(List.of(topic)).allTopicNames().get(30, TimeUnit.SECONDS)
                    .get(topic).topicId();
                assertNotEquals(Uuid.ZERO_UUID, topicId);
                try (KafkaObjectMetadataStore store = KafkaObjectMetadataStore.open(config)) {
                    store.prepare(objectId, 1_000L).get(30, TimeUnit.SECONDS);
                    store.commit(object).get(30, TimeUnit.SECONDS);
                    byte[] objectKey = SharedMetadataRecordCodec.objectKey(objectId);
                    ConsumerRecord<byte[], byte[]> commit = latestRecord(config, objectKey);
                    var decoded = SharedMetadataRecordCodec.decodeValue(
                        SharedMetadataRecordCodec.decodeKey(commit.key()), commit.value());
                    assertTrue(decoded instanceof SharedMetadataRecordCodec.CommittedObjectValue);
                    var committed = (SharedMetadataRecordCodec.CommittedObjectValue) decoded;
                    assertTrue(committed.fenced());
                    assertEquals(object, committed.metadata());

                    Properties probeProperties = config.producerProperties();
                    probeProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        "shared-object-v2-" + namespace + "-" + objectId);
                    OptionalLong result;
                    KafkaProducer<byte[], byte[]> previous = new KafkaProducer<>(probeProperties);
                    try {
                        // This real producer owns the same transactional ID and
                        // has an open transaction before the store seals.
                        previous.initTransactions();
                        previous.beginTransaction();
                        previous.send(new ProducerRecord<>(topic, 0,
                            SharedMetadataRecordCodec.brokerSequenceKey(998),
                            SharedMetadataRecordCodec.brokerSequenceValue(1L))).get(30, TimeUnit.SECONDS);
                        result = store.sealCommittedForExperiment(object, commit.offset(), epoch)
                            .get(60, TimeUnit.SECONDS);
                        Exception failure = assertThrows(Exception.class, previous::commitTransaction);
                        assertTrue(hasFencedCause(failure),
                            "Sealing must fence the actual prior Kafka producer generation");
                    } finally {
                        previous.close(Duration.ofSeconds(2L));
                    }
                    assertTrue(result.isPresent());
                    byte[] sealKey = SharedMetadataRecordCodec.objectCatalogSealKey(objectId);
                    ConsumerRecord<byte[], byte[]> sealRecord = latestRecord(config, sealKey);
                    assertEquals(result.getAsLong(), sealRecord.offset());
                    var seal = (SharedMetadataRecordCodec.ObjectCatalogSealValue)
                        SharedMetadataRecordCodec.decodeValue(
                            SharedMetadataRecordCodec.decodeKey(sealRecord.key()), sealRecord.value());
                    assertEquals(commit.offset(), seal.originalCommitOffset());
                    assertEquals(epoch, seal.retirementEpoch());
                    assertEquals(SharedMetadataRecordCodec.committedDescriptorSha256(object), seal.descriptorSha256());
                    assertEquals(object, store.committedObject(objectId).orElseThrow());
                    assertFalse(store.sealCommittedForExperiment(object, commit.offset(), epoch)
                        .get(60, TimeUnit.SECONDS).isPresent());
                    assertRecreatedCatalogRejectsSeal(admin, config, store, topic, topicId, object);
                }
            } finally {
                admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
            }
        }
    }

    private static boolean hasFencedCause(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof ProducerFencedException) {
                return true;
            }
        }
        return false;
    }

    private static void assertRecreatedCatalogRejectsSeal(
        Admin admin, SharedMetadataClientConfiguration config, KafkaObjectMetadataStore store,
        String topic, Uuid originalTopicId, SharedObjectMetadata original
    ) throws Exception {
        SharedObjectMetadata next = new SharedObjectMetadata(
            9_502L, original.objectSize(), original.objectChecksum(), original.ranges());
        store.prepare(next.objectId(), 1_001L).get(30, TimeUnit.SECONDS);
        store.commit(next).get(30, TimeUnit.SECONDS);
        long offset = latestRecord(config, SharedMetadataRecordCodec.objectKey(next.objectId())).offset();
        admin.deleteTopics(List.of(topic)).all().get(30, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        boolean created = false;
        while (!created && System.nanoTime() < deadline) {
            try {
                admin.createTopics(List.of(config.newMetadataTopic())).all().get(10, TimeUnit.SECONDS);
                created = true;
            } catch (ExecutionException failure) {
                if (!(failure.getCause() instanceof TopicExistsException)) {
                    throw failure;
                }
                Thread.sleep(250L);
            }
        }
        assertTrue(created, "The old catalog topic must be deleted before recreation");
        awaitReplacementTopicId(admin, topic, originalTopicId);
        // Consumer failure, observed identity change, or an absent candidate
        // all fail closed; none may report a successful seal of the old COMMIT.
        try {
            assertTrue(store.sealCommittedForExperiment(next, offset, 10L)
                .get(60, TimeUnit.SECONDS).isEmpty());
        } catch (ExecutionException expected) {
            assertTrue(expected.getCause() instanceof IllegalStateException);
        }
    }

    private static void awaitReplacementTopicId(Admin admin, String topic, Uuid oldId) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (System.nanoTime() < deadline) {
            try {
                var description = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topic);
                Uuid current = description == null ? null : description.topicId();
                if (current != null && !Uuid.ZERO_UUID.equals(current) && !oldId.equals(current)) {
                    return;
                }
            } catch (ExecutionException failure) {
                if (!(failure.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw failure;
                }
            }
            Thread.sleep(250L);
        }
        throw new AssertionError("The recreated catalog topic did not expose a new Topic ID");
    }

    private static SharedMetadataClientConfiguration configuration(String bootstrap, String namespace) {
        return SharedMetadataClientConfiguration.from(new StorageExtensionBrokerContext(
            namespace, 101,
            List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 1)),
            Map.of(
                SharedMetadataClientConfiguration.CLIENT_PREFIX + "bootstrap.servers", bootstrap,
                SharedMetadataClientConfiguration.FENCED_LIFECYCLE_CONFIG, true
            ), Time.SYSTEM
        ));
    }

    private static void awaitThreeRegisteredBrokers(Admin admin) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        int count;
        do {
            count = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).size();
            if (count < 3) {
                Thread.sleep(250L);
            }
        } while (count < 3 && System.nanoTime() < deadline);
        assertTrue(count >= 3, "The experimental seal test requires three registered Kafka brokers");
    }

    private static ConsumerRecord<byte[], byte[]> latestRecord(
        SharedMetadataClientConfiguration config, byte[] key
    ) {
        TopicPartition partition = new TopicPartition(SharedMetadataClientConfiguration.TOPIC_NAME, 0);
        ConsumerRecord<byte[], byte[]> latest = null;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(config.consumerProperties())) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200L))) {
                    if (record.offset() < end && Arrays.equals(record.key(), key)) {
                        latest = record;
                    }
                }
            }
            assertTrue(consumer.position(partition) >= end, "The read-committed catalog scan must finish");
        }
        if (latest == null) {
            throw new AssertionError("Expected catalog key was not present at the captured horizon");
        }
        return latest;
    }
}
