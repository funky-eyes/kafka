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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.FencedPreparedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.MetadataValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Experimental Kafka-transaction lifecycle writer; orphan cleanup is NOT enabled by this type.
 *
 * <p>One compacted object key preserves the final COMMIT-or-CLAIM state.
 * The test-only claimant below scans a complete topic; production must
 * instead use the continuously replayed metadata image and a post-fence
 * read-committed horizon before enabling any physical DELETE.</p>
 */
final class FencedObjectLifecycleArbiter {
    private static final long IO_TIMEOUT_SECONDS = 30L;

    private FencedObjectLifecycleArbiter() {
    }

    private static String transactionalId(String namespace, long objectId) {
        if (objectId <= 0L || namespace == null || !namespace.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException("Invalid fenced lifecycle identity");
        }
        String identity = "shared-object-v2-" + namespace + "-" + objectId;
        if (identity.length() > 256) {
            throw new IllegalArgumentException("Fenced lifecycle transactional.id exceeds Kafka's limit");
        }
        return identity;
    }

    private static Properties transactionalProperties(Properties original, String namespace, long objectId) {
        Properties result = new Properties();
        result.putAll(Objects.requireNonNull(original, "original"));
        result.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId(namespace, objectId));
        result.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        result.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        result.put(ProducerConfig.ACKS_CONFIG, "all");
        return result;
    }

    static final class Uploader implements AutoCloseable {
        private final KafkaProducer<byte[], byte[]> producer;
        private final String topic;
        private final long objectId;
        private boolean prepared;
        private boolean committed;

        Uploader(Properties original, String topic, String namespace, long objectId) {
            this.topic = Objects.requireNonNull(topic, "topic");
            this.objectId = objectId;
            producer = new KafkaProducer<>(transactionalProperties(original, namespace, objectId));
            try {
                // Must precede PREPARE; never reacquire this identity after being fenced.
                producer.initTransactions();
            } catch (RuntimeException failure) {
                producer.close();
                throw failure;
            }
        }

        long prepare(long createdTimeMs) throws Exception {
            if (prepared) {
                throw new IllegalStateException("Object already prepared");
            }
            long offset = append(SharedMetadataRecordCodec.fencedPreparedObjectValue(createdTimeMs));
            prepared = true;
            return offset;
        }

        long commit(SharedObjectMetadata metadata) throws Exception {
            Objects.requireNonNull(metadata, "metadata");
            if (!prepared || committed || metadata.objectId() != objectId) {
                throw new IllegalStateException("Object has no matching fenced PREPARE");
            }
            long offset = append(SharedMetadataRecordCodec.committedObjectValue(metadata));
            committed = true;
            return offset;
        }

        private long append(byte[] value) throws Exception {
            producer.beginTransaction();
            try {
                long offset = producer.send(new ProducerRecord<>(
                    topic, 0, SharedMetadataRecordCodec.objectKey(objectId), value
                )).get(IO_TIMEOUT_SECONDS, TimeUnit.SECONDS).offset();
                producer.commitTransaction();
                return offset;
            } catch (Exception failure) {
                try {
                    producer.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                throw failure;
            }
        }

        @Override
        public void close() {
            // Do not hold the metadata consumer callback for Kafka's default
            // producer-close timeout during leader loss.
            producer.close(Duration.ofSeconds(2L));
        }
    }

    static OptionalLong claimIfPrepared(
        Properties original, String topic, String namespace, long objectId, long expectedCreatedTimeMs
    ) throws Exception {
        try (KafkaProducer<byte[], byte[]> cleaner = new KafkaProducer<>(
            transactionalProperties(original, namespace, objectId)
        )) {
            // Synchronously fence the unique uploader that initialized this
            // transactional.id BEFORE PREPARE. A late COMMIT then fails.
            cleaner.initTransactions();
            MetadataValue latest = readLatestAfterFence(original, topic, objectId);
            if (!(latest instanceof FencedPreparedObjectValue prepared)
                || prepared.createdTimeMs() != expectedCreatedTimeMs) {
                return OptionalLong.empty();
            }
            cleaner.beginTransaction();
            try {
                long offset = cleaner.send(new ProducerRecord<>(
                    topic, 0, SharedMetadataRecordCodec.objectKey(objectId),
                    SharedMetadataRecordCodec.fencedClaimedObjectValue(expectedCreatedTimeMs)
                )).get(IO_TIMEOUT_SECONDS, TimeUnit.SECONDS).offset();
                cleaner.commitTransaction();
                return OptionalLong.of(offset);
            } catch (Exception failure) {
                try {
                    cleaner.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                throw failure;
            }
        }
    }

    private static MetadataValue readLatestAfterFence(Properties original, String topic, long objectId) {
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(original);
        consumerProperties.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);
        MetadataValue latest = null;
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            // In read_committed mode endOffsets is the last stable offset.
            // Capturing it after initTransactions prevents stale pre-fence decisions.
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(IO_TIMEOUT_SECONDS);
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(200L))) {
                    if (Arrays.equals(record.key(), key)) {
                        latest = SharedMetadataRecordCodec.decodeValue(
                            SharedMetadataRecordCodec.decodeKey(record.key()), record.value());
                    }
                }
            }
            if (consumer.position(partition) < end) {
                throw new IllegalStateException("Post-fence read-committed catalog replay did not complete");
            }
        }
        return latest;
    }
}
