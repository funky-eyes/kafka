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
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.CommittedObjectValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec.MetadataValue;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.Callable;
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
            // The test-only fixture verifies the same rule by scanning Kafka.
            this(original, topic, namespace, objectId,
                () -> readLatestAfterFence(original, topic, objectId) == null);
        }

        Uploader(Properties original, String topic, String namespace, long objectId,
                 Callable<Boolean> postFenceObjectIdAbsent) {
            this.topic = Objects.requireNonNull(topic, "topic");
            this.objectId = objectId;
            Objects.requireNonNull(postFenceObjectIdAbsent, "postFenceObjectIdAbsent");
            producer = new KafkaProducer<>(transactionalProperties(original, namespace, objectId));
            try {
                producer.initTransactions();
                // Fencing is not a permanent object-ID tombstone. Every new
                // writer must reject an existing PREPARED, CLAIMED or COMMIT.
                if (!Boolean.TRUE.equals(postFenceObjectIdAbsent.call())) {
                    throw new IllegalStateException("Fenced object identity already exists: " + objectId);
                }
            } catch (Exception failure) {
                producer.close(Duration.ofSeconds(2L));
                throw new IllegalStateException("Unable to initialize unique fenced upload " + objectId, failure);
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
        // CI-only full-topic scan: production uses the already-replayed metadata image.
        return claimAfterFencing(original, topic, namespace, objectId, expectedCreatedTimeMs,
            () -> {
                MetadataValue latest = readLatestAfterFence(original, topic, objectId);
                return latest instanceof FencedPreparedObjectValue prepared
                    && prepared.createdTimeMs() == expectedCreatedTimeMs;
            });
    }

    /**
     * The callback must capture a new read-committed LSO AFTER initTransactions
     * has fenced the upload producer, wait until that LSO has been applied,
     * and revalidate the exact original v2 PREPARED object generation.
     * Only the owning metadata-store implementation may supply that witness.
     */
    static OptionalLong claimAfterFencing(
        Properties original, String topic, String namespace, long objectId, long expectedCreatedTimeMs,
        Callable<Boolean> postFenceReadCommittedPrepared
    ) throws Exception {
        Objects.requireNonNull(postFenceReadCommittedPrepared, "postFenceReadCommittedPrepared");
        try (KafkaProducer<byte[], byte[]> cleaner = new KafkaProducer<>(
            transactionalProperties(original, namespace, objectId)
        )) {
            // Must complete before the read-committed horizon is captured.
            cleaner.initTransactions();
            if (!Boolean.TRUE.equals(postFenceReadCommittedPrepared.call())) {
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

    /**
     * Experimental catalog seal writer, deliberately NOT exposed by the
     * production metadata store. The same object-scoped transactional.id
     * fences the v2 PREPARE/COMMIT producer before sampling Kafka's
     * read_committed horizon. The original COMMIT must still be present at
     * exactly the expected offset with the complete immutable descriptor.
     *
     * <p>A seal is a durable Kafka catalog fact only. It is not a KRaft
     * RETIRE_INTENT or DELETE_READY grant, and does not establish all-Broker
     * read/write draining. Production use additionally requires a negotiated
     * mixed-version gate, an authenticated Controller handoff, and local
     * upload/reader fencing. This package-local primitive is intentionally
     * exercised only by integration tests until those contracts exist.</p>
     */
    static OptionalLong sealCommittedAfterFencing(
        Properties original,
        String topic,
        String namespace,
        SharedObjectMetadata descriptor,
        long expectedCommitOffset,
        long retirementEpoch
    ) throws Exception {
        Objects.requireNonNull(descriptor, "descriptor");
        if (expectedCommitOffset < 0L || retirementEpoch <= 0L) {
            throw new IllegalArgumentException("Catalog seal requires a committed offset and retirement epoch");
        }
        long objectId = descriptor.objectId();
        try (KafkaProducer<byte[], byte[]> sealer = new KafkaProducer<>(
            transactionalProperties(original, namespace, objectId)
        )) {
            // Kafka fences every older object-scoped transactional generation.
            // The read_committed scan MUST run after this call completes.
            sealer.initTransactions();
            CatalogSnapshot snapshot = readCatalogAfterFence(original, topic, objectId);
            if (snapshot.sealed()
                || snapshot.objectOffset() != expectedCommitOffset
                || !(snapshot.value() instanceof CommittedObjectValue committed)
                || !descriptor.equals(committed.metadata())) {
                return OptionalLong.empty();
            }
            sealer.beginTransaction();
            try {
                long offset = sealer.send(new ProducerRecord<>(
                    topic, 0, SharedMetadataRecordCodec.objectCatalogSealKey(objectId),
                    SharedMetadataRecordCodec.objectCatalogSealValue(
                        expectedCommitOffset, retirementEpoch, descriptor)
                )).get(IO_TIMEOUT_SECONDS, TimeUnit.SECONDS).offset();
                sealer.commitTransaction();
                return OptionalLong.of(offset);
            } catch (Exception failure) {
                try {
                    sealer.abortTransaction();
                } catch (RuntimeException abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
                throw failure;
            }
        }
    }

    private record CatalogSnapshot(MetadataValue value, long objectOffset, boolean sealed) {
    }

    private static MetadataValue readLatestAfterFence(Properties original, String topic, long objectId) {
        return readCatalogAfterFence(original, topic, objectId).value();
    }

    /**
     * Bounded-memory Kafka scan of one read_committed partition. We track
     * both compacted keys: a COMMITTED record cannot be borrowed from a
     * prior incarnation or overridden by a pre-existing catalog seal.
     * Object offsets are the actual Kafka offsets, not consumer positions.
     */
    private static CatalogSnapshot readCatalogAfterFence(Properties original, String topic, long objectId) {
        Properties consumerProperties = new Properties();
        consumerProperties.putAll(original);
        consumerProperties.remove(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        TopicPartition partition = new TopicPartition(topic, 0);
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);
        byte[] sealKey = SharedMetadataRecordCodec.objectCatalogSealKey(objectId);
        MetadataValue latest = null;
        long objectOffset = -1L;
        boolean sealed = false;
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
                        objectOffset = record.offset();
                    } else if (Arrays.equals(record.key(), sealKey)) {
                        // Decode even when the value is not needed, so a
                        // malformed or tombstoned seal never becomes evidence.
                        MetadataValue seal = SharedMetadataRecordCodec.decodeValue(
                            SharedMetadataRecordCodec.decodeKey(record.key()), record.value());
                        if (!(seal instanceof SharedMetadataRecordCodec.ObjectCatalogSealValue)) {
                            throw new IllegalStateException("Unexpected compacted catalog seal value");
                        }
                        sealed = true;
                    }
                }
            }
            if (consumer.position(partition) < end) {
                throw new IllegalStateException("Post-fence read-committed catalog replay did not complete");
            }
        }
        return new CatalogSnapshot(latest, objectOffset, sealed);
    }
}
