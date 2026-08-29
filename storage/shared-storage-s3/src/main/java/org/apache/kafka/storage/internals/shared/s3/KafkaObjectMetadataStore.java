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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.storage.internals.shared.metadata.BrokerObjectId;
import org.apache.kafka.storage.internals.shared.metadata.ObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Authoritative shared-object metadata store backed by the classic compacted Kafka topic
 * {@code __shared_storage_metadata}.
 *
 * <p>The topic is always a classic Kafka log. Startup manually assigns its single partition, captures a read-committed
 * end offset and replays from the beginning to that boundary before marking the metadata image READY. A daemon consumer
 * then tails live records. Any unexpected live-consumer or replay failure marks the image FAILED so remote reads and
 * uploads fail closed rather than continuing from stale metadata.</p>
 */
public final class KafkaObjectMetadataStore implements ObjectMetadataStore, AutoCloseable {
    private static final TopicPartition METADATA_PARTITION =
        new TopicPartition(SharedMetadataClientConfiguration.TOPIC_NAME, 0);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100L);
    private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

    private final SharedMetadataClientConfiguration configuration;
    private final Admin admin;
    private final KafkaProducer<byte[], byte[]> producer;
    private final KafkaProducer<byte[], byte[]> sequenceProducer;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final SharedMetadataImage image;
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile Thread consumerThread;

    private KafkaObjectMetadataStore(
        SharedMetadataClientConfiguration configuration,
        Admin admin,
        KafkaProducer<byte[], byte[]> producer,
        KafkaProducer<byte[], byte[]> sequenceProducer,
        KafkaConsumer<byte[], byte[]> consumer,
        SharedMetadataImage image
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.admin = Objects.requireNonNull(admin, "admin");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.sequenceProducer = Objects.requireNonNull(sequenceProducer, "sequenceProducer");
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.image = Objects.requireNonNull(image, "image");
    }

    public static KafkaObjectMetadataStore open(SharedMetadataClientConfiguration configuration) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        KafkaObjectMetadataStore store = new KafkaObjectMetadataStore(
            configuration,
            Admin.create(configuration.adminProperties()),
            new KafkaProducer<>(configuration.producerProperties()),
            new KafkaProducer<>(configuration.sequenceProducerProperties()),
            new KafkaConsumer<>(configuration.consumerProperties()),
            new SharedMetadataImage()
        );
        try {
            store.initialize();
            return store;
        } catch (Throwable t) {
            store.image.markFailed(t);
            try {
                store.close();
            } catch (Throwable closeError) {
                t.addSuppressed(closeError);
            }
            throw asIOException("Unable to initialize shared metadata store", t);
        }
    }

    private void initialize() throws Exception {
        createTopicIfNeeded();
        validateTopic();
        replayInitialImage();
        // initTransactions fences an older allocator using the same clusterId + brokerId transactional.id.
        sequenceProducer.initTransactions();
        image.markReady();
        startLiveConsumer();
    }

    private void createTopicIfNeeded() throws Exception {
        try {
            admin.createTopics(List.of(configuration.newMetadataTopic())).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    private void validateTopic() throws Exception {
        TopicDescription description = admin.describeTopics(
            List.of(SharedMetadataClientConfiguration.TOPIC_NAME)
        ).allTopicNames().get().get(SharedMetadataClientConfiguration.TOPIC_NAME);
        if (description == null || description.partitions().size() != 1) {
            throw new IllegalStateException(
                SharedMetadataClientConfiguration.TOPIC_NAME + " must have exactly one partition");
        }
        int replicas = description.partitions().get(0).replicas().size();
        if (replicas < configuration.replicationFactor()) {
            throw new IllegalStateException(
                SharedMetadataClientConfiguration.TOPIC_NAME + " replication factor " + replicas +
                    " is below required " + configuration.replicationFactor());
        }

        ConfigResource resource = new ConfigResource(
            ConfigResource.Type.TOPIC,
            SharedMetadataClientConfiguration.TOPIC_NAME
        );
        Config topicConfig = admin.describeConfigs(List.of(resource)).all().get().get(resource);
        if (topicConfig == null) {
            throw new IllegalStateException("Unable to read shared metadata topic configuration");
        }
        String cleanupPolicy = requiredConfig(topicConfig, TopicConfig.CLEANUP_POLICY_CONFIG);
        Set<String> policies = new HashSet<>();
        Arrays.stream(cleanupPolicy.split(","))
            .map(String::trim)
            .filter(value -> !value.isEmpty())
            .forEach(policies::add);
        if (!policies.equals(Set.of(TopicConfig.CLEANUP_POLICY_COMPACT))) {
            throw new IllegalStateException(
                SharedMetadataClientConfiguration.TOPIC_NAME + " must use cleanup.policy=compact only, got " +
                    cleanupPolicy);
        }
        int minIsr = Integer.parseInt(requiredConfig(topicConfig, TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
        if (minIsr < configuration.minInSyncReplicas()) {
            throw new IllegalStateException(
                SharedMetadataClientConfiguration.TOPIC_NAME + " min.insync.replicas " + minIsr +
                    " is below required " + configuration.minInSyncReplicas());
        }
    }

    private static String requiredConfig(Config config, String name) {
        if (config.get(name) == null || config.get(name).value() == null || config.get(name).value().isBlank()) {
            throw new IllegalStateException("Missing required topic configuration " + name);
        }
        return config.get(name).value();
    }

    private void replayInitialImage() {
        consumer.assign(List.of(METADATA_PARTITION));
        consumer.seekToBeginning(List.of(METADATA_PARTITION));
        long replayEnd = consumer.endOffsets(List.of(METADATA_PARTITION)).get(METADATA_PARTITION);
        while (consumer.position(METADATA_PARTITION) < replayEnd) {
            applyRecords(consumer.poll(POLL_TIMEOUT));
        }
    }

    private void startLiveConsumer() {
        Thread thread = new Thread(this::runLiveConsumer, "kafka-shared-metadata-consumer");
        thread.setDaemon(true);
        consumerThread = thread;
        thread.start();
    }

    private void runLiveConsumer() {
        try {
            while (!closed.get()) {
                applyRecords(consumer.poll(POLL_TIMEOUT));
            }
        } catch (WakeupException e) {
            if (!closed.get()) {
                image.markFailed(e);
            }
        } catch (Throwable t) {
            image.markFailed(t);
        }
    }

    private void applyRecords(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> record : records.records(METADATA_PARTITION)) {
            image.apply(record.key(), record.value());
        }
    }

    @Override
    public CompletableFuture<Void> prepare(long objectId, long createdTimeMs) {
        if (createdTimeMs < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("createdTimeMs must be non-negative"));
        }
        return writeObjectRecord(
            objectId,
            SharedMetadataRecordCodec.preparedObjectValue(createdTimeMs)
        );
    }

    @Override
    public CompletableFuture<Void> commit(SharedObjectMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        return writeObjectRecord(
            metadata.objectId(),
            SharedMetadataRecordCodec.committedObjectValue(metadata)
        );
    }

    @Override
    public CompletableFuture<Void> delete(long objectId) {
        return writeObjectRecord(objectId, null);
    }

    @Override
    public List<SharedObjectMetadata> committedObjects() {
        return image.committedObjects();
    }

    public List<SharedMetadataImage.PreparedObject> preparedObjects() {
        return image.preparedObjects();
    }

    public SharedMetadataImage.State state() {
        return image.state();
    }

    public synchronized SequenceBlock reserveSequenceBlock(int blockSize) throws IOException {
        requireReady();
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be positive");
        }
        long startInclusive = image.brokerReservedExclusiveSequence(configuration.brokerId());
        long limitExclusive = BrokerObjectId.MAX_SEQUENCE + 1L;
        if (startInclusive >= limitExclusive) {
            throw new IllegalStateException("Shared object ID sequence is exhausted for broker " + configuration.brokerId());
        }
        long requestedEnd;
        try {
            requestedEnd = Math.addExact(startInclusive, blockSize);
        } catch (ArithmeticException e) {
            requestedEnd = limitExclusive;
        }
        long endExclusive = Math.min(requestedEnd, limitExclusive);
        byte[] key = SharedMetadataRecordCodec.brokerSequenceKey(configuration.brokerId());
        byte[] value = SharedMetadataRecordCodec.brokerSequenceValue(endExclusive);
        try {
            sequenceProducer.beginTransaction();
            sequenceProducer.send(new ProducerRecord<>(
                SharedMetadataClientConfiguration.TOPIC_NAME,
                0,
                key,
                value
            )).get();
            sequenceProducer.commitTransaction();
            image.apply(key, value);
            return new SequenceBlock(startInclusive, endExclusive);
        } catch (Throwable t) {
            try {
                sequenceProducer.abortTransaction();
            } catch (Throwable abortError) {
                t.addSuppressed(abortError);
            }
            image.markFailed(t);
            if (t instanceof ProducerFencedException producerFencedException) {
                throw new IOException("Shared object ID allocator was fenced by another broker incarnation", producerFencedException);
            }
            throw asIOException("Unable to reserve shared object ID sequence block", t);
        }
    }

    private CompletableFuture<Void> writeObjectRecord(long objectId, byte[] value) {
        if (objectId <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be positive"));
        }
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shared metadata store is closed"));
        }
        if (!image.isReady()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Shared metadata image is not authoritative: " + image.state()));
        }
        byte[] key = SharedMetadataRecordCodec.objectKey(objectId);
        CompletableFuture<Void> result = new CompletableFuture<>();
        producer.send(new ProducerRecord<>(
            SharedMetadataClientConfiguration.TOPIC_NAME,
            0,
            key,
            value
        ), (metadata, exception) -> {
            if (exception != null) {
                result.completeExceptionally(exception);
                return;
            }
            try {
                image.apply(key, value);
                result.complete(null);
            } catch (Throwable t) {
                image.markFailed(t);
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    private void requireReady() {
        if (closed.get()) {
            throw new IllegalStateException("Shared metadata store is closed");
        }
        if (!image.isReady()) {
            throw new IllegalStateException("Shared metadata image is not authoritative: " + image.state());
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        consumer.wakeup();
        Thread thread = consumerThread;
        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join(CLOSE_JOIN_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        consumer.close();
        producer.close();
        sequenceProducer.close();
        admin.close();
    }

    private static IOException asIOException(String message, Throwable t) {
        Throwable cause = unwrapExecutionException(t);
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        if (cause instanceof IOException ioException) {
            return ioException;
        }
        return new IOException(message, cause);
    }

    private static Throwable unwrapExecutionException(Throwable t) {
        Throwable current = t;
        while (current instanceof ExecutionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    public record SequenceBlock(long startInclusive, long endExclusive) {
        public SequenceBlock {
            if (startInclusive <= 0 || endExclusive <= startInclusive) {
                throw new IllegalArgumentException(
                    "Invalid sequence block [" + startInclusive + ", " + endExclusive + ")");
            }
        }

        public long size() {
            return endExclusive - startInclusive;
        }
    }
}
