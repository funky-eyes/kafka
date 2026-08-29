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
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.storage.internals.shared.metadata.BrokerObjectId;
import org.apache.kafka.storage.internals.shared.metadata.ObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Authoritative shared-object metadata store backed by the classic compacted Kafka topic
 * {@code __shared_storage_metadata}.
 *
 * <p>The topic is always a classic Kafka log. Startup manually assigns its single partition, captures a read-committed
 * end offset and replays from the beginning to that boundary before marking the metadata image READY. A daemon consumer
 * then tails live records. Object state transitions complete only after that consumer has applied the producer-returned
 * offset. This is essential for cross-broker COMMIT versus orphan-cleanup ordering: a producer acknowledgement alone is
 * not evidence that its transition won against an earlier record from another broker.</p>
 */
public final class KafkaObjectMetadataStore implements ObjectMetadataStore, AutoCloseable {
    private static final TopicPartition METADATA_PARTITION =
        new TopicPartition(SharedMetadataClientConfiguration.TOPIC_NAME, 0);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(100L);
    private static final long TOPIC_CREATE_RETRY_BACKOFF_MS = 250L;
    private static final long CLOSE_JOIN_TIMEOUT_MS = 30_000L;

    private final SharedMetadataClientConfiguration configuration;
    private final Admin admin;
    private final KafkaProducer<byte[], byte[]> producer;
    private final KafkaProducer<byte[], byte[]> sequenceProducer;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final SharedMetadataImage image;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object appliedOffsetLock = new Object();
    private final NavigableMap<Long, List<CompletableFuture<Void>>> appliedOffsetWaiters = new TreeMap<>();

    private long appliedOffset = -1L;
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
        return open(configuration, ignored -> { });
    }

    /**
     * Opens the metadata store and mirrors every newly observed committed object to the supplied non-blocking listener.
     * The listener is invoked for both initial replay and live records, including commits produced by other brokers.
     */
    public static KafkaObjectMetadataStore open(
        SharedMetadataClientConfiguration configuration,
        Consumer<SharedObjectMetadata> committedObjectListener
    ) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        Objects.requireNonNull(committedObjectListener, "committedObjectListener");
        KafkaObjectMetadataStore store = new KafkaObjectMetadataStore(
            configuration,
            Admin.create(configuration.adminProperties()),
            new KafkaProducer<>(configuration.producerProperties()),
            new KafkaProducer<>(configuration.sequenceProducerProperties()),
            new KafkaConsumer<>(configuration.consumerProperties()),
            new SharedMetadataImage(committedObjectListener)
        );
        try {
            store.initialize();
            return store;
        } catch (Throwable t) {
            store.image.markFailed(t);
            store.failAppliedOffsetWaiters(t);
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
        while (!closed.get()) {
            try {
                admin.createTopics(List.of(configuration.newMetadataTopic())).all().get();
                return;
            } catch (ExecutionException e) {
                Throwable cause = unwrapExecutionException(e);
                if (cause instanceof TopicExistsException) {
                    return;
                }
                if (!isTransientTopicCreationFailure(cause)) {
                    throw e;
                }
                awaitReplicaCapacity();
            }
        }
        throw new InterruptedException("Shared metadata store closed while creating metadata topic");
    }

    /**
     * A fresh RF=3 cluster may start brokers one at a time. Only wait for replica capacity after the controller has
     * explicitly rejected topic creation for a transient reason; an already-existing RF=3 metadata topic must remain
     * usable when the cluster is temporarily running with fewer than three active brokers.
     */
    private void awaitReplicaCapacity() throws Exception {
        while (!closed.get()) {
            try {
                int activeBrokers = admin.describeCluster().nodes().get().size();
                if (activeBrokers >= configuration.replicationFactor()) {
                    return;
                }
            } catch (ExecutionException e) {
                Throwable cause = unwrapExecutionException(e);
                if (!(cause instanceof RetriableException)) {
                    throw e;
                }
            }
            Thread.sleep(TOPIC_CREATE_RETRY_BACKOFF_MS);
        }
        throw new InterruptedException("Shared metadata store closed while waiting for metadata replicas");
    }

    static boolean isTransientTopicCreationFailure(Throwable cause) {
        return cause instanceof InvalidReplicationFactorException || cause instanceof RetriableException;
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
                failAppliedOffsetWaiters(e);
            }
        } catch (Throwable t) {
            image.markFailed(t);
            failAppliedOffsetWaiters(t);
        }
    }

    private void applyRecords(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> record : records.records(METADATA_PARTITION)) {
            image.apply(record.key(), record.value());
            markApplied(record.offset());
        }
    }

    private void markApplied(long offset) {
        List<CompletableFuture<Void>> completed = new ArrayList<>();
        synchronized (appliedOffsetLock) {
            if (offset > appliedOffset) {
                appliedOffset = offset;
            }
            while (!appliedOffsetWaiters.isEmpty() && appliedOffsetWaiters.firstKey() <= appliedOffset) {
                completed.addAll(appliedOffsetWaiters.pollFirstEntry().getValue());
            }
        }
        completed.forEach(waiter -> waiter.complete(null));
    }

    private CompletableFuture<Void> awaitApplied(long offset) {
        synchronized (appliedOffsetLock) {
            if (appliedOffset >= offset) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> waiter = new CompletableFuture<>();
            appliedOffsetWaiters.computeIfAbsent(offset, ignored -> new ArrayList<>()).add(waiter);
            return waiter;
        }
    }

    private void failAppliedOffsetWaiters(Throwable cause) {
        List<CompletableFuture<Void>> failed = new ArrayList<>();
        synchronized (appliedOffsetLock) {
            for (Map.Entry<Long, List<CompletableFuture<Void>>> entry : appliedOffsetWaiters.entrySet()) {
                failed.addAll(entry.getValue());
            }
            appliedOffsetWaiters.clear();
        }
        failed.forEach(waiter -> waiter.completeExceptionally(cause));
    }

    @Override
    public CompletableFuture<Void> prepare(long objectId, long createdTimeMs) {
        if (createdTimeMs < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("createdTimeMs must be non-negative"));
        }
        return writeRecord(
            SharedMetadataRecordCodec.objectKey(objectId),
            SharedMetadataRecordCodec.preparedObjectValue(createdTimeMs)
        );
    }

    @Override
    public CompletableFuture<Void> commit(SharedObjectMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        return writeRecord(
            SharedMetadataRecordCodec.objectKey(metadata.objectId()),
            SharedMetadataRecordCodec.committedObjectValue(metadata)
        ).thenCompose(ignored -> image.committedObject(metadata.objectId())
            .filter(metadata::equals)
            .map(committed -> CompletableFuture.<Void>completedFuture(null))
            .orElseGet(() -> CompletableFuture.failedFuture(new IllegalStateException(
                "Object COMMIT lost to orphan cleanup fence: " + metadata.objectId()))));
    }

    @Override
    public CompletableFuture<Void> delete(long objectId) {
        return writeRecord(SharedMetadataRecordCodec.objectKey(objectId), null);
    }

    @Override
    public CompletableFuture<Boolean> claimCleanup(long objectId, long expectedCreatedTimeMs) {
        if (expectedCreatedTimeMs < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("expectedCreatedTimeMs must be non-negative"));
        }
        return writeRecord(
            SharedMetadataRecordCodec.objectCleanupKey(objectId),
            SharedMetadataRecordCodec.cleanupClaimedValue(expectedCreatedTimeMs)
        ).thenApply(ignored -> image.cleanupFenced(objectId, expectedCreatedTimeMs));
    }

    @Override
    public CompletableFuture<Void> completeCleanup(long objectId) {
        ObjectMetadataStore.PreparedObject claimed = image.cleanupClaimedObjects().stream()
            .filter(object -> object.objectId() == objectId)
            .findFirst()
            .orElse(null);
        if (claimed == null) {
            // Already DELETED is an idempotent success. Callers only complete a cleanup after a successful claim.
            return CompletableFuture.completedFuture(null);
        }
        return writeRecord(
            SharedMetadataRecordCodec.objectCleanupKey(objectId),
            SharedMetadataRecordCodec.cleanupDeletedValue(claimed.createdTimeMs())
        );
    }

    @Override
    public List<SharedObjectMetadata> committedObjects() {
        return image.committedObjects();
    }

    @Override
    public List<ObjectMetadataStore.PreparedObject> preparedObjects() {
        return image.preparedObjects();
    }

    @Override
    public List<ObjectMetadataStore.PreparedObject> cleanupClaimedObjects() {
        return image.cleanupClaimedObjects();
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

    private CompletableFuture<Void> writeRecord(byte[] key, byte[] value) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shared metadata store is closed"));
        }
        if (!image.isReady()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                "Shared metadata image is not authoritative: " + image.state()));
        }
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
            awaitApplied(metadata.offset()).whenComplete((ignored, applyError) -> {
                if (applyError != null) {
                    result.completeExceptionally(applyError);
                } else {
                    result.complete(null);
                }
            });
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
        failAppliedOffsetWaiters(new IllegalStateException("Shared metadata store is closed"));
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
