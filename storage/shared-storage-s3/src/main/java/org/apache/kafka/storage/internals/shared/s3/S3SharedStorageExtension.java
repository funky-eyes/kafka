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

import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.storage.internals.log.KafkaStorageExtension;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.StoragePartitionRoleListener;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.kafka.RoutingUnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.kafka.SharedCommitProgress;
import org.apache.kafka.storage.internals.shared.kafka.SharedPartitionRoleListener;
import org.apache.kafka.storage.internals.shared.kafka.SharedStorageConfiguration;
import org.apache.kafka.storage.internals.shared.kafka.SharedUnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.kafka.SharedUploadScheduler;
import org.apache.kafka.storage.internals.shared.metadata.BrokerObjectId;
import org.apache.kafka.storage.internals.shared.metadata.LocalRemoteObjectCheckpoint;
import org.apache.kafka.storage.internals.shared.object.ActiveObjectUploads;
import org.apache.kafka.storage.internals.shared.object.OrphanCleanupScheduler;
import org.apache.kafka.storage.internals.shared.object.OrphanObjectCleaner;
import org.apache.kafka.storage.internals.shared.object.SharedObjectPacker;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Production shared-storage extension using a broker-wide replicated WAL, Kafka-backed authoritative metadata and S3.
 *
 * <p>{@link #start(StorageExtensionContext)} performs only local WAL and remote-checkpoint recovery and installs the
 * shared log factory, so Kafka can load partitions before network listeners are available.
 * {@link #onBrokerReady(StorageExtensionBrokerContext)} then replays the classic metadata topic, fences the
 * broker-scoped object-ID allocator, restores authoritative remote coverage and finally starts asynchronous S3 upload
 * and orphan cleanup. Kafka's broker-startup deadline gates the returned future.</p>
 */
public final class S3SharedStorageExtension implements KafkaStorageExtension {
    private static final int DEFAULT_OBJECT_ID_BLOCK_SIZE = 4_096;
    private static final long BOOTSTRAP_EXECUTOR_STOP_TIMEOUT_SECONDS = 5L;
    private static final long METADATA_BOOTSTRAP_RETRY_BACKOFF_MS = 250L;
    private static final long METADATA_BOOTSTRAP_RETRY_TIMEOUT_MS = 30_000L;

    private SharedStorageConfiguration storageConfiguration;
    private SharedStorageEngine storage;
    private SharedCommitProgress commitProgress;
    private UnifiedLogFactory unifiedLogFactory;
    private StoragePartitionRoleListener partitionRoleListener;
    private ExecutorService bootstrapExecutor;
    private CompletableFuture<Void> brokerReadyFuture;
    private KafkaObjectMetadataStore metadataStore;
    private S3ObjectStore objectStore;
    private SharedUploadScheduler uploadScheduler;
    private OrphanCleanupScheduler orphanCleanupScheduler;
    private boolean closed;

    @Override
    public synchronized void start(StorageExtensionContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        if (storage != null || closed) {
            throw new IllegalStateException("S3 shared storage extension cannot be started in its current state");
        }

        SharedStorageConfiguration configuration = SharedStorageConfiguration.from(context);
        SharedWal wal = new FileSharedWal(
            configuration.walDir(),
            configuration.walCapacityBytes(),
            configuration.walSegmentBytes()
        );
        try {
            LocalRemoteObjectCheckpoint remoteCheckpoint =
                new LocalRemoteObjectCheckpoint(configuration.walDir());
            SharedStorageEngine newStorage = new SharedStorageEngine(wal, remoteCheckpoint);
            SharedCommitProgress newCommitProgress = new SharedCommitProgress();
            UnifiedLogFactory newFactory = new RoutingUnifiedLogFactory(
                configuration,
                new SharedUnifiedLogFactory(newStorage, newCommitProgress)
            );
            StoragePartitionRoleListener newPartitionRoleListener =
                new SharedPartitionRoleListener(configuration, newCommitProgress);
            ExecutorService newBootstrapExecutor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "kafka-shared-storage-bootstrap");
                thread.setDaemon(true);
                return thread;
            });

            storageConfiguration = configuration;
            storage = newStorage;
            commitProgress = newCommitProgress;
            unifiedLogFactory = newFactory;
            partitionRoleListener = newPartitionRoleListener;
            bootstrapExecutor = newBootstrapExecutor;
        } catch (Throwable t) {
            try {
                wal.close();
            } catch (Throwable closeError) {
                t.addSuppressed(closeError);
            }
            throw asIOException("Unable to initialize local shared WAL storage", t);
        }
    }

    @Override
    public synchronized UnifiedLogFactory unifiedLogFactory() {
        if (unifiedLogFactory == null) {
            throw new IllegalStateException("S3 shared storage extension has not been started");
        }
        return unifiedLogFactory;
    }

    @Override
    public synchronized StoragePartitionRoleListener partitionRoleListener() {
        if (partitionRoleListener == null) {
            throw new IllegalStateException("S3 shared storage extension has not been started");
        }
        return partitionRoleListener;
    }

    @Override
    public synchronized CompletableFuture<Void> onBrokerReady(StorageExtensionBrokerContext context) {
        Objects.requireNonNull(context, "context");
        if (storage == null || bootstrapExecutor == null || storageConfiguration == null || commitProgress == null) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("S3 shared storage extension has not been started"));
        }
        if (closed) {
            return CompletableFuture.failedFuture(new IllegalStateException("S3 shared storage extension is closed"));
        }
        if (brokerReadyFuture != null) {
            return brokerReadyFuture;
        }

        SharedStorageConfiguration configuration = storageConfiguration;
        SharedStorageEngine engine = storage;
        SharedCommitProgress progress = commitProgress;
        ExecutorService executor = bootstrapExecutor;
        brokerReadyFuture = CompletableFuture.runAsync(() -> {
            try {
                initializeRemotePlane(context, configuration, engine, progress);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        }, executor).whenComplete((ignored, error) -> executor.shutdown());
        return brokerReadyFuture;
    }

    private void initializeRemotePlane(
        StorageExtensionBrokerContext context,
        SharedStorageConfiguration configuration,
        SharedStorageEngine engine,
        SharedCommitProgress progress
    ) throws IOException {
        KafkaObjectMetadataStore newMetadataStore = null;
        S3ObjectStore newObjectStore = null;
        SharedUploadScheduler newUploadScheduler = null;
        OrphanCleanupScheduler newOrphanCleanupScheduler = null;
        boolean installed = false;
        try {
            SharedMetadataClientConfiguration metadataConfiguration =
                SharedMetadataClientConfiguration.from(context);
            // Initial replay and live commits from every broker update this engine's remote coverage.
            newMetadataStore = openMetadataStoreWithRetry(metadataConfiguration, engine);

            S3ObjectStoreConfig objectStoreConfig = objectStoreConfiguration(context);
            newObjectStore = new S3ObjectStore(objectStoreConfig);

            KafkaObjectMetadataStore.SequenceBlock initialBlock =
                newMetadataStore.reserveSequenceBlock(DEFAULT_OBJECT_ID_BLOCK_SIZE);
            LongSupplier objectIds = new BlockObjectIdSupplier(
                newMetadataStore,
                context.brokerId(),
                DEFAULT_OBJECT_ID_BLOCK_SIZE,
                initialBlock
            );
            ActiveObjectUploads activeUploads = new ActiveObjectUploads();
            SharedObjectUploadHook uploadHook =
                FileSharedObjectUploadBarrier.from(context.originals(), context.brokerId());
            SharedObjectUploader uploader = new SharedObjectUploader(
                newObjectStore,
                newMetadataStore,
                new SharedObjectPacker(),
                engine,
                activeUploads,
                uploadHook
            );
            newUploadScheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                objectIds,
                context.time()::milliseconds,
                configuration.objectTargetBytes()
            );
            newOrphanCleanupScheduler = new OrphanCleanupScheduler(
                new OrphanObjectCleaner(newObjectStore, newMetadataStore, activeUploads),
                context.time()::milliseconds,
                configuration.orphanGraceMs()
            );

            synchronized (this) {
                if (closed || storage != engine) {
                    throw new IOException("S3 shared storage extension closed or restarted during remote bootstrap");
                }
                metadataStore = newMetadataStore;
                objectStore = newObjectStore;
                uploadScheduler = newUploadScheduler;
                orphanCleanupScheduler = newOrphanCleanupScheduler;
                newUploadScheduler.start(configuration.uploadIntervalMs());
                newOrphanCleanupScheduler.start(configuration.orphanCleanupIntervalMs());
                installed = true;
            }
        } finally {
            if (!installed) {
                closeIgnoringFailure(newOrphanCleanupScheduler);
                closeIgnoringFailure(newUploadScheduler);
                closeIgnoringFailure(newObjectStore);
                closeIgnoringFailure(newMetadataStore);
            }
        }
    }

    private KafkaObjectMetadataStore openMetadataStoreWithRetry(
        SharedMetadataClientConfiguration metadataConfiguration,
        SharedStorageEngine engine
    ) throws IOException {
        long deadlineNanos = System.nanoTime() +
            TimeUnit.MILLISECONDS.toNanos(METADATA_BOOTSTRAP_RETRY_TIMEOUT_MS);
        Throwable lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                return KafkaObjectMetadataStore.open(metadataConfiguration, engine::commitRemoteObject);
            } catch (IOException | RuntimeException e) {
                if (!isRetriableMetadataBootstrapFailure(e)) {
                    if (e instanceof IOException ioException) {
                        throw ioException;
                    }
                    throw e;
                }
                lastFailure = e;
            }

            try {
                Thread.sleep(METADATA_BOOTSTRAP_RETRY_BACKOFF_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for shared metadata bootstrap", e);
            }
        }
        throw new IOException(
            "Timed out waiting for shared metadata topic to become locally readable",
            lastFailure
        );
    }

    static boolean isRetriableMetadataBootstrapFailure(Throwable failure) {
        Objects.requireNonNull(failure, "failure");
        Throwable current = failure;
        while (current != null) {
            if (current instanceof RetriableException) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause == current) {
                break;
            }
            current = cause;
        }
        return false;
    }

    private static S3ObjectStoreConfig objectStoreConfiguration(StorageExtensionBrokerContext context) {
        Map<String, Object> originals = new LinkedHashMap<>();
        context.originals().forEach(originals::put);
        Object configuredPrefix = originals.get(S3ObjectStoreConfig.KEY_PREFIX_CONFIG);
        if (configuredPrefix == null || configuredPrefix.toString().isBlank()) {
            originals.put(
                S3ObjectStoreConfig.KEY_PREFIX_CONFIG,
                "clusters/" + context.clusterId() + "/objects"
            );
        }
        return S3ObjectStoreConfig.from(originals);
    }

    @Override
    public void close() throws IOException {
        CompletableFuture<Void> readyFuture;
        ExecutorService executor;
        SharedUploadScheduler scheduler;
        OrphanCleanupScheduler cleanupScheduler;
        KafkaObjectMetadataStore metadata;
        S3ObjectStore objects;
        SharedStorageEngine engine;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            readyFuture = brokerReadyFuture;
            executor = bootstrapExecutor;
            scheduler = uploadScheduler;
            cleanupScheduler = orphanCleanupScheduler;
            metadata = metadataStore;
            objects = objectStore;
            engine = storage;
            brokerReadyFuture = null;
            bootstrapExecutor = null;
            uploadScheduler = null;
            orphanCleanupScheduler = null;
            metadataStore = null;
            objectStore = null;
            storageConfiguration = null;
            storage = null;
            commitProgress = null;
            unifiedLogFactory = null;
            partitionRoleListener = null;
        }

        if (readyFuture != null && !readyFuture.isDone()) {
            readyFuture.cancel(true);
        }
        if (executor != null) {
            executor.shutdownNow();
            awaitExecutorStop(executor);
        }

        IOException failure = null;
        failure = close(failure, cleanupScheduler);
        failure = close(failure, scheduler);
        failure = close(failure, metadata);
        failure = close(failure, objects);
        failure = close(failure, engine);
        if (failure != null) {
            throw failure;
        }
    }

    private static void awaitExecutorStop(ExecutorService executor) {
        try {
            executor.awaitTermination(BOOTSTRAP_EXECUTOR_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static IOException close(IOException failure, AutoCloseable resource) {
        if (resource == null) {
            return failure;
        }
        try {
            resource.close();
            return failure;
        } catch (Exception e) {
            if (failure == null) {
                return e instanceof IOException ioException
                    ? ioException
                    : new IOException("Unable to close S3 shared storage resource", e);
            }
            failure.addSuppressed(e);
            return failure;
        }
    }

    private static void closeIgnoringFailure(AutoCloseable resource) {
        if (resource == null) {
            return;
        }
        try {
            resource.close();
        } catch (Exception ignored) {
            // The primary bootstrap failure is reported to BrokerServer; cleanup failures are secondary here.
        }
    }

    private static IOException asIOException(String message, Throwable t) {
        if (t instanceof IOException ioException) {
            return ioException;
        }
        if (t instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new IOException(message, t);
    }

    private static final class BlockObjectIdSupplier implements LongSupplier {
        private final KafkaObjectMetadataStore metadataStore;
        private final int brokerId;
        private final int blockSize;
        private long nextSequence;
        private long endExclusive;

        private BlockObjectIdSupplier(
            KafkaObjectMetadataStore metadataStore,
            int brokerId,
            int blockSize,
            KafkaObjectMetadataStore.SequenceBlock initialBlock
        ) {
            this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
            this.brokerId = brokerId;
            this.blockSize = blockSize;
            install(Objects.requireNonNull(initialBlock, "initialBlock"));
        }

        @Override
        public synchronized long getAsLong() {
            if (nextSequence >= endExclusive) {
                try {
                    install(metadataStore.reserveSequenceBlock(blockSize));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return BrokerObjectId.compose(brokerId, nextSequence++);
        }

        private void install(KafkaObjectMetadataStore.SequenceBlock block) {
            nextSequence = block.startInclusive();
            endExclusive = block.endExclusive();
        }
    }
}