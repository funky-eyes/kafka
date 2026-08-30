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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Asynchronously converts Kafka-committed leader WAL batches into cross-partition shared objects.
 *
 * <p>Kafka callbacks never call this class. They only update {@link SharedCommitProgress}. This scheduler snapshots
 * those commit windows on its own thread, accepts current local leaders only, filters candidates strictly below Kafka
 * HW, merges them by physical WAL order and delegates the durable object/metadata protocol to
 * {@link SharedObjectUploader}.</p>
 *
 * <p>The same maintenance thread also persists remote COMMIT references into the broker-local crash-safe checkpoint.
 * Metadata-consumer callbacks only enqueue that work and never perform filesystem I/O. Checkpoint failure does not
 * stop additional remote uploads, but it remains observable through {@link #lastFailure()} and prevents later WAL
 * reclamation from treating the uncheckpointed ranges as locally recoverable.</p>
 */
public final class SharedUploadScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SharedUploadScheduler.class);

    private final SharedStorageEngine engine;
    private final SharedCommitProgress commitProgress;
    private final SharedObjectUploader uploader;
    private final LongSupplier objectIdSupplier;
    private final LongSupplier currentTimeMsSupplier;
    private final long targetObjectBytes;
    private final AtomicBoolean uploadInProgress = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> lastUploadFailure = new AtomicReference<>();
    private final AtomicReference<Throwable> lastMaintenanceFailure = new AtomicReference<>();
    private final AtomicReference<SelectionSummary> lastSelectionSummary = new AtomicReference<>();

    private ScheduledExecutorService executor;

    public SharedUploadScheduler(
        SharedStorageEngine engine,
        SharedCommitProgress commitProgress,
        SharedObjectUploader uploader,
        LongSupplier objectIdSupplier,
        LongSupplier currentTimeMsSupplier,
        long targetObjectBytes
    ) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.commitProgress = Objects.requireNonNull(commitProgress, "commitProgress");
        this.uploader = Objects.requireNonNull(uploader, "uploader");
        this.objectIdSupplier = Objects.requireNonNull(objectIdSupplier, "objectIdSupplier");
        this.currentTimeMsSupplier = Objects.requireNonNull(currentTimeMsSupplier, "currentTimeMsSupplier");
        if (targetObjectBytes <= 0) {
            throw new IllegalArgumentException("targetObjectBytes must be positive");
        }
        this.targetObjectBytes = targetObjectBytes;
    }

    public synchronized void start(long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive");
        }
        if (closed.get()) {
            throw new IllegalStateException("Shared upload scheduler is closed");
        }
        if (executor != null) {
            throw new IllegalStateException("Shared upload scheduler is already started");
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "kafka-shared-storage-uploader");
            thread.setDaemon(true);
            return thread;
        });
        LOG.info(
            "Started shared upload scheduler with intervalMs={} and targetObjectBytes={}",
            intervalMs,
            targetObjectBytes
        );
        executor.scheduleWithFixedDelay(this::runScheduledUpload, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Starts at most one asynchronous object upload and returns empty when there is no committed leader work or another
     * upload is already active. This method never blocks on object-store or metadata-store I/O.
     */
    public CompletableFuture<Optional<SharedObjectMetadata>> tryUploadOnce() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shared upload scheduler is closed"));
        }
        if (!uploadInProgress.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        final List<SharedStorageEngine.UploadCandidate> candidates;
        try {
            candidates = selectCandidates();
        } catch (RuntimeException e) {
            return synchronousFailure(e);
        }
        if (candidates.isEmpty()) {
            uploadInProgress.set(false);
            return CompletableFuture.completedFuture(Optional.empty());
        }

        final CompletableFuture<Optional<SharedObjectMetadata>> result;
        try {
            long objectId = objectIdSupplier.getAsLong();
            long createdTimeMs = currentTimeMsSupplier.getAsLong();
            if (objectId < 0) {
                throw new IllegalStateException("objectIdSupplier returned a negative object ID");
            }
            result = uploader
                .upload(objectId, createdTimeMs, candidates)
                .thenApply(Optional::of);
        } catch (RuntimeException e) {
            return synchronousFailure(e);
        }
        return result.whenComplete((ignored, error) -> {
            if (error == null) {
                lastUploadFailure.set(null);
            } else {
                lastUploadFailure.set(error);
                LOG.warn("Shared object upload failed", error);
            }
            uploadInProgress.set(false);
        });
    }

    private CompletableFuture<Optional<SharedObjectMetadata>> synchronousFailure(RuntimeException error) {
        lastUploadFailure.set(error);
        uploadInProgress.set(false);
        LOG.warn("Shared upload scheduling failed before the asynchronous object PUT started", error);
        return CompletableFuture.failedFuture(error);
    }

    public Optional<Throwable> lastFailure() {
        Throwable maintenance = lastMaintenanceFailure.get();
        return Optional.ofNullable(maintenance != null ? maintenance : lastUploadFailure.get());
    }

    /**
     * Persists queued authoritative remote COMMITs on the maintenance thread.
     *
     * @return number of object COMMITs crossed by the local checkpoint durability barrier, or zero after a failure
     */
    int checkpointRemoteCommitsOnce() {
        try {
            int checkpointed = engine.checkpointCommittedRemoteObjects();
            lastMaintenanceFailure.set(null);
            if (checkpointed > 0) {
                LOG.debug("Checkpointed {} committed shared objects for local WAL recovery", checkpointed);
            }
            return checkpointed;
        } catch (IOException e) {
            lastMaintenanceFailure.set(e);
            LOG.warn("Unable to persist committed shared-object ranges for local WAL recovery", e);
            return 0;
        }
    }

    List<SharedStorageEngine.UploadCandidate> selectCandidates() {
        Map<org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId,
            SharedCommitProgress.PartitionProgress> snapshot = commitProgress.snapshot();
        List<SharedStorageEngine.UploadCandidate> committed = new ArrayList<>();
        int leaderPartitions = 0;
        int openCommitWindows = 0;
        for (Map.Entry<org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId,
            SharedCommitProgress.PartitionProgress> entry : snapshot.entrySet()) {
            SharedCommitProgress.PartitionProgress progress = entry.getValue();
            if (!progress.isLeader()) {
                continue;
            }
            leaderPartitions++;
            if (progress.highWatermark() <= progress.logStartOffset()) {
                continue;
            }
            openCommitWindows++;
            committed.addAll(engine.uploadCandidates(
                entry.getKey(),
                progress.logStartOffset(),
                progress.highWatermark()
            ));
        }
        logSelectionSummary(new SelectionSummary(
            snapshot.size(),
            leaderPartitions,
            openCommitWindows,
            committed.size()
        ));

        committed.sort(Comparator
            .comparingLong((SharedStorageEngine.UploadCandidate candidate) -> candidate.location().segmentId())
            .thenComparingLong(candidate -> candidate.location().position()));

        if (committed.isEmpty()) {
            return List.of();
        }
        List<SharedStorageEngine.UploadCandidate> selected = new ArrayList<>();
        long selectedBytes = 0L;
        for (SharedStorageEngine.UploadCandidate candidate : committed) {
            int payloadBytes = candidate.location().payloadLength();
            if (!selected.isEmpty() && selectedBytes + payloadBytes > targetObjectBytes) {
                break;
            }
            selected.add(candidate);
            selectedBytes = Math.addExact(selectedBytes, payloadBytes);
            if (selectedBytes >= targetObjectBytes) {
                break;
            }
        }
        return List.copyOf(selected);
    }

    private void logSelectionSummary(SelectionSummary summary) {
        SelectionSummary previous = lastSelectionSummary.getAndSet(summary);
        if (!summary.equals(previous)) {
            LOG.info(
                "Shared upload gate state changed: trackedPartitions={}, leaders={}, openCommitWindows={}, candidates={}",
                summary.trackedPartitions(),
                summary.leaderPartitions(),
                summary.openCommitWindows(),
                summary.candidateCount()
            );
        }
    }

    private void runScheduledUpload() {
        checkpointRemoteCommitsOnce();
        tryUploadOnce().whenComplete((ignored, error) -> {
            if (error != null) {
                lastUploadFailure.set(error);
            }
        });
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private record SelectionSummary(
        int trackedPartitions,
        int leaderPartitions,
        int openCommitWindows,
        int candidateCount
    ) {
    }
}