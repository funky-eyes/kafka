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
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
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
 * <p>The periodic interval is only an evaluation cadence. Scheduled uploads are started when the eligible committed
 * bytes reach the target object size, when the oldest current candidate reaches the configured maximum linger, or when
 * broker-wide WAL usage crosses the configured pressure threshold. This avoids producing tiny objects every scheduler
 * tick while still bounding low-throughput publication latency and draining the WAL before capacity becomes critical.</p>
 *
 * <p>The same maintenance thread persists remote COMMIT references into the broker-local crash-safe checkpoint and
 * then reclaims only the rotating WAL prefix covered by that durable checkpoint. Metadata-consumer callbacks only
 * enqueue checkpoint work and never perform filesystem I/O. Maintenance failure remains observable through
 * {@link #lastFailure()} and fails closed for reclamation without blocking future asynchronous uploads.</p>
 */
public final class SharedUploadScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SharedUploadScheduler.class);
    static final long DEFAULT_MAX_LINGER_MS = 1_000L;
    static final int DEFAULT_WAL_PRESSURE_PERCENT = 70;

    private final SharedStorageEngine engine;
    private final SharedCommitProgress commitProgress;
    private final SharedObjectUploader uploader;
    private final LongSupplier objectIdSupplier;
    private final LongSupplier currentTimeMsSupplier;
    private final long targetObjectBytes;
    private final long maxLingerMs;
    private final int walPressurePercent;
    private final AtomicBoolean uploadInProgress = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> lastUploadFailure = new AtomicReference<>();
    private final AtomicReference<Throwable> lastMaintenanceFailure = new AtomicReference<>();
    private final AtomicReference<SelectionSummary> lastSelectionSummary = new AtomicReference<>();
    private final AtomicReference<PendingHead> pendingHead = new AtomicReference<>();

    private ScheduledExecutorService executor;

    public SharedUploadScheduler(
        SharedStorageEngine engine,
        SharedCommitProgress commitProgress,
        SharedObjectUploader uploader,
        LongSupplier objectIdSupplier,
        LongSupplier currentTimeMsSupplier,
        long targetObjectBytes
    ) {
        this(
            engine,
            commitProgress,
            uploader,
            objectIdSupplier,
            currentTimeMsSupplier,
            targetObjectBytes,
            DEFAULT_MAX_LINGER_MS,
            DEFAULT_WAL_PRESSURE_PERCENT
        );
    }

    public SharedUploadScheduler(
        SharedStorageEngine engine,
        SharedCommitProgress commitProgress,
        SharedObjectUploader uploader,
        LongSupplier objectIdSupplier,
        LongSupplier currentTimeMsSupplier,
        long targetObjectBytes,
        long maxLingerMs,
        int walPressurePercent
    ) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.commitProgress = Objects.requireNonNull(commitProgress, "commitProgress");
        this.uploader = Objects.requireNonNull(uploader, "uploader");
        this.objectIdSupplier = Objects.requireNonNull(objectIdSupplier, "objectIdSupplier");
        this.currentTimeMsSupplier = Objects.requireNonNull(currentTimeMsSupplier, "currentTimeMsSupplier");
        if (targetObjectBytes <= 0) {
            throw new IllegalArgumentException("targetObjectBytes must be positive");
        }
        if (maxLingerMs < 0) {
            throw new IllegalArgumentException("maxLingerMs must not be negative");
        }
        if (walPressurePercent <= 0 || walPressurePercent > 100) {
            throw new IllegalArgumentException("walPressurePercent must be in [1, 100]");
        }
        this.targetObjectBytes = targetObjectBytes;
        this.maxLingerMs = maxLingerMs;
        this.walPressurePercent = walPressurePercent;
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
            "Started shared upload scheduler with intervalMs={}, targetObjectBytes={}, maxLingerMs={}, " +
                "walPressurePercent={}",
            intervalMs,
            targetObjectBytes,
            maxLingerMs,
            walPressurePercent
        );
        executor.scheduleWithFixedDelay(this::runScheduledUpload, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Forces at most one asynchronous object upload and returns empty when there is no committed leader work or another
     * upload is already active. This is primarily useful for deterministic maintenance/tests; the periodic scheduler
     * uses the size/linger/WAL-pressure gate instead. This method never blocks on object-store or metadata-store I/O.
     */
    public CompletableFuture<Optional<SharedObjectMetadata>> tryUploadOnce() {
        return tryUpload(false);
    }

    CompletableFuture<Optional<SharedObjectMetadata>> tryScheduledUploadOnce() {
        return tryUpload(true);
    }

    private CompletableFuture<Optional<SharedObjectMetadata>> tryUpload(boolean applyTriggerGate) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Shared upload scheduler is closed"));
        }
        if (!uploadInProgress.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return selectForUpload(applyTriggerGate);
    }

    private CompletableFuture<Optional<SharedObjectMetadata>> selectForUpload(boolean applyTriggerGate) {
        final CandidateSelection selection;
        try {
            selection = selectCandidateBatch();
        } catch (RuntimeException e) {
            return synchronousFailure(e);
        }
        if (selection.candidates().isEmpty()) {
            pendingHead.set(null);
            uploadInProgress.set(false);
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return evaluateTrigger(selection, applyTriggerGate);
    }

    private CompletableFuture<Optional<SharedObjectMetadata>> evaluateTrigger(
        CandidateSelection selection,
        boolean applyTriggerGate
    ) {
        final long nowMs;
        try {
            nowMs = currentTimeMsSupplier.getAsLong();
        } catch (RuntimeException e) {
            return synchronousFailure(e);
        }
        if (applyTriggerGate && !shouldUpload(selection, nowMs)) {
            uploadInProgress.set(false);
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return startUpload(selection, nowMs);
    }

    private CompletableFuture<Optional<SharedObjectMetadata>> startUpload(
        CandidateSelection selection,
        long nowMs
    ) {
        final CompletableFuture<Optional<SharedObjectMetadata>> result;
        try {
            long objectId = objectIdSupplier.getAsLong();
            if (objectId < 0) {
                throw new IllegalStateException("objectIdSupplier returned a negative object ID");
            }
            result = uploader
                .upload(objectId, nowMs, selection.candidates())
                .thenApply(Optional::of);
        } catch (RuntimeException e) {
            return synchronousFailure(e);
        }
        return result.whenComplete(this::completeUpload);
    }

    private void completeUpload(Optional<SharedObjectMetadata> ignored, Throwable error) {
        if (error == null) {
            lastUploadFailure.set(null);
            pendingHead.set(null);
        } else {
            lastUploadFailure.set(error);
            LOG.warn("Shared object upload failed", error);
        }
        uploadInProgress.set(false);
    }

    private boolean shouldUpload(CandidateSelection selection, long nowMs) {
        if (selection.totalEligibleBytes() >= targetObjectBytes) {
            return true;
        }
        if (walPressureReached()) {
            return true;
        }
        long ageMs = pendingAgeMs(selection.candidates().get(0), nowMs);
        return ageMs >= maxLingerMs;
    }

    private long pendingAgeMs(SharedStorageEngine.UploadCandidate firstCandidate, long nowMs) {
        PendingHead current = pendingHead.get();
        if (current == null || !current.matches(firstCandidate) || nowMs < current.firstObservedMs()) {
            pendingHead.set(PendingHead.from(firstCandidate, nowMs));
            return 0L;
        }
        return nowMs - current.firstObservedMs();
    }

    private boolean walPressureReached() {
        long capacityBytes = engine.walCapacityBytes();
        long usedBytes = engine.walUsedBytes();
        return usedBytes >= percentage(capacityBytes, walPressurePercent);
    }

    private static long percentage(long value, int percent) {
        long quotient = value / 100L;
        long remainder = value % 100L;
        return Math.addExact(
            Math.multiplyExact(quotient, percent),
            Math.multiplyExact(remainder, percent) / 100L
        );
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

    long reclaimCheckpointedWalOnce() {
        if (lastMaintenanceFailure.get() != null) {
            return 0L;
        }
        try {
            long reclaimedBytes = engine.reclaimCheckpointedWal();
            lastMaintenanceFailure.set(null);
            if (reclaimedBytes > 0) {
                LOG.debug("Reclaimed {} bytes from checkpointed rotating shared WAL", reclaimedBytes);
            }
            return reclaimedBytes;
        } catch (IOException | RuntimeException e) {
            lastMaintenanceFailure.set(e);
            LOG.warn("Unable to reclaim checkpointed rotating shared WAL", e);
            return 0L;
        }
    }

    List<SharedStorageEngine.UploadCandidate> selectCandidates() {
        return selectCandidateBatch().candidates();
    }

    private CandidateSelection selectCandidateBatch() {
        Map<SharedPartitionId, SharedCommitProgress.PartitionProgress> snapshot = commitProgress.snapshot();
        List<SharedStorageEngine.UploadCandidate> committed = new ArrayList<>();
        int leaderPartitions = 0;
        int openCommitWindows = 0;
        for (Map.Entry<SharedPartitionId, SharedCommitProgress.PartitionProgress> entry : snapshot.entrySet()) {
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

        committed.sort(Comparator
            .comparingLong((SharedStorageEngine.UploadCandidate candidate) -> candidate.location().segmentId())
            .thenComparingLong(candidate -> candidate.location().position()));

        long totalEligibleBytes = totalEligibleBytes(committed);
        logSelectionSummary(new SelectionSummary(
            snapshot.size(),
            leaderPartitions,
            openCommitWindows,
            committed.size(),
            totalEligibleBytes
        ));

        if (committed.isEmpty()) {
            return new CandidateSelection(List.of(), 0L);
        }
        return new CandidateSelection(selectTargetBounded(committed), totalEligibleBytes);
    }

    private static long totalEligibleBytes(List<SharedStorageEngine.UploadCandidate> candidates) {
        long total = 0L;
        for (SharedStorageEngine.UploadCandidate candidate : candidates) {
            total = Math.addExact(total, candidate.location().payloadLength());
        }
        return total;
    }

    private List<SharedStorageEngine.UploadCandidate> selectTargetBounded(
        List<SharedStorageEngine.UploadCandidate> committed
    ) {
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
                "Shared upload gate state changed: trackedPartitions={}, leaders={}, openCommitWindows={}, " +
                    "candidates={}, eligibleBytes={}",
                summary.trackedPartitions(),
                summary.leaderPartitions(),
                summary.openCommitWindows(),
                summary.candidateCount(),
                summary.eligibleBytes()
            );
        }
    }

    private void runScheduledUpload() {
        checkpointRemoteCommitsOnce();
        reclaimCheckpointedWalOnce();
        tryScheduledUploadOnce().whenComplete((ignored, error) -> {
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
        pendingHead.set(null);
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private record CandidateSelection(
        List<SharedStorageEngine.UploadCandidate> candidates,
        long totalEligibleBytes
    ) {
    }

    private record PendingHead(
        SharedPartitionId partition,
        long segmentId,
        long position,
        long firstObservedMs
    ) {
        private static PendingHead from(SharedStorageEngine.UploadCandidate candidate, long firstObservedMs) {
            return new PendingHead(
                candidate.partition(),
                candidate.location().segmentId(),
                candidate.location().position(),
                firstObservedMs
            );
        }

        private boolean matches(SharedStorageEngine.UploadCandidate candidate) {
            return partition.equals(candidate.partition()) &&
                segmentId == candidate.location().segmentId() &&
                position == candidate.location().position();
        }
    }

    private record SelectionSummary(
        int trackedPartitions,
        int leaderPartitions,
        int openCommitWindows,
        int candidateCount,
        long eligibleBytes
    ) {
    }
}
