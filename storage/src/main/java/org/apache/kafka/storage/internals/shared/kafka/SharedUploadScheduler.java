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
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataRecordCodec;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Asynchronously converts Kafka-committed leader WAL batches into cross-partition shared objects.
 *
 * <p>Kafka callbacks never call this class. They only update {@link SharedCommitProgress}. This scheduler snapshots
 * those commit windows on its own thread, accepts current local leaders only, filters candidates strictly below Kafka
 * HW, merges them by logical WAL order and delegates the durable object/metadata protocol to
 * {@link SharedObjectUploader}.</p>
 *
 * <p>The periodic interval is only an evaluation cadence. Scheduled uploads are started when the eligible committed
 * bytes reach the target object size, when the oldest current candidate reaches the configured maximum linger, or when
 * broker-wide WAL usage crosses the configured pressure threshold. Multiple immutable objects may be in flight, but a
 * logical WAL record is reserved by at most one upload until that upload completes. This lets the object-store I/O
 * pool execute PUTs concurrently without allowing two concurrent selections to publish the same WAL range.</p>
 *
 * <p>The same maintenance thread persists remote COMMIT references into the broker-local crash-safe checkpoint and
 * then reclaims only the rotating WAL prefix covered by that durable checkpoint. Metadata-consumer callbacks only
 * enqueue checkpoint work and never perform filesystem I/O. Maintenance failure remains observable through
 * {@link #lastFailure()} and fails closed for reclamation without blocking future asynchronous uploads.</p>
 */
public final class SharedUploadScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SharedUploadScheduler.class);
    private static final long CLOSE_WAIT_LOG_INTERVAL_SECONDS = 30L;
    static final long DEFAULT_MAX_LINGER_MS = 1_000L;
    static final int DEFAULT_WAL_PRESSURE_PERCENT = 70;
    static final int DEFAULT_MAX_INFLIGHT = 1;

    private final SharedStorageEngine engine;
    private final SharedCommitProgress commitProgress;
    private final SharedObjectUploader uploader;
    private final LongSupplier objectIdSupplier;
    private final LongSupplier currentTimeMsSupplier;
    private final long targetObjectBytes;
    private final long maxLingerMs;
    private final int walPressurePercent;
    private final int maxInflight;
    private final AtomicInteger uploadsInProgress = new AtomicInteger();
    private final Set<CandidateKey> reservedCandidates = ConcurrentHashMap.newKeySet();
    private final Object uploadFailureLock = new Object();
    private final Map<CandidateKey, FailedCandidate> failedCandidates = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> lastMaintenanceFailure = new AtomicReference<>();
    private Throwable lastSchedulingFailure;
    private final AtomicReference<SelectionSummary> lastSelectionSummary = new AtomicReference<>();
    private final AtomicReference<PendingHead> pendingHead = new AtomicReference<>();
    private final Object uploadDrainMonitor = new Object();

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
            DEFAULT_WAL_PRESSURE_PERCENT,
            DEFAULT_MAX_INFLIGHT
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
        this(
            engine,
            commitProgress,
            uploader,
            objectIdSupplier,
            currentTimeMsSupplier,
            targetObjectBytes,
            maxLingerMs,
            walPressurePercent,
            DEFAULT_MAX_INFLIGHT
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
        int walPressurePercent,
        int maxInflight
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
        if (maxInflight <= 0) {
            throw new IllegalArgumentException("maxInflight must be positive");
        }
        this.targetObjectBytes = targetObjectBytes;
        this.maxLingerMs = maxLingerMs;
        this.walPressurePercent = walPressurePercent;
        this.maxInflight = maxInflight;
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
                "walPressurePercent={}, maxInflight={}",
            intervalMs,
            targetObjectBytes,
            maxLingerMs,
            walPressurePercent,
            maxInflight
        );
        executor.scheduleWithFixedDelay(this::runScheduledUpload, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Forces at most one new asynchronous object upload and returns empty when there is no committed leader work or all
     * upload slots are occupied. This is primarily useful for deterministic maintenance/tests; the periodic scheduler
     * may fill multiple available slots per evaluation. This method never blocks on object-store or metadata-store I/O.
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
        if (!tryAcquireUploadSlot()) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("Shared upload scheduler is closed"));
            }
            return CompletableFuture.completedFuture(Optional.empty());
        }
        return selectForUpload(applyTriggerGate);
    }

    private boolean tryAcquireUploadSlot() {
        while (!closed.get()) {
            int current = uploadsInProgress.get();
            if (current >= maxInflight) {
                return false;
            }
            if (uploadsInProgress.compareAndSet(current, current + 1)) {
                if (closed.get()) {
                    releaseUploadSlot();
                    return false;
                }
                return true;
            }
        }
        return false;
    }

    private void releaseUploadSlot() {
        synchronized (uploadDrainMonitor) {
            int remaining = uploadsInProgress.decrementAndGet();
            if (remaining < 0) {
                uploadsInProgress.incrementAndGet();
                throw new IllegalStateException("Shared upload slot accounting underflow");
            }
            uploadDrainMonitor.notifyAll();
        }
    }

    private CompletableFuture<Optional<SharedObjectMetadata>> selectForUpload(boolean applyTriggerGate) {
        final CandidateSelection selection;
        try {
            selection = selectCandidateBatch();
        } catch (RuntimeException e) {
            return synchronousFailure(e);
        }
        if (selection.candidates().isEmpty()) {
            clearSchedulingFailure();
            pendingHead.set(null);
            releaseUploadSlot();
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
        clearSchedulingFailure();
        if (applyTriggerGate && !shouldUpload(selection, nowMs)) {
            releaseUploadSlot();
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (!reserve(selection.candidates())) {
            releaseUploadSlot();
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
            releaseReservation(selection.candidates());
            recordCandidateFailure(selection.candidates(), e);
            releaseUploadSlot();
            LOG.warn("Shared object upload failed before the asynchronous object PUT started", e);
            return CompletableFuture.failedFuture(e);
        }
        return result.whenComplete((ignored, error) -> completeUpload(selection.candidates(), error));
    }

    private void completeUpload(List<SharedStorageEngine.UploadCandidate> candidates, Throwable error) {
        releaseReservation(candidates);
        if (error == null) {
            clearCandidateFailure(candidates);
            clearCompletedPendingHead(candidates);
        } else {
            recordCandidateFailure(candidates, error);
            LOG.warn("Shared object upload failed", error);
        }
        releaseUploadSlot();
    }

    private void clearCompletedPendingHead(List<SharedStorageEngine.UploadCandidate> candidates) {
        PendingHead current = pendingHead.get();
        if (current == null) {
            return;
        }
        for (SharedStorageEngine.UploadCandidate candidate : candidates) {
            if (current.matches(candidate)) {
                pendingHead.compareAndSet(current, null);
                return;
            }
        }
    }

    private void recordCandidateFailure(
        List<SharedStorageEngine.UploadCandidate> candidates,
        Throwable error
    ) {
        synchronized (uploadFailureLock) {
            for (SharedStorageEngine.UploadCandidate candidate : candidates) {
                failedCandidates.put(
                    CandidateKey.from(candidate),
                    new FailedCandidate(candidate, error)
                );
            }
        }
    }

    private void clearCandidateFailure(List<SharedStorageEngine.UploadCandidate> candidates) {
        synchronized (uploadFailureLock) {
            for (SharedStorageEngine.UploadCandidate candidate : candidates) {
                failedCandidates.remove(CandidateKey.from(candidate));
            }
        }
    }

    /**
     * Reconciles upload failure evidence without relying on the current bounded selection window.
     *
     * <p>A failed candidate can sit beyond the next object's byte/range budget, so absence from a bounded scan says
     * nothing about whether that failure is still live. Validate each failed key against the current leader snapshot,
     * WAL generation and remote coverage instead.</p>
     */
    private void reconcileFailedCandidates(
        Map<SharedPartitionId, SharedCommitProgress.PartitionProgress> snapshot
    ) {
        synchronized (uploadFailureLock) {
            failedCandidates.entrySet().removeIf(entry -> {
                SharedCommitProgress.PartitionProgress progress = snapshot.get(entry.getKey().partition());
                if (progress == null || !progress.isLeader() ||
                    progress.highWatermark() <= progress.logStartOffset()) {
                    return true;
                }
                return !engine.isUploadCandidateCurrent(
                    entry.getValue().candidate(),
                    progress.logStartOffset(),
                    progress.highWatermark()
                );
            });
        }
    }

    private void clearSchedulingFailure() {
        synchronized (uploadFailureLock) {
            lastSchedulingFailure = null;
        }
    }

    private boolean reserve(List<SharedStorageEngine.UploadCandidate> candidates) {
        List<CandidateKey> acquired = new ArrayList<>(candidates.size());
        for (SharedStorageEngine.UploadCandidate candidate : candidates) {
            CandidateKey key = CandidateKey.from(candidate);
            if (!reservedCandidates.add(key)) {
                acquired.forEach(reservedCandidates::remove);
                return false;
            }
            acquired.add(key);
        }
        return true;
    }

    private void releaseReservation(List<SharedStorageEngine.UploadCandidate> candidates) {
        for (SharedStorageEngine.UploadCandidate candidate : candidates) {
            reservedCandidates.remove(CandidateKey.from(candidate));
        }
    }

    private boolean shouldUpload(CandidateSelection selection, long nowMs) {
        if (selection.totalEligibleBytes() >= targetObjectBytes) {
            return true;
        }
        if (selection.candidates().size() >= SharedMetadataRecordCodec.MAX_COMMITTED_OBJECT_RANGES) {
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
        synchronized (uploadFailureLock) {
            lastSchedulingFailure = error;
        }
        releaseUploadSlot();
        LOG.warn("Shared upload scheduling failed before the asynchronous object PUT started", error);
        return CompletableFuture.failedFuture(error);
    }

    public Optional<Throwable> lastFailure() {
        Throwable maintenance = lastMaintenanceFailure.get();
        if (maintenance != null) {
            return Optional.of(maintenance);
        }
        synchronized (uploadFailureLock) {
            if (lastSchedulingFailure != null) {
                return Optional.of(lastSchedulingFailure);
            }
            return failedCandidates.values().stream().map(FailedCandidate::error).findFirst();
        }
    }

    int uploadsInProgress() {
        return uploadsInProgress.get();
    }

    int reservedCandidateCount() {
        return reservedCandidates.size();
    }

    int uploadCandidateCount() {
        SelectionSummary summary = lastSelectionSummary.get();
        return summary == null ? 0 : summary.candidateCount();
    }

    long eligibleUploadBytes() {
        SelectionSummary summary = lastSelectionSummary.get();
        return summary == null ? 0L : summary.eligibleBytes();
    }

    boolean uploadFailurePresent() {
        synchronized (uploadFailureLock) {
            return lastSchedulingFailure != null || !failedCandidates.isEmpty();
        }
    }

    boolean maintenanceFailurePresent() {
        return lastMaintenanceFailure.get() != null;
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
        PriorityQueue<CursorHead> heads = new PriorityQueue<>(
            Comparator.comparingLong(head -> head.candidate().location().walOffset())
        );
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
            SharedStorageEngine.UploadCandidateCursor cursor = engine.uploadCandidateCursor(
                entry.getKey(),
                progress.logStartOffset(),
                progress.highWatermark()
            );
            cursor.next().ifPresent(candidate -> heads.add(new CursorHead(cursor, candidate)));
        }

        reconcileFailedCandidates(snapshot);

        List<SharedStorageEngine.UploadCandidate> selected = new ArrayList<>(
            Math.min(SharedMetadataRecordCodec.MAX_COMMITTED_OBJECT_RANGES, 256)
        );
        long selectedBytes = 0L;
        while (!heads.isEmpty() &&
            selected.size() < SharedMetadataRecordCodec.MAX_COMMITTED_OBJECT_RANGES) {
            CursorHead head = heads.poll();
            SharedStorageEngine.UploadCandidate candidate = head.candidate();
            head.cursor().next().ifPresent(next -> heads.add(new CursorHead(head.cursor(), next)));

            if (reservedCandidates.contains(CandidateKey.from(candidate))) {
                continue;
            }

            int payloadBytes = candidate.location().payloadLength();
            long nextSelectedBytes = Math.addExact(selectedBytes, payloadBytes);
            if (!selected.isEmpty() && nextSelectedBytes > targetObjectBytes) {
                break;
            }
            selected.add(candidate);
            selectedBytes = nextSelectedBytes;
            if (selectedBytes >= targetObjectBytes) {
                break;
            }
        }

        logSelectionSummary(new SelectionSummary(
            snapshot.size(),
            leaderPartitions,
            openCommitWindows,
            selected.size(),
            selectedBytes,
            reservedCandidates.size(),
            uploadsInProgress.get()
        ));

        return new CandidateSelection(List.copyOf(selected), selectedBytes);
    }

    private void logSelectionSummary(SelectionSummary summary) {
        SelectionSummary previous = lastSelectionSummary.getAndSet(summary);
        if (!summary.equals(previous)) {
            LOG.info(
                "Shared upload gate state changed: trackedPartitions={}, leaders={}, openCommitWindows={}, " +
                    "candidates={}, eligibleBytes={}, reservedCandidates={}, uploadsInProgress={}",
                summary.trackedPartitions(),
                summary.leaderPartitions(),
                summary.openCommitWindows(),
                summary.candidateCount(),
                summary.eligibleBytes(),
                summary.reservedCandidateCount(),
                summary.uploadsInProgress()
            );
        }
    }

    private void runScheduledUpload() {
        checkpointRemoteCommitsOnce();
        reclaimCheckpointedWalOnce();
        for (int index = 0; index < maxInflight; index++) {
            tryScheduledUploadOnce();
        }
    }

    /**
     * Stops accepting and scheduling new uploads without waiting for already-started asynchronous uploads to finish.
     *
     * <p>The S3 extension uses this first shutdown phase before closing the metadata store. Closing metadata then
     * completes any upload futures waiting for metadata application exceptionally, after which {@link #close()} can
     * safely drain the remaining bounded object-store work.</p>
     *
     * @return true if the caller was interrupted while waiting for the scheduler executor to stop
     */
    public synchronized boolean stop() {
        if (!closed.compareAndSet(false, true)) {
            return false;
        }
        pendingHead.set(null);
        ScheduledExecutorService executorToStop = executor;
        if (executorToStop != null) {
            executorToStop.shutdownNow();
            executor = null;
        }
        return awaitExecutorStop(executorToStop);
    }

    @Override
    public void close() {
        boolean interrupted = stop();
        interrupted |= awaitUploadDrain();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean awaitExecutorStop(ScheduledExecutorService executorToStop) {
        if (executorToStop == null) {
            return false;
        }
        boolean interrupted = false;
        while (!executorToStop.isTerminated()) {
            try {
                if (!executorToStop.awaitTermination(CLOSE_WAIT_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("Shared upload scheduler executor is still running during close; interrupting it again");
                    executorToStop.shutdownNow();
                }
            } catch (InterruptedException e) {
                interrupted = true;
                executorToStop.shutdownNow();
            }
        }
        return interrupted;
    }

    private boolean awaitUploadDrain() {
        boolean interrupted = false;
        synchronized (uploadDrainMonitor) {
            while (uploadsInProgress.get() > 0) {
                try {
                    TimeUnit.SECONDS.timedWait(uploadDrainMonitor, CLOSE_WAIT_LOG_INTERVAL_SECONDS);
                    if (uploadsInProgress.get() > 0) {
                        LOG.warn(
                            "Still draining {} shared object upload(s) during scheduler close",
                            uploadsInProgress.get()
                        );
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        return interrupted;
    }

    private record CandidateSelection(
        List<SharedStorageEngine.UploadCandidate> candidates,
        long totalEligibleBytes
    ) {
    }

    private record CursorHead(
        SharedStorageEngine.UploadCandidateCursor cursor,
        SharedStorageEngine.UploadCandidate candidate
    ) {
    }

    private record FailedCandidate(
        SharedStorageEngine.UploadCandidate candidate,
        Throwable error
    ) {
    }

    private record CandidateKey(
        SharedPartitionId partition,
        long walOffset
    ) {
        private static CandidateKey from(SharedStorageEngine.UploadCandidate candidate) {
            return new CandidateKey(
                candidate.partition(),
                candidate.location().walOffset()
            );
        }
    }

    private record PendingHead(
        SharedPartitionId partition,
        long walOffset,
        long firstObservedMs
    ) {
        private static PendingHead from(SharedStorageEngine.UploadCandidate candidate, long firstObservedMs) {
            return new PendingHead(
                candidate.partition(),
                candidate.location().walOffset(),
                firstObservedMs
            );
        }

        private boolean matches(SharedStorageEngine.UploadCandidate candidate) {
            return partition.equals(candidate.partition()) &&
                walOffset == candidate.location().walOffset();
        }
    }

    private record SelectionSummary(
        int trackedPartitions,
        int leaderPartitions,
        int openCommitWindows,
        int candidateCount,
        long eligibleBytes,
        int reservedCandidateCount,
        int uploadsInProgress
    ) {
    }
}
