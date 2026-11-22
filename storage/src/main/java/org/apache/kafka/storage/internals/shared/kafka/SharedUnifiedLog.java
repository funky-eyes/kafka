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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogOffsetSnapshot;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.StorageAction;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared-storage specialization of {@link UnifiedLog} that fences Kafka appends against remote state recovery.
 *
 * <p>Metadata replay can reconstruct a durable remote prefix after the replica fetcher has already issued a fetch from
 * an older local LEO. Without a common synchronization boundary, recovery may replace/rebuild the active segment after
 * {@code UnifiedLog.append} validates the old LEO but before it mutates the segment. The stale append can then write into
 * a recovered offset index and fail with an out-of-order index offset.</p>
 *
 * <p>All production leader/follower appends acquire the read side of this fence before entering UnifiedLog's own log
 * lock. Remote recovery and Kafka truncation acquire the write side. This gives a single lock order (shared recovery
 * fence, then Kafka log lock) and keeps segment, LEO, producer-state and leader-epoch reconstruction atomic with
 * respect to both append validation and replica divergence truncation.</p>
 */
public final class SharedUnifiedLog extends UnifiedLog {
    private final ReentrantReadWriteLock remoteRecoveryFence = new ReentrantReadWriteLock();
    private volatile long remoteCommittedHighWatermarkFloor;
    private volatile SourceLogStartCallbackFence sourceLogStartTracker;
    private record ExperimentalVisibility(PartitionDurableOffsetTracker tracker, LogOffsetMetadata confirmed) { }
    private final AtomicReference<ExperimentalVisibility> experimentalVisibility = new AtomicReference<>();
    private record ExperimentalAsyncSettings(SharedPartitionId partition, int maxGroups, long maxBytes,
                                             Runnable recordNonBlockingRequest, Runnable recordEarlyAckAdmission) { }
    private volatile ExperimentalAsyncSettings experimentalAsyncSettings;
    private final Object experimentalTrackerLock = new Object();
    private PartitionDurableOffsetTracker experimentalOwnedTracker;

    void enableExperimentalAsyncAppend(SharedPartitionId partition, int maxGroups, long maxBytes) {
        enableExperimentalAsyncAppend(partition, maxGroups, maxBytes, () -> { }, () -> { });
    }

    void enableExperimentalAsyncAppend(SharedPartitionId partition, int maxGroups, long maxBytes,
                                       Runnable recordNonBlockingRequest, Runnable recordEarlyAckAdmission) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(recordNonBlockingRequest, "recordNonBlockingRequest");
        Objects.requireNonNull(recordEarlyAckAdmission, "recordEarlyAckAdmission");
        if (maxGroups <= 0 || maxBytes <= 0) throw new IllegalArgumentException("Invalid async append limits");
        if (experimentalAsyncSettings != null) throw new IllegalStateException("Async append is already configured");
        experimentalAsyncSettings = new ExperimentalAsyncSettings(partition, maxGroups, maxBytes,
            recordNonBlockingRequest, recordEarlyAckAdmission);
    }

    public void recordNonBlockingRequest() {
        Objects.requireNonNull(experimentalAsyncSettings, "async settings").recordNonBlockingRequest().run();
    }

    public void recordEarlyAckAdmission() {
        Objects.requireNonNull(experimentalAsyncSettings, "async settings").recordEarlyAckAdmission().run();
    }

    /** Only non-transactional records without producer state use the experimental pipeline. */
    public boolean allowsExperimentalAsyncAppend(MemoryRecords records) {
        if (experimentalAsyncSettings == null) return false;
        try {
            var batches = records.batches().iterator();
            if (!batches.hasNext()) return false;
            while (batches.hasNext()) {
                RecordBatch batch = batches.next();
                if (batch.hasProducerId() || batch.isTransactional() || batch.isControlBatch()) return false;
            }
            return true;
        } catch (RuntimeException invalidRecords) {
            return false;
        }
    }

    /** The caller must hold the partition leader read lock until this method returns. */
    public LogAppendInfo appendAsLeaderWithDurableWait(MemoryRecords records, int leaderEpoch, AppendOrigin origin,
        RequestLocal requestLocal, VerificationGuard verificationGuard, short transactionVersion) {
        CompletableFuture<LogAppendInfo> result = appendAsLeaderAsync(records, leaderEpoch, origin, requestLocal,
            verificationGuard, transactionVersion);
        awaitExperimental(result);
        return result.join();
    }

    /**
     * Reserves and publishes Kafka state synchronously, then completes only when the ordered WAL prefix is durable.
     * The caller must hold the partition leader read lock during this call and revalidate leadership before ACK.
     */
    public CompletableFuture<LogAppendInfo> appendAsLeaderAsync(MemoryRecords records, int leaderEpoch,
        AppendOrigin origin, RequestLocal requestLocal, VerificationGuard verificationGuard, short transactionVersion) {
        return submitAsLeader(records, leaderEpoch, origin, requestLocal, verificationGuard, transactionVersion)
            .confirmed();
    }

    /**
     * Separates successful local admission from ordered WAL confirmation.
     * The caller must independently fence leadership before using the admitted
     * result and must observe failures of the confirmation future.
     */
    public SubmittedAppend submitAsLeader(MemoryRecords records, int leaderEpoch,
        AppendOrigin origin, RequestLocal requestLocal, VerificationGuard verificationGuard, short transactionVersion) {
        ExperimentalAsyncSettings settings = Objects.requireNonNull(experimentalAsyncSettings, "async settings");
        TrackedAppend append;
        remoteRecoveryFence.readLock().lock();
        try {
            PartitionDurableOffsetTracker tracker;
            synchronized (experimentalTrackerLock) {
                if (experimentalOwnedTracker == null) {
                    experimentalOwnedTracker = new PartitionDurableOffsetTracker(1L, super.logEndOffset(),
                        Runnable::run, settings.maxGroups(), settings.maxBytes());
                }
                tracker = experimentalOwnedTracker;
            }
            append = appendAsLeaderTracked(records, leaderEpoch, origin, requestLocal, verificationGuard,
                transactionVersion, settings.partition(), tracker, 1L);
        } finally {
            remoteRecoveryFence.readLock().unlock();
        }
        return new SubmittedAppend(append.reserved().thenApply(ignored -> append.info()),
            append.confirmation().thenApply(ignored -> append.info()));
    }

    public record SubmittedAppend(CompletableFuture<LogAppendInfo> admitted, CompletableFuture<LogAppendInfo> confirmed) { }

    private static void awaitExperimental(CompletableFuture<?> future) {
        try {
            future.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new KafkaStorageException("Interrupted waiting for shared WAL durability", interrupted);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException runtime) throw runtime;
            if (cause instanceof Error error) throw error;
            throw new KafkaStorageException("Shared WAL durability failed", cause);
        }
    }

    /** Caller holds the recovery write lock, preventing new tracked admissions. */
    private void drainExperimentalAppend() {
        ExperimentalVisibility visible = experimentalVisibility.get();
        if (visible == null) return;
        PartitionDurableOffsetTracker tracker = visible.tracker();
        awaitExperimental(tracker.awaitReserved());
        if (tracker.confirmedOffset() != super.logEndOffset()) {
            throw new KafkaStorageException("Cannot retire an unconfirmed shared log append");
        }
        experimentalVisibility.updateAndGet(current -> current != null && current.tracker() == tracker ? null : current);
        synchronized (experimentalTrackerLock) {
            if (experimentalOwnedTracker == tracker) experimentalOwnedTracker = null;
        }
    }

    /** Fences a role or replication-factor transition against the experimental pipeline. */
    public void prepareForRoleChange() {
        if (experimentalAsyncSettings == null) return;
        remoteRecoveryFence.writeLock().lock();
        try {
            drainExperimentalAppend();
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }

    @Override
    protected boolean usesSharedStorage() {
        return true;
    }

    public SharedUnifiedLog(
        long logStartOffset,
        SharedLocalLog localLog,
        BrokerTopicStats brokerTopicStats,
        int producerIdExpirationCheckIntervalMs,
        LeaderEpochFileCache leaderEpochCache,
        ProducerStateManager producerStateManager,
        Optional<Uuid> topicId,
        boolean remoteStorageSystemEnable,
        LogOffsetsListener logOffsetsListener
    ) throws IOException {
        super(
            logStartOffset,
            localLog,
            brokerTopicStats,
            producerIdExpirationCheckIntervalMs,
            leaderEpochCache,
            producerStateManager,
            topicId,
            remoteStorageSystemEnable,
            logOffsetsListener
        );
    }

    /**
     * Install the native-log observation bridge after Kafka's constructor has
     * finished initializing the actual log start and high watermark.
     * This is only a local upload progress signal, never GC authorization.
     */
    void trackSourceLogStart(SharedCommitProgress progress, SharedPartitionId partition) {
        Objects.requireNonNull(progress, "progress");
        Objects.requireNonNull(partition, "partition");
        if (sourceLogStartTracker != null) {
            throw new IllegalStateException("Kafka source log-start tracker is already installed");
        }
        progress.onLogLoaded(partition, logStartOffset());
        progress.onHighWatermarkUpdated(partition, highWatermark());
        sourceLogStartTracker = new SourceLogStartCallbackFence(progress, partition);
    }

    /**
     * Observe only a successful native DeleteRecords or retention advancement.
     * Use the shared recovery-fence lock order for both update and publication.
     */
    @Override
    public boolean maybeIncrementLogStartOffset(long newLogStartOffset, LogStartOffsetIncrementReason reason) {
        // Kafka may call this while already holding its native log monitor.
        // Never acquire remoteRecoveryFence here: that reverses the lock order
        // used for truncation and metadata replay, and could deadlock.
        SourceLogStartCallbackFence tracker = sourceLogStartTracker;
        long generation = tracker == null ? -1L : tracker.captureGeneration();
        boolean changed = super.maybeIncrementLogStartOffset(newLogStartOffset, reason);
        if (changed && tracker != null) {
            tracker.observeAdvance(generation, logStartOffset());
        }
        return changed;
    }

    @Override
    public LogAppendInfo appendAsFollower(MemoryRecords records, int leaderEpoch) {
        if (experimentalAsyncSettings != null) {
            remoteRecoveryFence.writeLock().lock();
            try {
                drainExperimentalAppend();
                return super.appendAsFollower(records, leaderEpoch);
            } finally {
                remoteRecoveryFence.writeLock().unlock();
            }
        }
        remoteRecoveryFence.readLock().lock();
        try {
            return super.appendAsFollower(records, leaderEpoch);
        } finally {
            remoteRecoveryFence.readLock().unlock();
        }
    }

    @Override
    public LogAppendInfo appendAsLeader(
        MemoryRecords records,
        int leaderEpoch,
        AppendOrigin origin,
        RequestLocal requestLocal,
        VerificationGuard verificationGuard,
        short transactionVersion
    ) {
        if (experimentalAsyncSettings != null) {
            remoteRecoveryFence.writeLock().lock();
            try {
                drainExperimentalAppend();
                return super.appendAsLeader(records, leaderEpoch, origin, requestLocal, verificationGuard, transactionVersion);
            } finally {
                remoteRecoveryFence.writeLock().unlock();
            }
        }
        remoteRecoveryFence.readLock().lock();
        try {
            return super.appendAsLeader(
                records,
                leaderEpoch,
                origin,
                requestLocal,
                verificationGuard,
                transactionVersion
            );
        } finally {
            remoteRecoveryFence.readLock().unlock();
        }
    }

    /**
     * Experimental RF1 append entry point. It must not be exposed to broker
     * requests until leadership, fetch visibility, recovery and ACK gating use
     * the returned durable confirmation. The normal entry point stays synchronous.
     */
    TrackedAppend appendAsLeaderTracked(
        MemoryRecords records,
        int leaderEpoch,
        AppendOrigin origin,
        RequestLocal requestLocal,
        VerificationGuard verificationGuard,
        short transactionVersion,
        SharedPartitionId partition,
        PartitionDurableOffsetTracker tracker,
        long generation
    ) {
        remoteRecoveryFence.readLock().lock();
        try {
            return withLogLock(() -> {
                ExperimentalVisibility visibility = experimentalVisibility.get();
                if (visibility == null) {
                    LogOffsetMetadata initial = super.logEndOffsetMetadata();
                    if (initial.messageOffset != tracker.confirmedOffset()) {
                        throw new IllegalArgumentException("Tracker does not match the log end offset");
                    }
                    visibility = new ExperimentalVisibility(tracker, initial);
                    experimentalVisibility.set(visibility);
                }
                if (visibility.tracker() != tracker) throw new IllegalStateException("A different tracker owns this log");
                try (SharedAppendScope scope = SharedAppendScope.open(tracker, partition, generation)) {
                    try {
                        LogAppendInfo info = super.appendAsLeader(
                            records, leaderEpoch, origin, requestLocal, verificationGuard, transactionVersion);
                        LogOffsetMetadata appended = scope.hasReservation() ? super.logEndOffsetMetadata() : null;
                        scope.publish();
                        CompletableFuture<Void> confirmation = info.lastOffset() < 0L
                            ? CompletableFuture.completedFuture(null)
                            : tracker.awaitOffset(generation, Math.addExact(info.lastOffset(), 1L));
                        if (appended != null) {
                            confirmation = confirmation.thenRun(() -> experimentalVisibility.updateAndGet(current ->
                                current == null || current.tracker() != tracker ||
                                    current.confirmed().messageOffset >= appended.messageOffset
                                    ? current : new ExperimentalVisibility(tracker, appended)));
                        }
                        CompletableFuture<Void> reserved = scope.hasReservation() ? scope.walReserved() : confirmation;
                        return new TrackedAppend(info, confirmation, reserved);
                    } catch (RuntimeException | Error failure) {
                        if (scope.hasReservation()) tracker.fence(failure);
                        scope.fail(failure);
                        throw failure;
                    }
                }
            });
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        } finally {
            remoteRecoveryFence.readLock().unlock();
        }
    }

    @Override
    public long logEndOffset() {
        ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
        return visible == null ? super.logEndOffset() : visible.confirmed().messageOffset;
    }

    @Override
    public LogOffsetMetadata logEndOffsetMetadata() {
        ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
        return visible == null ? super.logEndOffsetMetadata() : visible.confirmed();
    }

    @Override
    public FetchDataInfo read(long startOffset, int maxLength, FetchIsolation isolation, boolean minOneMessage)
        throws IOException {
        ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
        if (visible != null && isolation == FetchIsolation.LOG_END) {
            // Replica fetch needs the confirmed leader end even when the HW
            // still waits for that replica; using HW here can deadlock RF2.
            return readWithMaxOffset(startOffset, maxLength, minOneMessage, visible.confirmed());
        }
        return super.read(startOffset, maxLength, isolation, minOneMessage);
    }

    @Override
    public LogOffsetSnapshot fetchOffsetSnapshot() throws IOException {
        if (experimentalVisibility == null || experimentalVisibility.get() == null) return super.fetchOffsetSnapshot();
        return withLogLock(() -> {
            LogOffsetSnapshot snapshot = super.fetchOffsetSnapshot();
            ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
            return visible == null ? snapshot : new LogOffsetSnapshot(snapshot.logStartOffset(),
                visible.confirmed(), snapshot.highWatermark(), snapshot.lastStableOffset());
        });
    }

    long experimentalReservedEndOffset() {
        return super.logEndOffset();
    }

    @Override
    public LogSegment roll(Optional<Long> expectedNextOffset) throws IOException {
        return withLogLock(() -> {
            ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
            if (visible != null) awaitExperimental(visible.tracker().awaitReserved());
            return super.roll(expectedNextOffset);
        });
    }

    @Override
    public void takeProducerSnapshot() throws IOException {
        withLogLock(() -> {
            ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
            if (visible != null) awaitExperimental(visible.tracker().awaitReserved());
            super.takeProducerSnapshot();
            return null;
        });
    }

    record TrackedAppend(LogAppendInfo info, CompletableFuture<Void> confirmation, CompletableFuture<Void> reserved) {
    }

    /**
     * Replica divergence truncation rebuilds Kafka's producer and leader-epoch state from the retained shared log.
     * Serialize it with metadata replay recovery so one path cannot publish a shorter LEO while the other still exposes
     * producer state reconstructed from a longer view.
     */
    @Override
    public boolean truncateTo(long targetOffset) {
        remoteRecoveryFence.writeLock().lock();
        try {
            if (experimentalAsyncSettings != null) drainExperimentalAppend();
            SourceLogStartCallbackFence tracker = sourceLogStartTracker;
            if (tracker != null) {
                tracker.invalidateForRebase();
            }
            boolean truncated = super.truncateTo(targetOffset);
            if (truncated) {
                observeLogRebase();
            }
            return truncated;
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }

    /**
     * Full truncation has the same compatibility-state mutation surface as partial truncation and therefore shares the
     * remote recovery write fence. The lock is reentrant because UnifiedLog.truncateTo may delegate to this method.
     */
    @Override
    public void truncateFullyAndStartAt(long newOffset, Optional<Long> logStartOffsetOpt) {
        remoteRecoveryFence.writeLock().lock();
        try {
            if (experimentalAsyncSettings != null) drainExperimentalAppend();
            SourceLogStartCallbackFence tracker = sourceLogStartTracker;
            if (tracker != null) {
                tracker.invalidateForRebase();
            }
            super.truncateFullyAndStartAt(newOffset, logStartOffsetOpt);
            observeLogRebase();
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }

    /**
     * Kafka's broker-local high-watermark checkpoint can lag remote metadata replay during startup. Once a remote
     * object is committed, its records were selected strictly below Kafka's high watermark and therefore form a
     * durable committed prefix. Do not allow a later stale local checkpoint to move the shared log below that prefix.
     */
    @Override
    public long updateHighWatermark(long highWatermark) throws IOException {
        return super.updateHighWatermark(applyRemoteCommittedHighWatermarkFloor(highWatermark));
    }

    @Override
    public long updateHighWatermark(LogOffsetMetadata highWatermark) throws IOException {
        ExperimentalVisibility visible = experimentalVisibility == null ? null : experimentalVisibility.get();
        LogOffsetMetadata bounded = visible != null && highWatermark.messageOffset > visible.confirmed().messageOffset
            ? visible.confirmed() : highWatermark;
        return super.updateHighWatermark(bounded);
    }

    /**
     * Followers can receive an older leader high watermark while their shared metadata view is converging. Preserve the
     * same remote-committed floor here without changing the behavior of ordinary Kafka logs.
     */
    @Override
    public Optional<Long> maybeUpdateHighWatermark(long highWatermark) throws IOException {
        return super.maybeUpdateHighWatermark(applyRemoteCommittedHighWatermarkFloor(highWatermark));
    }

    /**
     * Installs a durable remote lower bound without treating that lower bound as a replacement for Kafka's live high
     * watermark. The effective value is monotonic with respect to both the current Kafka HW and the remote committed
     * prefix: metadata replay may raise an older local checkpoint, but it must never lower an already-established HW.
     */
    public long installRemoteCommittedHighWatermarkFloor(long highWatermark) throws IOException {
        if (highWatermark < 0) {
            throw new IllegalArgumentException("remote committed high watermark must be non-negative");
        }
        remoteRecoveryFence.writeLock().lock();
        try {
            if (experimentalAsyncSettings != null) drainExperimentalAppend();
            remoteCommittedHighWatermarkFloor = Math.max(remoteCommittedHighWatermarkFloor, highWatermark);
            return super.updateHighWatermark(effectiveInstalledHighWatermark(
                highWatermark(),
                remoteCommittedHighWatermarkFloor
            ));
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }

    static long effectiveInstalledHighWatermark(long currentHighWatermark, long remoteCommittedFloor) {
        return Math.max(currentHighWatermark, remoteCommittedFloor);
    }

    long applyRemoteCommittedHighWatermarkFloor(long highWatermark) {
        return Math.max(highWatermark, remoteCommittedHighWatermarkFloor);
    }

    /**
     * Native truncation may reduce the source log start. A removed partition
     * is not re-created by a late observation.
     */
    private void observeLogRebase() {
        SourceLogStartCallbackFence tracker = sourceLogStartTracker;
        if (tracker != null) {
            tracker.observeRebase(logStartOffset());
        }
    }

    /**
     * Read-only Kafka source offsets under the same lock order as leader/follower
     * appends: remote-recovery read fence followed by Kafka's native log lock.
     * Call from broker diagnostic/retirement workers, NEVER from an existing
     * Kafka native log callback that already holds the log monitor.
     *
     * An inconsistent window is UNKNOWN, not a deletion permission. In
     * particular this observation proves neither WAL durability nor the
     * latest committed controller epoch.
     */
    Optional<NativeSourceWindow> captureNativeSourceWindow() throws IOException {
        remoteRecoveryFence.readLock().lock();
        try {
            return withLogLock(() -> NativeSourceWindow.fromBounds(
                logStartOffset(), highWatermark(), logEndOffset()
            ));
        } finally {
            remoteRecoveryFence.readLock().unlock();
        }
    }

    record NativeSourceWindow(long logStartOffset, long highWatermark, long logEndOffset) {
        NativeSourceWindow {
            if (logStartOffset < 0L || highWatermark < logStartOffset || logEndOffset < highWatermark) {
                throw new IllegalArgumentException("Invalid native Kafka source log offset window");
            }
        }

        static Optional<NativeSourceWindow> fromBounds(long start, long hw, long end) {
            if (start < 0L || hw < start || end < hw) {
                return Optional.empty();
            }
            return Optional.of(new NativeSourceWindow(start, hw, end));
        }
    }

    public <T> T withRemoteRecoveryFence(StorageAction<T, IOException> action) throws IOException {
        remoteRecoveryFence.writeLock().lock();
        try {
            if (experimentalAsyncSettings != null) drainExperimentalAppend();
            return withLogLock(action);
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        if (experimentalAsyncSettings == null) {
            super.close();
            return;
        }
        remoteRecoveryFence.writeLock().lock();
        try {
            try {
                drainExperimentalAppend();
            } catch (RuntimeException failure) {
                super.closeHandlers();
                throw failure;
            }
            super.close();
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }

    @Override
    public void delete() {
        if (experimentalAsyncSettings == null) {
            super.delete();
            return;
        }
        remoteRecoveryFence.writeLock().lock();
        try {
            drainExperimentalAppend();
            super.delete();
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }
}
