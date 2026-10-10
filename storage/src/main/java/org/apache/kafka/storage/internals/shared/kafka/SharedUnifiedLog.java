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
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogStartOffsetIncrementReason;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.StorageAction;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
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
     * Replica divergence truncation rebuilds Kafka's producer and leader-epoch state from the retained shared log.
     * Serialize it with metadata replay recovery so one path cannot publish a shorter LEO while the other still exposes
     * producer state reconstructed from a longer view.
     */
    @Override
    public boolean truncateTo(long targetOffset) {
        remoteRecoveryFence.writeLock().lock();
        try {
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

    public <T> T withRemoteRecoveryFence(StorageAction<T, IOException> action) throws IOException {
        remoteRecoveryFence.writeLock().lock();
        try {
            return withLogLock(action);
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }
}
