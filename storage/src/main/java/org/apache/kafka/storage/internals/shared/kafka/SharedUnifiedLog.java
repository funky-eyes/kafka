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
import org.apache.kafka.server.common.TransactionVersion;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.StorageAction;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.IOException;
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
 * lock. Remote recovery acquires the write side. This gives a single lock order (shared recovery fence, then Kafka log
 * lock) and makes recovery atomic with respect to the full append validation/state-update critical section.</p>
 */
public final class SharedUnifiedLog extends UnifiedLog {
    private final ReentrantReadWriteLock remoteRecoveryFence = new ReentrantReadWriteLock();

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

    public <T> T withRemoteRecoveryFence(StorageAction<T, IOException> action) throws IOException {
        remoteRecoveryFence.writeLock().lock();
        try {
            return action.execute();
        } finally {
            remoteRecoveryFence.writeLock().unlock();
        }
    }
}
