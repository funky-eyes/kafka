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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.storage.internals.checkpoint.PartitionMetadataFile;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.LoadedLogOffsets;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogLoader;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegmentFactory;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Kafka 4.3.x compatibility factory that builds a standard {@link UnifiedLog} over shared-storage physical segments.
 *
 * <p>This class deliberately reuses Kafka's UnifiedLog, producer state, leader epoch, ISR/HW and transaction logic.
 * Only the physical LocalLog/LogSegment implementation is substituted. Kafka's built-in tiered-storage path is never
 * enabled for a shared log; shared S3 durability is owned exclusively by {@link SharedStorageEngine}.</p>
 */
public final class SharedUnifiedLogFactory implements UnifiedLogFactory {
    private final SharedStorageEngine storage;
    private final SharedCommitProgress commitProgress;
    private final ConcurrentMap<File, LoadedSharedLog> loadedLogs = new ConcurrentHashMap<>();

    public SharedUnifiedLogFactory(SharedStorageEngine storage) {
        this(storage, new SharedCommitProgress());
    }

    public SharedUnifiedLogFactory(SharedStorageEngine storage, SharedCommitProgress commitProgress) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.commitProgress = Objects.requireNonNull(commitProgress, "commitProgress");
    }

    @Override
    public UnifiedLog create(UnifiedLogCreationContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        File dir = context.dir();
        Files.createDirectories(dir.toPath());
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(dir);
        if (UnifiedLog.isRemoteLogEnabled(
            context.remoteStorageSystemEnable(),
            context.config(),
            topicPartition.topic()
        )) {
            throw new IllegalArgumentException(
                "Kafka tiered storage and shared WAL/S3 storage cannot both be enabled for " + topicPartition);
        }

        Uuid effectiveTopicId = resolveAndPersistTopicId(
            dir,
            topicPartition,
            context.topicId(),
            context.logDirFailureChannel()
        );
        SharedPartitionId sharedPartition = new SharedPartitionId(
            effectiveTopicId.getMostSignificantBits(),
            effectiveTopicId.getLeastSignificantBits(),
            topicPartition.partition()
        );
        LogSegmentFactory segmentFactory = sharedSegmentFactory(storage, sharedPartition);

        LogSegments segments = new LogSegments(topicPartition);
        LeaderEpochFileCache leaderEpochCache = UnifiedLog.createLeaderEpochCache(
            dir,
            topicPartition,
            context.logDirFailureChannel(),
            Optional.empty(),
            context.scheduler()
        );
        ProducerStateManager producerStateManager = new ProducerStateManager(
            topicPartition,
            dir,
            context.maxTransactionTimeoutMs(),
            context.producerStateManagerConfig(),
            context.time()
        );

        // Shared object storage is intentionally not Kafka RemoteLogManager/tiered storage.
        LoadedLogOffsets offsets = new LogLoader(
            dir,
            topicPartition,
            context.config(),
            context.scheduler(),
            context.time(),
            context.logDirFailureChannel(),
            context.lastShutdownClean(),
            segments,
            context.logStartOffset(),
            context.recoveryPoint(),
            leaderEpochCache,
            producerStateManager,
            context.numRemainingSegments(),
            false,
            segmentFactory
        ).load();
        commitProgress.onLogLoaded(sharedPartition, offsets.logStartOffset());

        SharedLocalLog localLog = new SharedLocalLog(
            dir,
            context.config(),
            segments,
            offsets.recoveryPoint(),
            offsets.nextOffsetMetadata(),
            context.scheduler(),
            context.time(),
            topicPartition,
            context.logDirFailureChannel(),
            segmentFactory
        );
        SharedUnifiedLog log = new SharedUnifiedLog(
            offsets.logStartOffset(),
            localLog,
            context.brokerTopicStats(),
            context.producerIdExpirationCheckIntervalMs(),
            leaderEpochCache,
            producerStateManager,
            Optional.of(effectiveTopicId),
            false,
            context.logOffsetsListener()
        );
        log.addLogOffsetsListener(new LogOffsetsListener() {
            @Override
            public void onHighWatermarkUpdated(long highWatermark) {
                // Kafka may hold log locks here. Keep the shared side O(1), non-blocking and I/O-free.
                commitProgress.onHighWatermarkUpdated(sharedPartition, highWatermark);
            }
        });
        loadedLogs.put(
            dir.getAbsoluteFile(),
            new LoadedSharedLog(sharedPartition, localLog, log)
        );
        return log;
    }

    /**
     * Reconciles logs that were loaded before the Kafka-backed shared metadata image became readable.
     *
     * <p>Broker startup is deliberately allowed to complete while the RF&gt;1 metadata topic is unavailable so brokers
     * can be started sequentially. If the broker-local shared WAL and remote checkpoint were both lost, LogLoader can
     * therefore initially see an empty shared segment. Once metadata replay completes, remote recovery is serialized
     * against the complete leader/follower append critical section: rebuild the logical segment view, rebuild Kafka's
     * producer and leader-epoch compatibility state, then publish the reconstructed LEO/high watermark before allowing
     * appends to resume. This prevents an in-flight fetch that was issued from the pre-recovery LEO from appending into
     * an already-rebuilt offset index.</p>
     */
    public void reconcileRemoteStateAfterMetadataReplay() throws IOException {
        for (LoadedSharedLog loaded : loadedLogs.values()) {
            if (!Files.isDirectory(loaded.localLog().dir().toPath())) {
                continue;
            }

            loaded.log().withRemoteRecoveryFence(() -> {
                long logStartOffset = loaded.log().logStartOffset();
                long committedEnd = storage.remoteIndex()
                    .coverage(loaded.partition())
                    .contiguousEnd(logStartOffset);
                long currentEnd = loaded.localLog().logEndOffset();
                long materializedEnd = loaded.localLog().segments().activeSegment().readNextOffset();
                if (committedEnd <= currentEnd && committedEnd <= materializedEnd) {
                    loaded.log().installRemoteCommittedHighWatermarkFloor(committedEnd);
                    return null;
                }

                long recoveredEnd = reopenSegmentsFromSharedStorage(loaded);
                if (recoveredEnd < committedEnd) {
                    throw new IOException(
                        "Shared remote metadata covers through offset " + committedEnd +
                            " but reconstructed log " + loaded.log().topicPartition() +
                            " ends at " + recoveredEnd);
                }

                rebuildKafkaCompatibilityState(loaded, logStartOffset, recoveredEnd);
                // Reopening reads both remote objects and any surviving WAL. Publish the Kafka-visible LEO only after
                // the complete shared read view and compatibility state are rebuilt while appends remain fenced out.
                loaded.localLog().updateLogEndOffset(
                    Math.max(loaded.localLog().logEndOffset(), recoveredEnd)
                );
                loaded.log().installRemoteCommittedHighWatermarkFloor(committedEnd);
                return null;
            });
        }
    }

    private long reopenSegmentsFromSharedStorage(LoadedSharedLog loaded) throws IOException {
        SharedLocalLog localLog = loaded.localLog();
        List<Long> baseOffsets = List.copyOf(localLog.segments().baseOffsets());
        for (long baseOffset : baseOffsets) {
            SharedLogSegment replacement = SharedLogSegment.open(
                localLog.dir(),
                baseOffset,
                localLog.config(),
                localLog.time(),
                storage,
                loaded.partition(),
                true,
                ""
            );
            LogSegment previous = localLog.segments().add(replacement);
            if (previous != null && previous != replacement) {
                previous.close();
            }
        }
        return localLog.segments().activeSegment().readNextOffset();
    }

    private static void rebuildKafkaCompatibilityState(
        LoadedSharedLog loaded,
        long logStartOffset,
        long recoveredEnd
    ) throws IOException {
        ProducerStateManager producerStateManager = loaded.log().producerStateManager();
        LeaderEpochFileCache leaderEpochCache = loaded.log().leaderEpochCache();
        Optional<Integer> liveLeaderEpoch = resetLeaderEpochCacheForRemoteReplay(leaderEpochCache);

        producerStateManager.truncateFullyAndStartAt(logStartOffset);
        for (LogSegment segment : loaded.localLog().segments().values()) {
            segment.recover(producerStateManager, leaderEpochCache);
        }
        restoreLiveLeaderEpochAfterRemoteReplay(leaderEpochCache, liveLeaderEpoch, recoveredEnd);
    }

    /**
     * Startup can assign the current leader epoch while the shared log still looks empty. Those entries are anchored at
     * offset zero and would make historical remote batches look divergent after metadata replay. Preserve the newest
     * live epoch, but clear and persist the stale cache so {@link SharedLogSegment#recover} can reconstruct historical
     * epoch boundaries from the authoritative shared batches.
     */
    static Optional<Integer> resetLeaderEpochCacheForRemoteReplay(LeaderEpochFileCache leaderEpochCache) {
        Optional<Integer> liveLeaderEpoch = leaderEpochCache.latestEpoch();
        leaderEpochCache.clearAndFlush();
        return liveLeaderEpoch;
    }

    /**
     * If startup already advanced the partition epoch beyond the epochs carried by the recovered batches, re-anchor
     * that live epoch at the recovered LEO. This keeps the remote history intact while preserving Kafka's current-epoch
     * boundary for subsequent fetch/divergence checks.
     */
    static void restoreLiveLeaderEpochAfterRemoteReplay(
        LeaderEpochFileCache leaderEpochCache,
        Optional<Integer> liveLeaderEpoch,
        long recoveredEnd
    ) {
        if (liveLeaderEpoch.isEmpty()) {
            return;
        }
        Optional<Integer> recoveredLatestEpoch = leaderEpochCache.latestEpoch();
        if (recoveredLatestEpoch.isEmpty() || liveLeaderEpoch.get() > recoveredLatestEpoch.get()) {
            leaderEpochCache.assign(liveLeaderEpoch.get(), recoveredEnd);
        }
    }

    private static LogSegmentFactory sharedSegmentFactory(
        SharedStorageEngine storage,
        SharedPartitionId partition
    ) {
        return (segmentDir, baseOffset, segmentConfig, segmentTime, fileAlreadyExists,
                initFileSize, preallocate, fileSuffix) -> SharedLogSegment.open(
                    segmentDir,
                    baseOffset,
                    segmentConfig,
                    segmentTime,
                    storage,
                    partition,
                    fileAlreadyExists,
                    fileSuffix
                );
    }

    /**
     * Shared WAL records cannot be interpreted without a durable topic ID. Kafka normally schedules partition.metadata
     * flushing asynchronously; shared storage strengthens that boundary by persisting it before the first WAL append.
     */
    static Uuid resolveAndPersistTopicId(
        File dir,
        TopicPartition topicPartition,
        Optional<Uuid> requestedTopicId,
        LogDirFailureChannel logDirFailureChannel
    ) throws IOException {
        PartitionMetadataFile metadataFile = new PartitionMetadataFile(
            PartitionMetadataFile.newFile(dir),
            logDirFailureChannel
        );
        if (metadataFile.exists()) {
            Uuid persistedTopicId = metadataFile.read().topicId();
            if (requestedTopicId.filter(id -> !id.equals(persistedTopicId)).isPresent()) {
                throw new InconsistentTopicIdException(
                    "Tried to assign topic ID " + requestedTopicId.get() + " to shared log for " + topicPartition +
                        ", but partition.metadata contains " + persistedTopicId);
            }
            return persistedTopicId;
        }

        Uuid newTopicId = requestedTopicId.orElseThrow(() -> new IOException(
            "Shared storage requires a durable topic ID before loading " + topicPartition +
                "; partition.metadata is missing and no topic ID was supplied"));
        metadataFile.record(newTopicId);
        metadataFile.maybeFlush();
        return newTopicId;
    }

    private record LoadedSharedLog(
        SharedPartitionId partition,
        SharedLocalLog localLog,
        SharedUnifiedLog log
    ) {
    }
}
