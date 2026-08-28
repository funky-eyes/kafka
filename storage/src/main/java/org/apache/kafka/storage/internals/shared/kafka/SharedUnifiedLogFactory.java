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
import java.util.Objects;
import java.util.Optional;

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
        LogOffsetsListener offsetsListener = highWatermark -> {
            // Kafka invokes this callback while log locks may be held. Keep the shared side O(1), non-blocking and I/O-free.
            commitProgress.onHighWatermarkUpdated(sharedPartition, highWatermark);
            context.logOffsetsListener().onHighWatermarkUpdated(highWatermark);
        };
        return new UnifiedLog(
            offsets.logStartOffset(),
            localLog,
            context.brokerTopicStats(),
            context.producerIdExpirationCheckIntervalMs(),
            leaderEpochCache,
            producerStateManager,
            Optional.of(effectiveTopicId),
            false,
            offsetsListener
        );
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
}
