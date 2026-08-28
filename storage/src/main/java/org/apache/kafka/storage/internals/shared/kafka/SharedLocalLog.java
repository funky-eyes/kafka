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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.LocalLog;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegmentFactory;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.SegmentDeletionReason;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Kafka-version adapter that keeps the standard {@link LocalLog} behavior while ensuring that every runtime segment
 * creation continues to use the shared-storage {@link LogSegmentFactory}. Keeping these small overrides outside Kafka
 * core localizes future LocalLog API churn to the adapter rather than creating upstream merge conflicts.
 */
public final class SharedLocalLog extends LocalLog {
    private final LogSegmentFactory segmentFactory;
    private final String sharedLogPrefix;

    public SharedLocalLog(
        File dir,
        LogConfig config,
        LogSegments segments,
        long recoveryPoint,
        LogOffsetMetadata nextOffsetMetadata,
        Scheduler scheduler,
        Time time,
        TopicPartition topicPartition,
        LogDirFailureChannel logDirFailureChannel,
        LogSegmentFactory segmentFactory
    ) {
        super(
            dir,
            config,
            segments,
            recoveryPoint,
            nextOffsetMetadata,
            scheduler,
            time,
            topicPartition,
            logDirFailureChannel
        );
        this.segmentFactory = Objects.requireNonNull(segmentFactory, "segmentFactory");
        this.sharedLogPrefix = "[SharedLocalLog partition=" + topicPartition + ", dir=" + dir + "] ";
    }

    @Override
    public LogSegment createAndDeleteSegment(
        long newOffset,
        LogSegment segmentToDelete,
        boolean asyncDelete,
        SegmentDeletionReason reason
    ) throws IOException {
        if (newOffset == segmentToDelete.baseOffset()) {
            segmentToDelete.changeFileSuffixes("", LogFileUtils.DELETED_FILE_SUFFIX);
        }
        LogSegment newSegment = openSharedSegment(newOffset);
        segments().add(newSegment);

        reason.logReason(List.of(segmentToDelete));
        if (newOffset != segmentToDelete.baseOffset()) {
            segments().remove(segmentToDelete.baseOffset());
        }
        LocalLog.deleteSegmentFiles(
            List.of(segmentToDelete),
            asyncDelete,
            dir(),
            topicPartition(),
            config(),
            scheduler(),
            logDirFailureChannel(),
            sharedLogPrefix
        );
        return newSegment;
    }

    @Override
    public LogSegment roll(Long expectedNextOffset) {
        return LocalLog.maybeHandleIOException(
            logDirFailureChannel(),
            parentDir(),
            () -> "Error while rolling shared log segment for " + topicPartition() + " in dir " + dir().getParent(),
            () -> rollInternal(expectedNextOffset)
        );
    }

    private LogSegment rollInternal(Long expectedNextOffset) throws IOException {
        long start = time().hiResClockMs();
        checkIfMemoryMappedBufferClosed();
        long newOffset = Math.max(expectedNextOffset, logEndOffset());
        LogSegment activeSegment = segments().activeSegment();
        if (segments().contains(newOffset)) {
            return rollAtExistingOffset(expectedNextOffset, newOffset, activeSegment, start);
        }
        if (!segments().isEmpty() && newOffset < activeSegment.baseOffset()) {
            throw new KafkaException(
                "Trying to roll a new shared log segment for topic partition " + topicPartition() +
                    " with start offset " + newOffset + " =max(provided offset = " + expectedNextOffset +
                    ", LEO = " + logEndOffset() + ") lower than start offset of the active segment " + activeSegment);
        }

        prepareFilesForRoll(newOffset);
        segments().lastSegment().ifPresent(segment -> {
            try {
                segment.onBecomeInactiveSegment();
            } catch (IOException e) {
                throw new SegmentRollIOException(e);
            }
        });

        LogSegment newSegment;
        try {
            newSegment = openSharedSegment(newOffset);
        } catch (SegmentRollIOException e) {
            throw e.ioException;
        }
        segments().add(newSegment);
        updateLogEndOffset(nextOffsetMetadata().messageOffset);
        logger().info("Rolled new shared log segment at offset {} in {} ms.",
            newOffset, time().hiResClockMs() - start);
        return newSegment;
    }

    private LogSegment rollAtExistingOffset(
        Long expectedNextOffset,
        long newOffset,
        LogSegment activeSegment,
        long start
    ) throws IOException {
        if (activeSegment.baseOffset() == newOffset && activeSegment.size() == 0) {
            logger().warn(
                "Trying to roll a new shared log segment with start offset {}=max(provided offset = {}, LEO = {}) " +
                    "while it already exists and is active with size 0. Size of time index: {}, size of offset index: {}.",
                newOffset,
                expectedNextOffset,
                logEndOffset(),
                activeSegment.timeIndex().entries(),
                activeSegment.offsetIndex().entries()
            );
            LogSegment newSegment = createAndDeleteSegment(
                newOffset,
                activeSegment,
                true,
                toDelete -> logger().info("Deleting segments as part of shared log roll: {}", toDelete.stream()
                    .map(LogSegment::toString)
                    .collect(Collectors.joining(", ")))
            );
            updateLogEndOffset(nextOffsetMetadata().messageOffset);
            logger().info("Rolled new shared log segment at offset {} in {} ms.",
                newOffset, time().hiResClockMs() - start);
            return newSegment;
        }
        throw new KafkaException(
            "Trying to roll a new shared log segment for topic partition " + topicPartition() +
                " with start offset " + newOffset + " =max(provided offset = " + expectedNextOffset +
                ", LEO = " + logEndOffset() + ") while it already exists. Existing segment is " +
                segments().get(newOffset) + "."
        );
    }

    private void prepareFilesForRoll(long newOffset) throws IOException {
        File logFile = LogFileUtils.logFile(dir(), newOffset, "");
        File offsetIdxFile = LogFileUtils.offsetIndexFile(dir(), newOffset);
        File timeIdxFile = LogFileUtils.timeIndexFile(dir(), newOffset);
        File txnIdxFile = LogFileUtils.transactionIndexFile(dir(), newOffset);
        for (File file : List.of(logFile, offsetIdxFile, timeIdxFile, txnIdxFile)) {
            if (file.exists()) {
                logger().warn("Newly rolled shared segment file {} already exists; deleting it first", file.getAbsolutePath());
                Files.delete(file.toPath());
            }
        }
    }

    private LogSegment openSharedSegment(long baseOffset) throws IOException {
        return segmentFactory.open(
            dir(),
            baseOffset,
            config(),
            time(),
            false,
            config().initFileSize(),
            config().preallocate,
            ""
        );
    }

    private static final class SegmentRollIOException extends RuntimeException {
        private final IOException ioException;

        private SegmentRollIOException(IOException ioException) {
            super(ioException);
            this.ioException = ioException;
        }
    }
}
