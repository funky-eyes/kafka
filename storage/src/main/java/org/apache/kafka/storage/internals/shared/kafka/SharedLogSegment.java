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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegmentOffsetOverflowException;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.RollParams;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TimestampOffset;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Kafka 4.3.x compatibility segment backed by SharedStorageEngine rather than a per-partition .log payload file.
 *
 * <p>The inherited FileRecords is intentionally a zero-byte placeholder. Kafka offset/time/transaction index files
 * remain available as lightweight compatibility metadata, while all Kafka RecordBatch payload bytes live only in the
 * broker-wide shared WAL and, after asynchronous upload, shared object storage.</p>
 *
 * <p>Kafka's relativePositionInSegment is represented by a virtual byte position: the cumulative serialized Kafka
 * RecordBatch bytes in this logical segment. It is stable for Kafka metadata purposes and deliberately unrelated to
 * the physical position of a batch in the broker-wide WAL.</p>
 */
public final class SharedLogSegment extends LogSegment {
    private final SharedStorageEngine storage;
    private final SharedPartitionId partition;
    private final int indexIntervalBytes;
    private final Time time;
    private final ConcurrentNavigableMap<Long, BatchMetadata> batches = new ConcurrentSkipListMap<>();

    private volatile int logicalSize;
    private volatile int bytesSinceLastIndexEntry;
    private volatile long lastOffset;
    private volatile long firstBatchTimestamp = RecordBatch.NO_TIMESTAMP;
    private volatile TimestampOffset maxTimestampAndOffset = TimestampOffset.UNKNOWN;
    private volatile long createdMs;
    private volatile long lastModifiedMs;
    private volatile int latestLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;

    private SharedLogSegment(
        FileRecords placeholder,
        LazyIndex<OffsetIndex> offsetIndex,
        LazyIndex<TimeIndex> timeIndex,
        TransactionIndex txnIndex,
        long baseOffset,
        LogConfig config,
        Time time,
        SharedStorageEngine storage,
        SharedPartitionId partition
    ) {
        super(
            placeholder,
            offsetIndex,
            timeIndex,
            txnIndex,
            baseOffset,
            config.indexInterval,
            config.randomSegmentJitter(),
            time
        );
        this.storage = storage;
        this.partition = partition;
        this.indexIntervalBytes = config.indexInterval;
        this.time = time;
        this.lastOffset = baseOffset - 1;
        this.createdMs = time.milliseconds();
        this.lastModifiedMs = createdMs;
    }

    public static SharedLogSegment open(
        File dir,
        long baseOffset,
        LogConfig config,
        Time time,
        SharedStorageEngine storage,
        SharedPartitionId partition,
        boolean fileAlreadyExists,
        String fileSuffix
    ) throws IOException {
        Files.createDirectories(dir.toPath());
        int maxIndexSize = config.maxIndexSize;
        // Payload must never be preallocated here. This file is a compatibility placeholder only.
        FileRecords placeholder = FileRecords.open(
            LogFileUtils.logFile(dir, baseOffset, fileSuffix),
            fileAlreadyExists,
            0,
            false
        );
        if (placeholder.sizeInBytes() != 0) {
            placeholder.close();
            throw new IOException("Shared logical segment payload placeholder is not empty: " +
                LogFileUtils.logFile(dir, baseOffset, fileSuffix));
        }
        return new SharedLogSegment(
            placeholder,
            LazyIndex.forOffset(LogFileUtils.offsetIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
            LazyIndex.forTime(LogFileUtils.timeIndexFile(dir, baseOffset, fileSuffix), baseOffset, maxIndexSize),
            new TransactionIndex(baseOffset, LogFileUtils.transactionIndexFile(dir, baseOffset, fileSuffix)),
            baseOffset,
            config,
            time,
            storage,
            partition
        );
    }

    @Override
    public void append(long largestOffset, MemoryRecords records) throws IOException {
        if (records.sizeInBytes() == 0) {
            return;
        }

        List<KafkaRecordBatchAdapter.SerializedBatch> serialized = KafkaRecordBatchAdapter.serializeBatches(records);
        if (serialized.isEmpty()) {
            return;
        }
        List<SharedStorageEngine.OwnedDataBatch> appendGroup = new ArrayList<>(serialized.size());
        for (KafkaRecordBatchAdapter.SerializedBatch batch : serialized) {
            if (!offsetIndex().canAppendOffset(batch.lastOffset())) {
                throw new LogSegmentOffsetOverflowException(this, batch.lastOffset());
            }
            appendGroup.add(new SharedStorageEngine.OwnedDataBatch(
                batch.leaderEpoch(), batch.firstOffset(), batch.lastOffset(), batch.bytes()));
        }

        await(storage.appendOwnedBatchGroup(partition, appendGroup));

        int position = logicalSize;
        for (KafkaRecordBatchAdapter.SerializedBatch batch : serialized) {
            int batchSize = batch.bytes().remaining();
            BatchMetadata metadata = new BatchMetadata(
                batch.firstOffset(),
                batch.lastOffset(),
                position,
                batchSize,
                batch.maxTimestamp(),
                batch.leaderEpoch()
            );
            batches.put(batch.firstOffset(), metadata);

            if (firstBatchTimestamp == RecordBatch.NO_TIMESTAMP) {
                firstBatchTimestamp = batch.maxTimestamp();
            }
            if (batch.maxTimestamp() > maxTimestampAndOffset.timestamp()) {
                maxTimestampAndOffset = new TimestampOffset(batch.maxTimestamp(), batch.lastOffset());
            }
            if (bytesSinceLastIndexEntry > indexIntervalBytes) {
                offsetIndex().append(batch.lastOffset(), position);
                timeIndex().maybeAppend(maxTimestampAndOffset.timestamp(), maxTimestampAndOffset.offset());
                bytesSinceLastIndexEntry = 0;
            }

            position = Math.addExact(position, batchSize);
            bytesSinceLastIndexEntry = Math.addExact(bytesSinceLastIndexEntry, batchSize);
            lastOffset = Math.max(lastOffset, batch.lastOffset());
            latestLeaderEpoch = Math.max(latestLeaderEpoch, batch.leaderEpoch());
        }
        logicalSize = position;
        lastModifiedMs = time.milliseconds();

        if (lastOffset != largestOffset) {
            throw new IllegalStateException(
                "Kafka append largest offset mismatch: expected=" + largestOffset + ", actual=" + lastOffset);
        }
    }

    @Override
    public int size() {
        return logicalSize;
    }

    @Override
    public TimestampOffset readMaxTimestampAndOffsetSoFar() {
        return maxTimestampAndOffset;
    }

    @Override
    public long maxTimestampSoFar() {
        return maxTimestampAndOffset.timestamp();
    }

    @Override
    public boolean shouldRoll(RollParams rollParams) throws IOException {
        boolean reachedRollMs = timeWaitedForRoll(rollParams.now(), rollParams.maxTimestampInMessages()) >
            rollParams.maxSegmentMs() - rollJitterMs();
        int size = logicalSize;
        return size > rollParams.maxSegmentBytes() - rollParams.messagesSize() ||
            (size > 0 && reachedRollMs) ||
            offsetIndex().isFull() ||
            timeIndex().isFull() ||
            !offsetIndex().canAppendOffset(rollParams.maxOffsetInMessages());
    }

    @Override
    public FetchDataInfo read(
        long startOffset,
        int maxSize,
        Optional<Long> maxPositionOpt,
        boolean minOneMessage
    ) throws IOException {
        if (maxSize < 0) {
            throw new IllegalArgumentException("Invalid max size " + maxSize + " for shared log read");
        }
        BatchMetadata start = findBatch(startOffset);
        if (start == null) {
            return null;
        }

        LogOffsetMetadata offsetMetadata = new LogOffsetMetadata(
            start.firstOffset,
            baseOffset(),
            start.virtualPosition
        );
        if (maxSize == 0 || maxPositionOpt.isEmpty()) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY);
        }
        long maxPosition = Math.min(maxPositionOpt.get(), Integer.MAX_VALUE);
        if (start.virtualPosition >= maxPosition) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY);
        }
        if (!minOneMessage && start.sizeInBytes > maxSize) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, true, Optional.empty());
        }

        int allowedBytes = minOneMessage ? Math.max(maxSize, start.sizeInBytes) : maxSize;
        List<WalRecord> selected = new ArrayList<>();
        int totalBytes = 0;
        for (BatchMetadata metadata : batches.tailMap(start.firstOffset, true).values()) {
            if (metadata.virtualPosition >= maxPosition) {
                break;
            }
            if (metadata.virtualPosition + metadata.sizeInBytes > maxPosition) {
                break;
            }
            if (!selected.isEmpty() && (long) totalBytes + metadata.sizeInBytes > allowedBytes) {
                break;
            }
            if (selected.isEmpty() && metadata.sizeInBytes > allowedBytes) {
                break;
            }
            WalRecord record = storage.readLocal(partition, metadata.firstOffset)
                .orElseThrow(() -> new IOException(
                    "Shared WAL batch missing for " + partition + " at offset " + metadata.firstOffset));
            selected.add(record);
            totalBytes = Math.addExact(totalBytes, metadata.sizeInBytes);
            if (totalBytes >= allowedBytes) {
                break;
            }
        }

        if (selected.isEmpty()) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY);
        }
        ByteBuffer data = ByteBuffer.allocate(totalBytes);
        for (WalRecord record : selected) {
            data.put(record.payload());
        }
        data.flip();
        return new FetchDataInfo(offsetMetadata, MemoryRecords.readableRecords(data));
    }

    @Override
    public OptionalLong fetchUpperBoundOffset(OffsetPosition startOffsetPosition, int fetchSize) {
        long upperPosition = (long) startOffsetPosition.position() + fetchSize;
        for (BatchMetadata metadata : batches.values()) {
            if (metadata.virtualPosition >= upperPosition) {
                return OptionalLong.of(metadata.firstOffset);
            }
        }
        return OptionalLong.empty();
    }

    @Override
    public int truncateTo(long offset) throws IOException {
        Map.Entry<Long, BatchMetadata> firstInvalid = firstBatchAtOrOverlapping(offset);
        if (firstInvalid == null) {
            return 0;
        }

        int oldSize = logicalSize;
        int newSize = firstInvalid.getValue().virtualPosition;
        await(storage.truncate(partition, Math.max(latestLeaderEpoch, 0), offset));

        batches.tailMap(firstInvalid.getKey(), true).clear();
        offsetIndex().truncateTo(offset);
        timeIndex().truncateTo(offset);
        txnIndex().truncateTo(offset);
        offsetIndex().resize(offsetIndex().maxIndexSize());
        timeIndex().resize(timeIndex().maxIndexSize());

        logicalSize = newSize;
        bytesSinceLastIndexEntry = 0;
        recomputeTailMetadata();
        if (logicalSize == 0) {
            createdMs = time.milliseconds();
            firstBatchTimestamp = RecordBatch.NO_TIMESTAMP;
        }
        return oldSize - newSize;
    }

    @Override
    public long readNextOffset() {
        return lastOffset < baseOffset() ? baseOffset() : lastOffset + 1;
    }

    @Override
    public boolean hasOverflow() throws IOException {
        return lastOffset >= baseOffset() && !offsetIndex().canAppendOffset(lastOffset);
    }

    @Override
    public int recover(ProducerStateManager producerStateManager, LeaderEpochFileCache leaderEpochCache) {
        // Shared recovery is driven from WAL/object metadata. Do not scan the intentionally empty FileRecords placeholder.
        return 0;
    }

    @Override
    public void onBecomeInactiveSegment() throws IOException {
        if (maxTimestampAndOffset.timestamp() >= 0) {
            timeIndex().maybeAppend(maxTimestampAndOffset.timestamp(), maxTimestampAndOffset.offset(), true);
        }
        offsetIndex().trimToValidSize();
        timeIndex().trimToValidSize();
        log().trim();
    }

    @Override
    public long timeWaitedForRoll(long now, long messageTimestamp) {
        if (firstBatchTimestamp >= 0) {
            return messageTimestamp - firstBatchTimestamp;
        }
        return now - createdMs;
    }

    @Override
    public long getFirstBatchTimestamp() {
        return firstBatchTimestamp >= 0 ? firstBatchTimestamp : Long.MAX_VALUE;
    }

    @Override
    public Optional<FileRecords.TimestampAndOffset> findOffsetByTimestamp(long timestampMs, long startingOffset) throws IOException {
        BatchMetadata start = findBatch(startingOffset);
        if (start == null) {
            return Optional.empty();
        }
        for (BatchMetadata metadata : batches.tailMap(start.firstOffset, true).values()) {
            if (metadata.lastOffset < startingOffset || metadata.maxTimestamp < timestampMs) {
                continue;
            }
            WalRecord walRecord = storage.readLocal(partition, metadata.firstOffset)
                .orElseThrow(() -> new IOException(
                    "Shared WAL batch missing for timestamp lookup at offset " + metadata.firstOffset));
            MemoryRecords records = MemoryRecords.readableRecords(walRecord.payload());
            for (RecordBatch batch : records.batches()) {
                if (batch.timestampType() == TimestampType.LOG_APPEND_TIME && batch.maxTimestamp() >= timestampMs) {
                    for (Record record : batch) {
                        if (record.offset() >= startingOffset) {
                            return Optional.of(new FileRecords.TimestampAndOffset(
                                batch.maxTimestamp(), record.offset(), leaderEpoch(batch)));
                        }
                    }
                } else {
                    for (Record record : batch) {
                        if (record.offset() >= startingOffset && record.timestamp() >= timestampMs) {
                            return Optional.of(new FileRecords.TimestampAndOffset(
                                record.timestamp(), record.offset(), leaderEpoch(batch)));
                        }
                    }
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public long lastModified() {
        return lastModifiedMs;
    }

    @Override
    public void setLastModified(long ms) throws IOException {
        lastModifiedMs = ms;
        super.setLastModified(ms);
    }

    private BatchMetadata findBatch(long offset) {
        Map.Entry<Long, BatchMetadata> floor = batches.floorEntry(offset);
        if (floor != null && floor.getValue().lastOffset >= offset) {
            return floor.getValue();
        }
        Map.Entry<Long, BatchMetadata> ceiling = batches.ceilingEntry(offset);
        return ceiling == null ? null : ceiling.getValue();
    }

    private Map.Entry<Long, BatchMetadata> firstBatchAtOrOverlapping(long offset) {
        Map.Entry<Long, BatchMetadata> floor = batches.floorEntry(offset);
        if (floor != null && floor.getValue().lastOffset >= offset) {
            return floor;
        }
        return batches.ceilingEntry(offset);
    }

    private void recomputeTailMetadata() {
        if (batches.isEmpty()) {
            lastOffset = baseOffset() - 1;
            maxTimestampAndOffset = TimestampOffset.UNKNOWN;
            latestLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
            return;
        }
        BatchMetadata last = batches.lastEntry().getValue();
        lastOffset = last.lastOffset;
        maxTimestampAndOffset = TimestampOffset.UNKNOWN;
        latestLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        firstBatchTimestamp = batches.firstEntry().getValue().maxTimestamp;
        for (BatchMetadata metadata : batches.values()) {
            if (metadata.maxTimestamp > maxTimestampAndOffset.timestamp()) {
                maxTimestampAndOffset = new TimestampOffset(metadata.maxTimestamp, metadata.lastOffset);
            }
            latestLeaderEpoch = Math.max(latestLeaderEpoch, metadata.leaderEpoch);
        }
    }

    private static Optional<Integer> leaderEpoch(RecordBatch batch) {
        return batch.partitionLeaderEpoch() >= 0
            ? Optional.of(batch.partitionLeaderEpoch())
            : Optional.empty();
    }

    private static <T> T await(CompletableFuture<T> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for shared WAL durability", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("Shared WAL append failed", cause);
        }
    }

    private record BatchMetadata(
        long firstOffset,
        long lastOffset,
        int virtualPosition,
        int sizeInBytes,
        long maxTimestamp,
        int leaderEpoch
    ) {
    }
}
