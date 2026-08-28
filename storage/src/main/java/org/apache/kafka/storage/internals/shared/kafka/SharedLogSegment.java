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
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.CompletedTxn;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LazyIndex;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.log.LogOffsetMetadata;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.LogSegmentOffsetOverflowException;
import org.apache.kafka.storage.internals.log.OffsetIndex;
import org.apache.kafka.storage.internals.log.OffsetPosition;
import org.apache.kafka.storage.internals.log.ProducerAppendInfo;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.RollParams;
import org.apache.kafka.storage.internals.log.TimeIndex;
import org.apache.kafka.storage.internals.log.TimestampOffset;
import org.apache.kafka.storage.internals.log.TransactionIndex;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

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
    private static final int RECOVERY_READ_BATCH_SIZE = 256;

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
        SharedLogSegment segment = new SharedLogSegment(
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
        if (fileAlreadyExists) {
            segment.restoreLogicalMetadata(dir);
        }
        return segment;
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
    public FileRecords.LogOffsetPosition translateOffset(long offset) {
        BatchMetadata metadata = findBatch(offset);
        if (metadata == null) {
            return null;
        }
        return new FileRecords.LogOffsetPosition(
            metadata.firstOffset,
            metadata.virtualPosition,
            metadata.sizeInBytes
        );
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

        LogOffsetMetadata offsetMetadata = offsetMetadata(start);
        Optional<ReadWindow> window = readWindow(start, maxSize, maxPositionOpt, minOneMessage);
        if (window.isEmpty()) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY);
        }
        if (window.get().firstEntryIncomplete()) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, true, Optional.empty());
        }

        SharedStorageEngine.LocalReadResult readResult = storage.readLocalBatches(
            partition,
            start.firstOffset,
            window.get().maxBytes(),
            minOneMessage
        );
        return materializeFetch(offsetMetadata, readResult);
    }

    private LogOffsetMetadata offsetMetadata(BatchMetadata start) {
        return new LogOffsetMetadata(start.firstOffset, baseOffset(), start.virtualPosition);
    }

    private Optional<ReadWindow> readWindow(
        BatchMetadata start,
        int maxSize,
        Optional<Long> maxPositionOpt,
        boolean minOneMessage
    ) {
        if (maxSize == 0 || maxPositionOpt.isEmpty()) {
            return Optional.empty();
        }
        long maxPosition = Math.min(maxPositionOpt.get(), Integer.MAX_VALUE);
        if (start.virtualPosition >= maxPosition ||
            (long) start.virtualPosition + start.sizeInBytes > maxPosition) {
            return Optional.empty();
        }
        if (!minOneMessage && start.sizeInBytes > maxSize) {
            return Optional.of(new ReadWindow(0, true));
        }

        int allowedBytes = minOneMessage ? Math.max(maxSize, start.sizeInBytes) : maxSize;
        long positionBytes = maxPosition - start.virtualPosition;
        int maxBytes = (int) Math.min(allowedBytes, positionBytes);
        return Optional.of(new ReadWindow(maxBytes, false));
    }

    private static FetchDataInfo materializeFetch(
        LogOffsetMetadata offsetMetadata,
        SharedStorageEngine.LocalReadResult readResult
    ) {
        if (readResult.records().isEmpty()) {
            return new FetchDataInfo(offsetMetadata, MemoryRecords.EMPTY, readResult.firstBatchIncomplete(), Optional.empty());
        }
        ByteBuffer data = ByteBuffer.allocate(readResult.sizeInBytes());
        for (WalRecord record : readResult.records()) {
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
    public int recover(ProducerStateManager producerStateManager, LeaderEpochFileCache leaderEpochCache) throws IOException {
        offsetIndex().reset();
        timeIndex().reset();
        txnIndex().reset();
        int lastIndexPosition = 0;
        for (WalBatch recovered : readWalBatches(baseOffset(), nextLogicalSegmentBaseOffset(log().file().getParentFile()))) {
            RecordBatch batch = recovered.batch();
            BatchMetadata metadata = batches.get(recovered.location().firstOffset());
            if (metadata == null) {
                throw new IOException("Missing recovered shared batch metadata at offset " + recovered.location().firstOffset());
            }
            if (metadata.virtualPosition - lastIndexPosition > indexIntervalBytes) {
                offsetIndex().append(metadata.lastOffset, metadata.virtualPosition);
                timeIndex().maybeAppend(maxTimestampAt(metadata.firstOffset), metadata.lastOffset);
                lastIndexPosition = metadata.virtualPosition;
            }
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                int leaderEpoch = batch.partitionLeaderEpoch();
                if (leaderEpoch >= 0 &&
                    (leaderEpochCache.latestEpoch().isEmpty() || leaderEpoch > leaderEpochCache.latestEpoch().get())) {
                    leaderEpochCache.assign(leaderEpoch, batch.baseOffset());
                }
                updateProducerState(producerStateManager, batch);
            }
        }
        offsetIndex().trimToValidSize();
        if (maxTimestampAndOffset.timestamp() >= 0) {
            timeIndex().maybeAppend(maxTimestampAndOffset.timestamp(), maxTimestampAndOffset.offset(), true);
        }
        timeIndex().trimToValidSize();
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

    private void restoreLogicalMetadata(File dir) throws IOException {
        long endOffsetExclusive = nextLogicalSegmentBaseOffset(dir);
        int position = 0;
        for (WalBatch recovered : readWalBatches(baseOffset(), endOffsetExclusive)) {
            WalLocation location = recovered.location();
            if (location.firstOffset() < baseOffset() || location.lastOffset() >= endOffsetExclusive) {
                throw new IOException(
                    "WAL batch crosses shared logical segment boundary: base=" + baseOffset() +
                        ", end=" + endOffsetExclusive + ", location=" + location);
            }
            RecordBatch batch = recovered.batch();
            BatchMetadata metadata = new BatchMetadata(
                location.firstOffset(),
                location.lastOffset(),
                position,
                location.payloadLength(),
                batch.maxTimestamp(),
                location.leaderEpoch()
            );
            batches.put(location.firstOffset(), metadata);
            position = Math.addExact(position, location.payloadLength());
        }
        logicalSize = position;
        bytesSinceLastIndexEntry = 0;
        recomputeTailMetadata();
        long placeholderLastModified = log().file().lastModified();
        if (placeholderLastModified > 0) {
            lastModifiedMs = placeholderLastModified;
        }
    }

    private List<WalBatch> readWalBatches(long startOffset, long endOffsetExclusive) throws IOException {
        List<WalLocation> locations = storage.localLocations(partition, startOffset, endOffsetExclusive);
        if (locations.isEmpty()) {
            return List.of();
        }
        List<WalBatch> result = new ArrayList<>(locations.size());
        for (int start = 0; start < locations.size(); start += RECOVERY_READ_BATCH_SIZE) {
            int end = Math.min(start + RECOVERY_READ_BATCH_SIZE, locations.size());
            List<WalLocation> chunk = locations.subList(start, end);
            List<WalRecord> records = storage.readLocalLocations(chunk);
            if (records.size() != chunk.size()) {
                throw new IOException("Shared WAL recovery read count mismatch");
            }
            for (int i = 0; i < chunk.size(); i++) {
                result.add(decodeWalBatch(chunk.get(i), records.get(i)));
            }
        }
        return List.copyOf(result);
    }

    private static WalBatch decodeWalBatch(WalLocation location, WalRecord record) throws IOException {
        if (record.payload().remaining() != location.payloadLength()) {
            throw new IOException("Shared WAL payload length mismatch at " + location);
        }
        MemoryRecords records = MemoryRecords.readableRecords(record.payload());
        Iterator<? extends RecordBatch> iterator = records.batches().iterator();
        if (!iterator.hasNext()) {
            throw new IOException("Shared WAL DATA entry contains no Kafka RecordBatch at " + location);
        }
        RecordBatch batch = iterator.next();
        if (iterator.hasNext()) {
            throw new IOException("Shared WAL DATA entry contains multiple Kafka RecordBatches at " + location);
        }
        batch.ensureValid();
        if (batch.sizeInBytes() != location.payloadLength() ||
            batch.baseOffset() != location.firstOffset() ||
            batch.lastOffset() != location.lastOffset() ||
            batch.partitionLeaderEpoch() != location.leaderEpoch()) {
            throw new IOException("Shared WAL Kafka RecordBatch metadata mismatch at " + location);
        }
        return new WalBatch(location, batch);
    }

    private long nextLogicalSegmentBaseOffset(File dir) throws IOException {
        try (Stream<Path> paths = Files.list(dir.toPath())) {
            return paths
                .filter(Files::isRegularFile)
                .map(Path::toFile)
                .filter(LogFileUtils::isLogFile)
                .mapToLong(LogFileUtils::offsetFromFile)
                .filter(offset -> offset > baseOffset())
                .min()
                .orElse(Long.MAX_VALUE);
        }
    }

    private long maxTimestampAt(long inclusiveLastOffset) {
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        for (BatchMetadata metadata : batches.headMap(inclusiveLastOffset, true).values()) {
            maxTimestamp = Math.max(maxTimestamp, metadata.maxTimestamp);
        }
        return maxTimestamp;
    }

    private void updateProducerState(ProducerStateManager producerStateManager, RecordBatch batch) throws IOException {
        if (batch.hasProducerId()) {
            ProducerAppendInfo appendInfo = producerStateManager.prepareUpdate(batch.producerId(), AppendOrigin.REPLICATION);
            Optional<CompletedTxn> completedTxn = appendInfo.append(batch, Optional.empty());
            producerStateManager.update(appendInfo);
            if (completedTxn.isPresent()) {
                CompletedTxn txn = completedTxn.get();
                long lastStableOffset = producerStateManager.lastStableOffset(txn);
                updateTxnIndex(txn, lastStableOffset);
                producerStateManager.completeTxn(txn);
            }
        }
        producerStateManager.updateMapEndOffset(batch.lastOffset() + 1);
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
            firstBatchTimestamp = RecordBatch.NO_TIMESTAMP;
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

    private record ReadWindow(int maxBytes, boolean firstEntryIncomplete) {
    }

    private record WalBatch(WalLocation location, RecordBatch batch) {
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
