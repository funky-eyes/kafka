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
package org.apache.kafka.storage.internals.shared;

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.PartitionWalIndex;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalAppendResult;
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalPartitionKey;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka-independent core of the shared storage data plane.
 *
 * <p>Kafka owns leader election, ISR and HW. This class owns durable physical WAL state and immutable remote
 * coverage. Callers may only request upload candidates below Kafka's current HW; the engine never invents a
 * commit boundary of its own.</p>
 */
public final class SharedStorageEngine implements AutoCloseable {
    private final SharedWal wal;
    private final PartitionWalIndex walIndex = new PartitionWalIndex();
    private final RemoteObjectIndex remoteIndex = new RemoteObjectIndex();

    public SharedStorageEngine(SharedWal wal) throws IOException {
        this.wal = Objects.requireNonNull(wal, "wal");
        wal.replay(walIndex::apply);
    }

    /** Defensive-copy append for generic callers. */
    public CompletableFuture<WalLocation> appendData(
        SharedPartitionId partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer kafkaRecordBatch
    ) {
        return appendRecord(partition, leaderEpoch, firstOffset, lastOffset, kafkaRecordBatch, false);
    }

    /**
     * Zero-additional-copy append for adapters that already own an immutable Kafka RecordBatch buffer.
     * The caller transfers ownership and must never mutate the bytes after this call.
     */
    public CompletableFuture<WalLocation> appendOwnedData(
        SharedPartitionId partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer ownedKafkaRecordBatch
    ) {
        return appendRecord(partition, leaderEpoch, firstOffset, lastOffset, ownedKafkaRecordBatch, true);
    }

    /**
     * Durably appends all Kafka RecordBatches from one logical Kafka append as one crash-atomic WAL group.
     *
     * <p>This is the preferred Kafka-adapter API. The WAL performs one admission decision for the entire group and
     * replay exposes none of the batches unless the group's commit marker is durable. Buffers are owned by the caller
     * before this call and ownership is transferred to the WAL.</p>
     */
    public CompletableFuture<List<WalLocation>> appendOwnedBatchGroup(
        SharedPartitionId partition,
        List<OwnedDataBatch> batches
    ) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(batches, "batches");
        if (batches.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("batches must not be empty"));
        }

        List<WalRecord> records = new ArrayList<>(batches.size());
        for (OwnedDataBatch batch : batches) {
            Objects.requireNonNull(batch, "batch");
            records.add(WalRecord.dataOwned(
                partition.topicIdHigh(),
                partition.topicIdLow(),
                partition.partition(),
                batch.leaderEpoch(),
                batch.firstOffset(),
                batch.lastOffset(),
                batch.bytes()
            ));
        }

        return wal.appendBatch(records).thenApply(results -> applyDurableGroup(records, results));
    }

    private List<WalLocation> applyDurableGroup(List<WalRecord> records, List<WalAppendResult> results) {
        if (records.size() != results.size()) {
            throw new IllegalStateException(
                "WAL append group result mismatch: records=" + records.size() + ", results=" + results.size());
        }
        List<WalLocation> locations = new ArrayList<>(records.size());
        for (int i = 0; i < records.size(); i++) {
            WalRecord record = records.get(i);
            WalAppendResult result = results.get(i);
            walIndex.apply(record, result);
            locations.add(new WalLocation(
                result.segmentId(),
                result.position(),
                result.length(),
                record.leaderEpoch(),
                record.firstOffset(),
                record.lastOffset()
            ));
        }
        return List.copyOf(locations);
    }

    private CompletableFuture<WalLocation> appendRecord(
        SharedPartitionId partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer kafkaRecordBatch,
        boolean owned
    ) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(kafkaRecordBatch, "kafkaRecordBatch");
        WalRecord record = owned
            ? WalRecord.dataOwned(
                partition.topicIdHigh(), partition.topicIdLow(), partition.partition(), leaderEpoch,
                firstOffset, lastOffset, kafkaRecordBatch)
            : WalRecord.data(
                partition.topicIdHigh(), partition.topicIdLow(), partition.partition(), leaderEpoch,
                firstOffset, lastOffset, kafkaRecordBatch);
        return wal.append(record).thenApply(result -> {
            walIndex.apply(record, result);
            return new WalLocation(
                result.segmentId(), result.position(), result.length(), leaderEpoch, firstOffset, lastOffset);
        });
    }

    public CompletableFuture<Void> truncate(
        SharedPartitionId partition,
        int leaderEpoch,
        long truncateOffset
    ) {
        Objects.requireNonNull(partition, "partition");
        WalRecord record = WalRecord.truncate(
            partition.topicIdHigh(),
            partition.topicIdLow(),
            partition.partition(),
            leaderEpoch,
            truncateOffset
        );
        return wal.append(record).thenAccept(result -> walIndex.apply(record, result));
    }

    public Optional<WalRecord> readLocal(SharedPartitionId partition, long offset) throws IOException {
        Optional<WalLocation> location = walIndex.find(walKey(partition), offset);
        if (location.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(wal.read(location.get()));
    }

    /**
     * Reads complete Kafka RecordBatches from the local WAL starting at the batch containing {@code startOffset}.
     * No batch is split. If {@code minOneBatch} is true, the first batch may exceed {@code maxBytes}.
     */
    public LocalReadResult readLocalBatches(
        SharedPartitionId partition,
        long startOffset,
        int maxBytes,
        boolean minOneBatch
    ) throws IOException {
        Objects.requireNonNull(partition, "partition");
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative");
        }
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be non-negative");
        }

        List<WalLocation> selected = new ArrayList<>();
        int selectedBytes = 0;
        long firstBatchOffset = -1L;
        for (WalLocation location : walIndex.ranges(walKey(partition))) {
            if (location.lastOffset() < startOffset) {
                continue;
            }

            WalRecord record = wal.read(location);
            int batchBytes = record.payload().remaining();
            if (selected.isEmpty()) {
                firstBatchOffset = record.firstOffset();
                if (maxBytes == 0 && !minOneBatch) {
                    break;
                }
                if (batchBytes > maxBytes && !minOneBatch) {
                    break;
                }
            } else if ((long) selectedBytes + batchBytes > maxBytes) {
                break;
            }

            selected.add(location);
            selectedBytes = Math.addExact(selectedBytes, batchBytes);
            if (selectedBytes >= maxBytes && !(selected.size() == 1 && minOneBatch)) {
                break;
            }
        }

        if (selected.isEmpty()) {
            return new LocalReadResult(startOffset, List.of(), 0, false);
        }

        List<WalRecord> records = wal.readBatch(selected);
        boolean firstBatchIncomplete = records.get(0).payload().remaining() > maxBytes && !minOneBatch;
        return new LocalReadResult(firstBatchOffset, records, selectedBytes, firstBatchIncomplete);
    }

    public List<WalRecord> readUploadCandidates(List<UploadCandidate> candidates) throws IOException {
        Objects.requireNonNull(candidates, "candidates");
        List<WalLocation> locations = candidates.stream().map(UploadCandidate::location).toList();
        return wal.readBatch(locations);
    }

    /**
     * Returns complete WAL batches that are both Kafka-committed and not remotely covered.
     * highWatermark is Kafka's exclusive commit boundary: every returned batch has lastOffset < highWatermark.
     */
    public List<UploadCandidate> uploadCandidates(
        SharedPartitionId partition,
        long logStartOffset,
        long highWatermark
    ) {
        Objects.requireNonNull(partition, "partition");
        if (logStartOffset < 0 || highWatermark < logStartOffset) {
            throw new IllegalArgumentException(
                "Invalid Kafka commit window [" + logStartOffset + ", " + highWatermark + ")");
        }
        if (logStartOffset == highWatermark) {
            return List.of();
        }

        List<UploadCandidate> result = new ArrayList<>();
        for (WalLocation location : walIndex.ranges(walKey(partition))) {
            if (location.lastOffset() < logStartOffset || location.lastOffset() >= highWatermark) {
                continue;
            }
            OffsetRange logicalRange = new OffsetRange(location.firstOffset(), Math.addExact(location.lastOffset(), 1));
            if (!remoteIndex.coverage(partition).covers(logicalRange)) {
                result.add(new UploadCandidate(partition, logicalRange, location));
            }
        }
        return List.copyOf(result);
    }

    public void commitRemoteObject(SharedObjectMetadata object) {
        remoteIndex.add(Objects.requireNonNull(object, "object"));
    }

    public RemoteObjectIndex remoteIndex() {
        return remoteIndex;
    }

    public long walUsedBytes() {
        return wal.usedBytes();
    }

    public long walCapacityBytes() {
        return wal.capacityBytes();
    }

    @Override
    public void close() throws IOException {
        wal.close();
    }

    private static WalPartitionKey walKey(SharedPartitionId partition) {
        return new WalPartitionKey(partition.topicIdHigh(), partition.topicIdLow(), partition.partition());
    }

    public record OwnedDataBatch(
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer bytes
    ) {
        public OwnedDataBatch {
            if (firstOffset < 0 || lastOffset < firstOffset) {
                throw new IllegalArgumentException("invalid batch offset range");
            }
            Objects.requireNonNull(bytes, "bytes");
            if (!bytes.hasRemaining()) {
                throw new IllegalArgumentException("batch bytes must not be empty");
            }
            bytes = bytes.asReadOnlyBuffer();
        }
    }

    public record LocalReadResult(
        long firstBatchOffset,
        List<WalRecord> records,
        int sizeInBytes,
        boolean firstBatchIncomplete
    ) {
        public LocalReadResult {
            records = List.copyOf(records);
        }
    }

    public record UploadCandidate(SharedPartitionId partition, OffsetRange offsets, WalLocation location) {
    }
}
