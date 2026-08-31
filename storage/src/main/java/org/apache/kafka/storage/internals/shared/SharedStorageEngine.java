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

import org.apache.kafka.storage.internals.shared.metadata.LocalRemoteObjectCheckpoint;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.SharedObjectReader;
import org.apache.kafka.storage.internals.shared.wal.PartitionWalIndex;
import org.apache.kafka.storage.internals.shared.wal.RemoteCoverageWalReclaimPolicy;
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
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

/**
 * Kafka-independent core of the shared storage data plane.
 *
 * <p>Kafka owns leader election, ISR and HW. This class owns durable local WAL state and immutable remote coverage.
 * Callers may only request upload candidates below Kafka's current HW; the engine never invents a commit boundary of
 * its own. WAL addresses consumed here are logical and independent from the backend's physical allocation layout.</p>
 */
public final class SharedStorageEngine implements AutoCloseable {
    private final SharedWal wal;
    private final PartitionWalIndex walIndex = new PartitionWalIndex();
    private final RemoteObjectIndex remoteIndex = new RemoteObjectIndex();
    private final LocalRemoteObjectCheckpoint remoteCheckpoint;
    private final ConcurrentLinkedQueue<SharedObjectMetadata> pendingRemoteCheckpoints = new ConcurrentLinkedQueue<>();
    private final Object checkpointMaintenanceLock = new Object();
    private final Object walMaintenanceLock = new Object();
    private volatile SharedObjectReader remoteReader;

    public SharedStorageEngine(SharedWal wal) throws IOException {
        this(wal, null);
    }

    public SharedStorageEngine(SharedWal wal, LocalRemoteObjectCheckpoint remoteCheckpoint) throws IOException {
        this.wal = Objects.requireNonNull(wal, "wal");
        this.remoteCheckpoint = remoteCheckpoint;
        if (remoteCheckpoint != null) {
            remoteIndex.restore(remoteCheckpoint.references());
        }
        wal.replay(walIndex::apply);
    }

    public synchronized void installRemoteReader(SharedObjectReader reader) {
        Objects.requireNonNull(reader, "reader");
        if (remoteReader != null && remoteReader != reader) {
            throw new IllegalStateException("Shared remote reader is already installed");
        }
        remoteReader = reader;
    }

    public CompletableFuture<WalLocation> appendData(
        SharedPartitionId partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer kafkaRecordBatch
    ) {
        return appendRecord(partition, leaderEpoch, firstOffset, lastOffset, kafkaRecordBatch, false);
    }

    public CompletableFuture<WalLocation> appendOwnedData(
        SharedPartitionId partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer ownedKafkaRecordBatch
    ) {
        return appendRecord(partition, leaderEpoch, firstOffset, lastOffset, ownedKafkaRecordBatch, true);
    }

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
                result.offset(),
                result.length(),
                record.payload().remaining(),
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
                result.offset(),
                result.length(),
                record.payload().remaining(),
                leaderEpoch,
                firstOffset,
                lastOffset
            );
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
        synchronized (walMaintenanceLock) {
            Optional<WalLocation> location = walIndex.find(walKey(partition), offset);
            if (location.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(wal.read(location.get()));
        }
    }

    public Optional<ByteBuffer> readBatchBytes(SharedPartitionId partition, long offset) throws IOException {
        Optional<WalRecord> local = readLocal(partition, offset);
        if (local.isPresent()) {
            return Optional.of(local.get().payload().asReadOnlyBuffer());
        }
        SharedObjectReader reader = remoteReader;
        if (reader == null) {
            return Optional.empty();
        }
        return awaitRemote(reader.read(partition, offset));
    }

    public List<StoredBatchMetadata> storedBatches(
        SharedPartitionId partition,
        long startOffset,
        long endOffsetExclusive
    ) {
        Objects.requireNonNull(partition, "partition");
        if (startOffset < 0 || endOffsetExclusive < startOffset) {
            throw new IllegalArgumentException("invalid stored batch range");
        }
        TreeMap<Long, StoredBatchMetadata> merged = new TreeMap<>();
        for (RemoteObjectIndex.RangeReference reference : remoteIndex.ranges(partition)) {
            OffsetRange offsets = reference.range().offsets();
            if (offsets.endOffset() <= startOffset || offsets.startOffset() >= endOffsetExclusive) {
                continue;
            }
            merged.put(offsets.startOffset(), new StoredBatchMetadata(
                offsets.startOffset(),
                offsets.endOffset() - 1,
                reference.range().leaderEpoch(),
                reference.range().objectLength(),
                false
            ));
        }
        for (WalLocation location : localLocations(partition, startOffset, endOffsetExclusive)) {
            merged.put(location.firstOffset(), new StoredBatchMetadata(
                location.firstOffset(),
                location.lastOffset(),
                location.leaderEpoch(),
                location.payloadLength(),
                true
            ));
        }
        return List.copyOf(merged.values());
    }

    public BatchReadResult readBatches(
        SharedPartitionId partition,
        long startOffset,
        int maxBytes,
        boolean minOneBatch
    ) throws IOException {
        validateLocalRead(partition, startOffset, maxBytes);
        List<StoredBatchMetadata> known = storedBatches(partition, startOffset, Long.MAX_VALUE);
        if (known.isEmpty()) {
            return new BatchReadResult(startOffset, List.of(), 0, false);
        }

        List<ByteBuffer> selected = new ArrayList<>();
        int selectedBytes = 0;
        long firstBatchOffset = startOffset;
        for (StoredBatchMetadata metadata : known) {
            if (metadata.lastOffset() < startOffset) {
                continue;
            }
            boolean first = selected.isEmpty();
            if (first) {
                firstBatchOffset = metadata.firstOffset();
            }
            if (!canSelectPayload(metadata.payloadLength(), selectedBytes, first, maxBytes, minOneBatch)) {
                if (first) {
                    return new BatchReadResult(firstBatchOffset, List.of(), 0, !minOneBatch);
                }
                break;
            }
            ByteBuffer bytes = readBatchBytes(partition, metadata.firstOffset())
                .orElseThrow(() -> new IOException(
                    "Shared batch payload is unavailable from both WAL and remote storage at offset " +
                        metadata.firstOffset()));
            if (bytes.remaining() != metadata.payloadLength()) {
                throw new IOException(
                    "Shared batch payload length mismatch at offset " + metadata.firstOffset() +
                        ": expected=" + metadata.payloadLength() + ", actual=" + bytes.remaining());
            }
            selected.add(bytes.asReadOnlyBuffer());
            selectedBytes = Math.addExact(selectedBytes, bytes.remaining());
            if (selectedBytes >= maxBytes && !(selected.size() == 1 && minOneBatch)) {
                break;
            }
        }
        return new BatchReadResult(firstBatchOffset, List.copyOf(selected), selectedBytes, false);
    }

    public List<WalLocation> localLocations(
        SharedPartitionId partition,
        long startOffset,
        long endOffsetExclusive
    ) {
        Objects.requireNonNull(partition, "partition");
        if (startOffset < 0 || endOffsetExclusive < startOffset) {
            throw new IllegalArgumentException(
                "Invalid local WAL range [" + startOffset + ", " + endOffsetExclusive + ")");
        }
        if (startOffset == endOffsetExclusive) {
            return List.of();
        }

        synchronized (walMaintenanceLock) {
            List<WalLocation> result = new ArrayList<>();
            for (WalLocation location : walIndex.ranges(walKey(partition))) {
                if (location.lastOffset() < startOffset) {
                    continue;
                }
                if (location.firstOffset() >= endOffsetExclusive) {
                    break;
                }
                result.add(location);
            }
            return List.copyOf(result);
        }
    }

    public List<WalRecord> readLocalLocations(List<WalLocation> locations) throws IOException {
        Objects.requireNonNull(locations, "locations");
        synchronized (walMaintenanceLock) {
            return wal.readBatch(List.copyOf(locations));
        }
    }

    public LocalReadResult readLocalBatches(
        SharedPartitionId partition,
        long startOffset,
        int maxBytes,
        boolean minOneBatch
    ) throws IOException {
        validateLocalRead(partition, startOffset, maxBytes);
        synchronized (walMaintenanceLock) {
            ReadSelection selection = selectLocalBatches(partition, startOffset, maxBytes, minOneBatch);
            if (selection.locations().isEmpty()) {
                return new LocalReadResult(selection.firstBatchOffset(), List.of(), 0, selection.firstBatchIncomplete());
            }
            List<WalRecord> records = wal.readBatch(selection.locations());
            return new LocalReadResult(
                selection.firstBatchOffset(),
                records,
                selection.sizeInBytes(),
                selection.firstBatchIncomplete()
            );
        }
    }

    private static void validateLocalRead(SharedPartitionId partition, long startOffset, int maxBytes) {
        Objects.requireNonNull(partition, "partition");
        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be non-negative");
        }
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be non-negative");
        }
    }

    private ReadSelection selectLocalBatches(
        SharedPartitionId partition,
        long startOffset,
        int maxBytes,
        boolean minOneBatch
    ) {
        List<WalLocation> selected = new ArrayList<>();
        int selectedBytes = 0;
        long firstBatchOffset = startOffset;
        for (WalLocation location : walIndex.ranges(walKey(partition))) {
            if (location.lastOffset() < startOffset) {
                continue;
            }
            if (selected.isEmpty()) {
                firstBatchOffset = location.firstOffset();
            }
            if (!canSelectLocation(location, selectedBytes, selected.isEmpty(), maxBytes, minOneBatch)) {
                break;
            }
            selected.add(location);
            selectedBytes = Math.addExact(selectedBytes, location.payloadLength());
            if (selectedBytes >= maxBytes && !(selected.size() == 1 && minOneBatch)) {
                break;
            }
        }
        return new ReadSelection(firstBatchOffset, List.copyOf(selected), selectedBytes, false);
    }

    private static boolean canSelectLocation(
        WalLocation location,
        int selectedBytes,
        boolean first,
        int maxBytes,
        boolean minOneBatch
    ) {
        return canSelectPayload(location.payloadLength(), selectedBytes, first, maxBytes, minOneBatch);
    }

    private static boolean canSelectPayload(
        int payloadLength,
        int selectedBytes,
        boolean first,
        int maxBytes,
        boolean minOneBatch
    ) {
        if (first) {
            return minOneBatch || (maxBytes > 0 && payloadLength <= maxBytes);
        }
        return (long) selectedBytes + payloadLength <= maxBytes;
    }

    public List<WalRecord> readUploadCandidates(List<UploadCandidate> candidates) throws IOException {
        Objects.requireNonNull(candidates, "candidates");
        synchronized (walMaintenanceLock) {
            List<WalLocation> locations = candidates.stream().map(UploadCandidate::location).toList();
            return wal.readBatch(locations);
        }
    }

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

        synchronized (walMaintenanceLock) {
            List<UploadCandidate> result = new ArrayList<>();
            for (WalLocation location : walIndex.ranges(walKey(partition))) {
                if (location.lastOffset() < logStartOffset || location.lastOffset() >= highWatermark) {
                    continue;
                }
                OffsetRange logicalRange =
                    new OffsetRange(location.firstOffset(), Math.addExact(location.lastOffset(), 1));
                if (!remoteIndex.coverage(partition).covers(logicalRange)) {
                    result.add(new UploadCandidate(partition, logicalRange, location));
                }
            }
            return List.copyOf(result);
        }
    }

    public void commitRemoteObject(SharedObjectMetadata object) {
        SharedObjectMetadata committed = Objects.requireNonNull(object, "object");
        remoteIndex.add(committed);
        if (remoteCheckpoint != null) {
            pendingRemoteCheckpoints.add(committed);
        }
    }

    public int checkpointCommittedRemoteObjects() throws IOException {
        if (remoteCheckpoint == null) {
            return 0;
        }
        synchronized (checkpointMaintenanceLock) {
            List<SharedObjectMetadata> drained = new ArrayList<>();
            SharedObjectMetadata object;
            while ((object = pendingRemoteCheckpoints.poll()) != null) {
                drained.add(object);
            }
            if (drained.isEmpty()) {
                return 0;
            }
            try {
                remoteCheckpoint.addAll(drained);
                return drained.size();
            } catch (IOException e) {
                drained.forEach(pendingRemoteCheckpoints::add);
                throw e;
            }
        }
    }

    /**
     * Durably checkpoints current remote COMMITs and then releases only the logical WAL prefix covered by that exact
     * durable checkpoint snapshot. WALs exposing an exclusive logical reclamation watermark allow the in-memory index
     * to prune only reclaimed addresses while concurrent appends remain intact; implementations without such a
     * watermark fall back to full replay-based reconstruction.
     */
    public long reclaimCheckpointedWal() throws IOException {
        if (remoteCheckpoint == null) {
            return 0L;
        }
        synchronized (checkpointMaintenanceLock) {
            checkpointCommittedRemoteObjects();
            RemoteObjectIndex checkpointed = new RemoteObjectIndex();
            checkpointed.restore(remoteCheckpoint.references());
            synchronized (walMaintenanceLock) {
                long reclaimed = wal.reclaim(new RemoteCoverageWalReclaimPolicy(checkpointed));
                if (reclaimed > 0) {
                    long reclaimedBeforeOffset = wal.reclaimedBeforeOffset();
                    if (reclaimedBeforeOffset >= 0) {
                        walIndex.removeBefore(reclaimedBeforeOffset);
                    } else {
                        walIndex.clear();
                        wal.replay(walIndex::apply);
                    }
                }
                return reclaimed;
            }
        }
    }

    int pendingRemoteCheckpointCount() {
        return pendingRemoteCheckpoints.size();
    }

    public RemoteObjectIndex remoteIndex() {
        return remoteIndex;
    }

    public long walUsedBytes() {
        synchronized (walMaintenanceLock) {
            return wal.usedBytes();
        }
    }

    public long walCapacityBytes() {
        return wal.capacityBytes();
    }

    @Override
    public void close() throws IOException {
        synchronized (walMaintenanceLock) {
            wal.close();
        }
    }

    private static Optional<ByteBuffer> awaitRemote(CompletableFuture<Optional<ByteBuffer>> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while reading shared object storage", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            while (cause instanceof java.util.concurrent.CompletionException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof IOException ioException) {
                throw ioException;
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new IOException("Shared object read failed", cause);
        }
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

    public record StoredBatchMetadata(
        long firstOffset,
        long lastOffset,
        int leaderEpoch,
        int payloadLength,
        boolean local
    ) {
        public StoredBatchMetadata {
            if (firstOffset < 0 || lastOffset < firstOffset || payloadLength <= 0) {
                throw new IllegalArgumentException("invalid stored batch metadata");
            }
        }
    }

    public record BatchReadResult(
        long firstBatchOffset,
        List<ByteBuffer> batches,
        int sizeInBytes,
        boolean firstBatchIncomplete
    ) {
        public BatchReadResult {
            batches = batches.stream().map(ByteBuffer::asReadOnlyBuffer).toList();
        }
    }

    private record ReadSelection(
        long firstBatchOffset,
        List<WalLocation> locations,
        int sizeInBytes,
        boolean firstBatchIncomplete
    ) {
    }

    public record UploadCandidate(SharedPartitionId partition, OffsetRange offsets, WalLocation location) {
    }
}
