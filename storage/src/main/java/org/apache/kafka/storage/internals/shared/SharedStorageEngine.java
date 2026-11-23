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
import org.apache.kafka.storage.internals.shared.wal.WalBlockStats;
import org.apache.kafka.storage.internals.shared.wal.WalCorruptionException;
import org.apache.kafka.storage.internals.shared.wal.WalDurabilityStats;
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalPartitionKey;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;
import org.apache.kafka.storage.internals.shared.wal.WalRecordType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongFunction;

/**
 * Kafka-independent core of the shared storage data plane.
 *
 * <p>Kafka owns leader election, ISR and HW. This class owns durable local WAL state and immutable remote coverage.
 * Callers may only request upload candidates below Kafka's current HW; the engine never invents a commit boundary of
 * its own. WAL addresses consumed here are logical and independent from the backend's physical allocation layout.</p>
 */
public final class SharedStorageEngine implements AutoCloseable {
    private static final int DEFAULT_LOCAL_CACHE_BYTES = 16 * 1024 * 1024;
    private static final int CACHE_ENTRY_OVERHEAD = 128;
    private final SharedWal wal;
    private final PartitionWalIndex walIndex = new PartitionWalIndex();
    private final StartupWalReplayEvidence startupWalReplayEvidence;
    private final LocalWalDurabilityObservation liveWalDurability = new LocalWalDurabilityObservation();
    private final LongAdder trackedAppends = new LongAdder();
    private final LongAdder nonBlockingRequests = new LongAdder();
    private final LongAdder earlyAckAdmissions = new LongAdder();
    private final LongAdder localCacheHits = new LongAdder();
    private final LongAdder localCacheMisses = new LongAdder();
    private final LongAdder localCacheEvictions = new LongAdder();
    private final RemoteObjectIndex remoteIndex = new RemoteObjectIndex();
    private final LocalRemoteObjectCheckpoint remoteCheckpoint;
    private final ConcurrentLinkedQueue<SharedObjectMetadata> pendingRemoteCheckpoints = new ConcurrentLinkedQueue<>();
    private final Object checkpointMaintenanceLock = new Object();
    // File/Ring WALs complete append futures while holding their writer I/O
    // lock. Never acquire this maintenance lock in a completion callback:
    // reclamation takes it before entering WAL I/O, creating a lock cycle.
    private final Object walMaintenanceLock = new Object();
    // The cache has its own lock: a WAL completion must never acquire the
    // maintenance lock while reclaim/close may be waiting for that completion.
    // The limit includes a per-entry allowance for streams of tiny batches.
    private final Object localCacheLock = new Object();
    private final int localCacheLimit;
    private final Map<LocalCacheKey, WalRecord> localCache = new LinkedHashMap<>(16, 0.75f, true);
    private final AtomicLong localCacheGeneration = new AtomicLong();
    private volatile long localCacheBytes;
    private volatile SharedObjectReader remoteReader;

    public SharedStorageEngine(SharedWal wal) throws IOException {
        this(wal, null);
    }

    public SharedStorageEngine(SharedWal wal, LocalRemoteObjectCheckpoint remoteCheckpoint) throws IOException {
        // Backends must explicitly support cache-hit lifecycle validation.
        // RotatingFileSharedWal already has its own confirmed-record cache.
        this(wal, remoteCheckpoint,
            Objects.requireNonNull(wal, "wal").supportsCachedRead() ? DEFAULT_LOCAL_CACHE_BYTES : 0);
    }

    SharedStorageEngine(SharedWal wal, LocalRemoteObjectCheckpoint remoteCheckpoint, int localCacheLimit)
        throws IOException {
        if (localCacheLimit < 0) {
            throw new IllegalArgumentException("localCacheLimit must not be negative");
        }
        this.wal = Objects.requireNonNull(wal, "wal");
        this.remoteCheckpoint = remoteCheckpoint;
        this.localCacheLimit = localCacheLimit;
        try {
            if (remoteCheckpoint != null) {
                remoteIndex.restore(remoteCheckpoint.references());
            }
            StartupWalReplayEvidence.Builder recovery = new StartupWalReplayEvidence.Builder();
            wal.replay((record, location) -> {
                walIndex.apply(record, location);
                recovery.accept(record, location);
            });
            startupWalReplayEvidence = recovery.snapshot();
        } catch (IOException | RuntimeException failure) {
            // Construction has not returned an AutoCloseable owner. Release the
            // WAL backend on replay/restore failure without hiding the cause.
            try {
                wal.close();
            } catch (IOException | RuntimeException closeFailure) {
                if (closeFailure != failure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    public synchronized void installRemoteReader(SharedObjectReader reader) {
        Objects.requireNonNull(reader, "reader");
        if (remoteReader != null && remoteReader != reader) {
            throw new IllegalStateException("Shared remote reader is already installed");
        }
        remoteReader = reader;
    }

    public void recordTrackedAppend() {
        trackedAppends.increment();
    }

    public long trackedAppendCount() {
        return trackedAppends.sum();
    }

    public void recordNonBlockingRequest() {
        nonBlockingRequests.increment();
    }

    public long nonBlockingRequestCount() {
        return nonBlockingRequests.sum();
    }

    public void recordEarlyAckAdmission() {
        earlyAckAdmissions.increment();
    }

    public long earlyAckAdmissionCount() {
        return earlyAckAdmissions.sum();
    }

    public long localCacheHitCount() {
        return localCacheHits.sum();
    }

    public long localCacheMissCount() {
        return localCacheMisses.sum();
    }

    public long localCacheEvictionCount() {
        return localCacheEvictions.sum();
    }

    public long localCacheBytes() {
        return localCacheBytes;
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
        return submitOwnedBatchGroup(partition, batches).durable();
    }

    public record OwnedBatchSubmission(CompletableFuture<Void> reserved, CompletableFuture<List<WalLocation>> durable) { }

    public OwnedBatchSubmission submitOwnedBatchGroup(
        SharedPartitionId partition,
        List<OwnedDataBatch> batches
    ) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(batches, "batches");
        if (batches.isEmpty()) {
            IllegalArgumentException error = new IllegalArgumentException("batches must not be empty");
            return new OwnedBatchSubmission(CompletableFuture.failedFuture(error), CompletableFuture.failedFuture(error));
        }

        List<WalRecord> records = new ArrayList<>(batches.size());
        boolean cacheEnabled = localCacheLimit > CACHE_ENTRY_OVERHEAD;
        boolean singleExact = cacheEnabled && batches.size() == 1 && batches.get(0).exactHeapStorage;
        boolean[] exactHeapStorage = cacheEnabled && batches.size() > 1 ? new boolean[batches.size()] : null;
        long priorLastOffset = -1L;
        for (OwnedDataBatch batch : batches) {
            Objects.requireNonNull(batch, "batch");
            // Kafka record batches must be disjoint and ordered within
            // one WAL group. Reject malformed input before any WAL admission.
            if (batch.firstOffset() <= priorLastOffset) {
                IllegalArgumentException error = new IllegalArgumentException("Kafka batch group offsets overlap or move backwards");
                return new OwnedBatchSubmission(CompletableFuture.failedFuture(error), CompletableFuture.failedFuture(error));
            }
            priorLastOffset = batch.lastOffset();
            records.add(WalRecord.dataOwned(
                partition.topicIdHigh(),
                partition.topicIdLow(),
                partition.partition(),
                batch.leaderEpoch(),
                batch.firstOffset(),
                batch.lastOffset(),
                batch.bytes
            ));
            setExactStorage(exactHeapStorage, records.size() - 1, batch.exactHeapStorage);
        }

        // Transfer an immutable group definition to the asynchronous WAL.
        // The backend cannot change the members while the future is pending.
        List<WalRecord> immutableRecords = List.copyOf(records);
        long cacheGeneration = localCacheGeneration.get();
        CompletableFuture<Void> reserved = new CompletableFuture<>();
        CompletableFuture<List<WalLocation>> durable = walIndex.trackDurableWrite(walKey(partition), () -> {
            SharedWal.AppendSubmission submission;
            if (wal.supportsEarlyReservation()) {
                submission = wal.submitBatch(immutableRecords);
            } else {
                CompletableFuture<List<WalAppendResult>> completion = wal.appendBatch(immutableRecords);
                submission = new SharedWal.AppendSubmission(completion.thenApply(ignored -> null), completion);
            }
            submission.reserved().whenComplete((ignored, failure) -> {
                if (failure == null) reserved.complete(null);
                else reserved.completeExceptionally(failure);
            });
            return submission.durable().thenApply(results -> {
                List<WalLocation> published = walIndex.applyDataGroup(immutableRecords, List.copyOf(results));
                liveWalDurability.dataGroupCompleted(partition, published);
                if (cacheEnabled) {
                    for (int i = 0; i < published.size(); i++) {
                        WalLocation location = published.get(i);
                        if (walIndex.find(walKey(partition), location.firstOffset()).filter(location::equals).isPresent()) {
                            cacheLocalBatch(partition, location, immutableRecords.get(i), cacheGeneration,
                                exactHeapStorage == null ? singleExact : exactHeapStorage[i]);
                        }
                    }
                }
                return published;
            });
        });
        return new OwnedBatchSubmission(reserved, durable);
    }

    private static void setExactStorage(boolean[] storage, int index, boolean exact) {
        if (storage != null) storage[index] = exact;
    }

    private static WalLocation validatedLocation(WalRecord record, WalAppendResult result) {
        Objects.requireNonNull(result, "result");
        // WalLocation verifies the result's encoded length against the
        // original durable record payload (including zero-length TRUNCATE).
        WalLocation location = new WalLocation(
            result.offset(), result.length(), record.payload().remaining(),
            record.leaderEpoch(), record.firstOffset(), record.lastOffset()
        );
        // WalLocation validates the logical address and encoded payload span.
        return location;
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
        return walIndex.trackDurableWrite(walKey(partition), () ->
            wal.append(record).thenApply(result -> {
                WalLocation location = validatedLocation(record, result);
                walIndex.apply(record, result);
                liveWalDurability.dataCompleted(partition, location);
                return location;
            }));
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
        return walIndex.trackDurableWrite(walKey(partition), () ->
            wal.append(record).thenAccept(result -> {
                WalLocation location = validatedLocation(record, result);
                walIndex.apply(record, result);
                liveWalDurability.truncateCompleted(partition, location);
            }));
    }

    public Optional<WalRecord> readLocal(SharedPartitionId partition, long offset) throws IOException {
        synchronized (walMaintenanceLock) {
            Optional<WalLocation> location = walIndex.find(walKey(partition), offset);
            if (location.isEmpty()) {
                return Optional.empty();
            }
            WalRecord record = readCachedBatches(List.of(partition), List.of(location.get())).get(0);
            // A concurrently durable TRUNCATE or replacement may update
            // the partition index while the backend reads WAL bytes.
            requireCurrentLocalLocation(partition, location.get());
            return Optional.of(record);
        }
    }

    public Optional<ByteBuffer> readBatchBytes(SharedPartitionId partition, long offset) throws IOException {
        return readBatchBytes(partition, offset, null);
    }

    private Optional<ByteBuffer> readBatchBytes(
        SharedPartitionId partition, long offset, SharedObjectReader.ReadSession session
    ) throws IOException {
        Optional<WalRecord> local = readLocal(partition, offset);
        if (local.isPresent()) {
            return Optional.of(local.get().payload().asReadOnlyBuffer());
        }
        SharedObjectReader reader = remoteReader;
        if (reader == null) {
            return Optional.empty();
        }
        return awaitRemote(reader.read(partition, offset, session));
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
        var selected = selectStoredBatches(storedBatches(partition, startOffset, Long.MAX_VALUE),
            startOffset, maxBytes, minOneBatch);
        if (selected.batches().isEmpty())
            return new BatchReadResult(selected.firstOffset(), List.of(), 0, selected.firstBatchIncomplete());
        List<ByteBuffer> payloads = readSelectedLocalBatches(partition, selected.batches());
        resolveRemoteBatches(partition, selected.batches(), payloads);
        return new BatchReadResult(selected.firstOffset(), List.copyOf(payloads), selected.bytes(), false);
    }

    private record StoredReadSelection(long firstOffset, List<StoredBatchMetadata> batches, int bytes,
                                       boolean firstBatchIncomplete) { }

    private static StoredReadSelection selectStoredBatches(List<StoredBatchMetadata> known, long startOffset,
                                                            int maxBytes, boolean minOneBatch) {
        List<StoredBatchMetadata> selected = new ArrayList<>();
        int selectedBytes = 0;
        long firstBatchOffset = startOffset;
        for (StoredBatchMetadata metadata : known) {
            if (metadata.lastOffset() < startOffset) continue;
            boolean first = selected.isEmpty();
            if (first) firstBatchOffset = metadata.firstOffset();
            if (!canSelectPayload(metadata.payloadLength(), selectedBytes, first, maxBytes, minOneBatch)) {
                if (first) return new StoredReadSelection(firstBatchOffset, List.of(), 0, !minOneBatch);
                break;
            }
            selected.add(metadata);
            selectedBytes = Math.addExact(selectedBytes, metadata.payloadLength());
            if (selectedBytes >= maxBytes && !(selected.size() == 1 && minOneBatch)) break;
        }
        return new StoredReadSelection(firstBatchOffset, selected, selectedBytes, false);
    }

    private List<ByteBuffer> readSelectedLocalBatches(SharedPartitionId partition, List<StoredBatchMetadata> selected)
        throws IOException {
        List<ByteBuffer> payloads = new ArrayList<>(java.util.Collections.nCopies(selected.size(), null));
        List<WalLocation> locations = new ArrayList<>();
        List<Integer> localIndexes = new ArrayList<>();
        synchronized (walMaintenanceLock) {
            for (int i = 0; i < selected.size(); i++) {
                Optional<WalLocation> location = walIndex.find(walKey(partition), selected.get(i).firstOffset());
                if (location.isPresent()) {
                    locations.add(location.get());
                    localIndexes.add(i);
                }
            }
            if (!locations.isEmpty()) {
                List<WalRecord> records = readCachedBatches(java.util.Collections.nCopies(locations.size(), partition), locations);
                requireLocalBatchIdentity(partition, locations, records);
                for (int i = 0; i < locations.size(); i++) {
                    requireCurrentLocalLocation(partition, locations.get(i));
                    payloads.set(localIndexes.get(i), records.get(i).payload().asReadOnlyBuffer());
                }
            }
        }
        return payloads;
    }

    private void resolveRemoteBatches(SharedPartitionId partition, List<StoredBatchMetadata> selected,
                                      List<ByteBuffer> payloads) throws IOException {
        try (SharedObjectReader.ReadSession session = payloads.contains(null) ? openReadSession() : null) {
            for (int i = 0; i < selected.size(); i++) {
                StoredBatchMetadata metadata = selected.get(i);
                ByteBuffer bytes = payloads.get(i);
                if (bytes == null) {
                    SharedObjectReader reader = remoteReader;
                    bytes = reader == null ? null : awaitRemote(reader.read(partition, metadata.firstOffset(), session)).orElse(null);
                }
                if (bytes == null) {
                    throw new IOException("Shared batch payload is unavailable from both WAL and remote storage at offset " +
                        metadata.firstOffset());
                }
                if (bytes.remaining() != metadata.payloadLength()) {
                    throw new IOException("Shared batch payload length mismatch at offset " + metadata.firstOffset() +
                        ": expected=" + metadata.payloadLength() + ", actual=" + bytes.remaining());
                }
                payloads.set(i, bytes.asReadOnlyBuffer());
            }
        }
    }

    private SharedObjectReader.ReadSession openReadSession() {
        SharedObjectReader reader = remoteReader;
        return reader == null ? null : reader.openReadSession();
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

    /**
     * Current local-WAL Kafka batch coverage, including both startup replayed
     * records and live successful WAL Future/index publications. Diagnostic
     * only: no replicated WAL, source ownership or Controller CAS proof.
     */
    public CurrentLocalWalCoveragePrecheck.Finding localDurableWindowCoverage(
        SharedPartitionId partition,
        long sourceLogStart,
        long sourceHighWatermark
    ) {
        return CurrentLocalWalCoveragePrecheck.assess(
            localLocations(partition, sourceLogStart, sourceHighWatermark),
            sourceLogStart, sourceHighWatermark
        );
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
            List<WalRecord> records = readCachedBatches(
                java.util.Collections.nCopies(selection.locations().size(), partition), selection.locations());
            requireLocalBatchIdentity(partition, selection.locations(), records);
            for (WalLocation location : selection.locations()) {
                requireCurrentLocalLocation(partition, location);
            }
            return new LocalReadResult(
                selection.firstBatchOffset(),
                records,
                selection.sizeInBytes(),
                selection.firstBatchIncomplete()
            );
        }
    }

    /** Caller holds walMaintenanceLock. Backend reads remain batched for cache misses. */
    private List<WalRecord> readCachedBatches(List<SharedPartitionId> partitions, List<WalLocation> locations)
        throws IOException {
        List<WalRecord> records = new ArrayList<>(java.util.Collections.nCopies(locations.size(), null));
        List<WalLocation> misses = new ArrayList<>();
        List<Integer> missingIndexes = new ArrayList<>();
        boolean hasCacheHit = false;
        for (int i = 0; i < locations.size(); i++) {
            WalRecord cached;
            synchronized (localCacheLock) {
                cached = localCache.get(new LocalCacheKey(partitions.get(i), locations.get(i)));
            }
            if (cached == null) {
                localCacheMisses.increment();
                misses.add(locations.get(i));
                missingIndexes.add(i);
            } else {
                localCacheHits.increment();
                hasCacheHit = true;
                records.set(i, cached);
            }
        }
        if (hasCacheHit) {
            // The backend may have fenced itself after this entry was cached.
            // A cache hit must not bypass the read path's lifecycle checks.
            wal.validateCachedRead();
        }
        if (!misses.isEmpty()) {
            List<WalRecord> loaded = wal.readBatch(misses);
            if (loaded.size() != misses.size()) {
                throw new WalCorruptionException("Local WAL batch read count does not match selection");
            }
            for (int i = 0; i < loaded.size(); i++) {
                int index = missingIndexes.get(i);
                SharedPartitionId partition = partitions.get(index);
                WalLocation location = locations.get(index);
                WalRecord record = loaded.get(i);
                requireLocalDataIdentity(partition, location, record);
                requireCurrentLocalLocation(partition, location);
                records.set(index, record);
                cacheLocalBatch(partition, location, record, localCacheGeneration.get(), false);
            }
        }
        return records;
    }

    private void cacheLocalBatch(SharedPartitionId partition, WalLocation location, WalRecord record,
                                 long expectedGeneration, boolean exactHeapStorage) {
        long weight = (long) location.payloadLength() + CACHE_ENTRY_OVERHEAD;
        if (weight > localCacheLimit || localCacheGeneration.get() != expectedGeneration) {
            return;
        }
        // A transferred, exact-sized heap array is already independently
        // owned. Decoded records and slices of larger buffers must be copied
        // so the retained backing storage remains within the cache's budget.
        WalRecord cached = exactHeapStorage ? record : WalRecord.data(record.topicIdHigh(), record.topicIdLow(),
            record.partition(), record.leaderEpoch(), record.firstOffset(), record.lastOffset(), record.payload());
        LocalCacheKey key = new LocalCacheKey(partition, location);
        synchronized (localCacheLock) {
            if (localCacheGeneration.get() != expectedGeneration) return;
            WalRecord previous = localCache.put(key, cached);
            if (previous == null) {
                localCacheBytes += weight;
            }
            while (localCacheBytes > localCacheLimit) {
                Map.Entry<LocalCacheKey, WalRecord> eldest = localCache.entrySet().iterator().next();
                localCacheBytes -= (long) eldest.getKey().location().payloadLength() + CACHE_ENTRY_OVERHEAD;
                localCache.remove(eldest.getKey());
                localCacheEvictions.increment();
            }
        }
    }

    private void clearLocalCache() {
        synchronized (localCacheLock) {
            localCacheGeneration.incrementAndGet();
            localCache.clear();
            localCacheBytes = 0;
        }
    }

    private record LocalCacheKey(SharedPartitionId partition, WalLocation location) {
    }

    /**
     * A valid WAL CRC, physical address and Kafka batch offset do not prove
     * that an index points to the requested TopicId/partition. Reject
     * cross-partition aliases before serving Kafka reads or remote uploads.
     * This is a local data-integrity check, not replica durability authority.
     */
    static void requireLocalDataIdentity(
        SharedPartitionId partition,
        WalLocation location,
        WalRecord record
    ) throws WalCorruptionException {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(location, "location");
        if (record == null || record.type() != WalRecordType.DATA) {
            throw new WalCorruptionException("Expected a local WAL DATA record for " + partition);
        }
        if (record.topicIdHigh() != partition.topicIdHigh()
            || record.topicIdLow() != partition.topicIdLow()
            || record.partition() != partition.partition()) {
            throw new WalCorruptionException("Local WAL record belongs to another partition: " + partition);
        }
        if (record.leaderEpoch() != location.leaderEpoch()
            || record.firstOffset() != location.firstOffset()
            || record.lastOffset() != location.lastOffset()
            || record.payload().remaining() != location.payloadLength()) {
            throw new WalCorruptionException("Local WAL record differs from indexed Kafka batch: " + location);
        }
    }

    /**
     * Fail closed when a backend read completed against a WAL address that
     * the partition index stopped admitting while the read was in flight.
     * This is a post-read consistency check, not a global lock preventing
     * subsequent Kafka leader changes or future truncations.
     */
    private void requireCurrentLocalLocation(SharedPartitionId partition, WalLocation selected)
        throws WalCorruptionException {
        Optional<WalLocation> current = walIndex.find(walKey(partition), selected.firstOffset());
        if (current.isEmpty() || !current.get().equals(selected)) {
            throw new WalCorruptionException("Local WAL read selection is no longer indexed as current");
        }
    }

    private static void requireLocalBatchIdentity(
        SharedPartitionId partition,
        List<WalLocation> locations,
        List<WalRecord> records
    ) throws WalCorruptionException {
        if (records.size() != locations.size()) {
            throw new WalCorruptionException("Local WAL batch read count does not match selection");
        }
        for (int i = 0; i < records.size(); i++) {
            requireLocalDataIdentity(partition, locations.get(i), records.get(i));
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
            List<WalLocation> locations = new ArrayList<>(candidates.size());
            for (UploadCandidate candidate : candidates) {
                if (candidate == null || candidate.partition() == null
                    || candidate.location() == null || candidate.offsets() == null) {
                    throw new WalCorruptionException("WAL upload candidate lacks partition or indexed range");
                }
                if (candidate.offsets().startOffset() != candidate.location().firstOffset()
                    || candidate.offsets().endOffset() - 1L != candidate.location().lastOffset()) {
                    throw new WalCorruptionException("WAL upload candidate offsets disagree with indexed batch");
                }
                locations.add(candidate.location());
            }
            // The caller may be holding a cursor result from a WAL generation
            // that was truncated, replaced or reclaimed after selection.
            requireCurrentUploadLocations(candidates);
            List<SharedPartitionId> partitions = candidates.stream().map(UploadCandidate::partition).toList();
            List<WalRecord> records = readCachedBatches(partitions, locations);
            if (records.size() != candidates.size()) {
                throw new WalCorruptionException("WAL upload candidate read count does not match selection");
            }
            for (int i = 0; i < records.size(); i++) {
                requireLocalDataIdentity(candidates.get(i).partition(), locations.get(i), records.get(i));
            }
            // A concurrent WAL index mutation can invalidate a candidate
            // during backend I/O. Never return records from such a selection.
            requireCurrentUploadLocations(candidates);
            return records;
        }
    }

    private void requireCurrentUploadLocations(List<UploadCandidate> candidates) throws WalCorruptionException {
        for (UploadCandidate candidate : candidates) {
            Optional<WalLocation> current =
                walIndex.find(walKey(candidate.partition()), candidate.offsets().startOffset());
            if (current.isEmpty() || !current.get().equals(candidate.location())) {
                throw new WalCorruptionException("WAL upload candidate is no longer indexed as current");
            }
        }
    }

    /**
     * Opens a lazy cursor over committed, locally present ranges that still need remote coverage.
     *
     * <p>The cursor advances one WAL-index entry at a time and therefore keeps transient memory independent from the
     * partition backlog size. Each step briefly crosses the WAL maintenance fence so reclamation can continue between
     * cursor advances.</p>
     */
    public UploadCandidateCursor uploadCandidateCursor(
        SharedPartitionId partition,
        long logStartOffset,
        long highWatermark
    ) {
        Objects.requireNonNull(partition, "partition");
        validateUploadWindow(logStartOffset, highWatermark);
        return new UploadCandidateCursor(partition, logStartOffset, highWatermark);
    }

    public List<UploadCandidate> uploadCandidates(
        SharedPartitionId partition,
        long logStartOffset,
        long highWatermark
    ) {
        UploadCandidateCursor cursor = uploadCandidateCursor(partition, logStartOffset, highWatermark);
        List<UploadCandidate> result = new ArrayList<>();
        Optional<UploadCandidate> candidate;
        while ((candidate = cursor.next()).isPresent()) {
            result.add(candidate.get());
        }
        return List.copyOf(result);
    }

    /**
     * Tests whether a previously selected candidate still names the current local WAL generation and remains eligible
     * under the supplied Kafka commit window.
     */
    public long remoteRevision(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        return remoteIndex.revision(partition);
    }

    public long walMutationRevision(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        return walIndex.mutationRevision(walKey(partition));
    }

    /**
     * Frozen inventory from the backend WAL's startup replay, before live
     * append traffic. Not live WAL coverage and never a GC authorization.
     */
    public StartupWalReplayEvidence startupWalReplayEvidence() {
        return startupWalReplayEvidence;
    }

    /**
     * Post-start successful backend WAL durability and index-publication
     * completions. This is diagnostic only, NOT a replay/fsync certificate
     * across broker restart or Controller retirement authority.
     */
    public LocalWalDurabilityObservation.Snapshot liveWalDurabilityObservation(SharedPartitionId partition) {
        return liveWalDurability.snapshot(partition);
    }

    /**
     * Local WAL admission/completion revision, including non-destructive tail
     * appends. -1 means a pending write or a raced observation. A stable value
     * is NOT durable WAL replay proof and cannot authorize COMMITTED GC.
     */
    public long walAdmissionRevision(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        return walIndex.walAdmissionRevision(walKey(partition));
    }

    public boolean isUploadCandidateCurrent(
        UploadCandidate candidate,
        long logStartOffset,
        long highWatermark
    ) {
        Objects.requireNonNull(candidate, "candidate");
        validateUploadWindow(logStartOffset, highWatermark);
        if (logStartOffset == highWatermark) {
            return false;
        }

        synchronized (walMaintenanceLock) {
            Optional<WalLocation> current =
                walIndex.find(walKey(candidate.partition()), candidate.offsets().startOffset());
            if (current.isEmpty() || !current.get().equals(candidate.location())) {
                return false;
            }
            WalLocation location = current.get();
            if (location.lastOffset() < logStartOffset || location.lastOffset() >= highWatermark) {
                return false;
            }
            OffsetRange logicalRange =
                new OffsetRange(location.firstOffset(), Math.addExact(location.lastOffset(), 1));
            return logicalRange.equals(candidate.offsets()) &&
                !remoteIndex.coverage(candidate.partition()).covers(logicalRange);
        }
    }

    private static void validateUploadWindow(long logStartOffset, long highWatermark) {
        if (logStartOffset < 0 || highWatermark < logStartOffset) {
            throw new IllegalArgumentException(
                "Invalid Kafka commit window [" + logStartOffset + ", " + highWatermark + ")");
        }
    }

    public final class UploadCandidateCursor {
        private final SharedPartitionId partition;
        private final long logStartOffset;
        private final long highWatermark;
        private final WalPartitionKey walKey;
        private boolean first = true;
        private boolean exhausted;
        private long previousFirstOffset = -1L;
        private long previousWalOffset = -1L;

        private UploadCandidateCursor(
            SharedPartitionId partition,
            long logStartOffset,
            long highWatermark
        ) {
            this.partition = partition;
            this.logStartOffset = logStartOffset;
            this.highWatermark = highWatermark;
            this.walKey = walKey(partition);
            this.exhausted = logStartOffset == highWatermark;
        }

        public Optional<UploadCandidate> next() {
            while (!exhausted) {
                Optional<WalLocation> next;
                synchronized (walMaintenanceLock) {
                    next = first
                        ? walIndex.rangeAtOrAfter(walKey, logStartOffset)
                        : walIndex.rangeAfter(walKey, previousFirstOffset);
                }
                first = false;
                if (next.isEmpty()) {
                    exhausted = true;
                    return Optional.empty();
                }

                WalLocation location = next.get();
                previousFirstOffset = location.firstOffset();
                if (location.lastOffset() < logStartOffset) {
                    continue;
                }
                if (location.lastOffset() >= highWatermark) {
                    exhausted = true;
                    return Optional.empty();
                }
                if (previousWalOffset >= 0L && location.walOffset() <= previousWalOffset) {
                    throw new IllegalStateException(
                        "Partition WAL order moved backwards for " + partition +
                            ": previousWalOffset=" + previousWalOffset +
                            ", currentWalOffset=" + location.walOffset()
                    );
                }
                previousWalOffset = location.walOffset();

                OffsetRange logicalRange =
                    new OffsetRange(location.firstOffset(), Math.addExact(location.lastOffset(), 1));
                if (!remoteIndex.coverage(partition).covers(logicalRange)) {
                    return Optional.of(new UploadCandidate(partition, logicalRange, location));
                }
            }
            return Optional.empty();
        }
    }

    /**
     * Fail-closed bootstrap barrier for a local checkpoint restored before
     * authoritative metadata replay. It must be called after the complete
     * COMMITTED image is ready and before remote-control-plane admission.
     */
    public void validateLocalCheckpointAgainstCommittedInventory(List<SharedObjectMetadata> authoritative)
        throws IOException {
        Objects.requireNonNull(authoritative, "authoritative");
        if (remoteCheckpoint != null) {
            remoteCheckpoint.validateAgainstCommittedInventory(authoritative);
        }
    }

    /** Validate only locally checkpointed COMMITTED references at bootstrap. */
    public void validateLocalCheckpointAgainstCommittedLookup(
        LongFunction<Optional<SharedObjectMetadata>> lookup
    ) throws IOException {
        Objects.requireNonNull(lookup, "lookup");
        if (remoteCheckpoint != null) {
            remoteCheckpoint.validateAgainstCommittedLookup(lookup);
        }
    }

    public synchronized void commitRemoteObject(SharedObjectMetadata object) {
        SharedObjectMetadata committed = Objects.requireNonNull(object, "object");
        List<PartitionRevision> revisionsBefore = committed.ranges().stream()
            .map(range -> range.partition())
            .distinct()
            .map(partition -> new PartitionRevision(partition, remoteIndex.revision(partition)))
            .toList();
        remoteIndex.add(committed);
        boolean changed = revisionsBefore.stream().anyMatch(snapshot ->
            remoteIndex.revision(snapshot.partition()) != snapshot.revision());
        if (remoteCheckpoint != null && changed) {
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
                    clearLocalCache();
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

    /**
     * Returns the number of authoritative remote COMMITs waiting to cross the broker-local checkpoint durability
     * barrier. A growing value indicates that remote metadata is advancing faster than local reclaim eligibility.
     */
    public int pendingRemoteCheckpointCount() {
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

    public WalDurabilityStats walDurabilityStats() {
        return wal.durabilityStats();
    }

    public WalBlockStats walBlockStats() {
        return wal.blockStats();
    }

    @Override
    public void close() throws IOException {
        synchronized (walMaintenanceLock) {
            clearLocalCache();
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

    /** The caller transfers ownership of the payload and must not modify it. */
    public static final class OwnedDataBatch {
        private final int leaderEpoch;
        private final long firstOffset;
        private final long lastOffset;
        private final ByteBuffer bytes;
        private final boolean exactHeapStorage;

        public OwnedDataBatch(int leaderEpoch, long firstOffset, long lastOffset, ByteBuffer bytes) {
            if (firstOffset < 0 || lastOffset < firstOffset) {
                throw new IllegalArgumentException("invalid batch offset range");
            }
            Objects.requireNonNull(bytes, "bytes");
            if (!bytes.hasRemaining()) {
                throw new IllegalArgumentException("batch bytes must not be empty");
            }
            this.leaderEpoch = leaderEpoch;
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.exactHeapStorage = bytes.hasArray() && bytes.arrayOffset() == 0 && bytes.position() == 0 &&
                bytes.limit() == bytes.capacity() && bytes.array().length == bytes.capacity();
            this.bytes = bytes.asReadOnlyBuffer();
        }

        public int leaderEpoch() {
            return leaderEpoch;
        }

        public long firstOffset() {
            return firstOffset;
        }

        public long lastOffset() {
            return lastOffset;
        }

        public ByteBuffer bytes() {
            return bytes.asReadOnlyBuffer();
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

    private record PartitionRevision(SharedPartitionId partition, long revision) {
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
