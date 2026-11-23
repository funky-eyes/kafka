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
package org.apache.kafka.storage.internals.shared.wal;

import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/** Experimental SharedWal implementation. No automatic migration or legacy-writer fencing. */
public final class CoordinatedRingWalAdapter implements SharedWal {
    public static final int METADATA_BYTES = 3 * 4096;
    public static final int BANKED_METADATA_BYTES = 6 * 4096;
    public static final int MAX_BLOCK_BYTES = AlignedWalBlockCodec.MAX_BLOCK_BYTES;
    record BlockMetrics(long blocks, long groups, long bytes, long maxGroupsPerBlock) { }
    private static final int ALIGNMENT = 4096;

    private record Request(List<WalRecord> records, int bytes, CompletableFuture<Void> reserved,
                           CompletableFuture<List<WalAppendResult>> result) { }

    private final Object lock = new Object();
    private final WalIoBackend.Handle handle;
    private final WalIoBackend backend;
    private final RingWalOwnership ownership;
    private final BlockRingAllocator allocator;
    private final BlockRingCheckpoint checkpoint;
    private final RingBlockCommitCoordinator coordinator;
    private final long capacity;
    private final int maxBlock;
    private final long lingerNanos;
    private final int maxAdmissionGroups;
    private final long maxAdmissionBytes;
    private final ArrayDeque<Request> queue = new ArrayDeque<>();
    private final Set<Request> outstanding = new HashSet<>();
    private final CompletableFuture<Void> closed = new CompletableFuture<>();
    private final Thread assembler;
    private long queuedBytes;
    private long outstandingBytes;
    private long blocks;
    private long groups;
    private long bytes;
    private long maxGroups;
    private boolean closing;
    private Throwable failure;

    public static CoordinatedRingWalAdapter create(
            Path path, WalIoBackend backend, long capacity, int maxBlock, long lingerNanos)
            throws IOException {
        return create(path, backend, capacity, maxBlock, lingerNanos, 1024, 64L * 1024 * 1024);
    }

    public static CoordinatedRingWalAdapter create(
            Path path, WalIoBackend backend, long capacity, int maxBlock, long lingerNanos,
            int maxAdmissionGroups, long maxAdmissionBytes) throws IOException {
        return create(path, backend, capacity, maxBlock, lingerNanos, maxAdmissionGroups, maxAdmissionBytes,
            BlockRingCheckpoint.Layout.LEGACY);
    }

    public static CoordinatedRingWalAdapter createWithBankedCheckpoints(
            Path path, WalIoBackend backend, long capacity, int maxBlock, long lingerNanos,
            int maxAdmissionGroups, long maxAdmissionBytes) throws IOException {
        return create(path, backend, capacity, maxBlock, lingerNanos, maxAdmissionGroups, maxAdmissionBytes,
            BlockRingCheckpoint.Layout.BANKED);
    }

    private static CoordinatedRingWalAdapter create(
            Path path, WalIoBackend backend, long capacity, int maxBlock, long lingerNanos,
            int maxAdmissionGroups, long maxAdmissionBytes, BlockRingCheckpoint.Layout layout) throws IOException {
        validateConfiguration(capacity, maxBlock, lingerNanos);
        validateAdmission(maxAdmissionGroups, maxAdmissionBytes);
        if (!backend.supportsPreallocation()) throw new IOException("WAL backend requires preallocation");
        RingWalOwnership ownership = RingWalOwnership.acquire(path);
        WalIoBackend.Handle handle = null;
        UUID incarnation = UUID.randomUUID();
        Path stage = path.resolveSibling(path.getFileName() + ".initializing-" + incarnation);
        boolean published = false;
        try {
            handle = backend.create(stage);
            long size = Math.addExact(layout.metadataBytes(ALIGNMENT), capacity);
            handle.preallocate(size);
            handle.force();
            var checkpoint = BlockRingCheckpoint.initialize(handle, ALIGNMENT, capacity,
                incarnation.getMostSignificantBits(), incarnation.getLeastSignificantBits());
            Files.createLink(path, stage);
            published = true;
            flushParent(path);
            Files.delete(stage);
            flushParent(path);
            return new CoordinatedRingWalAdapter(
                    handle,
                    backend,
                    ownership,
                    capacity,
                    maxBlock,
                    lingerNanos,
                    maxAdmissionGroups,
                    maxAdmissionBytes,
                    checkpoint,
                    new BlockRingRecovery.Scan(List.of(), -1));
        } catch (Throwable t) {
            try {
                if (handle != null) handle.close();
            } catch (Throwable close) {
                t.addSuppressed(close);
            }
            if (!published) {
                try {
                    Files.deleteIfExists(stage);
                } catch (Throwable cleanup) {
                    t.addSuppressed(cleanup);
                }
            }
            try {
                ownership.close();
            } catch (Throwable close) {
                t.addSuppressed(close);
            }
            throw t;
        }
    }

    public static CoordinatedRingWalAdapter reopen(
            Path path, WalIoBackend backend, long capacity, int maxBlock, long lingerNanos)
            throws IOException {
        return reopen(path, backend, capacity, maxBlock, lingerNanos, 1024, 64L * 1024 * 1024);
    }

    public static CoordinatedRingWalAdapter reopen(
            Path path, WalIoBackend backend, long capacity, int maxBlock, long lingerNanos,
            int maxAdmissionGroups, long maxAdmissionBytes) throws IOException {
        validateConfiguration(capacity, maxBlock, lingerNanos);
        return reopen(path, backend, maxBlock, lingerNanos, maxAdmissionGroups, maxAdmissionBytes,
            handle -> BlockRingCheckpoint.open(handle, ALIGNMENT, capacity));
    }

    public static CoordinatedRingWalAdapter reopenByFileSize(
            Path path, WalIoBackend backend, long fileSize, long lingerNanos,
            int maxAdmissionGroups, long maxAdmissionBytes) throws IOException {
        return reopen(path, backend, MAX_BLOCK_BYTES, lingerNanos, maxAdmissionGroups, maxAdmissionBytes,
            handle -> BlockRingCheckpoint.openByFileSize(handle, ALIGNMENT, fileSize));
    }

    @FunctionalInterface
    private interface CheckpointOpener {
        BlockRingCheckpoint.Opened open(WalIoBackend.Handle handle) throws IOException;
    }

    private static CoordinatedRingWalAdapter reopen(
            Path path, WalIoBackend backend, int maxBlock, long lingerNanos,
            int maxAdmissionGroups, long maxAdmissionBytes, CheckpointOpener opener) throws IOException {
        validateAdmission(maxAdmissionGroups, maxAdmissionBytes);
        RingWalOwnership ownership = RingWalOwnership.acquire(path);
        WalIoBackend.Handle handle = null;
        try {
            flushParent(path);
            handle = backend.reopen(path);
            var opened = opener.open(handle);
            long recoveredCapacity = opened.checkpoint().capacity();
            return new CoordinatedRingWalAdapter(
                    handle, backend, ownership, recoveredCapacity, (int) Math.min(recoveredCapacity, maxBlock), lingerNanos,
                    maxAdmissionGroups, maxAdmissionBytes, opened.checkpoint(), opened.scan());
        } catch (Throwable t) {
            try {
                if (handle != null) handle.close();
            } catch (Throwable close) {
                t.addSuppressed(close);
            }
            try {
                ownership.close();
            } catch (Throwable close) {
                t.addSuppressed(close);
            }
            throw t;
        }
    }

    private CoordinatedRingWalAdapter(
            WalIoBackend.Handle handle,
            WalIoBackend backend,
            RingWalOwnership ownership,
            long capacity,
            int maxBlock,
            long lingerNanos,
            int maxAdmissionGroups,
            long maxAdmissionBytes,
            BlockRingCheckpoint checkpoint,
            BlockRingRecovery.Scan scan)
            throws IOException {
        if (lingerNanos < 0) throw new IllegalArgumentException("Invalid linger");
        this.handle = handle;
        this.backend = backend;
        this.ownership = ownership;
        this.capacity = capacity;
        this.maxBlock = maxBlock;
        this.lingerNanos = lingerNanos;
        this.maxAdmissionGroups = maxAdmissionGroups;
        this.maxAdmissionBytes = maxAdmissionBytes;
        this.checkpoint = checkpoint;
        var state = checkpoint.current();
        this.allocator =
                BlockRingAllocator.recover(capacity, ALIGNMENT, state.head(), state.tail(), scan);
        var identity = checkpoint.identity();
        var writer =
                new ParallelRingBlockWriter(
                        handle, allocator, checkpoint.metadataBytes(), capacity, ALIGNMENT,
                        identity.high(), identity.low(), 4, 128);
        this.coordinator =
                new RingBlockCommitCoordinator(
                        allocator,
                        checkpoint,
                        writer,
                        handle,
                        checkpoint.metadataBytes(),
                        scan,
                        ALIGNMENT,
                        maxBlock,
                        identity.high(),
                        identity.low());
        this.assembler = new Thread(this::assemble, "parallel-block-wal-assembler");
        assembler.start();
    }

    private static void flushParent(Path path) throws IOException {
        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent == null) throw new IOException("WAL path has no parent");
        Utils.flushDir(parent);
    }

    private static void validateConfiguration(long capacity, int maxBlock, long lingerNanos) {
        if (capacity <= 0 || capacity % ALIGNMENT != 0 || capacity > Long.MAX_VALUE - 3L * ALIGNMENT)
            throw new IllegalArgumentException("Invalid ring capacity");
        if (maxBlock < ALIGNMENT || maxBlock % ALIGNMENT != 0 || maxBlock > AlignedWalBlockCodec.MAX_BLOCK_BYTES ||
            maxBlock > capacity || lingerNanos < 0) throw new IllegalArgumentException("Invalid block configuration");
    }

    private static void validateAdmission(int groups, long bytes) {
        if (groups <= 0 || bytes <= 0) throw new IllegalArgumentException("Invalid WAL admission limits");
    }

    @Override
    public CompletableFuture<List<WalAppendResult>> appendBatch(List<WalRecord> input) {
        return submitBatch(input).durable();
    }

    @Override
    public boolean supportsEarlyReservation() {
        return true;
    }

    @Override
    public AppendSubmission submitBatch(List<WalRecord> input) {
        java.util.Objects.requireNonNull(input, "records");
        final List<WalRecord> records;
        long size;
        try {
            records = List.copyOf(input);
            if (records.isEmpty()) throw new IllegalArgumentException("Empty WAL group");
            size = WalRecordCodec.HEADER_BYTES;
            for (WalRecord record : records) {
                if (record.type() == WalRecordType.GROUP_COMMIT)
                    throw new IllegalArgumentException("Internal record");
                size = Math.addExact(size, WalRecordCodec.encodedLength(record));
            }
        } catch (Throwable error) {
            return failedSubmission(error);
        }
        if (size + AlignedWalBlockCodec.HEADER_BYTES > maxBlock)
            return failedSubmission(new WalCapacityExceededException("WAL group exceeds block limit"));
        var request = new Request(records, Math.toIntExact(size), new CompletableFuture<>(), new CompletableFuture<>());
        synchronized (lock) {
            if (closing || failure != null)
                return failedSubmission(new IOException("WAL closed", failure));
            if (outstanding.size() >= maxAdmissionGroups || size > maxAdmissionBytes - outstandingBytes)
                return failedSubmission(
                        new WalCapacityExceededException("WAL queue full"));
            outstanding.add(request);
            outstandingBytes += size;
            queue.addLast(request);
            queuedBytes += size;
            lock.notifyAll();
        }
        return new AppendSubmission(request.reserved().copy(), request.result().copy());
    }

    private static AppendSubmission failedSubmission(Throwable error) {
        return new AppendSubmission(CompletableFuture.failedFuture(error), CompletableFuture.failedFuture(error));
    }

    private void assemble() {
        try {
            while (true) {
                var batch = awaitBatch();
                if (batch.isEmpty()) return;
                var completion = coordinator.appendGroups(batch.stream().map(Request::records).toList());
                if (completion.isCompletedExceptionally()) {
                    completion.whenComplete((ignored, error) -> batch.forEach(request ->
                        request.reserved().completeExceptionally(normalize(error))));
                } else {
                    // appendGroups reserved physical ring space before returning.
                    batch.forEach(request -> request.reserved().complete(null));
                }
                completion.whenComplete((results, error) -> complete(batch, results, error));
            }
        } catch (Throwable t) {
            fail(t);
        }
    }

    private List<Request> awaitBatch() throws InterruptedException {
        List<Request> batch = new ArrayList<>();
        int used = AlignedWalBlockCodec.HEADER_BYTES;
        synchronized (lock) {
            while (queue.isEmpty() && !closing && failure == null) lock.wait();
            if (failure != null || closing && queue.isEmpty()) return batch;
            long deadline = System.nanoTime() + lingerNanos;
            while (!closing
                    && failure == null
                    && queue.size() < maxAdmissionGroups
                    && queuedBytes < maxBlock - used
                    && deadline - System.nanoTime() > 0)
                TimeUnit.NANOSECONDS.timedWait(lock, deadline - System.nanoTime());
            while (!queue.isEmpty() && queue.peekFirst().bytes() <= maxBlock - used) {
                Request request = queue.removeFirst();
                used += request.bytes();
                queuedBytes -= request.bytes();
                batch.add(request);
            }
        }
        return batch;
    }

    private void complete(
            List<Request> batch, List<List<WalAppendResult>> results, Throwable error) {
        Throwable cause = normalize(error);
        if (cause != null && !(cause instanceof WalCapacityExceededException)) {
            fail(cause);
            return;
        }
        synchronized (lock) {
            if (failure != null) cause = failure;
            for (Request request : batch) {
                if (outstanding.remove(request)) outstandingBytes -= request.bytes();
            }
            if (cause == null) {
                blocks++;
                groups += batch.size();
                maxGroups = Math.max(maxGroups, batch.size());
                for (var group : results) for (var result : group) bytes += result.length();
            }
        }
        for (int i = 0; i < batch.size(); i++) {
            if (cause == null) batch.get(i).result().complete(results.get(i));
            else batch.get(i).result().completeExceptionally(cause);
        }
    }

    private static Throwable normalize(Throwable error) {
        Throwable cause = error;
        while (cause instanceof CompletionException && cause.getCause() != null)
            cause = cause.getCause();
        if (cause instanceof BlockRingAllocator.CapacityException
                || cause instanceof ParallelRingBlockWriter.BusyException)
            return new WalCapacityExceededException(cause.getMessage());
        return cause;
    }

    private void fail(Throwable error) {
        List<Request> failed;
        synchronized (lock) {
            if (failure != null) return;
            failure = error;
            failed = List.copyOf(outstanding);
            outstanding.clear();
            outstandingBytes = 0;
            queue.clear();
            queuedBytes = 0;
            lock.notifyAll();
        }
        for (Request request : failed) {
            request.reserved().completeExceptionally(error);
            request.result().completeExceptionally(error);
        }
    }

    @Override
    public boolean supportsCachedRead() {
        return true;
    }

    @Override
    public void validateCachedRead() throws IOException {
        coordinator.validateCachedRead();
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        return validateRead(location, coordinator.read(location.walOffset()));
    }

    @Override
    public List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        List<WalLocation> requested = List.copyOf(locations);
        if (requested.size() == 1) return List.of(read(requested.get(0)));
        var entries = coordinator.readBatch(requested.stream().map(WalLocation::walOffset).toList());
        List<WalRecord> records = new ArrayList<>(requested.size());
        for (int i = 0; i < requested.size(); i++) records.add(validateRead(requested.get(i), entries.get(i)));
        return List.copyOf(records);
    }

    private static WalRecord validateRead(WalLocation location, AlignedWalBlockCodec.Entry entry) throws IOException {
        var record = entry.record();
        if (entry.length() != location.length()
                || record.payload().remaining() != location.payloadLength()
                || record.firstOffset() != location.firstOffset()
                || record.lastOffset() != location.lastOffset()
                || record.leaderEpoch() != location.leaderEpoch())
            throw new WalCorruptionException("WAL location mismatch");
        return record;
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        coordinator.replay(consumer);
    }

    @Override
    public long reclaim(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        return coordinator.reclaim(policy, desiredBytes);
    }

    @Override
    public long reclaimedBeforeOffset() {
        try {
            return checkpoint.current().head();
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public long usedBytes() {
        return allocator.usedBytes();
    }

    @Override
    public long capacityBytes() {
        return capacity;
    }

    @Override
    public WalDurabilityStats durabilityStats() {
        return coordinator.durabilityStats();
    }

    BlockMetrics blockMetrics() {
        synchronized (lock) {
            return new BlockMetrics(blocks, groups, bytes, maxGroups);
        }
    }

    @Override
    public WalBlockStats blockStats() {
        var metrics = blockMetrics();
        return new WalBlockStats(metrics.blocks(), metrics.groups(), metrics.bytes(), metrics.maxGroupsPerBlock());
    }

    @Override
    public void close() throws IOException {
        if (Thread.currentThread() == assembler || coordinator.isCallbackContext())
            throw new IOException("Cannot close from a WAL callback");
        boolean owner;
        synchronized (lock) {
            owner = !closing;
            if (owner) {
                closing = true;
                lock.notifyAll();
            }
        }
        if (!owner) {
            awaitClosed();
            return;
        }
        try {
            boolean interrupted = awaitAssembler();
            IOException error = closeResources();
            if (interrupted) {
                Thread.currentThread().interrupt();
                if (error == null) error = new IOException("Interrupted closing WAL");
            }
            if (error != null) throw error;
            closed.complete(null);
        } catch (Throwable t) {
            closed.completeExceptionally(t);
            throw t;
        }
    }

    private void awaitClosed() throws IOException {
        try {
            closed.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException io) throw io;
            throw new IOException("WAL close failed", e.getCause());
        }
    }

    private IOException closeResources() {
        IOException error = null;
        try {
            coordinator.close();
        } catch (IOException e) {
            error = e;
        }
        try {
            handle.close();
        } catch (IOException e) {
            if (error == null) error = e;
            else error.addSuppressed(e);
        }
        try {
            ownership.close();
        } catch (IOException e) {
            if (error == null) error = e;
            else error.addSuppressed(e);
        }
        try {
            backend.close();
        } catch (IOException e) {
            if (error == null) error = e;
            else error.addSuppressed(e);
        }
        return error;
    }

    private boolean awaitAssembler() {
        boolean interrupted = false;
        while (assembler.isAlive()) {
            try {
                assembler.join();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        return interrupted;
    }
}
