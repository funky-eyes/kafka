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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Experimental block-level coordinator for the opt-in SharedWal implementation. */
final class RingBlockCommitCoordinator implements AutoCloseable {
    private static final int MAX_RECLAIM_BLOCKS = 64;
    private static final class Pending {
        final BlockRingAllocator.Address address;
        final long lastId;
        final int groupCount;
        final long recordBytes;
        final AtomicReference<List<List<WalAppendResult>>> locations;
        final CompletableFuture<List<List<WalAppendResult>>> result = new CompletableFuture<>();

        Pending(
                BlockRingAllocator.Address address,
                long lastId,
                int groupCount,
                long recordBytes,
                AtomicReference<List<List<WalAppendResult>>> locations) {
            this.address = address;
            this.lastId = lastId;
            this.groupCount = groupCount;
            this.recordBytes = recordBytes;
            this.locations = locations;
        }
    }

    private static final class Committed {
        private final BlockRingAllocator.Address address;
        private boolean published;

        Committed(BlockRingAllocator.Address address, boolean published) {
            this.address = address;
            this.published = published;
        }

        BlockRingAllocator.Address address() {
            return address;
        }
    }

    private final Object lock = new Object();
    private final BlockRingAllocator allocator;
    private final BlockRingCheckpoint checkpoint;
    private final ParallelRingBlockWriter writer;
    private final WalIoBackend.Handle reader;
    private final long dataStart;
    private final int alignment;
    private final int maxBlock;
    private final long high;
    private final long low;
    private final TreeMap<Long, Pending> pending = new TreeMap<>();
    private final TreeMap<Long, Committed> committed = new TreeMap<>();
    private final ThreadLocal<Boolean> replaying = ThreadLocal.withInitial(() -> false);
    private final ThreadLocal<Boolean> inReclaim = ThreadLocal.withInitial(() -> false);
    private volatile Thread checkpointThread;
    private volatile Thread completionThread;
    private final ExecutorService checkpoints =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "ring-checkpoints");
                        checkpointThread = thread;
                        return thread;
                    });
    private final ExecutorService completions =
            Executors.newSingleThreadExecutor(
                    task -> {
                        Thread thread = new Thread(task, "ring-completions");
                        completionThread = thread;
                        return thread;
                    });
    private long nextGroup;
    private long committedTail;
    private long durableCohorts;
    private long durableGroups;
    private long durableBytes;
    private long maxGroupsPerCohort;
    private boolean scheduled;
    private boolean rebasing;
    private boolean reclaiming;
    private boolean closing;
    private int activeReads;
    private Throwable failure;

    RingBlockCommitCoordinator(
            BlockRingAllocator allocator,
            BlockRingCheckpoint checkpoint,
            ParallelRingBlockWriter writer,
            WalIoBackend.Handle reader,
            long dataStart,
            BlockRingRecovery.Scan recovered,
            int alignment,
            int maxBlock,
            long high,
            long low)
            throws IOException {
        if (allocator == null
                || checkpoint == null
                || writer == null
                || reader == null
                || recovered == null)
            throw new IllegalArgumentException("Invalid ring coordinator configuration");
        if (dataStart < 0 || alignment < 4096 || (alignment & (alignment - 1)) != 0)
            throw new IllegalArgumentException("Invalid ring coordinator configuration");
        if (maxBlock < alignment
                || maxBlock % alignment != 0
                || maxBlock > AlignedWalBlockCodec.MAX_BLOCK_BYTES
                || !checkpoint.matchesLayout(alignment, high, low))
            throw new IllegalArgumentException("Invalid ring coordinator configuration");
        this.allocator = allocator;
        this.checkpoint = checkpoint;
        this.writer = writer;
        this.reader = reader;
        this.dataStart = dataStart;
        this.alignment = alignment;
        this.maxBlock = maxBlock;
        this.high = high;
        this.low = low;
        var state = checkpoint.current();
        if (allocator.committedTail() != state.tail())
            throw new IOException("Allocator and checkpoint differ");
        if (recovered.lastGroup() != state.lastGroup())
            throw new IOException("Recovered groups and checkpoint differ");
        state = recoverEmpty(allocator, checkpoint, state);
        restoreBlocks(allocator, recovered);
        this.committedTail = state.tail();
        this.nextGroup = Math.addExact(state.lastGroup(), 1);
    }

    private static BlockRingCheckpoint.State recoverEmpty(BlockRingAllocator allocator,
            BlockRingCheckpoint checkpoint, BlockRingCheckpoint.State state) throws IOException {
        if (state.head() != state.tail()) return state;
        long target = allocator.emptyRebaseTarget();
        if (target == state.tail()) return state;
        checkpoint.rebaseEmpty(target);
        allocator.emptyRebaseCheckpointForced(target);
        return checkpoint.current();
    }

    private void restoreBlocks(BlockRingAllocator allocator, BlockRingRecovery.Scan recovered)
            throws IOException {
        var addresses = allocator.committedAddresses();
        if (addresses.size() != recovered.blocks().size())
            throw new IOException("Recovered block count differs");
        for (int i = 0; i < addresses.size(); i++) {
            var address = addresses.get(i);
            var block = recovered.blocks().get(i);
            if (address.reservedStart() != block.reservedStart()
                    || address.blockStart() != block.blockStart()
                    || address.end() != block.end())
                throw new IOException("Recovered block addresses differ");
            committed.put(address.blockStart(), new Committed(address, true));
        }
    }

    private record Prepared(List<List<WalRecord>> groups, int length, long recordBytes) { }

    private Prepared prepare(List<List<WalRecord>> input) {
        if (input == null || input.isEmpty()) throw new IllegalArgumentException("Empty block");
        List<List<WalRecord>> groups = new ArrayList<>();
        long bytes = AlignedWalBlockCodec.HEADER_BYTES;
        for (List<WalRecord> group : input) {
            var records = List.copyOf(group);
            if (records.isEmpty()) throw new IllegalArgumentException("Empty group");
            bytes = Math.addExact(bytes, WalRecordCodec.HEADER_BYTES);
            for (WalRecord record : records) {
                if (record.type() == WalRecordType.GROUP_COMMIT)
                    throw new IllegalArgumentException("Internal record");
                bytes = Math.addExact(bytes, WalRecordCodec.encodedLength(record));
            }
            groups.add(records);
        }
        if (bytes > maxBlock) throw new IllegalArgumentException("Block too large");
        int length = Math.toIntExact(((bytes + alignment - 1) / alignment) * alignment);
        if (length > maxBlock) throw new IllegalArgumentException("Block too large");
        long recordBytes = bytes - AlignedWalBlockCodec.HEADER_BYTES - (long) groups.size() * WalRecordCodec.HEADER_BYTES;
        return new Prepared(groups, length, recordBytes);
    }

    /** One block may contain multiple atomic groups. Caller owns request aggregation. */
    CompletableFuture<List<List<WalAppendResult>>> appendGroups(List<List<WalRecord>> input) {
        Prepared prepared = prepare(input);
        List<List<WalRecord>> groups = prepared.groups();
        int length = prepared.length();
        synchronized (lock) {
            if (closing || failure != null)
                return CompletableFuture.failedFuture(
                        new IOException("Ring coordinator fenced or closed", failure));
            if (rebasing)
                return CompletableFuture.failedFuture(new ParallelRingBlockWriter.BusyException("Empty ring rebase in progress"));
            long after = Math.addExact(nextGroup, groups.size());
            List<AlignedWalBlockCodec.Group> encoded = new ArrayList<>();
            long id = nextGroup;
            for (var records : groups) encoded.add(new AlignedWalBlockCodec.Group(id++, records));
            AtomicReference<List<List<WalAppendResult>>> locations =
                    new AtomicReference<>();
            final ParallelRingBlockWriter.Submission submission;
            try {
                submission =
                        writer.submitOwned(
                                length,
                                offset -> {
                                    var block =
                                            AlignedWalBlockCodec.encodeWithLocations(
                                                    offset, high, low, alignment, encoded);
                                    locations.set(block.groups());
                                    return block.bytes();
                                });
            } catch (IOException e) {
                return CompletableFuture.failedFuture(e);
            }
            Pending item = new Pending(submission.address(), after - 1, groups.size(), prepared.recordBytes(), locations);
            pending.put(item.address.reservedStart(), item);
            nextGroup = after;
            submission.forced().whenComplete((address, error) -> onForced(error));
            return item.result.copy();
        }
    }

    private void onForced(Throwable error) {
        synchronized (lock) {
            if (failure != null) return;
            if (error != null) {
                fenceLocked(error);
                return;
            }
            if (!scheduled) {
                scheduled = true;
                checkpoints.execute(this::commitLoop);
            }
        }
    }

    private void commitLoop() {
        try {
            while (true) {
                List<Pending> cohort = new ArrayList<>();
                long tail;
                synchronized (lock) {
                    if (failure != null) return;
                    long forcedTail = allocator.contiguousForcedTail();
                    tail = committedTail;
                    for (Pending item : pending.values()) {
                        if (item.address.reservedStart() != tail || item.address.end() > forcedTail) break;
                        tail = item.address.end();
                        cohort.add(item);
                    }
                    if (cohort.isEmpty()) {
                        scheduled = false;
                        return;
                    }
                }
                checkpoint.advanceTail(tail, cohort.get(cohort.size() - 1).lastId);
                synchronized (lock) {
                    if (failure != null) return;
                    allocator.tailCheckpointForced(tail);
                    committedTail = tail;
                    long cohortGroups = 0;
                    for (Pending item : cohort) {
                        pending.remove(item.address.reservedStart());
                        committed.put(
                                item.address.blockStart(), new Committed(item.address, false));
                        cohortGroups += item.groupCount;
                        durableBytes += item.recordBytes;
                    }
                    durableCohorts++;
                    durableGroups += cohortGroups;
                    maxGroupsPerCohort = Math.max(maxGroupsPerCohort, cohortGroups);
                    completions.execute(() -> complete(cohort));
                }
            }
        } catch (Throwable t) {
            synchronized (lock) {
                fenceLocked(t);
            }
        }
    }

    /** Read and revalidate the whole committed block while its physical range is pinned. */
    AlignedWalBlockCodec.Entry read(long offset) throws IOException {
        final Committed block;
        final BlockRingAllocator.Pin pin;
        synchronized (lock) {
            if (closing || failure != null)
                throw new IOException("Ring coordinator fenced or closed", failure);
            var candidate = committed.floorEntry(offset);
            if (candidate == null || offset >= candidate.getValue().address().end())
                throw new IOException("Unknown WAL location");
            block = candidate.getValue();
            pin = allocator.pin(block.address());
            activeReads++;
        }
        try (pin) {
            var decoded = readBlock(block.address());
            for (var group : decoded.groups())
                for (var entry : group.records()) {
                    if (entry.walOffset() == offset) return entry;
                }
            throw new IOException("Unknown WAL record");
        } finally {
            synchronized (lock) {
                activeReads--;
                lock.notifyAll();
            }
        }
    }

    private AlignedWalBlockCodec.DecodedBlock readBlock(BlockRingAllocator.Address address)
            throws IOException {
        int length = Math.toIntExact(address.end() - address.blockStart());
        ByteBuffer data = ByteBuffer.allocate(length);
        long offset = Math.addExact(dataStart, address.physical());
        while (data.hasRemaining()) {
            int read = reader.read(data, offset + data.position());
            if (read <= 0) throw new IOException("Short ring read");
        }
        return AlignedWalBlockCodec.decode(data.flip(), address.blockStart(), high, low, alignment);
    }

    void replay(WalReplayConsumer consumer) throws IOException {
        Objects.requireNonNull(consumer, "consumer");
        List<Committed> snapshot;
        List<BlockRingAllocator.Pin> pins = new ArrayList<>();
        synchronized (lock) {
            checkOpen();
            snapshot = List.copyOf(committed.values());
            try {
                for (Committed block : snapshot) pins.add(allocator.pin(block.address()));
            } catch (IOException e) {
                for (var pin : pins) pin.close();
                throw e;
            }
            activeReads++;
        }
        boolean previous = replaying.get();
        replaying.set(true);
        try {
            for (Committed block : snapshot) {
                for (var group : readBlock(block.address()).groups())
                    for (var entry : group.records())
                        consumer.accept(
                                entry.record(),
                                new WalAppendResult(entry.walOffset(), entry.length()));
            }
        } finally {
            replaying.set(previous);
            for (var pin : pins) pin.close();
            synchronized (lock) {
                activeReads--;
                lock.notifyAll();
            }
        }
    }

    /**
     * Reclaim complete oldest blocks only after policy approval, reader drainage and head force.
     */
    long reclaim(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        Objects.requireNonNull(policy, "policy");
        if (desiredBytes <= 0) throw new IllegalArgumentException("Invalid desired bytes");
        if (replaying.get()) throw new IOException("Cannot reclaim from replay callback");
        synchronized (lock) {
            checkOpen();
            if (reclaiming) return 0;
            reclaiming = true;
        }
        inReclaim.set(true);
        try {
            long released = 0;
            while (released < desiredBytes) {
                long count = reclaimBatch(policy, desiredBytes - released);
                if (count == 0) break;
                released = Math.addExact(released, count);
            }
            return released;
        } finally {
            inReclaim.remove();
            synchronized (lock) {
                reclaiming = false;
                lock.notifyAll();
            }
        }
    }

    private record ReclaimSnapshot(List<Committed> blocks, List<BlockRingAllocator.Pin> pins) { }
    private record ApprovedPrefix(List<Committed> blocks, long lastId) { }

    private ReclaimSnapshot pinReclaimCandidates(long desiredBytes) throws IOException {
        List<Committed> candidates = new ArrayList<>();
        List<BlockRingAllocator.Pin> pins = new ArrayList<>();
        synchronized (lock) {
            checkHealthy();
            long selectedBytes = 0;
            try {
                for (Committed block : committed.values()) {
                    if (!block.published || candidates.size() == MAX_RECLAIM_BLOCKS || selectedBytes >= desiredBytes)
                        break;
                    pins.add(allocator.pin(block.address()));
                    candidates.add(block);
                    selectedBytes = Math.addExact(selectedBytes,
                        block.address().end() - block.address().reservedStart());
                }
            } catch (IOException | RuntimeException failure) {
                for (var pin : pins) pin.close();
                throw failure;
            }
        }
        return new ReclaimSnapshot(candidates, pins);
    }

    private ApprovedPrefix approveReclaimPrefix(List<Committed> candidates, WalReclaimPolicy policy) throws IOException {
        List<Committed> approved = new ArrayList<>();
        long lastId = -1;
        for (Committed block : candidates) {
            var decoded = readBlock(block.address());
            if (!isCovered(decoded, policy)) break;
            approved.add(block);
            lastId = decoded.groups().get(decoded.groups().size() - 1).id();
        }
        return new ApprovedPrefix(approved, lastId);
    }

    private boolean isCovered(AlignedWalBlockCodec.DecodedBlock block, WalReclaimPolicy policy) {
        for (var group : block.groups()) {
            for (var entry : group.records()) {
                if (!policy.canReclaim(entry.record(), new WalAppendResult(entry.walOffset(), entry.length())))
                    return false;
            }
        }
        return true;
    }

    private List<BlockRingAllocator.Retirement> prepareRetirements(List<Committed> approved) throws IOException {
        synchronized (lock) {
            checkHealthy();
            var iterator = committed.values().iterator();
            for (Committed block : approved) {
                if (!iterator.hasNext() || iterator.next() != block)
                    throw new IOException("Reclaim prefix changed");
            }
            return allocator.prepareRetirements(approved.stream().map(Committed::address).toList());
        }
    }

    private long reclaimBatch(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        ReclaimSnapshot snapshot = pinReclaimCandidates(desiredBytes);
        ApprovedPrefix approved;
        List<BlockRingAllocator.Retirement> retirements;
        try {
            approved = approveReclaimPrefix(snapshot.blocks(), policy);
            if (approved.blocks().isEmpty()) return 0;
            retirements = prepareRetirements(approved.blocks());
        } finally {
            for (var pin : snapshot.pins()) pin.close();
        }
        for (var retirement : retirements) awaitReaders(retirement);
        var done = new CompletableFuture<Long>();
        synchronized (lock) {
            checkHealthy();
            checkpoints.execute(() -> finishRetirements(approved.blocks(), retirements, approved.lastId(), done));
        }
        try {
            return done.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            synchronized (lock) {
                fenceLocked(e);
            }
            throw new IOException("Interrupted waiting for ring head checkpoint", e);
        } catch (ExecutionException e) {
            throw new IOException("Ring head checkpoint failed", e.getCause());
        }
    }

    private void finishRetirements(List<Committed> blocks, List<BlockRingAllocator.Retirement> retirements,
                                   long lastId, CompletableFuture<Long> done) {
        try {
            checkpoint.advanceHead(retirements.get(retirements.size() - 1).newHead(), lastId);
            long rebase = -1;
            long released = 0;
            synchronized (lock) {
                checkHealthy();
                for (int i = 0; i < blocks.size(); i++) {
                    allocator.headCheckpointsForced(retirements.get(i));
                    Committed block = blocks.get(i);
                    committed.remove(block.address().blockStart());
                    released = Math.addExact(released, block.address().end() - block.address().reservedStart());
                }
                if (committed.isEmpty() && pending.isEmpty()) {
                    long target = allocator.emptyRebaseTarget();
                    if (target != committedTail) {
                        rebasing = true;
                        rebase = target;
                    }
                }
            }
            if (rebase >= 0) {
                checkpoint.rebaseEmpty(rebase);
                synchronized (lock) {
                    checkHealthy();
                    allocator.emptyRebaseCheckpointForced(rebase);
                    committedTail = rebase;
                    rebasing = false;
                }
            }
            done.complete(released);
        } catch (Throwable t) {
            synchronized (lock) {
                fenceLocked(t);
            }
            done.completeExceptionally(t);
        }
    }

    private void awaitReaders(BlockRingAllocator.Retirement retirement) throws IOException {
        try {
            retirement.readersDrained().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            synchronized (lock) {
                fenceLocked(e);
            }
            throw new IOException("Interrupted waiting for ring readers", e);
        } catch (ExecutionException e) {
            synchronized (lock) {
                fenceLocked(e);
            }
            throw new IOException("Reader drainage failed", e);
        }
    }

    private void checkHealthy() throws IOException {
        if (failure != null) throw new IOException("Ring coordinator fenced", failure);
    }

    private void checkOpen() throws IOException {
        if (closing || failure != null)
            throw new IOException("Ring coordinator fenced or closed", failure);
    }

    void validateCachedRead() throws IOException {
        synchronized (lock) {
            checkOpen();
        }
    }

    WalDurabilityStats durabilityStats() {
        synchronized (lock) {
            return new WalDurabilityStats(durableCohorts, durableGroups, durableBytes, 0, maxGroupsPerCohort);
        }
    }

    boolean isCallbackContext() {
        return replaying.get() || inReclaim.get()
                || Thread.currentThread() == checkpointThread
                || Thread.currentThread() == completionThread;
    }

    private void complete(List<Pending> cohort) {
        try {
            for (Pending item : cohort) {
                item.result.complete(item.locations.get());
                synchronized (lock) {
                    Committed block = committed.get(item.address.blockStart());
                    if (block != null) block.published = true;
                }
            }
        } catch (Throwable t) {
            for (Pending item : cohort) item.result.completeExceptionally(t);
            synchronized (lock) {
                fenceLocked(t);
            }
        }
    }

    private void fenceLocked(Throwable error) {
        if (failure != null) return;
        failure = error;
        allocator.fence();
        List<Pending> failed = new ArrayList<>(pending.values());
        pending.clear();
        completions.execute(() -> failed.forEach(item -> item.result.completeExceptionally(error)));
    }

    @Override
    public void close() throws IOException {
        if (isCallbackContext()) throw new IOException("Cannot close coordinator from callback");
        boolean interrupted = false;
        synchronized (lock) {
            closing = true;
            while (activeReads != 0 || reclaiming) {
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        IOException error = null;
        try {
            writer.close();
        } catch (IOException e) {
            error = e;
        }
        checkpoints.shutdown();
        try {
            await(checkpoints);
        } catch (IOException e) {
            error = merge(error, e);
        }
        completions.shutdown();
        try {
            await(completions);
        } catch (IOException e) {
            error = merge(error, e);
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            error = merge(error, new IOException("Interrupted while closing ring coordinator"));
        }
        if (error != null) throw error;
    }

    private static IOException merge(IOException first, IOException next) {
        if (first == null) return next;
        first.addSuppressed(next);
        return first;
    }

    private static void await(ExecutorService executor) throws IOException {
        boolean interrupted = false;
        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing ring coordinator");
        }
    }
}
