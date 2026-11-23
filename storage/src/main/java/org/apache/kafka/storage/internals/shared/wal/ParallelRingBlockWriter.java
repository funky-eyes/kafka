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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongFunction;

/**
 * Experimental physical ring writer. A future means only that this block and
 * its padding have been forced; it is not a checkpoint commit or Kafka ACK.
 * On any admitted I/O/encoding failure the allocator is fenced. The owner
 * must serialize checkpoint publication and perform lifecycle/ownership setup.
 */
final class ParallelRingBlockWriter implements AutoCloseable {
    static final class BusyException extends IOException {
        BusyException(String message) {
            super(message);
        }
    }
    record Submission(BlockRingAllocator.Address address, CompletableFuture<BlockRingAllocator.Address> forced) { }
    record WindowSubmission(BlockRingAllocator.Address address, long firstGroupId,
                            CompletableFuture<List<List<WalAppendResult>>> forced) { }
    private record WrittenBlock(BlockRingAllocator.Address address,
                                CompletableFuture<BlockRingAllocator.Address> result) { }

    private final Object lock = new Object();
    private final WalIoBackend.Handle io;
    private final BlockRingAllocator allocator;
    private final long dataStart;
    private final long capacity;
    private final int alignment;
    private final long high;
    private final long low;
    private final int maxPending;
    private final ExecutorService writers;
    private final ThreadLocal<Boolean> worker = ThreadLocal.withInitial(() -> false);
    private List<WrittenBlock> awaitingForce = new ArrayList<>();
    private int pending;
    private boolean forcing;
    private boolean closed;

    ParallelRingBlockWriter(WalIoBackend.Handle io, BlockRingAllocator allocator, long dataStart, long capacity,
                            int alignment, long high, long low, int writerCount, int maxPending) {
        if (io == null || allocator == null || dataStart < 0 || capacity <= 0 || alignment < 4096)
            throw new IllegalArgumentException("Invalid ring writer configuration");
        if ((alignment & (alignment - 1)) != 0 || capacity % alignment != 0 ||
            dataStart > Long.MAX_VALUE - capacity || !allocator.matchesLayout(capacity, alignment) ||
            !allocator.matchesWindowWriter(io, dataStart, high, low))
            throw new IllegalArgumentException("Invalid ring writer configuration");
        if (writerCount <= 0 || maxPending <= 0)
            throw new IllegalArgumentException("Invalid ring writer configuration");
        this.io = io;
        this.allocator = allocator;
        this.dataStart = dataStart;
        this.capacity = capacity;
        this.alignment = alignment;
        this.high = high;
        this.low = low;
        this.maxPending = maxPending;
        this.writers = Executors.newFixedThreadPool(writerCount);
    }

    /** Encoder transfers its buffer until this write finishes. */
    CompletableFuture<BlockRingAllocator.Address> appendOwned(int length, LongFunction<ByteBuffer> encoder) {
        try {
            return submitOwned(length, encoder).forced();
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /** Returns the reservation synchronously, before its asynchronous force can complete. */
    Submission submitOwned(int length, LongFunction<ByteBuffer> encoder) throws IOException {
        if (encoder == null) throw new IllegalArgumentException("Missing block encoder");
        CompletableFuture<BlockRingAllocator.Address> result = new CompletableFuture<>();
        synchronized (lock) {
            if (closed) throw new IOException("Ring writer closed");
            if (pending >= maxPending) throw new BusyException("Ring writer busy");
            final BlockRingAllocator.Address address = allocator.reserve(length);
            pending++;
            try {
                writers.execute(() -> write(address, length, encoder, result));
            } catch (Throwable t) {
                pending--;
                allocator.fence();
                result.completeExceptionally(t);
            }
            return new Submission(address, result.copy());
        }
    }

    /**
     * A restricted window writer owns group-id assignment and only reports the
     * data force. It never grants Kafka visibility, checkpoint commit or trim.
     */
    WindowSubmission submitWindowed(int length, List<List<WalRecord>> input) throws IOException {
        List<List<WalRecord>> groups = copyWindowGroups(input);
        CompletableFuture<BlockRingAllocator.Address> result = new CompletableFuture<>();
        AtomicReference<List<List<WalAppendResult>>> locations = new AtomicReference<>();
        synchronized (lock) {
            if (closed) throw new IOException("Ring writer closed");
            if (pending >= maxPending) throw new BusyException("Ring writer busy");
            var reserved = allocator.reserveWindow(length, groups.size());
            pending++;
            try {
                writers.execute(() -> write(reserved.address(), length, offset -> {
                    List<AlignedWalBlockCodec.Group> encoded = new ArrayList<>(groups.size());
                    for (int i = 0; i < groups.size(); i++)
                        encoded.add(new AlignedWalBlockCodec.Group(reserved.firstGroupId() + i, groups.get(i)));
                    var block = AlignedWalBlockCodec.encodeWithLocations(offset, high, low, alignment, encoded);
                    locations.set(block.groups());
                    return block.bytes();
                }, result));
            } catch (Throwable failure) {
                pending--;
                allocator.fence();
                result.completeExceptionally(failure);
            }
            var forced = result.thenApply(ignored -> {
                var written = locations.get();
                if (written == null) throw new IllegalStateException("Missing encoded WAL locations");
                return written;
            });
            return new WindowSubmission(reserved.address(), reserved.firstGroupId(), forced.copy());
        }
    }

    private static List<List<WalRecord>> copyWindowGroups(List<List<WalRecord>> input) {
        if (input == null || input.isEmpty()) throw new IllegalArgumentException("Empty WAL groups");
        List<List<WalRecord>> groups = new ArrayList<>(input.size());
        for (var group : input) {
            var records = List.copyOf(group);
            if (records.isEmpty()) throw new IllegalArgumentException("Empty WAL group");
            for (var record : records) {
                if (record.type() == WalRecordType.GROUP_COMMIT) throw new IllegalArgumentException("Internal record");
            }
            groups.add(records);
        }
        return groups;
    }

    private void write(BlockRingAllocator.Address address, int length, LongFunction<ByteBuffer> encoder,
                       CompletableFuture<BlockRingAllocator.Address> result) {
        worker.set(true);
        try {
            try {
                writeBlock(address, length, encoder);
            } catch (Throwable t) {
                allocator.fence();
                synchronized (lock) {
                    pending--;
                }
                result.completeExceptionally(t);
                return;
            }
            enqueueForForce(new WrittenBlock(address, result));
        } finally {
            worker.remove();
        }
    }

    private void writeBlock(BlockRingAllocator.Address address, int length, LongFunction<ByteBuffer> encoder)
            throws IOException {
        ByteBuffer data = encoder.apply(address.blockStart());
        if (data == null || data.remaining() != length) throw new IOException("Encoded length differs from reservation");
        if (address.padding() != 0) {
            ByteBuffer padding = BlockRingRecovery.padding(address.reservedStart(), address.padding(), high, low,
                alignment, capacity);
            writeFully(padding, dataStart + address.reservedStart() % capacity);
        }
        writeFully(data.duplicate(), dataStart + address.physical());
    }

    private void enqueueForForce(WrittenBlock block) {
        synchronized (lock) {
            awaitingForce.add(block);
            if (forcing) return;
            forcing = true;
        }
        forceWrittenBlocks();
    }

    private void forceWrittenBlocks() {
        while (true) {
            List<WrittenBlock> cohort;
            synchronized (lock) {
                if (awaitingForce.isEmpty()) {
                    forcing = false;
                    return;
                }
                cohort = awaitingForce;
                awaitingForce = new ArrayList<>();
            }
            Throwable failure = null;
            try {
                // A force covers only the writes completed before this cohort was taken.
                io.force();
                for (WrittenBlock block : cohort) allocator.dataForced(block.address());
            } catch (Throwable t) {
                allocator.fence();
                failure = t;
            }
            synchronized (lock) {
                pending -= cohort.size();
            }
            for (WrittenBlock block : cohort) {
                if (failure == null) block.result().complete(block.address());
                else block.result().completeExceptionally(failure);
            }
        }
    }

    private void writeFully(ByteBuffer source, long offset) throws IOException {
        while (source.hasRemaining()) {
            int written = io.write(source, offset);
            if (written <= 0) throw new IOException("Short ring block write");
            offset += written;
        }
    }

    /** Waits for admitted writes; the handle remains owned by the caller. */
    @Override
    public void close() throws IOException {
        if (worker.get()) throw new IOException("Cannot close ring writer from its callback");
        synchronized (lock) {
            if (!closed) {
                closed = true;
                writers.shutdown();
            }
        }
        boolean interrupted = false;
        while (!writers.isTerminated()) {
            try {
                writers.awaitTermination(1, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while closing ring writer");
        }
    }
}
