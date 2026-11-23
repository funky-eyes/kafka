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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Experimental ring reservation and reader-pin state machine. It performs no
 * I/O. Callers must durably force the data and
 * head/tail checkpoints before invoking the corresponding confirmation methods.
 */
final class BlockRingAllocator {
    static final class CapacityException extends IOException {
        CapacityException(String message) {
            super(message);
        }
    }
    record Address(long reservedStart, long blockStart, long end, long physical, long generation, long padding) { }

    private enum State {
        RESERVED, FORCED, COMMITTED, RETIRING, QUARANTINED
    }
    private static final class Entry {
        final Address address;
        State state = State.RESERVED;
        int pins;
        CompletableFuture<Void> drained;
        Entry(Address address) {
            this.address = address;
        }
    }

    final class Pin implements AutoCloseable {
        private final Entry entry;
        private final AtomicBoolean closed = new AtomicBoolean();
        private Pin(Entry entry) {
            this.entry = entry;
        }
        Address address() {
            return entry.address;
        }
        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) return;
            CompletableFuture<Void> notify = null;
            synchronized (lock) {
                entry.pins--;
                if (entry.pins == 0 && entry.drained != null) notify = entry.drained;
            }
            if (notify != null) notify.complete(null);
        }
    }

    static final class Retirement {
        private final Entry entry;
        private Retirement(Entry entry) {
            this.entry = entry;
        }
        long newHead() {
            return entry.address.end();
        }
        CompletableFuture<Void> readersDrained() {
            return entry.drained.copy();
        }
    }

    private final Object lock = new Object();
    private final NavigableMap<Long, Entry> entries = new TreeMap<>();
    private final long capacity;
    private final int alignment;
    private BlockRingCheckpoint windowCheckpoint;
    private long nextWindowGroup;
    private long head;
    private long reservedTail;
    private long committedTail;
    private boolean fenced;

    BlockRingAllocator(long capacity, int alignment) {
        if (alignment <= 0 || capacity <= 0 || capacity % alignment != 0)
            throw new IllegalArgumentException("Invalid ring layout");
        this.capacity = capacity;
        this.alignment = alignment;
    }

    boolean matchesLayout(long expectedCapacity, int expectedAlignment) {
        return capacity == expectedCapacity && alignment == expectedAlignment;
    }

    long committedTail() throws IOException {
        synchronized (lock) {
            checkOpen();
            return committedTail;
        }
    }

    long usedBytes() {
        synchronized (lock) {
            return reservedTail - head;
        }
    }

    /** The next physical origin is safe only when no reservation or retained block exists. */
    long emptyRebaseTarget() throws IOException {
        synchronized (lock) {
            checkOpen();
            if (!entries.isEmpty() || head != reservedTail || committedTail != reservedTail)
                throw new IOException("Cannot rebase a nonempty ring");
            long physical = reservedTail % capacity;
            return physical == 0 ? reservedTail : Math.addExact(reservedTail, capacity - physical);
        }
    }

    /** Call only after the empty head/tail checkpoint itself is durably forced. */
    void emptyRebaseCheckpointForced(long offset) throws IOException {
        synchronized (lock) {
            if (offset != emptyRebaseTarget()) throw new IOException("Invalid empty ring rebase");
            head = offset;
            reservedTail = offset;
            committedTail = offset;
        }
    }

    /** Restores only a prefix already validated against the durable ring checkpoint. */
    static BlockRingAllocator recover(long capacity, int alignment, long head, long tail,
                                      BlockRingRecovery.Scan scan) throws IOException {
        BlockRingAllocator result = new BlockRingAllocator(capacity, alignment);
        if (head < 0 || tail < head || tail - head > capacity || head % alignment != 0 ||
            tail % alignment != 0 || scan == null) throw new IOException("Invalid recovered ring bounds");
        long cursor = head;
        for (BlockRingRecovery.RecoveredBlock block : scan.blocks()) {
            long start = block.blockStart();
            long end = block.end();
            if (!result.validRecoveredBlock(block, cursor, tail))
                throw new IOException("Invalid recovered ring block");
            Address address = new Address(cursor, start, end, start % capacity, start / capacity, start - cursor);
            Entry entry = new Entry(address);
            entry.state = State.COMMITTED;
            result.entries.put(cursor, entry);
            cursor = end;
        }
        if (cursor != tail) throw new IOException("Incomplete recovered ring blocks");
        result.head = head;
        result.reservedTail = tail;
        result.committedTail = tail;
        return result;
    }

    record WindowRecovery(BlockRingAllocator allocator, long firstGroupId) { }

    /**
     * Preserve every byte in the old scan window as occupied, including holes.
     * The caller must reconcile it before publication or reclamation. A new
     * reservation is permitted only within a separately forced scan ceiling.
     */
    static WindowRecovery recoverWindow(BlockRingCheckpoint checkpoint, BlockRingRecovery.Scan prefix,
                                        long capacity, int alignment) throws IOException {
        if (checkpoint == null || !checkpoint.windowed() || checkpoint.capacity() != capacity)
            throw new IOException("Invalid windowed WAL checkpoint");
        var identity = checkpoint.identity();
        if (!checkpoint.matchesLayout(alignment, identity.high(), identity.low()))
            throw new IOException("Invalid windowed WAL alignment");
        var state = checkpoint.current();
        if (prefix == null || prefix.lastGroup() != state.lastGroup())
            throw new IOException("Recovered prefix group differs from checkpoint");
        BlockRingAllocator result = recover(capacity, alignment, state.head(), state.tail(), prefix);
        if (state.scanCeiling() > state.tail()) {
            long start = state.tail();
            Address gap = new Address(start, start, state.scanCeiling(), start % capacity, start / capacity, 0);
            Entry quarantined = new Entry(gap);
            quarantined.state = State.QUARANTINED;
            result.entries.put(start, quarantined);
        }
        result.reservedTail = state.scanCeiling();
        result.windowCheckpoint = checkpoint;
        // Every allocated group consumes at least one byte inside the old
        // window. Jump past this conservative upper bound even for groups
        // whose headers were torn or whose writes never reached the disk.
        final long firstGroup;
        try {
            firstGroup = Math.addExact(Math.addExact(state.lastGroup(), state.scanCeiling() - state.tail()), 1);
        } catch (ArithmeticException exhausted) {
            throw new IOException("WAL recovery group id space exhausted", exhausted);
        }
        result.nextWindowGroup = firstGroup;
        return new WindowRecovery(result, firstGroup);
    }

    private boolean validRecoveredBlock(BlockRingRecovery.RecoveredBlock block, long cursor, long tail) {
        long start = block.blockStart(), end = block.end(), remaining = capacity - cursor % capacity;
        if (block.reservedStart() != cursor || start < cursor || end <= start || end > tail) return false;
        if (start % alignment != 0 || end % alignment != 0 || end - start > capacity) return false;
        if (start != cursor && (start - cursor != remaining || start % capacity != 0 || end - start <= remaining))
            return false;
        return end - start <= capacity - start % capacity;
    }

    List<Address> committedAddresses() throws IOException {
        synchronized (lock) {
            checkOpen();
            return entries.values().stream().filter(entry -> entry.state == State.COMMITTED)
                .map(entry -> entry.address).toList();
        }
    }

    record WindowAddress(Address address, long firstGroupId, int groupCount) { }

    Address reserve(int length) throws IOException {
        validateLength(length);
        synchronized (lock) {
            if (windowCheckpoint != null) throw new IOException("Windowed reservations require group fencing");
            return reserveLocked(length);
        }
    }

    /** Reserve physical space and a fresh group-id range in one critical section. */
    WindowAddress reserveWindow(int length, int groupCount) throws IOException {
        validateLength(length);
        if (groupCount <= 0 || groupCount > length) throw new IllegalArgumentException("Invalid group count");
        synchronized (lock) {
            if (windowCheckpoint == null) throw new IOException("Not a windowed allocator");
            long after;
            try {
                after = Math.addExact(nextWindowGroup, groupCount);
            } catch (ArithmeticException exhausted) {
                throw new IOException("WAL recovery group id space exhausted", exhausted);
            }
            final Address address;
            try {
                address = reserveLocked(length);
            } catch (ArithmeticException exhausted) {
                throw new IOException("WAL logical address space exhausted", exhausted);
            }
            long firstGroup = nextWindowGroup;
            nextWindowGroup = after;
            return new WindowAddress(address, firstGroup, groupCount);
        }
    }

    boolean matchesWindowWriter(WalIoBackend.Handle io, long dataStart, long high, long low) {
        return windowCheckpoint == null || windowCheckpoint.matchesWindowWriter(io, dataStart, high, low);
    }

    private void validateLength(int length) {
        if (length <= 0 || length % alignment != 0 || length > capacity)
            throw new IllegalArgumentException("Invalid block length");
    }

    private Address reserveLocked(int length) throws IOException {
        checkOpen();
        long physical = reservedTail % capacity;
        long remaining = capacity - physical;
        long padding = remaining < length ? remaining : 0;
        long start = Math.addExact(reservedTail, padding);
        long end = Math.addExact(start, length);
        if (end - head > capacity) throw new CapacityException("Ring capacity exceeded");
        if (windowCheckpoint != null && end > windowCheckpoint.current().scanCeiling())
            throw new CapacityException("Reservation exceeds the forced WAL scan ceiling");
        Address address = new Address(reservedTail, start, end, start % capacity, start / capacity, padding);
        entries.put(reservedTail, new Entry(address));
        reservedTail = end;
        return address;
    }

    void dataForced(Address address) throws IOException {
        synchronized (lock) {
            checkOpen();
            Entry entry = find(address);
            if (entry.state != State.RESERVED) throw new IOException("Unexpected data completion");
            // If padding is nonzero, its authenticated marker must also have
            // completed and been forced before this notification.
            entry.state = State.FORCED;
        }
    }

    long contiguousForcedTail() throws IOException {
        synchronized (lock) {
            checkOpen();
            long cursor = committedTail;
            for (Entry entry : entries.tailMap(committedTail, true).values()) {
                if (entry.address.reservedStart() != cursor || entry.state != State.FORCED) break;
                cursor = entry.address.end();
            }
            return cursor;
        }
    }

    /** Only after the target tail checkpoint has itself completed and been forced. */
    void tailCheckpointForced(long newTail) throws IOException {
        synchronized (lock) {
            checkOpen();
            if (newTail <= committedTail || newTail > contiguousForcedTail())
                throw new IOException("Tail is not a forced contiguous prefix");
            var end = entries.lowerEntry(newTail);
            if (end == null || end.getValue().address.end() != newTail)
                throw new IOException("Tail must end on a block boundary");
            for (Entry entry : entries.subMap(committedTail, true, newTail, false).values()) {
                entry.state = State.COMMITTED;
            }
            committedTail = newTail;
        }
    }

    Pin pin(Address address) throws IOException {
        synchronized (lock) {
            checkOpen();
            Entry entry = find(address);
            if (entry.state != State.COMMITTED) throw new IOException("Block is not readable");
            entry.pins++;
            return new Pin(entry);
        }
    }

    /** Caller must establish durable remote coverage for every block in the prefix. */
    List<Retirement> prepareRetirements(List<Address> addresses) throws IOException {
        synchronized (lock) {
            checkOpen();
            if (addresses.isEmpty()) throw new IOException("Empty retirement prefix");
            var iterator = entries.values().iterator();
            List<Entry> prefix = new ArrayList<>(addresses.size());
            for (Address address : addresses) {
                if (!iterator.hasNext()) throw new IOException("Retirement prefix is not present");
                Entry entry = iterator.next();
                if (entry.address != address || entry.state != State.COMMITTED)
                    throw new IOException("Retirement prefix is not safely reclaimable");
                prefix.add(entry);
            }
            List<Retirement> retirements = new ArrayList<>(prefix.size());
            for (Entry entry : prefix) {
                entry.state = State.RETIRING;
                entry.drained = new CompletableFuture<>();
                if (entry.pins == 0) entry.drained.complete(null);
                retirements.add(new Retirement(entry));
            }
            return retirements;
        }
    }

    /** Only after reader drainage and all required head checkpoint copies are forced. */
    void headCheckpointsForced(Retirement retirement) throws IOException {
        synchronized (lock) {
            checkOpen();
            Entry first = entries.get(head);
            if (retirement == null || first == null || first != retirement.entry ||
                first.state != State.RETIRING || first.pins != 0)
                throw new IOException("Retirement is not ready");
            entries.remove(head);
            head = first.address.end();
        }
    }

    void fence() {
        synchronized (lock) {
            fenced = true;
        }
    }

    private Entry find(Address address) throws IOException {
        Entry entry = entries.get(address.reservedStart());
        if (entry == null || entry.address != address) throw new IOException("Unknown or retired block");
        return entry;
    }
    private void checkOpen() throws IOException {
        if (fenced) throw new IOException("Ring allocator fenced");
    }
}
