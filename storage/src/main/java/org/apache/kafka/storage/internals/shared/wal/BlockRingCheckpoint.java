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
import java.util.zip.CRC32C;

/**
 * Experimental serialized ring checkpoints. Confirmation is not a Kafka ACK.
 * The caller must first force the complete data prefix for tail advancement,
 * or prove durable remote coverage and drain reader pins for head advancement.
 * Slot fault isolation and filesystem force semantics are external assumptions.
 */
final class BlockRingCheckpoint {
    private static final int MAGIC = 0x4b575243;
    private static final int VERSION = 1;
    private static final int CRC_OFFSET = 80;
    record State(long generation, long head, long tail, long retiredLastGroup, long lastGroup) { }
    record Opened(BlockRingCheckpoint checkpoint, BlockRingRecovery.Scan scan) { }
    record Identity(long high, long low) { }

    private final WalIoBackend.Handle io;
    private final int alignment;
    private final long capacity;
    private final long high;
    private final long low;
    private State current;
    private boolean fenced;

    private BlockRingCheckpoint(WalIoBackend.Handle io, int alignment, long capacity, long high, long low, State current) {
        this.io = io;
        this.alignment = alignment;
        this.capacity = capacity;
        this.high = high;
        this.low = low;
        this.current = current;
    }

    /** Fresh, exclusively owned, preallocated file; caller owns crash-safe publication. */
    static BlockRingCheckpoint initialize(WalIoBackend.Handle io, int alignment, long capacity, long high, long low)
        throws IOException {
        validateLayout(alignment, capacity);
        var initial = new State(1, 0, 0, -1, -1);
        var checkpoint = new BlockRingCheckpoint(io, alignment, capacity, high, low, initial);
        for (int slot = 0; slot < 3; slot++) checkpoint.writeSlot(initial, slot);
        return checkpoint;
    }

    /** Verify committed contents before repairing copies or accepting further writes. */
    static Opened open(WalIoBackend.Handle io, int alignment, long capacity, long high, long low) throws IOException {
        validateLayout(alignment, capacity);
        var checkpoint = new BlockRingCheckpoint(io, alignment, capacity, high, low, null);
        State[] slots = new State[3];
        State newest = null;
        int valid = 0;
        for (int slot = 0; slot < 3; slot++) {
            slots[slot] = checkpoint.readSlot(slot);
            if (slots[slot] != null) {
                valid++;
                if (newest == null || slots[slot].generation() > newest.generation()) newest = slots[slot];
            }
        }
        if (valid < 2) throw new IOException("Fewer than two valid ring checkpoints");
        verifyCopies(slots, newest);
        var scan = BlockRingRecovery.scan(io, 3L * alignment, capacity, alignment, newest.head(), newest.tail(),
            newest.retiredLastGroup(), newest.lastGroup(), high, low);
        for (int slot = 0; slot < 3; slot++) {
            if (!newest.equals(slots[slot])) checkpoint.writeSlot(newest, slot);
        }
        synchronized (checkpoint) {
            checkpoint.current = newest;
        }
        return new Opened(checkpoint, scan);
    }

    /** Discover the incarnation from a valid slot, then require the normal two-copy recovery checks. */
    static Opened open(WalIoBackend.Handle io, int alignment, long capacity) throws IOException {
        validateLayout(alignment, capacity);
        Identity identity = null;
        for (int slot = 0; slot < 3; slot++) {
            ByteBuffer header = ByteBuffer.allocate(alignment);
            long offset = (long) slot * alignment;
            while (header.hasRemaining()) {
                int read = io.read(header, offset + header.position());
                if (read <= 0) throw new IOException("Short ring checkpoint read");
            }
            header.flip();
            long high = header.getLong(64), low = header.getLong(72);
            var candidate = new BlockRingCheckpoint(io, alignment, capacity, high, low, null);
            State state = candidate.readSlot(slot);
            if (state != null) {
                Identity current = new Identity(high, low);
                if (identity != null && !identity.equals(current))
                    throw new IOException("Conflicting valid ring checkpoint identities");
                identity = current;
            }
        }
        if (identity == null) throw new IOException("No valid ring checkpoint identity");
        return open(io, alignment, capacity, identity.high(), identity.low());
    }

    Identity identity() {
        return new Identity(high, low);
    }

    private static void verifyCopies(State[] slots, State newest) throws IOException {
        for (State old : slots) {
            if (old == null) continue;
            if (newest.generation() - old.generation() > 2 || old.head() > newest.head() ||
                old.tail() > newest.tail() || old.retiredLastGroup() > newest.retiredLastGroup() ||
                old.lastGroup() > newest.lastGroup()) throw new IOException("Inconsistent ring checkpoint copies");
            if (old.generation() == newest.generation() && !old.equals(newest))
                throw new IOException("Inconsistent ring checkpoint copies");
        }
    }

    synchronized State current() throws IOException {
        checkOpen();
        return current;
    }

    boolean matchesLayout(int expectedAlignment, long expectedHigh, long expectedLow) {
        return alignment == expectedAlignment && high == expectedHigh && low == expectedLow;
    }

    /** Caller confirms every block and any wrap marker are forced in logical order. */
    synchronized State advanceTail(long tail, long lastGroup) throws IOException {
        checkOpen();
        if (tail <= current.tail() || lastGroup <= current.lastGroup())
            throw new IOException("Tail checkpoint did not advance");
        return commit(new State(nextGeneration(), current.head(), tail, current.retiredLastGroup(), lastGroup));
    }

    /** Caller confirms reader drainage and durable coverage through the new head. */
    synchronized State advanceHead(long head, long retiredLastGroup) throws IOException {
        checkOpen();
        if (head <= current.head() || retiredLastGroup <= current.retiredLastGroup())
            throw new IOException("Head checkpoint did not advance");
        return commit(new State(nextGeneration(), head, current.tail(), retiredLastGroup, current.lastGroup()));
    }

    /** Moves an entirely empty logical window to the next physical origin. */
    synchronized State rebaseEmpty(long newOffset) throws IOException {
        checkOpen();
        if (current.head() != current.tail() || current.tail() % capacity == 0 ||
            newOffset != Math.addExact(current.tail(), capacity - current.tail() % capacity))
            throw new IOException("Invalid empty ring rebase");
        return commit(new State(nextGeneration(), newOffset, newOffset,
            current.retiredLastGroup(), current.lastGroup()));
    }

    private State commit(State candidate) throws IOException {
        if (!valid(candidate)) throw new IOException("Invalid ring checkpoint transition");
        try {
            int first = (int) (candidate.generation() % 3);
            writeSlot(candidate, first);
            writeSlot(candidate, (first + 1) % 3);
            current = candidate;
            return candidate;
        } catch (Throwable t) {
            fenced = true;
            throw t;
        }
    }

    private long nextGeneration() throws IOException {
        if (current.generation() == Long.MAX_VALUE) throw new IOException("Ring checkpoint generation exhausted");
        return current.generation() + 1;
    }

    private void checkOpen() throws IOException {
        if (fenced) throw new IOException("Ring checkpoint is fenced");
    }

    private boolean valid(State state) {
        if (state.generation() <= 0 || state.head() < 0 || state.tail() < state.head()) return false;
        if (state.tail() - state.head() > capacity || state.head() % alignment != 0 || state.tail() % alignment != 0)
            return false;
        if (state.retiredLastGroup() < -1 || state.lastGroup() < state.retiredLastGroup()) return false;
        return state.head() != state.tail() || state.retiredLastGroup() == state.lastGroup();
    }

    private void writeSlot(State state, int slot) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(alignment);
        data.putInt(MAGIC).putInt(VERSION).putInt(alignment).putInt(0).putLong(capacity)
            .putLong(state.generation()).putLong(state.head()).putLong(state.tail())
            .putLong(state.retiredLastGroup()).putLong(state.lastGroup()).putLong(high).putLong(low).putInt(0);
        data.putInt(CRC_OFFSET, checksum(data));
        data.position(0);
        long offset = (long) slot * alignment;
        while (data.hasRemaining()) {
            int written = io.write(data, offset);
            if (written <= 0) throw new IOException("Short ring checkpoint write");
            offset += written;
        }
        io.force();
    }

    private State readSlot(int slot) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(alignment);
        long offset = (long) slot * alignment;
        while (data.hasRemaining()) {
            int read = io.read(data, offset + data.position());
            if (read <= 0) throw new IOException("Short ring checkpoint read");
        }
        data.flip();
        if (data.getInt(0) != MAGIC || data.getInt(4) != VERSION || data.getInt(8) != alignment ||
            data.getInt(12) != 0 || data.getLong(16) != capacity) return null;
        if (data.getLong(64) != high || data.getLong(72) != low || data.getInt(CRC_OFFSET) != checksum(data))
            return null;
        var state = new State(data.getLong(24), data.getLong(32), data.getLong(40), data.getLong(48), data.getLong(56));
        return valid(state) ? state : null;
    }

    private static void validateLayout(int alignment, long capacity) {
        if (alignment < 4096 || (alignment & (alignment - 1)) != 0 || capacity <= 0 ||
            capacity % alignment != 0 || capacity > Long.MAX_VALUE - 3L * alignment)
            throw new IllegalArgumentException("Invalid ring checkpoint layout");
    }

    private static int checksum(ByteBuffer source) {
        CRC32C crc = new CRC32C();
        ByteBuffer bytes = source.duplicate();
        bytes.position(0).limit(CRC_OFFSET);
        crc.update(bytes);
        bytes.limit(source.capacity()).position(CRC_OFFSET + Integer.BYTES);
        crc.update(bytes);
        return (int) crc.getValue();
    }
}
