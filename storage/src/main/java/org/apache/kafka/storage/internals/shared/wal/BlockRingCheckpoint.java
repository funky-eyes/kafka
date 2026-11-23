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
    private static final int CRC_OFFSET = 80;
    private static final int BANK_COPIES = 3;
    enum Layout {
        LEGACY(1, 3, 0),
        BANKED(2, 2 * BANK_COPIES, 0),
        // A separate metadata length lets open-by-file-size distinguish this
        // format without changing the interpretation of existing WAL files.
        WINDOWED(3, 2 * BANK_COPIES, 1);

        final int version;
        final int slots;
        final int reservedSectors;

        Layout(int version, int slots, int reservedSectors) {
            this.version = version;
            this.slots = slots;
            this.reservedSectors = reservedSectors;
        }

        long metadataBytes(int alignment) {
            return (long) (slots + reservedSectors) * alignment;
        }
    }

    record State(long generation, long head, long tail, long retiredLastGroup, long lastGroup,
                 long scanCeiling) { }
    record Opened(BlockRingCheckpoint checkpoint, BlockRingRecovery.Scan scan) { }
    record Identity(long high, long low) { }
    private record Selected(BlockRingCheckpoint checkpoint, State[] slots, State newest) { }
    private record Detected(Layout layout, long capacity, Identity identity) { }

    private final WalIoBackend.Handle io;
    private final int alignment;
    private final Layout layout;
    private final long capacity;
    private final long high;
    private final long low;
    private State current;
    private boolean windowAllocatorClaimed;
    private boolean fenced;

    private BlockRingCheckpoint(WalIoBackend.Handle io, int alignment, long capacity, long high, long low,
                                State current, Layout layout) {
        this.io = io;
        this.alignment = alignment;
        this.layout = layout;
        this.capacity = capacity;
        this.high = high;
        this.low = low;
        this.current = current;
    }

    /** Fresh, exclusively owned, preallocated file; caller owns crash-safe publication. */
    static BlockRingCheckpoint initialize(WalIoBackend.Handle io, int alignment, long capacity, long high, long low)
        throws IOException {
        Layout layout = layoutForSize(io.size(), alignment, capacity);
        validateLayout(alignment, capacity, layout);
        var initial = new State(1, 0, 0, -1, -1, 0);
        var checkpoint = new BlockRingCheckpoint(io, alignment, capacity, high, low, initial, layout);
        for (int slot = 0; slot < layout.slots; slot++) {
            checkpoint.writeSlot(initial, slot);
            if (layout == Layout.LEGACY) io.force();
        }
        if (layout != Layout.LEGACY) io.force();
        return checkpoint;
    }

    /** Verify committed contents before repairing copies or accepting further writes. */
    static Opened open(WalIoBackend.Handle io, int alignment, long capacity, long high, long low) throws IOException {
        return open(io, alignment, capacity, high, low, layoutForSize(io.size(), alignment, capacity));
    }

    private static Opened open(WalIoBackend.Handle io, int alignment, long capacity, long high, long low,
                               Layout layout) throws IOException {
        Selected selected = select(io, alignment, capacity, high, low, layout);
        BlockRingCheckpoint checkpoint = selected.checkpoint();
        State newest = selected.newest();
        // Until sparse-window recovery and Kafka reconciliation are wired,
        // opening a potentially nonempty speculative region must fail closed.
        if (layout == Layout.WINDOWED && newest.scanCeiling() != newest.tail())
            throw new IOException("Unreconciled WAL recovery window");
        var scan = BlockRingRecovery.scan(io, checkpoint.metadataBytes(), capacity, alignment,
            newest.head(), newest.tail(), newest.retiredLastGroup(), newest.lastGroup(), high, low);
        checkpoint.repair(selected.slots(), newest);
        synchronized (checkpoint) {
            checkpoint.current = newest;
        }
        return new Opened(checkpoint, scan);
    }

    /**
     * Select a windowed checkpoint for the internal restart fence. The caller
     * must exclusively own the file; this does not repair metadata, publish
     * candidates, authorize reclamation or enable Broker startup.
     */
    static Opened openWindowForReservationFence(WalIoBackend.Handle io, int alignment, long capacity,
                                                 long high, long low) throws IOException {
        Layout layout = layoutForSize(io.size(), alignment, capacity);
        if (layout != Layout.WINDOWED) throw new IOException("WAL does not use a scan window");
        Selected selected = select(io, alignment, capacity, high, low, layout);
        BlockRingCheckpoint checkpoint = selected.checkpoint();
        State state = selected.newest();
        var scan = BlockRingRecovery.scan(io, checkpoint.metadataBytes(), capacity, alignment,
            state.head(), state.tail(), state.retiredLastGroup(), state.lastGroup(), high, low);
        if (!state.equals(select(io, alignment, capacity, high, low, layout).newest()))
            throw new IOException("WAL checkpoint changed during reservation recovery");
        synchronized (checkpoint) {
            checkpoint.current = state;
        }
        return new Opened(checkpoint, scan);
    }

    /**
     * Inspect only after validating the stored identity, checkpoints and strict
     * prefix. The caller must hold exclusive WAL ownership. This method does
     * not repair metadata, publish records or authorize physical reuse.
     */
    static State inspectWindow(WalIoBackend.Handle io, int alignment, long capacity, long high, long low,
                               int maxBlock, BlockRingRecovery.WindowVisitor visitor) throws IOException {
        Layout layout = layoutForSize(io.size(), alignment, capacity);
        if (layout != Layout.WINDOWED) throw new IOException("WAL does not use a scan window");
        Selected selected = select(io, alignment, capacity, high, low, layout);
        State state = selected.newest();
        long dataStart = selected.checkpoint().metadataBytes();
        BlockRingRecovery.scan(io, dataStart, capacity, alignment, state.head(), state.tail(),
            state.retiredLastGroup(), state.lastGroup(), high, low);
        BlockRingRecovery.inspectWindow(io, dataStart, capacity, alignment, maxBlock,
            state.head(), state.tail(), state.scanCeiling(), high, low, visitor);
        if (!state.equals(select(io, alignment, capacity, high, low, layout).newest()))
            throw new IOException("WAL checkpoint changed during window inspection");
        return state;
    }

    /** Bounded metadata-only snapshot; no partially scanned result escapes on failure. */
    static BlockRingRecovery.WindowSnapshot inspectWindowSnapshot(WalIoBackend.Handle io, int alignment,
        long capacity, long high, long low, int maxBlock, int maxItems, int maxRecords) throws IOException {
        var builder = new BlockRingRecovery.WindowSnapshotBuilder(maxItems, maxRecords);
        State state = inspectWindow(io, alignment, capacity, high, low, maxBlock, builder);
        return builder.complete(high, low, capacity, state);
    }

    private static Selected select(WalIoBackend.Handle io, int alignment, long capacity, long high, long low,
                                   Layout layout) throws IOException {
        validateLayout(alignment, capacity, layout);
        var checkpoint = new BlockRingCheckpoint(io, alignment, capacity, high, low, null, layout);
        State[] slots = new State[layout.slots];
        State newest = null;
        int valid = 0;
        for (int slot = 0; slot < layout.slots; slot++) {
            slots[slot] = checkpoint.readSlot(slot);
            if (slots[slot] != null) {
                valid++;
                if (newest == null || slots[slot].generation() > newest.generation()) newest = slots[slot];
            }
        }
        int requiredValid = layout == Layout.LEGACY ? 2 : BANK_COPIES;
        if (valid < requiredValid) throw new IOException("Insufficient valid ring checkpoints");
        if (layout == Layout.WINDOWED && (!hasValidCopy(slots, 0) || !hasValidCopy(slots, BANK_COPIES)))
            throw new IOException("A WAL scan-window checkpoint bank is unreadable");
        verifyCopies(slots, newest);
        return new Selected(checkpoint, slots, newest);
    }

    private static boolean hasValidCopy(State[] slots, int start) {
        for (int slot = start; slot < start + BANK_COPIES; slot++) {
            if (slots[slot] != null) return true;
        }
        return false;
    }

    private void repair(State[] slots, State newest) throws IOException {
        if (layout == Layout.LEGACY) {
            for (int slot = 0; slot < layout.slots; slot++) {
                if (!newest.equals(slots[slot])) {
                    writeSlot(newest, slot);
                    io.force();
                }
            }
            return;
        }

        // Repair only the newest bank and leave every intact copy alone. In
        // particular, never overwrite the sole surviving newest copy on reopen.
        int first = bankStart(newest);
        boolean repaired = false;
        for (int slot = first; slot < first + BANK_COPIES; slot++) {
            if (!newest.equals(slots[slot])) {
                writeSlot(newest, slot);
                repaired = true;
            }
        }
        if (repaired) io.force();
    }

    static Opened open(WalIoBackend.Handle io, int alignment, long capacity) throws IOException {
        Layout layout = layoutForSize(io.size(), alignment, capacity);
        validateLayout(alignment, capacity, layout);
        if (layout == Layout.WINDOWED) throw new IOException("Windowed WAL recovery is not enabled for broker use");
        Identity identity = discoverIdentity(io, alignment, capacity, layout);
        if (identity == null) throw new IOException("No valid ring checkpoint identity");
        return open(io, alignment, capacity, identity.high(), identity.low(), layout);
    }

    /** Select the format from checksummed headers, with a fixed configured file size. */
    static Opened openByFileSize(WalIoBackend.Handle io, int alignment, long size) throws IOException {
        Detected detected = detectByFileSize(io, alignment, size);
        if (detected.layout() == Layout.WINDOWED)
            throw new IOException("Windowed WAL recovery is not enabled for broker use");
        return open(io, alignment, detected.capacity(), detected.identity().high(), detected.identity().low(),
            detected.layout());
    }

    /** Read-only inspection; the caller must hold exclusive ownership throughout use of the snapshot. */
    static BlockRingRecovery.WindowSnapshot inspectWindowSnapshotByFileSize(WalIoBackend.Handle io, int alignment,
        long size, int maxBlock, int maxItems, int maxRecords) throws IOException {
        Detected detected = detectByFileSize(io, alignment, size);
        if (detected.layout() != Layout.WINDOWED) throw new IOException("WAL does not use a scan window");
        return inspectWindowSnapshot(io, alignment, detected.capacity(), detected.identity().high(),
            detected.identity().low(), maxBlock, maxItems, maxRecords);
    }

    private static Detected detectByFileSize(WalIoBackend.Handle io, int alignment, long size) throws IOException {
        if (io.size() != size) throw new IOException("Unexpected ring WAL file size");
        Detected detected = null;
        for (Layout layout : Layout.values()) {
            long capacity = size - layout.metadataBytes(alignment);
            if (capacity <= 0) continue;
            validateLayout(alignment, capacity, layout);
            Identity candidate = discoverIdentity(io, alignment, capacity, layout);
            if (candidate != null) {
                if (detected != null) throw new IOException("Conflicting ring checkpoint formats");
                detected = new Detected(layout, capacity, candidate);
            }
        }
        if (detected == null) throw new IOException("No valid ring checkpoint format");
        return detected;
    }

    private static Layout layoutForSize(long size, int alignment, long capacity) throws IOException {
        for (Layout layout : Layout.values()) {
            if (size - layout.metadataBytes(alignment) == capacity) return layout;
        }
        throw new IOException("Unexpected ring WAL file size");
    }

    private static Identity discoverIdentity(WalIoBackend.Handle io, int alignment, long capacity, Layout layout)
        throws IOException {
        Identity identity = null;
        for (int slot = 0; slot < layout.slots; slot++) {
            ByteBuffer header = readSlotBytes(io, alignment, slot);
            long high = header.getLong(64), low = header.getLong(72);
            var candidate = new BlockRingCheckpoint(io, alignment, capacity, high, low, null, layout);
            if (candidate.decodeSlot(header, slot) != null) {
                Identity current = new Identity(high, low);
                if (identity != null && !identity.equals(current))
                    throw new IOException("Conflicting valid ring checkpoint identities");
                identity = current;
            }
        }
        return identity;
    }

    long metadataBytes() {
        return layout.metadataBytes(alignment);
    }

    long capacity() {
        return capacity;
    }

    Identity identity() {
        return new Identity(high, low);
    }

    private static void verifyCopies(State[] slots, State newest) throws IOException {
        for (State old : slots) {
            if (old == null) continue;
            if (newest.generation() - old.generation() > 2 || old.head() > newest.head() ||
                old.tail() > newest.tail() || old.retiredLastGroup() > newest.retiredLastGroup() ||
                old.lastGroup() > newest.lastGroup() || old.scanCeiling() > newest.scanCeiling())
                throw new IOException("Inconsistent ring checkpoint copies");
            if (old.generation() == newest.generation() && !old.equals(newest))
                throw new IOException("Inconsistent ring checkpoint copies");
        }
    }

    synchronized State current() throws IOException {
        checkOpen();
        return current;
    }

    boolean windowed() {
        return layout == Layout.WINDOWED;
    }

    synchronized void claimWindowAllocator(State expected) throws IOException {
        if (layout != Layout.WINDOWED || windowAllocatorClaimed)
            throw new IOException("WAL checkpoint already has a window allocator");
        if (!current().equals(expected)) throw new IOException("WAL checkpoint changed during allocator recovery");
        windowAllocatorClaimed = true;
    }

    boolean matchesWindowWriter(WalIoBackend.Handle handle, long start, long writerHigh, long writerLow) {
        return io == handle && start == metadataBytes() && high == writerHigh && low == writerLow;
    }

    boolean matchesLayout(int expectedAlignment, long expectedHigh, long expectedLow) {
        return alignment == expectedAlignment && high == expectedHigh && low == expectedLow;
    }

    /** Caller confirms every block and any wrap marker are forced in logical order. */
    synchronized State advanceTail(long tail, long lastGroup) throws IOException {
        checkOpen();
        if (tail <= current.tail() || lastGroup <= current.lastGroup())
            throw new IOException("Tail checkpoint did not advance");
        long ceiling = layout == Layout.WINDOWED ? current.scanCeiling() : tail;
        return commit(new State(nextGeneration(), current.head(), tail, current.retiredLastGroup(), lastGroup, ceiling));
    }

    /** Persist the exclusive scan bound before permitting writes in its extension. */
    synchronized State extendScanCeiling(long ceiling) throws IOException {
        checkOpen();
        if (layout != Layout.WINDOWED || ceiling <= current.scanCeiling())
            throw new IOException("Invalid WAL scan window extension");
        return commit(new State(nextGeneration(), current.head(), current.tail(), current.retiredLastGroup(),
            current.lastGroup(), ceiling));
    }

    /** Caller confirms reader drainage and durable coverage through the new head. */
    synchronized State advanceHead(long head, long retiredLastGroup) throws IOException {
        checkOpen();
        if (head <= current.head() || retiredLastGroup <= current.retiredLastGroup())
            throw new IOException("Head checkpoint did not advance");
        return commit(new State(nextGeneration(), head, current.tail(), retiredLastGroup, current.lastGroup(),
            current.scanCeiling()));
    }

    /** Moves an entirely empty logical window to the next physical origin. */
    synchronized State rebaseEmpty(long newOffset) throws IOException {
        checkOpen();
        if (current.head() != current.tail() || current.scanCeiling() != current.tail() || current.tail() % capacity == 0 ||
            newOffset != Math.addExact(current.tail(), capacity - current.tail() % capacity))
            throw new IOException("Invalid empty ring rebase");
        return commit(new State(nextGeneration(), newOffset, newOffset,
            current.retiredLastGroup(), current.lastGroup(), newOffset));
    }

    private State commit(State candidate) throws IOException {
        if (!valid(candidate)) throw new IOException("Invalid ring checkpoint transition");
        try {
            if (layout == Layout.LEGACY) {
                int first = (int) (candidate.generation() % 3);
                writeSlot(candidate, first);
                io.force();
                writeSlot(candidate, (first + 1) % 3);
                io.force();
            } else {
                // Leave all three copies of the current generation intact until
                // every slot in the other bank is written and one force succeeds.
                int first = bankStart(candidate);
                for (int slot = first; slot < first + BANK_COPIES; slot++) writeSlot(candidate, slot);
                io.force();
            }
            current = candidate;
            return candidate;
        } catch (Throwable t) {
            fenced = true;
            throw t;
        }
    }

    private static int bankStart(State state) {
        return (int) (state.generation() % 2) * BANK_COPIES;
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
        if (state.scanCeiling() < state.tail() || state.scanCeiling() - state.head() > capacity ||
            state.scanCeiling() % alignment != 0 || (layout != Layout.WINDOWED && state.scanCeiling() != state.tail()))
            return false;
        if (state.retiredLastGroup() < -1 || state.lastGroup() < state.retiredLastGroup()) return false;
        return state.head() != state.tail() || state.retiredLastGroup() == state.lastGroup();
    }

    private void writeSlot(State state, int slot) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(alignment);
        data.putInt(MAGIC).putInt(layout.version).putInt(alignment).putInt(0).putLong(capacity)
            .putLong(state.generation()).putLong(state.head()).putLong(state.tail())
            .putLong(state.retiredLastGroup()).putLong(state.lastGroup()).putLong(high).putLong(low).putInt(0);
        if (layout == Layout.WINDOWED) data.putLong(state.scanCeiling());
        data.putInt(CRC_OFFSET, checksum(data));
        data.position(0);
        long offset = (long) slot * alignment;
        while (data.hasRemaining()) {
            int written = io.write(data, offset);
            if (written <= 0) throw new IOException("Short ring checkpoint write");
            offset += written;
        }
    }

    private State readSlot(int slot) throws IOException {
        return decodeSlot(readSlotBytes(io, alignment, slot), slot);
    }

    private static ByteBuffer readSlotBytes(WalIoBackend.Handle io, int alignment, int slot) throws IOException {
        ByteBuffer data = ByteBuffer.allocate(alignment);
        long offset = (long) slot * alignment;
        while (data.hasRemaining()) {
            int read = io.read(data, offset + data.position());
            if (read <= 0) throw new IOException("Short ring checkpoint read");
        }
        return data.flip();
    }

    private State decodeSlot(ByteBuffer data, int slot) {
        if (data.getInt(0) != MAGIC || data.getInt(4) != layout.version || data.getInt(8) != alignment ||
            data.getInt(12) != 0 || data.getLong(16) != capacity) return null;
        if (data.getLong(64) != high || data.getLong(72) != low || data.getInt(CRC_OFFSET) != checksum(data))
            return null;
        long tail = data.getLong(40);
        long ceiling = layout == Layout.WINDOWED ? data.getLong(CRC_OFFSET + Integer.BYTES) : tail;
        var state = new State(data.getLong(24), data.getLong(32), tail, data.getLong(48), data.getLong(56), ceiling);
        if (layout != Layout.LEGACY && state.generation() > 1 &&
            slot / BANK_COPIES != bankStart(state) / BANK_COPIES) return null;
        return valid(state) ? state : null;
    }

    private static void validateLayout(int alignment, long capacity, Layout layout) {
        if (alignment < 4096 || (alignment & (alignment - 1)) != 0 || capacity <= 0 ||
            capacity % alignment != 0 || capacity > Long.MAX_VALUE - layout.metadataBytes(alignment))
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
