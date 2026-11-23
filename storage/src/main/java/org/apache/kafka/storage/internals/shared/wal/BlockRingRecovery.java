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
import java.util.zip.CRC32C;

/** Experimental ring framing/recovery; callers supply an already forced checkpoint. */
final class BlockRingRecovery {
    private static final int PADDING_MAGIC = 0x4b575031;
    private static final int PADDING_VERSION = 1;
    private static final int CRC_OFFSET = 48;

    record RecoveredBlock(long reservedStart, long blockStart, long end,
                          List<AlignedWalBlockCodec.DecodedGroup> groups) { }
    record Scan(List<RecoveredBlock> blocks, long lastGroup) { }

    /**
     * Inspection results are not a durability, continuity or Kafka visibility certificate.
     * A later read can fail after callbacks have already run, so visitors must
     * stage results and discard them if inspection does not complete.
     */
    interface WindowVisitor {
        void candidate(long start, AlignedWalBlockCodec.DecodedBlock block) throws IOException;
        void unclassified(long start, long end) throws IOException;
    }

    private BlockRingRecovery() { }

    /** Only the first aligned sector is written; the remainder is skipped at recovery. */
    static ByteBuffer padding(long cursor, long span, long high, long low, int alignment, long capacity) {
        validateLayout(alignment, capacity);
        if (cursor < 0 || cursor % alignment != 0 || span <= 0 || span != capacity - cursor % capacity ||
            span % alignment != 0 || cursor > Long.MAX_VALUE - span)
            throw new IllegalArgumentException("Invalid ring padding");
        ByteBuffer marker = ByteBuffer.allocate(alignment);
        marker.putInt(PADDING_MAGIC).putInt(PADDING_VERSION).putLong(cursor).putLong(span).putLong(high).putLong(low)
            .putLong(capacity).putInt(0);
        marker.putInt(CRC_OFFSET, checksum(marker));
        return marker.position(0);
    }

    /** Rebuilds only [head, tail), rejecting stale generations and malformed committed data. */
    static Scan scan(WalIoBackend.Handle io, long dataStart, long capacity, int alignment,
                     long head, long tail, long retiredLastGroup, long lastGroup,
                     long high, long low) throws IOException {
        validateLayout(alignment, capacity);
        validateBounds(dataStart, capacity, alignment, head, tail, retiredLastGroup, lastGroup);
        long cursor = head;
        long previous = retiredLastGroup;
        List<RecoveredBlock> blocks = new ArrayList<>();
        Reader reader = new Reader(io, dataStart, capacity, alignment, high, low);
        while (cursor < tail) {
            RecoveredBlock block = reader.read(cursor, tail);
            previous = lastGroup(block, previous);
            blocks.add(block);
            cursor = block.end();
        }
        if (cursor != tail || previous != lastGroup) throw new IOException("Incomplete committed ring prefix");
        return new Scan(List.copyOf(blocks), previous);
    }

    /**
     * Inspect the speculative part of a bounded window without publishing any
     * record. Unclassified sectors include unwritten, stale and damaged bytes;
     * callers must not interpret them as safe holes. The checkpointed prefix
     * must be validated separately with {@link #scan}.
     */
    static void inspectWindow(WalIoBackend.Handle io, long dataStart, long capacity, int alignment,
                              int maxBlock, long head, long tail, long ceiling, long high, long low,
                              WindowVisitor visitor) throws IOException {
        validateLayout(alignment, capacity);
        Objects.requireNonNull(visitor, "visitor");
        validateInspectionBounds(dataStart, capacity, alignment, maxBlock, head, tail, ceiling);

        long cursor = tail;
        long unclassifiedStart = -1;
        while (cursor < ceiling) {
            long physical = cursor % capacity;
            ByteBuffer header = readFully(io, dataStart + physical, alignment);
            AlignedWalBlockCodec.DecodedBlock block = inspectCandidate(io, header, dataStart, capacity, alignment,
                maxBlock, cursor, ceiling, high, low);
            if (block == null) {
                if (unclassifiedStart < 0) unclassifiedStart = cursor;
                cursor = Math.addExact(cursor, alignment);
            } else {
                if (unclassifiedStart >= 0) {
                    visitor.unclassified(unclassifiedStart, cursor);
                    unclassifiedStart = -1;
                }
                visitor.candidate(cursor, block);
                cursor = Math.addExact(cursor, block.length());
            }
        }
        if (unclassifiedStart >= 0) visitor.unclassified(unclassifiedStart, ceiling);
    }

    private static void validateInspectionBounds(long dataStart, long capacity, int alignment, int maxBlock,
                                                 long head, long tail, long ceiling) throws IOException {
        if (dataStart < 0 || dataStart > Long.MAX_VALUE - capacity || head < 0 || tail < head || ceiling < tail)
            throw new IOException("Invalid WAL inspection window");
        if (ceiling - head > capacity || head % alignment != 0 || tail % alignment != 0 || ceiling % alignment != 0)
            throw new IOException("Invalid WAL inspection window");
        if (maxBlock < alignment || maxBlock % alignment != 0 || maxBlock > AlignedWalBlockCodec.MAX_BLOCK_BYTES ||
            maxBlock > capacity)
            throw new IOException("Invalid WAL inspection block limit");
    }

    private static AlignedWalBlockCodec.DecodedBlock inspectCandidate(WalIoBackend.Handle io, ByteBuffer header,
        long dataStart, long capacity, int alignment, int maxBlock, long cursor, long ceiling, long high, long low)
        throws IOException {
        if (header.getInt(0) != AlignedWalBlockCodec.MAGIC || header.getInt(4) != AlignedWalBlockCodec.VERSION)
            return null;
        if (header.getLong(16) != cursor || header.getLong(24) != high || header.getLong(32) != low)
            return null;
        long physical = cursor % capacity;
        int length = header.getInt(8);
        if (length < alignment || length > maxBlock || length % alignment != 0 ||
            length > ceiling - cursor || length > capacity - physical)
            return null;
        ByteBuffer bytes = readFully(io, dataStart + physical, length);
        try {
            return AlignedWalBlockCodec.decode(bytes, cursor, high, low, alignment);
        } catch (IOException invalid) {
            // Never turn damaged bytes into an empty or reclaimable range.
            return null;
        }
    }

    private static void validateBounds(long dataStart, long capacity, int alignment, long head, long tail,
                                       long retiredLastGroup, long lastGroup) throws IOException {
        if (dataStart < 0 || dataStart > Long.MAX_VALUE - capacity || head < 0 || tail < head)
            throw new IOException("Invalid committed ring checkpoint");
        if (tail - head > capacity || head % alignment != 0 || tail % alignment != 0 ||
            retiredLastGroup < -1 || lastGroup < retiredLastGroup)
            throw new IOException("Invalid committed ring checkpoint");
    }

    private static long lastGroup(RecoveredBlock block, long previous) throws IOException {
        for (var group : block.groups()) {
            if (previous == Long.MAX_VALUE || group.id() != previous + 1)
                throw new IOException("Noncontiguous committed ring groups");
            previous = group.id();
        }
        return previous;
    }

    private record Reader(WalIoBackend.Handle io, long dataStart, long capacity, int alignment, long high, long low) {
        RecoveredBlock read(long reservedStart, long tail) throws IOException {
            long cursor = reservedStart;
            long physical = cursor % capacity;
            ByteBuffer header = readFully(io, dataStart + physical, alignment);
            if (header.getInt(0) == PADDING_MAGIC) {
                long span = capacity - physical;
                validatePadding(header, cursor, span, tail);
                cursor += span;
                physical = 0;
                header = readFully(io, dataStart, alignment);
            }
            int length = header.getInt(8);
            if (length < alignment || length % alignment != 0 || length > AlignedWalBlockCodec.MAX_BLOCK_BYTES ||
                length > tail - cursor || length > capacity - physical)
                throw new IOException("Invalid committed ring block length");
            var block = AlignedWalBlockCodec.decode(readFully(io, dataStart + physical, length), cursor,
                high, low, alignment);
            return new RecoveredBlock(reservedStart, cursor, cursor + length, block.groups());
        }

        private void validatePadding(ByteBuffer header, long cursor, long span, long tail) throws IOException {
            if (header.getInt(4) != PADDING_VERSION || header.getLong(8) != cursor ||
                header.getLong(16) != span || header.getLong(24) != high || header.getLong(32) != low)
                throw new IOException("Invalid committed ring padding");
            if (header.getLong(40) != capacity || header.getInt(CRC_OFFSET) != checksum(header) || span >= tail - cursor)
                throw new IOException("Invalid committed ring padding");
        }
    }

    private static void validateLayout(int alignment, long capacity) {
        if (alignment < 4096 || (alignment & (alignment - 1)) != 0 || capacity <= 0 ||
            capacity % alignment != 0) throw new IllegalArgumentException("Invalid ring layout");
    }

    private static ByteBuffer readFully(WalIoBackend.Handle io, long offset, int length) throws IOException {
        ByteBuffer result = ByteBuffer.allocate(length);
        while (result.hasRemaining()) {
            int n = io.read(result, offset + result.position());
            if (n <= 0) throw new IOException("Short ring read");
        }
        return result.flip();
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
