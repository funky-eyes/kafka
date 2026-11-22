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
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Experimental block framing for the opt-in parallel block WAL. */
final class AlignedWalBlockCodec {
    static final int MAGIC = 0x4b574233; // KWB3
    static final int HEADER_BYTES = 64;
    static final int CRC_OFFSET = 48;
    static final int MAX_BLOCK_BYTES = 256 * 1024 * 1024;
    private static final int VERSION = 1;

    private AlignedWalBlockCodec() { }

    record Group(long id, List<WalRecord> records) {
        Group {
            if (id < 0 || records == null || records.isEmpty())
                throw new IllegalArgumentException("invalid group");
            records = List.copyOf(records);
            for (WalRecord record : records) {
                if (record.type() == WalRecordType.GROUP_COMMIT)
                    throw new IllegalArgumentException("group commit is internal");
            }
        }
    }

    record Entry(WalRecord record, long walOffset, int length) { }
    record DecodedGroup(long id, List<Entry> records) { }
    record DecodedBlock(int length, List<DecodedGroup> groups) { }
    record EncodedBlock(ByteBuffer bytes, List<List<WalAppendResult>> groups) { }

    static ByteBuffer encode(long offset, long incarnationHigh, long incarnationLow,
                             int alignment, List<Group> groups) {
        return encodeWithLocations(offset, incarnationHigh, incarnationLow, alignment, groups).bytes();
    }

    static EncodedBlock encodeWithLocations(long offset, long incarnationHigh, long incarnationLow,
                                            int alignment, List<Group> groups) {
        checkAlignment(alignment);
        if (offset < 0 || offset % alignment != 0 || groups == null || groups.isEmpty())
            throw new IllegalArgumentException("invalid block offset or groups");
        List<WalRecordCodec.EncodedRecord> encoded = new ArrayList<>();
        long payload = 0;
        long previousId = -1;
        for (Group group : groups) {
            Objects.requireNonNull(group, "group");
            if (group.id() <= previousId) throw new IllegalArgumentException("non-increasing group id");
            previousId = group.id();
            for (WalRecord record : group.records()) {
                WalRecordCodec.EncodedRecord item = WalRecordCodec.encode(record);
                payload = Math.addExact(payload, item.totalLength());
                encoded.add(item);
            }
            WalRecordCodec.EncodedRecord commit = WalRecordCodec.encode(WalRecord.groupCommit(group.id(), group.records().size()));
            payload = Math.addExact(payload, commit.totalLength());
            encoded.add(commit);
        }
        long unaligned = Math.addExact(HEADER_BYTES, payload);
        long rounded = Math.multiplyExact(Math.floorDiv(Math.addExact(unaligned, alignment - 1L), alignment), alignment);
        if (rounded > MAX_BLOCK_BYTES || offset > Long.MAX_VALUE - rounded)
            throw new IllegalArgumentException("block exceeds supported size or address range");
        int length = Math.toIntExact(rounded);
        ByteBuffer output = ByteBuffer.allocate(length).order(ByteOrder.BIG_ENDIAN);
        output.putInt(MAGIC).putInt(VERSION).putInt(length).putInt(Math.toIntExact(payload));
        output.putLong(offset).putLong(incarnationHigh).putLong(incarnationLow);
        output.putInt(groups.size()).putInt(0).putInt(0).putInt(0).putLong(0L);
        List<List<WalAppendResult>> locations = new ArrayList<>();
        int encodedIndex = 0;
        for (Group group : groups) {
            List<WalAppendResult> groupLocations = new ArrayList<>();
            for (int i = 0; i < group.records().size(); i++) {
                WalRecordCodec.EncodedRecord item = encoded.get(encodedIndex++);
                groupLocations.add(new WalAppendResult(Math.addExact(offset, output.position()), item.totalLength()));
                output.put(item.header()).put(item.payload());
            }
            WalRecordCodec.EncodedRecord commit = encoded.get(encodedIndex++);
            output.put(commit.header()).put(commit.payload());
            locations.add(List.copyOf(groupLocations));
        }
        output.putInt(CRC_OFFSET, checksum(output));
        output.position(0).limit(length);
        return new EncodedBlock(output.asReadOnlyBuffer(), List.copyOf(locations));
    }

    static DecodedBlock decode(ByteBuffer bytes, long expectedOffset, long incarnationHigh,
                               long incarnationLow, int alignment) throws IOException {
        checkAlignment(alignment);
        Objects.requireNonNull(bytes, "bytes");
        ByteBuffer block = bytes.slice().order(ByteOrder.BIG_ENDIAN);
        if (block.remaining() < HEADER_BYTES) throw corrupt("truncated block header");
        int length = block.getInt(8);
        int payloadLength = block.getInt(12);
        int groupCount = block.getInt(40);
        validateHeader(block, expectedOffset, incarnationHigh, incarnationLow, alignment);
        int payloadEnd = HEADER_BYTES + payloadLength;
        for (int i = payloadEnd; i < length; i++) if (block.get(i) != 0) throw corrupt("nonzero padding");
        ByteBuffer payload = block.duplicate();
        payload.position(HEADER_BYTES).limit(payloadEnd);
        MemoryHandle handle = new MemoryHandle(payload.slice());
        List<DecodedGroup> groups = new ArrayList<>();
        List<Entry> pending = new ArrayList<>();
        long cursor = 0, previousGroupId = -1;
        while (cursor < payloadLength) {
            WalRecordCodec.ReadResult result = WalRecordCodec.read(handle, cursor);
            if (result.status() != WalRecordCodec.ReadStatus.COMPLETE || result.length() <= 0)
                throw corrupt("incomplete record in block");
            WalRecord record = result.record();
            if (record.type() == WalRecordType.GROUP_COMMIT) {
                if (record.groupRecordCount() != pending.size() || record.groupId() <= previousGroupId)
                    throw corrupt("invalid group commit");
                previousGroupId = record.groupId();
                groups.add(new DecodedGroup(record.groupId(), List.copyOf(pending)));
                pending.clear();
            } else {
                pending.add(new Entry(record, Math.addExact(expectedOffset, HEADER_BYTES + cursor), result.length()));
            }
            cursor = Math.addExact(cursor, result.length());
        }
        if (!pending.isEmpty() || groups.size() != groupCount) throw corrupt("incomplete or missing group");
        return new DecodedBlock(length, List.copyOf(groups));
    }

    private static void validateHeader(ByteBuffer block, long expectedOffset, long high, long low,
                                       int alignment) throws WalCorruptionException {
        validateShape(block, expectedOffset, alignment);
        validateIdentity(block, expectedOffset, high, low);
    }

    private static void validateShape(ByteBuffer block, long expectedOffset, int alignment) throws WalCorruptionException {
        int length = block.getInt(8);
        int payload = block.getInt(12);
        if (block.getInt(0) != MAGIC || block.getInt(4) != VERSION || length < HEADER_BYTES ||
            length > MAX_BLOCK_BYTES) throw corrupt("invalid block magic, version or size");
        if (length % alignment != 0 || length != block.remaining() || expectedOffset < 0 ||
            expectedOffset % alignment != 0 || expectedOffset > Long.MAX_VALUE - length)
            throw corrupt("invalid block length or offset");
        if (payload <= 0 || payload > length - HEADER_BYTES || block.getInt(40) <= 0)
            throw corrupt("invalid block payload or group count");
    }

    private static void validateIdentity(ByteBuffer block, long expectedOffset, long high,
                                         long low) throws WalCorruptionException {
        if (block.getLong(16) != expectedOffset || block.getLong(24) != high || block.getLong(32) != low)
            throw corrupt("invalid block identity");
        if (block.getInt(44) != 0 || block.getInt(52) != 0 || block.getLong(56) != 0 ||
            checksum(block) != block.getInt(CRC_OFFSET)) throw corrupt("invalid block flags or checksum");
    }

    private static void checkAlignment(int alignment) {
        if (alignment < HEADER_BYTES || (alignment & (alignment - 1)) != 0)
            throw new IllegalArgumentException("alignment must be a power of two at least " + HEADER_BYTES);
    }

    private static int checksum(ByteBuffer bytes) {
        ByteBuffer view = bytes.duplicate();
        view.position(0).limit(bytes.limit());
        CRC32C crc = new CRC32C();
        view.limit(CRC_OFFSET);
        crc.update(view);
        view.limit(bytes.limit()).position(CRC_OFFSET + Integer.BYTES);
        crc.update(view);
        return (int) crc.getValue();
    }

    private static WalCorruptionException corrupt(String message) {
        return new WalCorruptionException(message);
    }

    private static final class MemoryHandle implements WalIoBackend.Handle {
        private final ByteBuffer data;
        MemoryHandle(ByteBuffer data) {
            this.data = data.asReadOnlyBuffer();
        }
        public long size() {
            return data.limit();
        }
        public int read(ByteBuffer dst, long position) {
            if (position < 0 || position >= data.limit()) return -1;
            ByteBuffer source = data.duplicate();
            source.position((int) position).limit(Math.min(data.limit(), (int) position + dst.remaining()));
            int n = source.remaining();
            dst.put(source);
            return n;
        }
        public int write(ByteBuffer src, long position) {
            throw new UnsupportedOperationException();
        }
        public void truncate(long size) {
            throw new UnsupportedOperationException();
        }
        public void force() {
            throw new UnsupportedOperationException();
        }
        public void seal() {
            throw new UnsupportedOperationException();
        }
        public void close() { }
    }
}
