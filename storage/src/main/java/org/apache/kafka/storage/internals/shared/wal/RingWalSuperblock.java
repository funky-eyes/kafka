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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Crash-safe metadata codec for the fixed-capacity ring WAL.
 *
 * <p>Two 4 KiB copies live before the data region. Checkpoints alternate copies by sequence number. Recovery validates
 * magic, version, configured data capacity, logical head/tail invariants and CRC32C, then chooses the valid copy with
 * the highest sequence. A torn latest write therefore falls back to the previous durable copy without scanning or
 * trusting partially updated allocation metadata.</p>
 */
final class RingWalSuperblock {
    static final int MAGIC = 0x4b575231; // KWR1
    // Version 2 requires authenticated markers for wrap padding large enough to contain a WAL record.
    static final short VERSION = 2;
    static final int SUPERBLOCK_BYTES = RingWalLayout.SUPERBLOCK_BYTES;

    private static final int CHECKSUM_POSITION = 48;
    private static final int ENCODED_FIELDS_BYTES = CHECKSUM_POSITION + Integer.BYTES;

    private RingWalSuperblock() {
    }

    static int copyIndex(long sequence) {
        if (sequence < 0) {
            throw new IllegalArgumentException("sequence must be non-negative");
        }
        return (int) (sequence & 1L);
    }

    static ByteBuffer encode(State state) {
        Objects.requireNonNull(state, "state");
        ByteBuffer target = ByteBuffer.allocate(SUPERBLOCK_BYTES).order(ByteOrder.BIG_ENDIAN);
        target.putInt(MAGIC);
        target.putShort(VERSION);
        target.putShort((short) 0);
        target.putLong(state.sequence());
        target.putLong(state.headOffset());
        target.putLong(state.tailOffset());
        target.putLong(state.dataCapacityBytes());
        target.putLong(0L);

        long checksum = crc32c(target, 0, CHECKSUM_POSITION);
        target.putInt((int) checksum);
        target.position(0);
        target.limit(SUPERBLOCK_BYTES);
        return target.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    static State decode(ByteBuffer bytes) throws WalCorruptionException {
        Objects.requireNonNull(bytes, "bytes");
        ByteBuffer source = bytes.duplicate().order(ByteOrder.BIG_ENDIAN);
        if (source.remaining() < ENCODED_FIELDS_BYTES) {
            throw new WalCorruptionException(
                "Ring WAL superblock is truncated: remaining=" + source.remaining());
        }

        int base = source.position();
        int magic = source.getInt();
        short version = source.getShort();
        source.getShort();
        long sequence = source.getLong();
        long headOffset = source.getLong();
        long tailOffset = source.getLong();
        long dataCapacityBytes = source.getLong();
        source.getLong();
        long storedChecksum = Integer.toUnsignedLong(source.getInt());

        if (magic != MAGIC) {
            throw new WalCorruptionException("Invalid ring WAL superblock magic: " + Integer.toHexString(magic));
        }
        if (version != VERSION) {
            throw new WalCorruptionException("Unsupported ring WAL superblock version: " + version);
        }
        long actualChecksum = crc32c(source, base, CHECKSUM_POSITION);
        if (storedChecksum != actualChecksum) {
            throw new WalCorruptionException(
                "Ring WAL superblock checksum mismatch: expected=" + storedChecksum + ", actual=" + actualChecksum);
        }
        validateState(sequence, headOffset, tailOffset, dataCapacityBytes);
        return new State(sequence, headOffset, tailOffset, dataCapacityBytes);
    }

    static State selectNewest(ByteBuffer first, ByteBuffer second, long expectedDataCapacityBytes)
        throws WalCorruptionException {
        if (expectedDataCapacityBytes <= 0) {
            throw new IllegalArgumentException("expectedDataCapacityBytes must be positive");
        }
        State firstState = tryDecode(first, expectedDataCapacityBytes);
        State secondState = tryDecode(second, expectedDataCapacityBytes);
        if (firstState == null && secondState == null) {
            throw new WalCorruptionException("No valid ring WAL superblock copy remains");
        }
        if (firstState == null) {
            return secondState;
        }
        if (secondState == null) {
            return firstState;
        }
        if (firstState.sequence() == secondState.sequence() && !firstState.equals(secondState)) {
            throw new WalCorruptionException(
                "Conflicting ring WAL superblocks share sequence " + firstState.sequence());
        }
        return firstState.sequence() >= secondState.sequence() ? firstState : secondState;
    }

    private static State tryDecode(ByteBuffer bytes, long expectedDataCapacityBytes) {
        if (bytes == null) {
            return null;
        }
        try {
            State state = decode(bytes);
            return state.dataCapacityBytes() == expectedDataCapacityBytes ? state : null;
        } catch (WalCorruptionException e) {
            return null;
        }
    }

    private static void validateState(
        long sequence,
        long headOffset,
        long tailOffset,
        long dataCapacityBytes
    ) throws WalCorruptionException {
        if (sequence < 0) {
            throw new WalCorruptionException("Ring WAL superblock has negative sequence: " + sequence);
        }
        if (dataCapacityBytes <= 0) {
            throw new WalCorruptionException("Ring WAL superblock has invalid data capacity: " + dataCapacityBytes);
        }
        if (headOffset < 0 || tailOffset < headOffset) {
            throw new WalCorruptionException(
                "Ring WAL superblock has invalid logical window: head=" + headOffset + ", tail=" + tailOffset);
        }
        if (tailOffset - headOffset > dataCapacityBytes) {
            throw new WalCorruptionException(
                "Ring WAL superblock logical window exceeds capacity: head=" + headOffset +
                    ", tail=" + tailOffset + ", capacity=" + dataCapacityBytes);
        }
    }

    private static long crc32c(ByteBuffer source, int position, int length) {
        ByteBuffer duplicate = source.duplicate();
        duplicate.position(position);
        duplicate.limit(Math.addExact(position, length));
        CRC32C crc = new CRC32C();
        crc.update(duplicate);
        return crc.getValue();
    }

    record State(long sequence, long headOffset, long tailOffset, long dataCapacityBytes) {
        State {
            if (sequence < 0 || headOffset < 0 || tailOffset < headOffset || dataCapacityBytes <= 0 ||
                tailOffset - headOffset > dataCapacityBytes) {
                throw new IllegalArgumentException("invalid ring WAL superblock state");
            }
        }

        State next(long newHeadOffset, long newTailOffset) {
            return new State(Math.addExact(sequence, 1L), newHeadOffset, newTailOffset, dataCapacityBytes);
        }
    }
}
