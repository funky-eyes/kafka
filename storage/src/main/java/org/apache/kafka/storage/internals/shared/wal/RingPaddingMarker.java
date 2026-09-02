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
import java.util.zip.CRC32C;

/**
 * Authenticated marker for a wrap-padding region large enough to otherwise hide a valid WAL record.
 *
 * <p>Small physical suffixes shorter than {@link WalRecordCodec#MIN_RECORD_BYTES} can never contain the beginning of a
 * complete WAL record and therefore remain zero-only padding. Larger suffixes carry this marker at their logical start.
 * Recovery validates the absolute next-generation boundary, padding length and CRC before skipping the suffix. This
 * prevents a fully zeroed/corrupted committed record near the end of a generation from being mistaken for padding.</p>
 */
final class RingPaddingMarker {
    static final int MAGIC = 0x4b525032; // KRP2
    static final short VERSION = 1;
    static final int MARKER_BYTES = 32;

    private static final int CHECKSUM_POSITION = 28;

    private RingPaddingMarker() {
    }

    static ByteBuffer encode(long paddingOffset, long boundaryOffset) {
        long paddingBytes = validateBounds(paddingOffset, boundaryOffset);
        if (paddingBytes < WalRecordCodec.MIN_RECORD_BYTES) {
            throw new IllegalArgumentException(
                "Ring padding marker requires at least " + WalRecordCodec.MIN_RECORD_BYTES +
                    " bytes, actual=" + paddingBytes);
        }

        ByteBuffer target = ByteBuffer.allocate(MARKER_BYTES).order(ByteOrder.BIG_ENDIAN);
        target.putInt(MAGIC);
        target.putShort(VERSION);
        target.putShort((short) 0);
        target.putLong(boundaryOffset);
        target.putLong(paddingBytes);
        target.putInt(0);
        target.putInt((int) crc32c(target, 0, CHECKSUM_POSITION));
        target.flip();
        return target.asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    static void validate(ByteBuffer bytes, long paddingOffset, long expectedBoundaryOffset)
        throws WalCorruptionException {
        ByteBuffer source = markerBytes(bytes, paddingOffset);
        int base = source.position();
        validateMagic(source.getInt(), paddingOffset);
        validateVersion(source.getShort(), paddingOffset);
        validateFlags(source.getShort(), paddingOffset);
        long boundaryOffset = source.getLong();
        long paddingBytes = source.getLong();
        validateReserved(source.getInt(), paddingOffset);
        validateChecksum(source, base, Integer.toUnsignedLong(source.getInt()), paddingOffset);

        long expectedPaddingBytes = expectedPaddingBytes(paddingOffset, expectedBoundaryOffset);
        validatePhysicalBoundary(
            boundaryOffset,
            paddingBytes,
            paddingOffset,
            expectedBoundaryOffset,
            expectedPaddingBytes
        );
    }

    private static ByteBuffer markerBytes(ByteBuffer bytes, long paddingOffset) throws WalCorruptionException {
        if (bytes == null || bytes.remaining() < MARKER_BYTES) {
            throw new WalCorruptionException("Ring WAL padding marker is truncated at logical offset " + paddingOffset);
        }
        return bytes.duplicate().order(ByteOrder.BIG_ENDIAN);
    }

    private static void validateMagic(int magic, long paddingOffset) throws WalCorruptionException {
        if (magic != MAGIC) {
            throw new WalCorruptionException(
                "Invalid ring WAL padding marker magic at logical offset " + paddingOffset + ": " +
                    Integer.toHexString(magic));
        }
    }

    private static void validateVersion(short version, long paddingOffset) throws WalCorruptionException {
        if (version != VERSION) {
            throw new WalCorruptionException(
                "Unsupported ring WAL padding marker version at logical offset " + paddingOffset + ": " + version);
        }
    }

    private static void validateFlags(short flags, long paddingOffset) throws WalCorruptionException {
        if (flags != 0) {
            throw new WalCorruptionException("Unsupported ring WAL padding marker flags at logical offset " + paddingOffset);
        }
    }

    private static void validateReserved(int reserved, long paddingOffset) throws WalCorruptionException {
        if (reserved != 0) {
            throw new WalCorruptionException("Unsupported ring WAL padding marker flags at logical offset " + paddingOffset);
        }
    }

    private static void validateChecksum(ByteBuffer source, int base, long storedChecksum, long paddingOffset)
        throws WalCorruptionException {
        long actualChecksum = crc32c(source, base, CHECKSUM_POSITION);
        if (storedChecksum != actualChecksum) {
            throw new WalCorruptionException(
                "Ring WAL padding marker checksum mismatch at logical offset " + paddingOffset);
        }
    }

    private static long expectedPaddingBytes(long paddingOffset, long expectedBoundaryOffset)
        throws WalCorruptionException {
        try {
            return validateBounds(paddingOffset, expectedBoundaryOffset);
        } catch (IllegalArgumentException e) {
            throw new WalCorruptionException("Invalid ring WAL padding bounds at logical offset " + paddingOffset);
        }
    }

    private static void validatePhysicalBoundary(
        long boundaryOffset,
        long paddingBytes,
        long paddingOffset,
        long expectedBoundaryOffset,
        long expectedPaddingBytes
    ) throws WalCorruptionException {
        if (expectedPaddingBytes < WalRecordCodec.MIN_RECORD_BYTES ||
            boundaryOffset != expectedBoundaryOffset || paddingBytes != expectedPaddingBytes) {
            throw new WalCorruptionException(
                "Ring WAL padding marker does not match physical boundary at logical offset " + paddingOffset +
                    ": markerBoundary=" + boundaryOffset + ", expectedBoundary=" + expectedBoundaryOffset +
                    ", markerBytes=" + paddingBytes + ", expectedBytes=" + expectedPaddingBytes);
        }
    }

    private static long validateBounds(long paddingOffset, long boundaryOffset) {
        if (paddingOffset < 0 || boundaryOffset <= paddingOffset) {
            throw new IllegalArgumentException(
                "Invalid ring padding bounds: start=" + paddingOffset + ", boundary=" + boundaryOffset);
        }
        return boundaryOffset - paddingOffset;
    }

    private static long crc32c(ByteBuffer source, int position, int length) {
        ByteBuffer duplicate = source.duplicate();
        duplicate.position(position);
        duplicate.limit(Math.addExact(position, length));
        CRC32C crc = new CRC32C();
        crc.update(duplicate);
        return crc.getValue();
    }
}
