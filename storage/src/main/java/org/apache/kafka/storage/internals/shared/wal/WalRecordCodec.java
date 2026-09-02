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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

final class WalRecordCodec {
    static final int MAGIC = 0x4b535731; // KSW1
    static final short VERSION = 1;
    static final int PREFIX_BYTES = 16;
    static final int FIXED_BODY_BYTES = 44;
    static final int HEADER_BYTES = PREFIX_BYTES + FIXED_BODY_BYTES;
    static final int MIN_RECORD_BYTES = HEADER_BYTES;
    static final int MAX_RECORD_BYTES = 128 * 1024 * 1024;

    private WalRecordCodec() {
    }

    /**
     * Encodes only the WAL header and keeps the payload as a separate owned view.
     *
     * <p>This deliberately avoids materializing a second full-size buffer for Kafka RecordBatch payloads. The writer
     * persists the header and payload with positional writes and one durability barrier for the drained group.</p>
     */
    static EncodedRecord encode(WalRecord record) {
        ByteBuffer payload = record.payloadUnsafe();
        int payloadLength = payload.remaining();
        int totalLength = Math.addExact(HEADER_BYTES, payloadLength);
        if (totalLength > MAX_RECORD_BYTES) {
            throw new IllegalArgumentException("WAL record exceeds maximum size: " + totalLength);
        }

        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
        header.putInt(MAGIC);
        header.putShort(VERSION);
        header.put(record.type().id());
        header.put((byte) 0);
        header.putInt(totalLength);
        header.putInt(0); // CRC placeholder
        header.putLong(record.topicIdHigh());
        header.putLong(record.topicIdLow());
        header.putInt(record.partition());
        header.putInt(record.leaderEpoch());
        header.putLong(record.firstOffset());
        header.putLong(record.lastOffset());
        header.putInt(payloadLength);

        CRC32C crc = new CRC32C();
        ByteBuffer fixedBody = header.duplicate();
        fixedBody.flip();
        fixedBody.position(PREFIX_BYTES);
        crc.update(fixedBody);
        crc.update(payload.duplicate());
        header.putInt(12, (int) crc.getValue());
        header.flip();

        return new EncodedRecord(header, payload, totalLength);
    }

    static ReadResult read(WalIoBackend.Handle handle, long position) throws IOException {
        long remaining = handle.size() - position;
        if (remaining == 0) {
            return ReadResult.eof(position);
        }
        if (remaining < PREFIX_BYTES) {
            return ReadResult.partial(position);
        }

        Prefix prefix = readPrefix(handle, position);
        validatePrefix(prefix, position);
        if (remaining < prefix.totalLength()) {
            return ReadResult.partial(position);
        }

        WalRecordType type = recordType(prefix.typeId(), position);
        ByteBuffer body = readAndValidateBody(handle, position, prefix);
        WalRecord record = decodeRecord(type, body, position);
        return ReadResult.complete(position, prefix.totalLength(), record);
    }

    private static Prefix readPrefix(WalIoBackend.Handle handle, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(PREFIX_BYTES).order(ByteOrder.BIG_ENDIAN);
        readFully(handle, buffer, position);
        buffer.flip();
        return new Prefix(
            buffer.getInt(),
            buffer.getShort(),
            buffer.get(),
            buffer.get(),
            buffer.getInt(),
            buffer.getInt()
        );
    }

    private static void validatePrefix(Prefix prefix, long position) throws WalCorruptionException {
        if (prefix.magic() != MAGIC) {
            throw new WalCorruptionException(
                "Invalid WAL magic at position " + position + ": " + Integer.toHexString(prefix.magic()));
        }
        if (prefix.version() != VERSION) {
            throw new WalCorruptionException(
                "Unsupported WAL version at position " + position + ": " + prefix.version());
        }
        if (prefix.flags() != 0) {
            throw new WalCorruptionException(
                "Unsupported WAL flags at position " + position + ": " + prefix.flags());
        }
        if (prefix.totalLength() < MIN_RECORD_BYTES || prefix.totalLength() > MAX_RECORD_BYTES) {
            throw new WalCorruptionException(
                "Invalid WAL record length at position " + position + ": " + prefix.totalLength());
        }
    }

    private static WalRecordType recordType(byte typeId, long position) throws WalCorruptionException {
        try {
            return WalRecordType.forId(typeId);
        } catch (IllegalArgumentException e) {
            throw new WalCorruptionException("Invalid WAL record type at position " + position + ": " + typeId);
        }
    }

    private static ByteBuffer readAndValidateBody(
        WalIoBackend.Handle handle,
        long position,
        Prefix prefix
    ) throws IOException {
        int bodyLength = prefix.totalLength() - PREFIX_BYTES;
        ByteBuffer body = ByteBuffer.allocate(bodyLength).order(ByteOrder.BIG_ENDIAN);
        readFully(handle, body, position + PREFIX_BYTES);
        body.flip();

        CRC32C crc = new CRC32C();
        crc.update(body.duplicate());
        if ((int) crc.getValue() != prefix.expectedCrc()) {
            throw new WalCorruptionException("WAL checksum mismatch at position " + position);
        }
        return body;
    }

    private static WalRecord decodeRecord(
        WalRecordType type,
        ByteBuffer body,
        long position
    ) throws WalCorruptionException {
        DecodedBody decoded = decodeBody(body, position);
        try {
            return switch (type) {
                case DATA -> decodeData(decoded);
                case TRUNCATE -> decodeTruncate(decoded);
                case GROUP_COMMIT -> decodeGroupCommit(decoded);
            };
        } catch (IllegalArgumentException e) {
            throw new WalCorruptionException(
                "Invalid WAL record body at position " + position + ": " + e.getMessage());
        }
    }

    private static DecodedBody decodeBody(ByteBuffer body, long position) throws WalCorruptionException {
        long topicIdHigh = body.getLong();
        long topicIdLow = body.getLong();
        int partition = body.getInt();
        int leaderEpoch = body.getInt();
        long firstOffset = body.getLong();
        long lastOffset = body.getLong();
        int payloadLength = body.getInt();
        if (payloadLength < 0 || payloadLength != body.remaining()) {
            throw new WalCorruptionException(
                "Invalid WAL payload length at position " + position + ": " + payloadLength);
        }
        return new DecodedBody(
            topicIdHigh,
            topicIdLow,
            partition,
            leaderEpoch,
            firstOffset,
            lastOffset,
            body.slice()
        );
    }

    private static WalRecord decodeData(DecodedBody body) {
        return WalRecord.dataOwned(
            body.topicIdHigh(),
            body.topicIdLow(),
            body.partition(),
            body.leaderEpoch(),
            body.firstOffset(),
            body.lastOffset(),
            body.payload()
        );
    }

    private static WalRecord decodeTruncate(DecodedBody body) {
        requireEmptyPayload(body, "TRUNCATE");
        if (body.firstOffset() != body.lastOffset()) {
            throw new IllegalArgumentException("TRUNCATE first and last offsets must match");
        }
        return WalRecord.truncate(
            body.topicIdHigh(),
            body.topicIdLow(),
            body.partition(),
            body.leaderEpoch(),
            body.firstOffset()
        );
    }

    private static WalRecord decodeGroupCommit(DecodedBody body) {
        requireEmptyPayload(body, "GROUP_COMMIT");
        if (body.topicIdLow() != 0L || body.leaderEpoch() != 0 ||
            body.firstOffset() != 0L || body.lastOffset() != 0L) {
            throw new IllegalArgumentException("invalid GROUP_COMMIT record body");
        }
        return WalRecord.groupCommit(body.topicIdHigh(), body.partition());
    }

    private static void requireEmptyPayload(DecodedBody body, String type) {
        if (body.payload().hasRemaining()) {
            throw new IllegalArgumentException(type + " must not contain payload bytes");
        }
    }

    private static void readFully(WalIoBackend.Handle handle, ByteBuffer buffer, long position) throws IOException {
        long currentPosition = position;
        while (buffer.hasRemaining()) {
            int read = handle.read(buffer, currentPosition);
            if (read < 0) {
                throw new EOFException("Unexpected EOF while reading WAL at position " + currentPosition);
            }
            if (read == 0) {
                throw new IOException("Unable to make progress reading WAL at position " + currentPosition);
            }
            currentPosition += read;
        }
    }

    static final class EncodedRecord {
        private final ByteBuffer header;
        private final ByteBuffer payload;
        private final int totalLength;

        private EncodedRecord(ByteBuffer header, ByteBuffer payload, int totalLength) {
            this.header = header.asReadOnlyBuffer();
            this.payload = payload.asReadOnlyBuffer();
            this.totalLength = totalLength;
        }

        ByteBuffer header() {
            return header.duplicate();
        }

        ByteBuffer payload() {
            return payload.duplicate();
        }

        int totalLength() {
            return totalLength;
        }
    }

    private record Prefix(
        int magic,
        short version,
        byte typeId,
        byte flags,
        int totalLength,
        int expectedCrc
    ) {
    }

    private record DecodedBody(
        long topicIdHigh,
        long topicIdLow,
        int partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer payload
    ) {
    }

    enum ReadStatus {
        COMPLETE,
        EOF,
        PARTIAL
    }

    record ReadResult(ReadStatus status, long position, int length, WalRecord record) {
        static ReadResult complete(long position, int length, WalRecord record) {
            return new ReadResult(ReadStatus.COMPLETE, position, length, record);
        }

        static ReadResult eof(long position) {
            return new ReadResult(ReadStatus.EOF, position, 0, null);
        }

        static ReadResult partial(long position) {
            return new ReadResult(ReadStatus.PARTIAL, position, 0, null);
        }
    }
}
