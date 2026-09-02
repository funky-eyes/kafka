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
package org.apache.kafka.storage.internals.shared.object;

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Binary layout for immutable stream objects stored in the remote object store.
 *
 * <p>The format deliberately has no Kafka LogSegment or local WAL-file identity. A data block describes one
 * logical stream range and may contain multiple Kafka RecordBatch payloads. The trailing index and footer allow
 * readers and compactors to discover blocks with bounded range reads without consulting local WAL layout.</p>
 */
final class StreamObjectFormat {
    static final int OBJECT_MAGIC = 0x4b534f32; // KSO2
    static final short VERSION = 2;
    static final int OBJECT_HEADER_BYTES = 16;

    static final int DATA_BLOCK_HEADER_BYTES = 56;
    static final int BATCH_ENTRY_HEADER_BYTES = 24;

    static final int INDEX_MAGIC = 0x4b534932; // KSI2
    static final int INDEX_HEADER_BYTES = 16;
    static final int INDEX_ENTRY_BYTES = 64;

    static final int FOOTER_MAGIC = 0x4b534632; // KSF2
    static final int FOOTER_BYTES = 32;

    private StreamObjectFormat() {
    }

    static void writeObjectHeader(ByteBuffer target, long objectId) {
        target.putInt(OBJECT_MAGIC);
        target.putShort(VERSION);
        target.putShort((short) 0);
        target.putLong(objectId);
    }

    static void writeDataBlockHeader(
        ByteBuffer target,
        SharedPartitionId partition,
        int leaderEpoch,
        OffsetRange offsets,
        int batchCount,
        int dataLength,
        long checksum
    ) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(offsets, "offsets");
        target.putLong(partition.topicIdHigh());
        target.putLong(partition.topicIdLow());
        target.putInt(partition.partition());
        target.putInt(leaderEpoch);
        target.putLong(offsets.startOffset());
        target.putLong(offsets.endOffset());
        target.putInt(batchCount);
        target.putInt(dataLength);
        target.putInt((int) checksum);
        target.putInt(0);
    }

    static void writeBatchEntryHeader(
        ByteBuffer target,
        OffsetRange offsets,
        int payloadLength,
        long checksum
    ) {
        Objects.requireNonNull(offsets, "offsets");
        target.putLong(offsets.startOffset());
        target.putLong(offsets.endOffset());
        target.putInt(payloadLength);
        target.putInt((int) checksum);
    }

    static void writeIndexBlock(ByteBuffer target, List<DataBlockIndexEntry> entries) {
        Objects.requireNonNull(entries, "entries");
        target.putInt(INDEX_MAGIC);
        target.putShort(VERSION);
        target.putShort((short) 0);
        target.putInt(entries.size());
        target.putInt(Math.multiplyExact(entries.size(), INDEX_ENTRY_BYTES));
        for (DataBlockIndexEntry entry : entries) {
            target.putLong(entry.partition().topicIdHigh());
            target.putLong(entry.partition().topicIdLow());
            target.putInt(entry.partition().partition());
            target.putInt(entry.leaderEpoch());
            target.putLong(entry.offsets().startOffset());
            target.putLong(entry.offsets().endOffset());
            target.putLong(entry.blockPosition());
            target.putInt(entry.blockLength());
            target.putInt(entry.batchCount());
            target.putInt((int) entry.checksum());
            target.putInt(0);
        }
    }

    static void writeFooter(
        ByteBuffer target,
        long indexPosition,
        int indexLength,
        int dataBlockCount,
        long indexChecksum,
        long objectBodyChecksum
    ) {
        target.putInt(FOOTER_MAGIC);
        target.putShort(VERSION);
        target.putShort((short) 0);
        target.putLong(indexPosition);
        target.putInt(indexLength);
        target.putInt(dataBlockCount);
        target.putInt((int) indexChecksum);
        target.putInt((int) objectBodyChecksum);
    }

    static Footer readFooter(ByteBuffer source) throws IOException {
        Objects.requireNonNull(source, "source");
        ByteBuffer object = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        int objectStart = object.position();
        int objectLength = object.remaining();
        if (objectLength < OBJECT_HEADER_BYTES + INDEX_HEADER_BYTES + FOOTER_BYTES) {
            throw new IOException("Stream object is too short: " + objectLength);
        }
        validateObjectHeader(object, objectStart);

        int footerPosition = object.limit() - FOOTER_BYTES;
        object.position(footerPosition);
        int magic = object.getInt();
        short version = object.getShort();
        object.getShort();
        long indexPosition = object.getLong();
        int indexLength = object.getInt();
        int dataBlockCount = object.getInt();
        long indexChecksum = Integer.toUnsignedLong(object.getInt());
        long objectBodyChecksum = Integer.toUnsignedLong(object.getInt());

        if (magic != FOOTER_MAGIC || version != VERSION) {
            throw new IOException("Unsupported stream object footer: magic=" + magic + ", version=" + version);
        }
        if (indexPosition < OBJECT_HEADER_BYTES || indexLength < INDEX_HEADER_BYTES || dataBlockCount < 0) {
            throw new IOException("Invalid stream object footer bounds");
        }
        long indexEnd = checkedAdd(indexPosition, indexLength, "Stream object footer index range overflows");
        if (indexEnd != objectLength - FOOTER_BYTES) {
            throw new IOException("Stream object index does not immediately precede footer");
        }
        long actualBodyChecksum = crc32c(source, objectStart, footerPosition - objectStart);
        if (actualBodyChecksum != objectBodyChecksum) {
            throw new IOException(
                "Stream object body checksum mismatch: expected=" + objectBodyChecksum +
                    ", actual=" + actualBodyChecksum
            );
        }
        return new Footer(indexPosition, indexLength, dataBlockCount, indexChecksum, objectBodyChecksum);
    }

    static List<DataBlockIndexEntry> readIndex(ByteBuffer source, Footer footer) throws IOException {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(footer, "footer");
        validateFooterForIndexRead(footer);
        ByteBuffer object = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        int objectStart = object.position();
        long absoluteIndexPosition = checkedAdd(
            objectStart,
            footer.indexPosition(),
            "Stream object absolute index position overflows"
        );
        int indexPosition = checkedInt(absoluteIndexPosition, "Stream object index position exceeds buffer range");
        int indexEnd = checkedInt(
            checkedAdd(indexPosition, footer.indexLength(), "Stream object index end overflows"),
            "Stream object index end exceeds buffer range"
        );
        if (indexEnd > object.limit()) {
            throw new IOException("Stream object index is outside object bounds");
        }

        long actualIndexChecksum = crc32c(source, indexPosition, footer.indexLength());
        if (actualIndexChecksum != footer.indexChecksum()) {
            throw new IOException(
                "Stream object index checksum mismatch: expected=" + footer.indexChecksum() +
                    ", actual=" + actualIndexChecksum
            );
        }

        object.position(indexPosition);
        int magic = object.getInt();
        short version = object.getShort();
        object.getShort();
        int entryCount = object.getInt();
        int entriesLength = object.getInt();
        if (magic != INDEX_MAGIC || version != VERSION) {
            throw new IOException("Unsupported stream object index: magic=" + magic + ", version=" + version);
        }
        validateIndexShape(entryCount, entriesLength, footer);

        List<DataBlockIndexEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            entries.add(readIndexEntry(object, i));
        }
        validateDataBlockLayout(entries, footer.indexPosition());
        return List.copyOf(entries);
    }

    private static void validateObjectHeader(ByteBuffer object, int objectStart) throws IOException {
        ByteBuffer header = object.duplicate().order(ByteOrder.BIG_ENDIAN);
        header.position(objectStart);
        int magic = header.getInt();
        short version = header.getShort();
        if (magic != OBJECT_MAGIC || version != VERSION) {
            throw new IOException("Unsupported stream object header: magic=" + magic + ", version=" + version);
        }
    }

    private static void validateFooterForIndexRead(Footer footer) throws IOException {
        if (footer.indexPosition() < OBJECT_HEADER_BYTES ||
            footer.indexLength() < INDEX_HEADER_BYTES || footer.dataBlockCount() < 0) {
            throw new IOException("Invalid stream object footer bounds");
        }
    }

    private static void validateIndexShape(int entryCount, int entriesLength, Footer footer) throws IOException {
        if (entryCount < 0 || entriesLength < 0 || entryCount != footer.dataBlockCount()) {
            throw new IOException("Stream object index length/count mismatch");
        }
        int expectedEntriesLength = checkedMultiply(
            entryCount,
            INDEX_ENTRY_BYTES,
            "Stream object index entry bytes overflow"
        );
        int expectedIndexLength = checkedInt(
            checkedAdd(INDEX_HEADER_BYTES, expectedEntriesLength, "Stream object index length overflows"),
            "Stream object index length exceeds buffer range"
        );
        if (entriesLength != expectedEntriesLength || footer.indexLength() != expectedIndexLength) {
            throw new IOException("Stream object index length/count mismatch");
        }
    }

    private static DataBlockIndexEntry readIndexEntry(ByteBuffer object, int entryIndex) throws IOException {
        try {
            SharedPartitionId partition = new SharedPartitionId(object.getLong(), object.getLong(), object.getInt());
            int leaderEpoch = object.getInt();
            OffsetRange offsets = new OffsetRange(object.getLong(), object.getLong());
            long blockPosition = object.getLong();
            int blockLength = object.getInt();
            int batchCount = object.getInt();
            long checksum = Integer.toUnsignedLong(object.getInt());
            object.getInt();
            return new DataBlockIndexEntry(
                partition,
                leaderEpoch,
                offsets,
                blockPosition,
                blockLength,
                batchCount,
                checksum
            );
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid stream object index entry " + entryIndex, e);
        }
    }

    private static void validateDataBlockLayout(List<DataBlockIndexEntry> entries, long indexPosition) throws IOException {
        long expectedBlockPosition = OBJECT_HEADER_BYTES;
        for (DataBlockIndexEntry entry : entries) {
            if (entry.blockPosition() != expectedBlockPosition) {
                throw new IOException(
                    "Stream object data block index is non-contiguous: expectedPosition=" + expectedBlockPosition +
                        ", actualPosition=" + entry.blockPosition()
                );
            }
            long blockEnd = checkedAdd(
                entry.blockPosition(),
                entry.blockLength(),
                "Stream object data block range overflows"
            );
            if (blockEnd > indexPosition) {
                throw new IOException(
                    "Stream object data block extends into index: blockEnd=" + blockEnd +
                        ", indexPosition=" + indexPosition
                );
            }
            expectedBlockPosition = blockEnd;
        }
        if (expectedBlockPosition != indexPosition) {
            throw new IOException(
                "Stream object data blocks do not cover the complete data section: blockEnd=" + expectedBlockPosition +
                    ", indexPosition=" + indexPosition
            );
        }
    }

    private static long checkedAdd(long left, long right, String message) throws IOException {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException e) {
            throw new IOException(message, e);
        }
    }

    private static int checkedMultiply(int left, int right, String message) throws IOException {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException e) {
            throw new IOException(message, e);
        }
    }

    private static int checkedInt(long value, String message) throws IOException {
        try {
            return Math.toIntExact(value);
        } catch (ArithmeticException e) {
            throw new IOException(message, e);
        }
    }

    static long crc32c(ByteBuffer source, int position, int length) {
        if (position < 0 || length < 0 || position > source.limit() - length) {
            throw new IllegalArgumentException("CRC32C range is outside buffer bounds");
        }
        ByteBuffer data = source.duplicate();
        data.position(position);
        data.limit(position + length);
        CRC32C crc = new CRC32C();
        crc.update(data);
        return crc.getValue();
    }

    record Footer(
        long indexPosition,
        int indexLength,
        int dataBlockCount,
        long indexChecksum,
        long objectBodyChecksum
    ) {
    }

    record DataBlockIndexEntry(
        SharedPartitionId partition,
        int leaderEpoch,
        OffsetRange offsets,
        long blockPosition,
        int blockLength,
        int batchCount,
        long checksum
    ) {
        DataBlockIndexEntry {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(offsets, "offsets");
            if (blockPosition < OBJECT_HEADER_BYTES || blockLength < DATA_BLOCK_HEADER_BYTES || batchCount <= 0) {
                throw new IllegalArgumentException("Invalid stream data block index entry");
            }
        }
    }
}
