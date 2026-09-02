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
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Parses and verifies one KSO2 DataBlock fetched through a bounded object-store range read. */
final class StreamObjectDataBlockReader {
    private StreamObjectDataBlockReader() {
    }

    static DataBlockSnapshot read(
        ByteBuffer source,
        StreamObjectFormat.DataBlockIndexEntry expected
    ) throws IOException {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(expected, "expected");
        if (source.remaining() != expected.blockLength()) {
            throw new IOException(
                "Stream data block length mismatch: expected=" + expected.blockLength() +
                    ", actual=" + source.remaining()
            );
        }

        ByteBuffer block = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        int blockStart = block.position();
        Header header = readHeader(block);
        validateHeader(header, expected);
        int dataStart = block.position();
        int dataEnd = checkedEnd(dataStart, header.dataLength(), block.limit(), "Stream data block payload");
        long actualBlockChecksum = StreamObjectFormat.crc32c(block, dataStart, header.dataLength());
        if (actualBlockChecksum != header.checksum()) {
            throw new IOException(
                "Stream data block checksum mismatch: expected=" + header.checksum() +
                    ", actual=" + actualBlockChecksum
            );
        }

        List<BatchEntry> batches = readBatches(block, dataEnd, header);
        if (block.position() != dataEnd || dataEnd != blockStart + expected.blockLength()) {
            throw new IOException("Trailing or missing bytes in stream data block");
        }
        return new DataBlockSnapshot(header.partition(), header.leaderEpoch(), header.offsets(), batches);
    }

    private static Header readHeader(ByteBuffer block) throws IOException {
        if (block.remaining() < StreamObjectFormat.DATA_BLOCK_HEADER_BYTES) {
            throw new IOException("Stream data block is shorter than its header");
        }
        try {
            SharedPartitionId partition = new SharedPartitionId(block.getLong(), block.getLong(), block.getInt());
            int leaderEpoch = block.getInt();
            OffsetRange offsets = new OffsetRange(block.getLong(), block.getLong());
            int batchCount = block.getInt();
            int dataLength = block.getInt();
            long checksum = Integer.toUnsignedLong(block.getInt());
            int reserved = block.getInt();
            if (batchCount <= 0 || dataLength <= 0 || reserved != 0) {
                throw new IllegalArgumentException("invalid data block header fields");
            }
            return new Header(partition, leaderEpoch, offsets, batchCount, dataLength, checksum);
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            throw new IOException("Invalid stream data block header", e);
        }
    }

    private static void validateHeader(
        Header header,
        StreamObjectFormat.DataBlockIndexEntry expected
    ) throws IOException {
        int expectedLength;
        try {
            expectedLength = Math.addExact(StreamObjectFormat.DATA_BLOCK_HEADER_BYTES, header.dataLength());
        } catch (ArithmeticException e) {
            throw new IOException("Stream data block length overflows", e);
        }
        if (!header.partition().equals(expected.partition()) ||
            header.leaderEpoch() != expected.leaderEpoch() ||
            !header.offsets().equals(expected.offsets()) ||
            header.batchCount() != expected.batchCount() ||
            header.checksum() != expected.checksum() ||
            expectedLength != expected.blockLength()) {
            throw new IOException("Stream data block header does not match index entry");
        }
    }

    private static List<BatchEntry> readBatches(ByteBuffer block, int dataEnd, Header header) throws IOException {
        List<BatchEntry> batches = new ArrayList<>(header.batchCount());
        long expectedOffset = header.offsets().startOffset();
        for (int i = 0; i < header.batchCount(); i++) {
            if (dataEnd - block.position() < StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES) {
                throw new IOException("Truncated stream batch entry header at index " + i);
            }
            BatchEntry batch = readBatch(block, dataEnd, i);
            if (batch.offsets().startOffset() != expectedOffset) {
                throw new IOException("Non-contiguous stream batch offsets at index " + i);
            }
            expectedOffset = batch.offsets().endOffset();
            batches.add(batch);
        }
        if (expectedOffset != header.offsets().endOffset()) {
            throw new IOException("Stream batch offsets do not cover indexed DataBlock range");
        }
        return List.copyOf(batches);
    }

    private static BatchEntry readBatch(ByteBuffer block, int dataEnd, int batchIndex) throws IOException {
        try {
            OffsetRange offsets = new OffsetRange(block.getLong(), block.getLong());
            int payloadLength = block.getInt();
            long checksum = Integer.toUnsignedLong(block.getInt());
            if (payloadLength <= 0) {
                throw new IllegalArgumentException("batch payload length must be positive");
            }
            int payloadEnd = checkedEnd(
                block.position(),
                payloadLength,
                dataEnd,
                "Stream batch payload at index " + batchIndex
            );
            ByteBuffer payload = block.slice().order(ByteOrder.BIG_ENDIAN);
            payload.limit(payloadLength);
            long actualChecksum = StreamObjectFormat.crc32c(payload, payload.position(), payload.remaining());
            if (actualChecksum != checksum) {
                throw new IOException(
                    "Stream batch checksum mismatch at index " + batchIndex +
                        ": expected=" + checksum + ", actual=" + actualChecksum
                );
            }
            block.position(payloadEnd);
            return new BatchEntry(offsets, checksum, payload.asReadOnlyBuffer());
        } catch (IllegalArgumentException | BufferUnderflowException e) {
            throw new IOException("Invalid stream batch entry at index " + batchIndex, e);
        }
    }

    private static int checkedEnd(int start, int length, int limit, String label) throws IOException {
        int end;
        try {
            end = Math.addExact(start, length);
        } catch (ArithmeticException e) {
            throw new IOException(label + " range overflows", e);
        }
        if (start < 0 || length < 0 || end > limit) {
            throw new IOException(label + " exceeds DataBlock bounds");
        }
        return end;
    }

    record DataBlockSnapshot(
        SharedPartitionId partition,
        int leaderEpoch,
        OffsetRange offsets,
        List<BatchEntry> batches
    ) {
        DataBlockSnapshot {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(offsets, "offsets");
            batches = List.copyOf(Objects.requireNonNull(batches, "batches"));
        }

        ByteBuffer batch(SharedObjectRange target) throws IOException {
            Objects.requireNonNull(target, "target");
            if (!partition.equals(target.partition()) || leaderEpoch != target.leaderEpoch() ||
                target.offsets().startOffset() < offsets.startOffset() ||
                target.offsets().endOffset() > offsets.endOffset()) {
                throw new IOException("Remote batch metadata is outside indexed DataBlock");
            }
            for (BatchEntry batch : batches) {
                if (batch.offsets().equals(target.offsets())) {
                    if (batch.checksum() != target.checksum() || batch.payload().remaining() != target.objectLength()) {
                        throw new IOException("Remote batch metadata disagrees with DataBlock entry");
                    }
                    return batch.payload().asReadOnlyBuffer();
                }
            }
            throw new IOException("Remote batch logical range is missing from indexed DataBlock");
        }
    }

    record BatchEntry(OffsetRange offsets, long checksum, ByteBuffer payload) {
        BatchEntry {
            Objects.requireNonNull(offsets, "offsets");
            Objects.requireNonNull(payload, "payload");
            payload = payload.asReadOnlyBuffer();
        }
    }

    private record Header(
        SharedPartitionId partition,
        int leaderEpoch,
        OffsetRange offsets,
        int batchCount,
        int dataLength,
        long checksum
    ) {
    }
}
