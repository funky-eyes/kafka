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

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Self-indexed stream-object wire format for shared storage.
 *
 * <p>The object is deliberately independent from Kafka LogSegment and from the physical layout of the broker WAL.
 * Kafka RecordBatch payloads are written as data blocks. A block index maps logical partition/offset ranges to those
 * blocks and a fixed footer locates the index with one tail range-read. This is the same shape used by stream-oriented
 * object stores: data blocks first, searchable metadata second, footer last.</p>
 *
 * <pre>
 * +------------------+ 0
 * | object header    |
 * +------------------+
 * | data block 0     |
 * +------------------+
 * | data block 1     |
 * +------------------+
 * | ...              |
 * +------------------+
 * | block index      | &lt;-- footer.indexStart
 * +------------------+
 * | footer           | fixed 48 bytes
 * +------------------+ object size
 * </pre>
 */
public final class StreamObjectCodec {
    static final int MAGIC = 0x4b534f32; // KSO2
    static final short VERSION = 2;
    static final byte DATA_BLOCK_MAGIC = 0x5a;
    static final byte DATA_BLOCK_FLAGS = 0x02;
    static final long FOOTER_MAGIC = 0x88e241b785f4cff7L;

    static final int OBJECT_HEADER_BYTES = 16;
    static final int DATA_BLOCK_HEADER_BYTES = 16;
    static final int INDEX_ENTRY_BYTES = 64;
    static final int FOOTER_BYTES = 48;

    private StreamObjectCodec() {
    }

    /** Encodes multiple logical streams into one immutable self-indexed object. */
    public static ByteBuffer encode(long objectId, List<DataBlock> blocks) {
        if (objectId < 0) {
            throw new IllegalArgumentException("objectId must be non-negative");
        }
        Objects.requireNonNull(blocks, "blocks");
        if (blocks.isEmpty()) {
            throw new IllegalArgumentException("blocks must not be empty");
        }

        List<DataBlock> immutableBlocks = new ArrayList<>(blocks.size());
        long dataBytes = OBJECT_HEADER_BYTES;
        for (DataBlock block : blocks) {
            DataBlock copy = Objects.requireNonNull(block, "block").copy();
            immutableBlocks.add(copy);
            dataBytes = Math.addExact(dataBytes, DATA_BLOCK_HEADER_BYTES + (long) copy.payloadLength());
        }
        long indexBytes = Math.multiplyExact((long) immutableBlocks.size(), INDEX_ENTRY_BYTES);
        long totalBytes = Math.addExact(Math.addExact(dataBytes, indexBytes), FOOTER_BYTES);
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Stream object exceeds Java ByteBuffer limit: " + totalBytes);
        }

        ByteBuffer object = ByteBuffer.allocate((int) totalBytes).order(ByteOrder.BIG_ENDIAN);
        writeObjectHeader(object, objectId);

        List<BlockIndex> indexes = new ArrayList<>(immutableBlocks.size());
        for (DataBlock block : immutableBlocks) {
            ByteBuffer payload = block.payload();
            long checksum = crc32c(payload);
            long blockPosition = object.position();
            writeDataBlockHeader(object, payload.remaining(), checksum);
            object.put(payload);
            int blockLength = Math.toIntExact(object.position() - blockPosition);
            indexes.add(new BlockIndex(
                block.partition(),
                block.leaderEpoch(),
                block.startOffset(),
                block.endOffset(),
                blockPosition,
                blockLength,
                block.payloadLength(),
                checksum
            ));
        }

        long indexStart = object.position();
        for (BlockIndex index : indexes) {
            writeIndexEntry(object, index);
        }
        int indexLength = Math.toIntExact(object.position() - indexStart);
        long contentChecksum = crc32c(prefix(object, object.position()));
        writeFooter(object, indexStart, indexLength, indexes.size(), objectId, dataBytes, contentChecksum);
        object.flip();
        return object;
    }

    /** Parses and validates the header, index and footer without copying payload bytes. */
    public static DecodedObject decode(ByteBuffer encoded) {
        ByteBuffer object = readOnlyBigEndian(encoded);
        requireSize(object, OBJECT_HEADER_BYTES + INDEX_ENTRY_BYTES + FOOTER_BYTES);

        Header header = readObjectHeader(object);
        Footer footer = readFooter(object);
        if (footer.objectId() != header.objectId()) {
            throw corrupt("Header/footer object id mismatch");
        }
        validateFooterBounds(object, footer);

        long actualContentChecksum = crc32c(range(object, 0, footer.footerPosition()));
        if (actualContentChecksum != footer.contentChecksum()) {
            throw corrupt("Object content checksum mismatch");
        }

        List<BlockIndex> indexes = readIndexes(object, footer);
        validateIndexes(footer, indexes);
        return new DecodedObject(header.objectId(), indexes, footer);
    }

    /** Reads and verifies one Kafka RecordBatch payload using an index entry. */
    public static ByteBuffer readBlock(ByteBuffer encoded, BlockIndex index) {
        Objects.requireNonNull(index, "index");
        ByteBuffer object = readOnlyBigEndian(encoded);
        if (index.blockPosition() < OBJECT_HEADER_BYTES ||
            index.blockLength() < DATA_BLOCK_HEADER_BYTES ||
            index.blockPosition() + index.blockLength() > object.limit()) {
            throw corrupt("Data block is outside object bounds");
        }

        ByteBuffer block = range(object, index.blockPosition(), index.blockLength());
        byte magic = block.get();
        byte flags = block.get();
        block.getShort(); // reserved
        int payloadLength = block.getInt();
        long checksum = Integer.toUnsignedLong(block.getInt());
        block.getInt(); // reserved
        if (magic != DATA_BLOCK_MAGIC) {
            throw corrupt("Invalid data block magic");
        }
        if (flags != DATA_BLOCK_FLAGS) {
            throw corrupt("Unsupported data block flags: " + flags);
        }
        if (payloadLength != index.payloadLength() ||
            index.blockLength() != DATA_BLOCK_HEADER_BYTES + payloadLength ||
            payloadLength != block.remaining()) {
            throw corrupt("Data block length mismatch");
        }
        if (checksum != index.checksum()) {
            throw corrupt("Data block header/index checksum mismatch");
        }

        ByteBuffer payload = block.slice().asReadOnlyBuffer();
        long actualChecksum = crc32c(payload);
        if (actualChecksum != checksum) {
            throw corrupt("Data block payload checksum mismatch");
        }
        return payload;
    }

    private static void writeObjectHeader(ByteBuffer target, long objectId) {
        target.putInt(MAGIC);
        target.putShort(VERSION);
        target.putShort((short) 0);
        target.putLong(objectId);
    }

    private static Header readObjectHeader(ByteBuffer object) {
        ByteBuffer header = range(object, 0, OBJECT_HEADER_BYTES);
        int magic = header.getInt();
        short version = header.getShort();
        header.getShort(); // flags
        long objectId = header.getLong();
        if (magic != MAGIC) {
            throw corrupt("Invalid stream object magic");
        }
        if (version != VERSION) {
            throw corrupt("Unsupported stream object version: " + version);
        }
        if (objectId < 0) {
            throw corrupt("Negative stream object id");
        }
        return new Header(objectId);
    }

    private static void writeDataBlockHeader(ByteBuffer target, int payloadLength, long checksum) {
        target.put(DATA_BLOCK_MAGIC);
        target.put(DATA_BLOCK_FLAGS);
        target.putShort((short) 0);
        target.putInt(payloadLength);
        target.putInt((int) checksum);
        target.putInt(0);
    }

    private static void writeIndexEntry(ByteBuffer target, BlockIndex index) {
        target.putLong(index.partition().topicIdHigh());
        target.putLong(index.partition().topicIdLow());
        target.putInt(index.partition().partition());
        target.putInt(index.leaderEpoch());
        target.putLong(index.startOffset());
        target.putLong(index.endOffset());
        target.putLong(index.blockPosition());
        target.putInt(index.blockLength());
        target.putInt(index.payloadLength());
        target.putLong(index.checksum());
    }

    private static List<BlockIndex> readIndexes(ByteBuffer object, Footer footer) {
        ByteBuffer indexBuffer = range(object, footer.indexStart(), footer.indexLength());
        List<BlockIndex> indexes = new ArrayList<>(footer.blockCount());
        for (int i = 0; i < footer.blockCount(); i++) {
            SharedPartitionId partition = new SharedPartitionId(
                indexBuffer.getLong(),
                indexBuffer.getLong(),
                indexBuffer.getInt()
            );
            int leaderEpoch = indexBuffer.getInt();
            long startOffset = indexBuffer.getLong();
            long endOffset = indexBuffer.getLong();
            long blockPosition = indexBuffer.getLong();
            int blockLength = indexBuffer.getInt();
            int payloadLength = indexBuffer.getInt();
            long checksum = indexBuffer.getLong();
            indexes.add(new BlockIndex(
                partition,
                leaderEpoch,
                startOffset,
                endOffset,
                blockPosition,
                blockLength,
                payloadLength,
                checksum
            ));
        }
        return List.copyOf(indexes);
    }

    private static void writeFooter(
        ByteBuffer target,
        long indexStart,
        int indexLength,
        int blockCount,
        long objectId,
        long dataLength,
        long contentChecksum
    ) {
        target.putLong(indexStart);
        target.putInt(indexLength);
        target.putInt(blockCount);
        target.putLong(objectId);
        target.putLong(dataLength);
        target.putLong(contentChecksum);
        target.putLong(FOOTER_MAGIC);
    }

    private static Footer readFooter(ByteBuffer object) {
        long footerPosition = object.limit() - (long) FOOTER_BYTES;
        ByteBuffer footer = range(object, footerPosition, FOOTER_BYTES);
        long indexStart = footer.getLong();
        int indexLength = footer.getInt();
        int blockCount = footer.getInt();
        long objectId = footer.getLong();
        long dataLength = footer.getLong();
        long contentChecksum = footer.getLong();
        long magic = footer.getLong();
        if (magic != FOOTER_MAGIC) {
            throw corrupt("Invalid stream object footer magic");
        }
        return new Footer(
            indexStart,
            indexLength,
            blockCount,
            objectId,
            dataLength,
            contentChecksum,
            footerPosition
        );
    }

    private static void validateFooterBounds(ByteBuffer object, Footer footer) {
        if (footer.blockCount() <= 0) {
            throw corrupt("Stream object has no data blocks");
        }
        long expectedIndexLength = Math.multiplyExact((long) footer.blockCount(), INDEX_ENTRY_BYTES);
        if (expectedIndexLength != footer.indexLength()) {
            throw corrupt("Block index length does not match block count");
        }
        if (footer.indexStart() < OBJECT_HEADER_BYTES ||
            footer.indexLength() < 0 ||
            footer.indexStart() + footer.indexLength() != footer.footerPosition()) {
            throw corrupt("Invalid block index bounds");
        }
        if (footer.dataLength() != footer.indexStart() || footer.dataLength() > object.limit()) {
            throw corrupt("Invalid data area length");
        }
    }

    private static void validateIndexes(Footer footer, List<BlockIndex> indexes) {
        long nextMinimumPosition = OBJECT_HEADER_BYTES;
        for (BlockIndex index : indexes) {
            if (index.startOffset() < 0 || index.endOffset() <= index.startOffset()) {
                throw corrupt("Invalid logical stream range");
            }
            if (index.leaderEpoch() < 0) {
                throw corrupt("Negative leader epoch");
            }
            if (index.payloadLength() < 0 ||
                index.blockLength() != DATA_BLOCK_HEADER_BYTES + index.payloadLength()) {
                throw corrupt("Invalid indexed block length");
            }
            if (index.blockPosition() < nextMinimumPosition ||
                index.blockPosition() + index.blockLength() > footer.indexStart()) {
                throw corrupt("Indexed data blocks overlap or exceed data area");
            }
            nextMinimumPosition = index.blockPosition() + index.blockLength();
        }
        if (nextMinimumPosition != footer.indexStart()) {
            throw corrupt("Unindexed bytes remain in data area");
        }
    }

    private static ByteBuffer readOnlyBigEndian(ByteBuffer source) {
        Objects.requireNonNull(source, "source");
        return source.duplicate().order(ByteOrder.BIG_ENDIAN).asReadOnlyBuffer().order(ByteOrder.BIG_ENDIAN);
    }

    private static ByteBuffer prefix(ByteBuffer source, int endExclusive) {
        return range(source.duplicate().order(ByteOrder.BIG_ENDIAN), 0, endExclusive);
    }

    private static ByteBuffer range(ByteBuffer source, long position, long length) {
        if (position < 0 || length < 0 || position + length > source.limit() ||
            position > Integer.MAX_VALUE || length > Integer.MAX_VALUE) {
            throw corrupt("Requested object range is outside buffer bounds");
        }
        ByteBuffer duplicate = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        duplicate.position((int) position);
        duplicate.limit(Math.toIntExact(position + length));
        return duplicate.slice().order(ByteOrder.BIG_ENDIAN);
    }

    private static void requireSize(ByteBuffer object, int minimumSize) {
        if (object.limit() < minimumSize) {
            throw corrupt("Stream object is too small: " + object.limit());
        }
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }

    private static IllegalArgumentException corrupt(String message) {
        return new IllegalArgumentException(message);
    }

    private record Header(long objectId) {
    }

    public record DataBlock(
        SharedPartitionId partition,
        int leaderEpoch,
        long startOffset,
        long endOffset,
        ByteBuffer payload
    ) {
        public DataBlock {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(payload, "payload");
            if (leaderEpoch < 0) {
                throw new IllegalArgumentException("leaderEpoch must be non-negative");
            }
            if (startOffset < 0 || endOffset <= startOffset) {
                throw new IllegalArgumentException("invalid logical stream range");
            }
            payload = payload.duplicate().asReadOnlyBuffer();
        }

        @Override
        public ByteBuffer payload() {
            return payload.duplicate().asReadOnlyBuffer();
        }

        int payloadLength() {
            return payload.remaining();
        }

        DataBlock copy() {
            return new DataBlock(partition, leaderEpoch, startOffset, endOffset, payload);
        }
    }

    public record BlockIndex(
        SharedPartitionId partition,
        int leaderEpoch,
        long startOffset,
        long endOffset,
        long blockPosition,
        int blockLength,
        int payloadLength,
        long checksum
    ) {
        public BlockIndex {
            Objects.requireNonNull(partition, "partition");
        }
    }

    public record Footer(
        long indexStart,
        int indexLength,
        int blockCount,
        long objectId,
        long dataLength,
        long contentChecksum,
        long footerPosition
    ) {
    }

    public record DecodedObject(long objectId, List<BlockIndex> indexes, Footer footer) {
        public DecodedObject {
            indexes = List.copyOf(indexes);
            Objects.requireNonNull(footer, "footer");
        }
    }
}
