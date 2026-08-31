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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Packs committed WAL records into immutable stream objects.
 *
 * <p>Object formation is deliberately independent from Kafka LogSegment and local WAL-file boundaries. Records for
 * the same logical stream and leader epoch are coalesced into DataBlocks up to a target block size. Every object ends
 * with a DataBlock index and footer so remote reads and compaction can discover stream ranges without local WAL
 * layout knowledge.</p>
 */
public final class SharedObjectPacker {
    static final int DEFAULT_TARGET_DATA_BLOCK_BYTES = 1024 * 1024;

    private final int targetDataBlockBytes;

    public SharedObjectPacker() {
        this(DEFAULT_TARGET_DATA_BLOCK_BYTES);
    }

    SharedObjectPacker(int targetDataBlockBytes) {
        if (targetDataBlockBytes < StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES) {
            throw new IllegalArgumentException("targetDataBlockBytes is too small");
        }
        this.targetDataBlockBytes = targetDataBlockBytes;
    }

    public PackedObject pack(
        long objectId,
        List<SharedStorageEngine.UploadCandidate> candidates,
        SharedStorageEngine engine
    ) throws IOException {
        if (objectId < 0) {
            throw new IllegalArgumentException("objectId must be non-negative");
        }
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(engine, "engine");
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }

        List<Batch> batches = readBatches(candidates, engine);
        List<DataBlock> dataBlocks = buildDataBlocks(batches);
        int indexLength = Math.addExact(
            StreamObjectFormat.INDEX_HEADER_BYTES,
            Math.multiplyExact(dataBlocks.size(), StreamObjectFormat.INDEX_ENTRY_BYTES)
        );
        long totalSize = Math.addExact(
            StreamObjectFormat.OBJECT_HEADER_BYTES + (long) indexLength + StreamObjectFormat.FOOTER_BYTES,
            serializedDataBlockBytes(dataBlocks)
        );
        if (totalSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Packed object exceeds Java ByteBuffer limit: " + totalSize);
        }

        ByteBuffer object = ByteBuffer.allocate((int) totalSize).order(ByteOrder.BIG_ENDIAN);
        StreamObjectFormat.writeObjectHeader(object, objectId);
        List<SharedObjectRange> ranges = new ArrayList<>(batches.size());
        List<StreamObjectFormat.DataBlockIndexEntry> indexEntries = new ArrayList<>(dataBlocks.size());
        for (DataBlock block : dataBlocks) {
            writeDataBlock(object, block, ranges, indexEntries);
        }

        int indexPosition = object.position();
        StreamObjectFormat.writeIndexBlock(object, indexEntries);
        long indexChecksum = StreamObjectFormat.crc32c(object, indexPosition, indexLength);
        int footerPosition = object.position();
        long objectBodyChecksum = StreamObjectFormat.crc32c(object, 0, footerPosition);
        StreamObjectFormat.writeFooter(
            object,
            indexPosition,
            indexLength,
            dataBlocks.size(),
            indexChecksum,
            objectBodyChecksum
        );
        if (object.hasRemaining()) {
            throw new IllegalStateException("Stream object size accounting left unwritten bytes: " + object.remaining());
        }

        object.flip();
        long objectChecksum = crc32c(object);
        SharedObjectMetadata metadata = new SharedObjectMetadata(objectId, totalSize, objectChecksum, ranges);
        return new PackedObject(object, metadata);
    }

    private static List<Batch> readBatches(
        List<SharedStorageEngine.UploadCandidate> candidates,
        SharedStorageEngine engine
    ) throws IOException {
        List<WalRecord> records = engine.readUploadCandidates(candidates);
        if (records.size() != candidates.size()) {
            throw new IOException("WAL batch read returned an unexpected record count");
        }

        List<Batch> batches = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            SharedStorageEngine.UploadCandidate candidate = candidates.get(i);
            WalRecord record = records.get(i);
            if (record.firstOffset() != candidate.offsets().startOffset() ||
                Math.addExact(record.lastOffset(), 1) != candidate.offsets().endOffset()) {
                throw new IOException("WAL candidate logical range changed before packing: " + candidate);
            }
            ByteBuffer payload = record.payload();
            batches.add(new Batch(candidate, record.leaderEpoch(), payload, crc32c(payload)));
        }
        return batches;
    }

    private List<DataBlock> buildDataBlocks(List<Batch> batches) {
        Map<StreamEpoch, List<DataBlock>> blocksByStream = new LinkedHashMap<>();
        for (Batch batch : batches) {
            StreamEpoch stream = new StreamEpoch(batch.candidate().partition(), batch.leaderEpoch());
            List<DataBlock> streamBlocks = blocksByStream.computeIfAbsent(stream, ignored -> new ArrayList<>());
            int entryBytes = Math.addExact(
                StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES,
                batch.payload().remaining()
            );
            DataBlock current = streamBlocks.isEmpty() ? null : streamBlocks.get(streamBlocks.size() - 1);
            if (current == null || !current.canAppend(batch, entryBytes, targetDataBlockBytes)) {
                current = new DataBlock(stream, batch, entryBytes);
                streamBlocks.add(current);
            } else {
                current.append(batch, entryBytes);
            }
        }

        List<DataBlock> result = new ArrayList<>();
        for (List<DataBlock> streamBlocks : blocksByStream.values()) {
            result.addAll(streamBlocks);
        }
        return List.copyOf(result);
    }

    private static long serializedDataBlockBytes(List<DataBlock> dataBlocks) {
        long total = 0;
        for (DataBlock block : dataBlocks) {
            total = Math.addExact(
                total,
                Math.addExact(StreamObjectFormat.DATA_BLOCK_HEADER_BYTES, (long) block.dataBytes())
            );
        }
        return total;
    }

    private static void writeDataBlock(
        ByteBuffer object,
        DataBlock block,
        List<SharedObjectRange> ranges,
        List<StreamObjectFormat.DataBlockIndexEntry> indexEntries
    ) {
        int blockPosition = object.position();
        int dataPosition = Math.addExact(blockPosition, StreamObjectFormat.DATA_BLOCK_HEADER_BYTES);
        object.position(dataPosition);

        for (Batch batch : block.batches()) {
            ByteBuffer payload = batch.payload().duplicate();
            StreamObjectFormat.writeBatchEntryHeader(
                object,
                batch.candidate().offsets(),
                payload.remaining(),
                batch.checksum()
            );
            long payloadPosition = object.position();
            int payloadLength = payload.remaining();
            object.put(payload);
            ranges.add(new SharedObjectRange(
                batch.candidate().partition(),
                batch.candidate().offsets(),
                batch.leaderEpoch(),
                payloadPosition,
                payloadLength,
                batch.checksum()
            ));
        }

        int blockEnd = object.position();
        int dataLength = blockEnd - dataPosition;
        long blockChecksum = StreamObjectFormat.crc32c(object, dataPosition, dataLength);
        object.position(blockPosition);
        StreamObjectFormat.writeDataBlockHeader(
            object,
            block.stream().partition(),
            block.stream().leaderEpoch(),
            block.offsets(),
            block.batches().size(),
            dataLength,
            blockChecksum
        );
        object.position(blockEnd);
        indexEntries.add(new StreamObjectFormat.DataBlockIndexEntry(
            block.stream().partition(),
            block.stream().leaderEpoch(),
            block.offsets(),
            blockPosition,
            blockEnd - blockPosition,
            block.batches().size(),
            blockChecksum
        ));
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }

    private record Batch(
        SharedStorageEngine.UploadCandidate candidate,
        int leaderEpoch,
        ByteBuffer payload,
        long checksum
    ) {
        private Batch {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(payload, "payload");
            payload = payload.asReadOnlyBuffer();
        }
    }

    private record StreamEpoch(SharedPartitionId partition, int leaderEpoch) {
        private StreamEpoch {
            Objects.requireNonNull(partition, "partition");
        }
    }

    private static final class DataBlock {
        private final StreamEpoch stream;
        private final List<Batch> batches = new ArrayList<>();
        private long startOffset;
        private long endOffset;
        private int dataBytes;

        private DataBlock(StreamEpoch stream, Batch first, int entryBytes) {
            this.stream = stream;
            this.startOffset = first.candidate().offsets().startOffset();
            this.endOffset = first.candidate().offsets().endOffset();
            this.dataBytes = entryBytes;
            this.batches.add(first);
        }

        private boolean canAppend(Batch next, int entryBytes, int targetBytes) {
            return next.candidate().offsets().startOffset() == endOffset &&
                Math.addExact(dataBytes, entryBytes) <= targetBytes;
        }

        private void append(Batch next, int entryBytes) {
            batches.add(next);
            endOffset = next.candidate().offsets().endOffset();
            dataBytes = Math.addExact(dataBytes, entryBytes);
        }

        private StreamEpoch stream() {
            return stream;
        }

        private List<Batch> batches() {
            return batches;
        }

        private OffsetRange offsets() {
            return new OffsetRange(startOffset, endOffset);
        }

        private int dataBytes() {
            return dataBytes;
        }
    }
}
