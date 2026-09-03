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
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;

/**
 * Plans and serializes committed WAL records into immutable KSO2 stream objects.
 *
 * <p>Planning uses only WAL index metadata, so the exact object size and DataBlock layout are known before metadata
 * PREPARE without loading payload bytes. Serialization then pulls at most one DataBlock from the WAL at a time and
 * emits fixed-size upload parts. With the defaults this bounds transient object-building heap to roughly one 16 MiB
 * upload part plus one 1 MiB DataBlock instead of one object-sized buffer.</p>
 */
public final class SharedObjectPacker {
    static final int DEFAULT_TARGET_DATA_BLOCK_BYTES = 1024 * 1024;
    static final int DEFAULT_UPLOAD_PART_BYTES = 16 * 1024 * 1024;

    private final int targetDataBlockBytes;
    private final int targetUploadPartBytes;

    public SharedObjectPacker() {
        this(DEFAULT_TARGET_DATA_BLOCK_BYTES, DEFAULT_UPLOAD_PART_BYTES);
    }

    SharedObjectPacker(int targetDataBlockBytes) {
        this(targetDataBlockBytes, DEFAULT_UPLOAD_PART_BYTES);
    }

    SharedObjectPacker(int targetDataBlockBytes, int targetUploadPartBytes) {
        if (targetDataBlockBytes < StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES) {
            throw new IllegalArgumentException("targetDataBlockBytes is too small");
        }
        if (targetUploadPartBytes <= 0) {
            throw new IllegalArgumentException("targetUploadPartBytes must be positive");
        }
        this.targetDataBlockBytes = targetDataBlockBytes;
        this.targetUploadPartBytes = targetUploadPartBytes;
    }

    /**
     * Compatibility materialization path used by format tests and non-streaming callers.
     * Production upload uses {@link #plan(long, List, SharedStorageEngine)} directly.
     */
    public PackedObject pack(
        long objectId,
        List<SharedStorageEngine.UploadCandidate> candidates,
        SharedStorageEngine engine
    ) throws IOException {
        UploadPlan plan = plan(objectId, candidates, engine);
        List<ByteBuffer> parts = new ArrayList<>();
        try (ObjectStore.PartSource source = plan.openPartSource()) {
            ByteBuffer part;
            while ((part = source.nextPart()) != null) {
                parts.add(part.asReadOnlyBuffer());
            }
        }
        return new PackedObject(parts, plan.metadata());
    }

    /** Builds an exact byte-layout plan without reading any WAL payload. */
    public UploadPlan plan(
        long objectId,
        List<SharedStorageEngine.UploadCandidate> candidates,
        SharedStorageEngine engine
    ) {
        if (objectId <= 0) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(engine, "engine");
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }

        List<BatchPlan> batches = batchPlans(candidates);
        List<DataBlockPlan> unpositioned = buildDataBlocks(batches);
        int indexLength = Math.addExact(
            StreamObjectFormat.INDEX_HEADER_BYTES,
            Math.multiplyExact(unpositioned.size(), StreamObjectFormat.INDEX_ENTRY_BYTES)
        );
        long position = StreamObjectFormat.OBJECT_HEADER_BYTES;
        List<DataBlockPlan> blocks = new ArrayList<>(unpositioned.size());
        for (DataBlockPlan block : unpositioned) {
            DataBlockPlan positioned = block.at(position);
            blocks.add(positioned);
            position = Math.addExact(position, positioned.serializedBytes());
        }
        long indexPosition = position;
        long objectSize = Math.addExact(
            Math.addExact(indexPosition, indexLength),
            StreamObjectFormat.FOOTER_BYTES
        );
        return new UploadPlan(
            objectId,
            objectSize,
            indexPosition,
            indexLength,
            List.copyOf(blocks),
            engine,
            targetUploadPartBytes
        );
    }

    private static List<BatchPlan> batchPlans(List<SharedStorageEngine.UploadCandidate> candidates) {
        List<BatchPlan> result = new ArrayList<>(candidates.size());
        for (SharedStorageEngine.UploadCandidate candidate : candidates) {
            Objects.requireNonNull(candidate, "candidate");
            WalLocation location = candidate.location();
            if (location.firstOffset() != candidate.offsets().startOffset() ||
                Math.addExact(location.lastOffset(), 1) != candidate.offsets().endOffset()) {
                throw new IllegalArgumentException("WAL candidate logical range is inconsistent: " + candidate);
            }
            result.add(new BatchPlan(
                candidate,
                location.leaderEpoch(),
                location.payloadLength()
            ));
        }
        return List.copyOf(result);
    }

    private List<DataBlockPlan> buildDataBlocks(List<BatchPlan> batches) {
        Map<StreamEpoch, List<DataBlockPlan>> blocksByStream = new LinkedHashMap<>();
        for (BatchPlan batch : batches) {
            StreamEpoch stream = new StreamEpoch(batch.candidate().partition(), batch.leaderEpoch());
            List<DataBlockPlan> streamBlocks = blocksByStream.computeIfAbsent(stream, ignored -> new ArrayList<>());
            int entryBytes = Math.addExact(StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES, batch.payloadLength());
            DataBlockPlan current = streamBlocks.isEmpty() ? null : streamBlocks.get(streamBlocks.size() - 1);
            if (current == null || !current.canAppend(batch, entryBytes, targetDataBlockBytes)) {
                streamBlocks.add(DataBlockPlan.first(stream, batch, entryBytes));
            } else {
                current.append(batch, entryBytes);
            }
        }

        List<DataBlockPlan> result = new ArrayList<>();
        for (List<DataBlockPlan> streamBlocks : blocksByStream.values()) {
            result.addAll(streamBlocks);
        }
        return result;
    }

    /** One-shot upload plan whose metadata becomes available after the byte source has been fully drained. */
    public static final class UploadPlan {
        private final long objectId;
        private final long objectSize;
        private final long indexPosition;
        private final int indexLength;
        private final List<DataBlockPlan> blocks;
        private final SharedStorageEngine engine;
        private final int targetUploadPartBytes;
        private final AtomicBoolean opened = new AtomicBoolean();
        private volatile SharedObjectMetadata metadata;

        private UploadPlan(
            long objectId,
            long objectSize,
            long indexPosition,
            int indexLength,
            List<DataBlockPlan> blocks,
            SharedStorageEngine engine,
            int targetUploadPartBytes
        ) {
            this.objectId = objectId;
            this.objectSize = objectSize;
            this.indexPosition = indexPosition;
            this.indexLength = indexLength;
            this.blocks = blocks;
            this.engine = engine;
            this.targetUploadPartBytes = targetUploadPartBytes;
        }

        public long objectSize() {
            return objectSize;
        }

        public ObjectStore.PartSource openPartSource() {
            if (!opened.compareAndSet(false, true)) {
                throw new IllegalStateException("Shared object upload plan is one-shot");
            }
            return new PlannedPartSource(this);
        }

        public SharedObjectMetadata metadata() {
            SharedObjectMetadata completed = metadata;
            if (completed == null) {
                throw new IllegalStateException("Shared object metadata is unavailable before serialization completes");
            }
            return completed;
        }

        private void complete(long checksum, List<SharedObjectRange> ranges) {
            metadata = new SharedObjectMetadata(objectId, objectSize, checksum, ranges);
        }
    }

    private static final class PlannedPartSource implements ObjectStore.PartSource {
        private final UploadPlan plan;
        private final CRC32C objectChecksum = new CRC32C();
        private final CRC32C bodyChecksum = new CRC32C();
        private final List<SharedObjectRange> ranges = new ArrayList<>();
        private final List<StreamObjectFormat.DataBlockIndexEntry> indexEntries = new ArrayList<>();
        private ByteBuffer currentChunk;
        private int nextBlock;
        private Stage stage = Stage.HEADER;
        private long emittedBytes;
        private boolean closed;

        private PlannedPartSource(UploadPlan plan) {
            this.plan = plan;
        }

        @Override
        public ByteBuffer nextPart() throws IOException {
            if (closed) {
                throw new IOException("Shared object part source is closed");
            }
            if (emittedBytes == plan.objectSize) {
                return null;
            }
            long remainingObjectBytes = plan.objectSize - emittedBytes;
            int partCapacity = (int) Math.min(plan.targetUploadPartBytes, remainingObjectBytes);
            ByteBuffer part = ByteBuffer.allocate(partCapacity);
            while (part.hasRemaining()) {
                if (currentChunk == null || !currentChunk.hasRemaining()) {
                    currentChunk = nextChunk();
                    if (currentChunk == null) {
                        throw new IOException(
                            "KSO2 serializer ended early: emitted=" + emittedBytes + ", expected=" + plan.objectSize);
                    }
                }
                copy(currentChunk, part);
            }
            part.flip();
            emittedBytes = Math.addExact(emittedBytes, part.remaining());
            if (emittedBytes == plan.objectSize) {
                if (stage != Stage.DONE || currentChunk == null || currentChunk.hasRemaining()) {
                    throw new IOException("KSO2 serializer size accounting completed before the footer ended");
                }
                plan.complete(objectChecksum.getValue(), List.copyOf(ranges));
            }
            return part.asReadOnlyBuffer();
        }

        private ByteBuffer nextChunk() throws IOException {
            return switch (stage) {
                case HEADER -> headerChunk();
                case DATA_BLOCKS -> dataBlockChunk();
                case INDEX -> indexChunk();
                case FOOTER -> footerChunk();
                case DONE -> null;
            };
        }

        private ByteBuffer headerChunk() {
            ByteBuffer header = ByteBuffer.allocate(StreamObjectFormat.OBJECT_HEADER_BYTES).order(ByteOrder.BIG_ENDIAN);
            StreamObjectFormat.writeObjectHeader(header, plan.objectId);
            header.flip();
            updateBodyAndObjectChecksums(header);
            stage = Stage.DATA_BLOCKS;
            return header;
        }

        private ByteBuffer dataBlockChunk() throws IOException {
            if (nextBlock >= plan.blocks.size()) {
                stage = Stage.INDEX;
                return nextChunk();
            }
            DataBlockPlan block = plan.blocks.get(nextBlock++);
            ByteBuffer serialized = serializeBlock(block);
            updateBodyAndObjectChecksums(serialized);
            return serialized;
        }

        private ByteBuffer serializeBlock(DataBlockPlan block) throws IOException {
            List<WalLocation> locations = block.batches().stream()
                .map(batch -> batch.candidate().location())
                .toList();
            List<WalRecord> records = plan.engine.readLocalLocations(locations);
            if (records.size() != block.batches().size()) {
                throw new IOException("WAL DataBlock read returned an unexpected record count");
            }

            ByteBuffer serialized = ByteBuffer.allocate(block.serializedBytes()).order(ByteOrder.BIG_ENDIAN);
            int dataPosition = StreamObjectFormat.DATA_BLOCK_HEADER_BYTES;
            serialized.position(dataPosition);
            for (int i = 0; i < block.batches().size(); i++) {
                BatchPlan batch = block.batches().get(i);
                WalRecord record = records.get(i);
                validateRecord(batch, record);
                ByteBuffer payload = record.payload();
                long payloadChecksum = crc32c(payload);
                StreamObjectFormat.writeBatchEntryHeader(
                    serialized,
                    batch.candidate().offsets(),
                    payload.remaining(),
                    payloadChecksum
                );
                long payloadPosition = Math.addExact(block.blockPosition(), serialized.position());
                int payloadLength = payload.remaining();
                serialized.put(payload.duplicate());
                ranges.add(new SharedObjectRange(
                    batch.candidate().partition(),
                    batch.candidate().offsets(),
                    batch.leaderEpoch(),
                    payloadPosition,
                    payloadLength,
                    payloadChecksum
                ));
            }

            int blockEnd = serialized.position();
            int dataLength = blockEnd - dataPosition;
            long blockChecksum = StreamObjectFormat.crc32c(serialized, dataPosition, dataLength);
            serialized.position(0);
            StreamObjectFormat.writeDataBlockHeader(
                serialized,
                block.stream().partition(),
                block.stream().leaderEpoch(),
                block.offsets(),
                block.batches().size(),
                dataLength,
                blockChecksum
            );
            serialized.position(blockEnd);
            serialized.flip();
            indexEntries.add(new StreamObjectFormat.DataBlockIndexEntry(
                block.stream().partition(),
                block.stream().leaderEpoch(),
                block.offsets(),
                block.blockPosition(),
                block.serializedBytes(),
                block.batches().size(),
                blockChecksum
            ));
            return serialized;
        }

        private static void validateRecord(BatchPlan batch, WalRecord record) throws IOException {
            if (record.firstOffset() != batch.candidate().offsets().startOffset() ||
                Math.addExact(record.lastOffset(), 1) != batch.candidate().offsets().endOffset() ||
                record.leaderEpoch() != batch.leaderEpoch() ||
                record.payload().remaining() != batch.payloadLength()) {
                throw new IOException("WAL candidate changed before KSO2 serialization: " + batch.candidate());
            }
        }

        private ByteBuffer indexChunk() throws IOException {
            if (indexEntries.size() != plan.blocks.size()) {
                throw new IOException("KSO2 index serialization started before all DataBlocks were produced");
            }
            ByteBuffer index = ByteBuffer.allocate(plan.indexLength).order(ByteOrder.BIG_ENDIAN);
            StreamObjectFormat.writeIndexBlock(index, indexEntries);
            if (index.hasRemaining()) {
                throw new IOException("KSO2 index size accounting left unwritten bytes: " + index.remaining());
            }
            index.flip();
            updateBodyAndObjectChecksums(index);
            stage = Stage.FOOTER;
            return index;
        }

        private ByteBuffer footerChunk() {
            ByteBuffer indexBytes = ByteBuffer.allocate(plan.indexLength).order(ByteOrder.BIG_ENDIAN);
            StreamObjectFormat.writeIndexBlock(indexBytes, indexEntries);
            indexBytes.flip();
            long indexChecksum = crc32c(indexBytes);

            ByteBuffer footer = ByteBuffer.allocate(StreamObjectFormat.FOOTER_BYTES).order(ByteOrder.BIG_ENDIAN);
            StreamObjectFormat.writeFooter(
                footer,
                plan.indexPosition,
                plan.indexLength,
                plan.blocks.size(),
                indexChecksum,
                bodyChecksum.getValue()
            );
            footer.flip();
            objectChecksum.update(footer.duplicate());
            stage = Stage.DONE;
            return footer;
        }

        private void updateBodyAndObjectChecksums(ByteBuffer bytes) {
            bodyChecksum.update(bytes.duplicate());
            objectChecksum.update(bytes.duplicate());
        }

        private static void copy(ByteBuffer source, ByteBuffer target) {
            int bytes = Math.min(source.remaining(), target.remaining());
            ByteBuffer slice = source.duplicate();
            slice.limit(slice.position() + bytes);
            target.put(slice);
            source.position(source.position() + bytes);
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }

    private record BatchPlan(
        SharedStorageEngine.UploadCandidate candidate,
        int leaderEpoch,
        int payloadLength
    ) {
        private BatchPlan {
            Objects.requireNonNull(candidate, "candidate");
            if (payloadLength < 0) {
                throw new IllegalArgumentException("payloadLength must be non-negative");
            }
        }
    }

    private record StreamEpoch(SharedPartitionId partition, int leaderEpoch) {
        private StreamEpoch {
            Objects.requireNonNull(partition, "partition");
        }
    }

    private static final class DataBlockPlan {
        private final StreamEpoch stream;
        private final List<BatchPlan> batches = new ArrayList<>();
        private long startOffset;
        private long endOffset;
        private int dataBytes;
        private long blockPosition = -1L;

        private DataBlockPlan(StreamEpoch stream, BatchPlan first, int entryBytes) {
            this.stream = stream;
            this.startOffset = first.candidate().offsets().startOffset();
            this.endOffset = first.candidate().offsets().endOffset();
            this.dataBytes = entryBytes;
            this.batches.add(first);
        }

        private static DataBlockPlan first(StreamEpoch stream, BatchPlan first, int entryBytes) {
            return new DataBlockPlan(stream, first, entryBytes);
        }

        private boolean canAppend(BatchPlan next, int entryBytes, int targetBytes) {
            return next.candidate().offsets().startOffset() == endOffset &&
                Math.addExact(dataBytes, entryBytes) <= targetBytes;
        }

        private void append(BatchPlan next, int entryBytes) {
            batches.add(next);
            endOffset = next.candidate().offsets().endOffset();
            dataBytes = Math.addExact(dataBytes, entryBytes);
        }

        private DataBlockPlan at(long position) {
            if (blockPosition >= 0) {
                throw new IllegalStateException("DataBlock position is already assigned");
            }
            blockPosition = position;
            return this;
        }

        private StreamEpoch stream() {
            return stream;
        }

        private List<BatchPlan> batches() {
            return batches;
        }

        private OffsetRange offsets() {
            return new OffsetRange(startOffset, endOffset);
        }

        private int serializedBytes() {
            return Math.addExact(StreamObjectFormat.DATA_BLOCK_HEADER_BYTES, dataBytes);
        }

        private long blockPosition() {
            if (blockPosition < 0) {
                throw new IllegalStateException("DataBlock position has not been assigned");
            }
            return blockPosition;
        }
    }

    private enum Stage {
        HEADER,
        DATA_BLOCKS,
        INDEX,
        FOOTER,
        DONE
    }
}
