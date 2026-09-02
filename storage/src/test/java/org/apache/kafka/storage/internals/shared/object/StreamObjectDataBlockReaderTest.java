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
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamObjectDataBlockReaderTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(31, 41, 2);

    @TempDir
    Path tempDir;

    @Test
    void shouldResolveExactLogicalBatchWithoutUsingMetadataPhysicalPosition() throws Exception {
        PackedObject packed = packObject(900);
        ByteBuffer object = packed.bytes();
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(object);
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(object, footer).get(0);
        ByteBuffer blockBytes = slice(object, block.blockPosition(), block.blockLength());

        StreamObjectDataBlockReader.DataBlockSnapshot snapshot =
            StreamObjectDataBlockReader.read(blockBytes, block);
        assertEquals(2, snapshot.batches().size());

        SharedObjectRange original = packed.metadata().ranges().get(1);
        SharedObjectRange wrongPhysicalPosition = new SharedObjectRange(
            original.partition(),
            original.offsets(),
            original.leaderEpoch(),
            0,
            original.objectLength(),
            original.checksum()
        );
        ByteBuffer payload = snapshot.batch(wrongPhysicalPosition);
        assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(payload));
    }

    @Test
    void shouldRejectDataBlockPayloadCorruption() throws Exception {
        PackedObject packed = packObject(901);
        ByteBuffer object = packed.bytes();
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(object);
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(object, footer).get(0);
        ByteBuffer blockBytes = writableCopy(slice(object, block.blockPosition(), block.blockLength()));
        int firstPayload = StreamObjectFormat.DATA_BLOCK_HEADER_BYTES + StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES;
        blockBytes.put(firstPayload, (byte) (blockBytes.get(firstPayload) ^ 0x5a));

        IOException error = assertThrows(
            IOException.class,
            () -> StreamObjectDataBlockReader.read(blockBytes, block)
        );
        assertTrue(error.getMessage().contains("checksum"));
    }

    @Test
    void shouldRejectMetadataLogicalRangeMissingFromIndexedBlock() throws Exception {
        PackedObject packed = packObject(902);
        ByteBuffer object = packed.bytes();
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(object);
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(object, footer).get(0);
        StreamObjectDataBlockReader.DataBlockSnapshot snapshot = StreamObjectDataBlockReader.read(
            slice(object, block.blockPosition(), block.blockLength()),
            block
        );

        SharedObjectRange first = packed.metadata().ranges().get(0);
        SharedObjectRange missing = new SharedObjectRange(
            first.partition(),
            new OffsetRange(1, 4),
            first.leaderEpoch(),
            first.objectPosition(),
            first.objectLength(),
            first.checksum()
        );
        IOException error = assertThrows(IOException.class, () -> snapshot.batch(missing));
        assertTrue(error.getMessage().contains("missing") || error.getMessage().contains("outside"));
    }

    private PackedObject packObject(long objectId) throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("data-block-reader-" + objectId), 1024 * 1024, 4096))) {
            append(engine, 0, 4, new byte[]{1, 2, 3, 4, 5});
            append(engine, 5, 9, new byte[]{6, 7, 8, 9});
            return new SharedObjectPacker().pack(
                objectId,
                engine.uploadCandidates(PARTITION, 0, 10),
                engine
            );
        }
    }

    private static void append(SharedStorageEngine engine, long firstOffset, long lastOffset, byte[] payload)
        throws Exception {
        engine.appendData(PARTITION, 6, firstOffset, lastOffset, ByteBuffer.wrap(payload))
            .get(10, TimeUnit.SECONDS);
    }

    private static ByteBuffer slice(ByteBuffer source, long position, int length) {
        ByteBuffer slice = source.duplicate();
        slice.position(Math.toIntExact(position));
        slice.limit(Math.toIntExact(position + length));
        return slice.slice().asReadOnlyBuffer();
    }

    private static ByteBuffer writableCopy(ByteBuffer source) {
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source.duplicate());
        copy.flip();
        return copy;
    }

    private static byte[] toArray(ByteBuffer source) {
        ByteBuffer data = source.duplicate();
        byte[] result = new byte[data.remaining()];
        data.get(result);
        return result;
    }
}
