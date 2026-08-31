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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamObjectFormatTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(1, 2, 0);
    private static final SharedPartitionId P1 = new SharedPartitionId(3, 4, 1);

    @TempDir
    Path tempDir;

    @Test
    void shouldPackMultipleBatchesPerStreamBlockAndIndexMultipleStreams() throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("stream-object-v2"), 1024 * 1024, 4096))) {
            append(engine, P0, 3, 10, 19, new byte[]{1, 2, 3});
            append(engine, P1, 7, 30, 39, new byte[]{4, 5});
            append(engine, P0, 3, 20, 29, new byte[]{6, 7, 8, 9});

            List<SharedStorageEngine.UploadCandidate> p0 = engine.uploadCandidates(P0, 10, 30);
            List<SharedStorageEngine.UploadCandidate> p1 = engine.uploadCandidates(P1, 30, 40);
            assertEquals(2, p0.size());
            assertEquals(1, p1.size());

            List<SharedStorageEngine.UploadCandidate> interleaved = List.of(p0.get(0), p1.get(0), p0.get(1));
            PackedObject packed = new SharedObjectPacker().pack(700, interleaved, engine);
            ByteBuffer bytes = packed.bytes();

            assertEquals(StreamObjectFormat.OBJECT_MAGIC, bytes.getInt(0));
            StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(bytes);
            List<StreamObjectFormat.DataBlockIndexEntry> index = StreamObjectFormat.readIndex(bytes, footer);
            assertEquals(2, footer.dataBlockCount());
            assertEquals(2, index.size());

            StreamObjectFormat.DataBlockIndexEntry p0Block = index.get(0);
            assertEquals(P0, p0Block.partition());
            assertEquals(3, p0Block.leaderEpoch());
            assertEquals(new OffsetRange(10, 30), p0Block.offsets());
            assertEquals(2, p0Block.batchCount());

            StreamObjectFormat.DataBlockIndexEntry p1Block = index.get(1);
            assertEquals(P1, p1Block.partition());
            assertEquals(7, p1Block.leaderEpoch());
            assertEquals(new OffsetRange(30, 40), p1Block.offsets());
            assertEquals(1, p1Block.batchCount());

            assertEquals(3, packed.metadata().ranges().size());
            assertRangePayload(packed, P0, new OffsetRange(10, 20), new byte[]{1, 2, 3});
            assertRangePayload(packed, P0, new OffsetRange(20, 30), new byte[]{6, 7, 8, 9});
            assertRangePayload(packed, P1, new OffsetRange(30, 40), new byte[]{4, 5});
            assertTrue(footer.indexPosition() > p1Block.blockPosition());
        }
    }

    @Test
    void shouldSplitLargeLogicalStreamIntoMultipleIndexedBlocks() throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("stream-object-v2-split"), 1024 * 1024, 4096))) {
            append(engine, P0, 3, 0, 9, new byte[32]);
            append(engine, P0, 3, 10, 19, new byte[32]);

            PackedObject packed = new SharedObjectPacker(56).pack(
                701,
                engine.uploadCandidates(P0, 0, 20),
                engine
            );
            StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
            List<StreamObjectFormat.DataBlockIndexEntry> index = StreamObjectFormat.readIndex(packed.bytes(), footer);

            assertEquals(2, index.size());
            assertEquals(new OffsetRange(0, 10), index.get(0).offsets());
            assertEquals(new OffsetRange(10, 20), index.get(1).offsets());
            assertEquals(1, index.get(0).batchCount());
            assertEquals(1, index.get(1).batchCount());
        }
    }

    private static void append(
        SharedStorageEngine engine,
        SharedPartitionId partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        byte[] payload
    ) throws Exception {
        engine.appendData(partition, leaderEpoch, firstOffset, lastOffset, ByteBuffer.wrap(payload))
            .get(10, TimeUnit.SECONDS);
    }

    private static void assertRangePayload(
        PackedObject packed,
        SharedPartitionId partition,
        OffsetRange offsets,
        byte[] expected
    ) {
        SharedObjectRange range = packed.metadata().ranges().stream()
            .filter(candidate -> candidate.partition().equals(partition) && candidate.offsets().equals(offsets))
            .findFirst()
            .orElseThrow();
        ByteBuffer object = packed.bytes().duplicate();
        object.position(Math.toIntExact(range.objectPosition()));
        object.limit(Math.toIntExact(range.objectPosition() + range.objectLength()));
        ByteBuffer payload = object.slice();
        List<Byte> actual = new ArrayList<>();
        while (payload.hasRemaining()) {
            actual.add(payload.get());
        }
        byte[] bytes = new byte[actual.size()];
        for (int i = 0; i < actual.size(); i++) {
            bytes[i] = actual.get(i);
        }
        assertArrayEquals(expected, bytes);
    }
}
