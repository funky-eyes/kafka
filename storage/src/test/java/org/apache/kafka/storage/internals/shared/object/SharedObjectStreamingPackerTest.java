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
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalCorruptionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedObjectStreamingPackerTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(11, 12, 0);

    @TempDir
    Path tempDir;

    @Test
    void shouldPlanWithoutMetadataAndSerializeExactKso2AcrossBoundedParts() throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("streaming-packer"), 1024 * 1024, 4096))) {
            for (int i = 0; i < 8; i++) {
                byte[] payload = new byte[40];
                payload[0] = (byte) i;
                long firstOffset = i * 10L;
                engine.appendData(P0, 3, firstOffset, firstOffset + 9, ByteBuffer.wrap(payload))
                    .get(10, TimeUnit.SECONDS);
            }

            SharedObjectPacker packer = new SharedObjectPacker(80, 128);
            SharedObjectPacker.UploadPlan plan = packer.plan(
                8_001L,
                engine.uploadCandidates(P0, 0, 80),
                engine
            );

            assertThrows(IllegalStateException.class, plan::metadata);
            List<ByteBuffer> parts = new ArrayList<>();
            long emitted = 0L;
            try (ObjectStore.PartSource source = plan.openPartSource()) {
                ByteBuffer part;
                while ((part = source.nextPart()) != null) {
                    parts.add(part);
                    emitted += part.remaining();
                }
            }

            assertEquals(plan.objectSize(), emitted);
            assertTrue(parts.size() > 1);
            for (int i = 0; i < parts.size() - 1; i++) {
                assertEquals(128, parts.get(i).remaining());
            }
            assertThrows(IllegalStateException.class, plan::openPartSource);

            PackedObject packed = new PackedObject(parts, plan.metadata());
            ByteBuffer object = packed.bytes();
            assertEquals(StreamObjectFormat.OBJECT_MAGIC, object.getInt(0));
            StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(object);
            assertEquals(8, StreamObjectFormat.readIndex(object, footer).size());
            assertEquals(8, packed.metadata().ranges().size());
            assertEquals(packed.metadata().objectChecksum(), crc32c(object));
        }
    }

    @Test
    void streamingUploadReusesConfirmedFetchCacheAndRejectsTruncatedPlan() throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("streaming-cache"), 1024 * 1024, 4096))) {
            engine.appendData(P0, 3, 0, 9, ByteBuffer.wrap(new byte[]{1, 2, 3}))
                .get(10, TimeUnit.SECONDS);
            List<SharedStorageEngine.UploadCandidate> candidates = engine.uploadCandidates(P0, 0, 10);
            assertEquals(1, engine.readLocal(P0, 0).orElseThrow().payload().get());
            long hitsBeforeUpload = engine.localCacheHitCount();

            SharedObjectPacker packer = new SharedObjectPacker(80, 128);
            PackedObject packed = packer.pack(8_002L, candidates, engine);
            assertEquals(1, packed.metadata().ranges().size());
            assertEquals(hitsBeforeUpload + 1, engine.localCacheHitCount());

            SharedObjectPacker.UploadPlan stale = packer.plan(8_003L, candidates, engine);
            engine.truncate(P0, 4, 0).get(10, TimeUnit.SECONDS);
            try (ObjectStore.PartSource source = stale.openPartSource()) {
                assertThrows(WalCorruptionException.class, () -> {
                    while (source.nextPart() != null) {
                        // The first part may contain only the object header.
                    }
                });
            }
        }
    }

    @Test
    void streamingUploadDoesNotPopulateCacheOnMiss() throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("streaming-cache-miss"), 1024 * 1024, 4096))) {
            engine.appendData(P0, 3, 0, 9, ByteBuffer.wrap(new byte[]{1, 2, 3}))
                .get(10, TimeUnit.SECONDS);
            List<SharedStorageEngine.UploadCandidate> candidates = engine.uploadCandidates(P0, 0, 10);
            assertEquals(0, engine.localCacheBytes());
            SharedObjectPacker packer = new SharedObjectPacker(80, 128);

            assertEquals(1, packer.pack(8_004L, candidates, engine).metadata().ranges().size());
            assertEquals(1, engine.localCacheMissCount());
            assertEquals(0, engine.localCacheBytes());

            assertEquals(1, engine.readLocal(P0, 0).orElseThrow().payload().get());
            assertTrue(engine.localCacheBytes() > 0);
            assertEquals(1, packer.pack(8_005L, candidates, engine).metadata().ranges().size());
            assertEquals(1, engine.localCacheHitCount());
        }
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }
}
