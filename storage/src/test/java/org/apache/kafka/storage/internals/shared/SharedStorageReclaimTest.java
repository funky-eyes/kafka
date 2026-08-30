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
package org.apache.kafka.storage.internals.shared;

import org.apache.kafka.storage.internals.shared.metadata.LocalRemoteObjectCheckpoint;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.ObjectStore;
import org.apache.kafka.storage.internals.shared.object.SharedObjectReader;
import org.apache.kafka.storage.internals.shared.wal.RotatingFileSharedWal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageReclaimTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11, 22, 0);
    private static final long OBJECT_ID = 101;
    private static final long WAL_CAPACITY_BYTES = 255;
    private static final long WAL_SEGMENT_BYTES = 255;

    @TempDir
    Path tempDir;

    @Test
    void shouldColdReadAfterPhysicalWalReclaimAndEngineRestart() throws Exception {
        Path walDir = tempDir.resolve("wal");
        byte[] payload = new byte[96];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i * 13);
        }
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        objectStore.put(OBJECT_ID, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);

        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(walDir);
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new RotatingFileSharedWal(walDir, WAL_CAPACITY_BYTES, WAL_SEGMENT_BYTES),
            checkpoint
        )) {
            engine.installRemoteReader(new SharedObjectReader(objectStore, engine.remoteIndex()));
            engine.appendData(PARTITION, 3, 0, 0, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);
            assertTrue(engine.readLocal(PARTITION, 0).isPresent());

            engine.commitRemoteObject(metadata(payload));
            assertEquals(1, engine.pendingRemoteCheckpointCount());
            long reclaimed = engine.reclaimCheckpointedWal();

            assertTrue(reclaimed > 0,
                "checkpointed remote coverage must release the complete local WAL prefix under capacity pressure");
            assertEquals(0, engine.pendingRemoteCheckpointCount());
            assertFalse(engine.readLocal(PARTITION, 0).isPresent(), "reclaimed payload must no longer be local");
            assertArrayEquals(payload, bytes(engine.readBatchBytes(PARTITION, 0).orElseThrow()));
        }

        LocalRemoteObjectCheckpoint reopenedCheckpoint = new LocalRemoteObjectCheckpoint(walDir);
        try (SharedStorageEngine restarted = new SharedStorageEngine(
            new RotatingFileSharedWal(walDir, WAL_CAPACITY_BYTES, WAL_SEGMENT_BYTES),
            reopenedCheckpoint
        )) {
            restarted.installRemoteReader(new SharedObjectReader(objectStore, restarted.remoteIndex()));
            assertFalse(restarted.readLocal(PARTITION, 0).isPresent());
            assertTrue(restarted.remoteIndex().coverage(PARTITION).covers(new OffsetRange(0, 1)));
            assertArrayEquals(payload, bytes(restarted.readBatchBytes(PARTITION, 0).orElseThrow()));
        }
    }

    private static SharedObjectMetadata metadata(byte[] payload) {
        long checksum = checksum(payload);
        return new SharedObjectMetadata(
            OBJECT_ID,
            payload.length,
            checksum,
            List.of(new SharedObjectRange(
                PARTITION,
                new OffsetRange(0, 1),
                3,
                0,
                payload.length,
                checksum
            ))
        );
    }

    private static long checksum(byte[] payload) {
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        return crc.getValue();
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static final class InMemoryObjectStore implements ObjectStore {
        private final Map<Long, byte[]> objects = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            objects.put(objectId, bytes(data));
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
            byte[] data = objects.get(objectId);
            if (data == null) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("missing object " + objectId));
            }
            int start = Math.toIntExact(position);
            int end = Math.addExact(start, length);
            if (start < 0 || end > data.length) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("invalid object range"));
            }
            return CompletableFuture.completedFuture(
                ByteBuffer.wrap(Arrays.copyOfRange(data, start, end)).asReadOnlyBuffer()
            );
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            objects.remove(objectId);
            return CompletableFuture.completedFuture(null);
        }
    }
}
