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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsynchronousFileChannelWalIoBackendTest {
    @TempDir
    Path tempDir;

    @Test
    void aioPositionalReadWriteAndDurableReopenMatchNioFormat() throws Exception {
        Path path = tempDir.resolve("aio-physical.wal");
        byte[] first = "java-aio-".getBytes(StandardCharsets.UTF_8);
        byte[] second = "wal".getBytes(StandardCharsets.UTF_8);
        byte[] third = "-batch".getBytes(StandardCharsets.UTF_8);
        byte[] payload = ByteBuffer.allocate(first.length + second.length + third.length)
            .put(first).put(second).put(third).array();
        try (WalIoBackend backend = new AsynchronousFileChannelWalIoBackend();
             WalIoBackend.Handle handle = backend.create(path)) {
            assertFalse(backend.supportsDirectIo());
            assertTrue(backend.supportsPreallocation());
            assertThrows(IllegalArgumentException.class, () -> handle.writeBatch(List.of(
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(first), 0L),
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(second), 1L))));
            handle.writeBatch(List.of(
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(first), 0L),
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(second), first.length),
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(third), first.length + second.length)));
            handle.force();
        }
        assertBatchLimits();
        try (WalIoBackend backend = new FileChannelWalIoBackend();
             WalIoBackend.Handle reader = backend.openRead(path)) {
            ByteBuffer result = ByteBuffer.allocate(payload.length);
            assertEquals(payload.length, reader.read(result, 0L));
            result.flip();
            byte[] recovered = new byte[result.remaining()];
            result.get(recovered);
            assertArrayEquals(payload, recovered);
        }
    }

    private void assertBatchLimits() throws Exception {
        Path path = tempDir.resolve("aio-batch-limits.wal");
        try (WalIoBackend backend = new AsynchronousFileChannelWalIoBackend();
             WalIoBackend.Handle handle = backend.create(path)) {
            assertThrows(IllegalArgumentException.class, () ->
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(new byte[2]), Long.MAX_VALUE));
            List<WalIoBackend.WriteRequest> tooMany = new ArrayList<>();
            for (int i = 0; i <= WalIoBackend.MAX_WRITE_BATCH_REQUESTS; i++) {
                tooMany.add(new WalIoBackend.WriteRequest(ByteBuffer.wrap(new byte[1]), i));
            }
            assertThrows(IllegalArgumentException.class, () -> handle.writeBatch(tooMany));
            assertThrows(IllegalArgumentException.class, () -> handle.writeBatch(List.of(
                new WalIoBackend.WriteRequest(
                    ByteBuffer.allocate((int) WalIoBackend.MAX_WRITE_BATCH_BYTES), 0L),
                new WalIoBackend.WriteRequest(ByteBuffer.wrap(new byte[1]),
                    WalIoBackend.MAX_WRITE_BATCH_BYTES))));
            assertEquals(0L, handle.size());
        }
    }

    @Test
    void aioPreallocationAndTruncatePreservePhysicalCapacity() throws Exception {
        Path path = tempDir.resolve("aio-preallocation.wal");
        long capacity = 64L * 1024L + 19L;
        try (WalIoBackend backend = new AsynchronousFileChannelWalIoBackend();
             WalIoBackend.Handle handle = backend.create(path)) {
            handle.preallocate(capacity);
            handle.force();
            assertEquals(capacity, handle.size());
            assertThrows(IOException.class, () -> handle.preallocate(capacity));
            handle.truncate(1024L);
            handle.force();
            assertEquals(1024L, handle.size());
        }
        assertEquals(1024L, Files.size(path));
    }

    @Test
    void aioRingAcknowledgedGroupsColdReplayOnNioBackend() throws Exception {
        Path path = tempDir.resolve("aio-ring.wal");
        WalRecord confirmed = record(0L, "ring-aio");
        long totalCapacity = RingWalLayout.DATA_START + 64L * 1024L;
        try (RingSharedWal wal = new RingSharedWal(
            path, totalCapacity, new AsynchronousFileChannelWalIoBackend())) {
            assertEquals(1, wal.appendBatch(List.of(confirmed)).join().size());
        }
        try (RingSharedWal restored = new RingSharedWal(
            path, totalCapacity, new FileChannelWalIoBackend())) {
            List<WalRecord> records = new ArrayList<>();
            restored.replay((record, ignored) -> records.add(record));
            assertEquals(1, records.size());
            assertArrayEquals(bytes(confirmed.payload()), bytes(records.get(0).payload()));
        }
    }

    @Test
    void aioRotatingWalAcknowledgedGroupsColdReplayOnNioBackend() throws Exception {
        Path dir = tempDir.resolve("aio-rotating");
        WalRecord confirmed = record(0L, "rotating-aio");
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(
            dir, 64L * 1024L, 16L * 1024L, new AsynchronousFileChannelWalIoBackend())) {
            assertEquals(1, wal.appendBatch(List.of(confirmed)).join().size());
        }
        try (RotatingFileSharedWal restored = new RotatingFileSharedWal(
            dir, 64L * 1024L, 16L * 1024L, new FileChannelWalIoBackend())) {
            List<WalRecord> records = new ArrayList<>();
            restored.replay((record, ignored) -> records.add(record));
            assertEquals(1, records.size());
            assertArrayEquals(bytes(confirmed.payload()), bytes(records.get(0).payload()));
        }
    }

    @Test
    void aioRejectsOperationsAfterClosingHandle() throws Exception {
        Path path = tempDir.resolve("aio-closed.wal");
        try (WalIoBackend backend = new AsynchronousFileChannelWalIoBackend()) {
            WalIoBackend.Handle handle = backend.create(path);
            handle.close();
            assertThrows(IOException.class, () -> handle.write(
                ByteBuffer.wrap("closed".getBytes(StandardCharsets.UTF_8)), 0L));
            assertThrows(IOException.class, handle::force);
        }
    }

    private static WalRecord record(long offset, String value) {
        return WalRecord.data(1L, 2L, 0, 3, offset, offset,
            ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }
}
