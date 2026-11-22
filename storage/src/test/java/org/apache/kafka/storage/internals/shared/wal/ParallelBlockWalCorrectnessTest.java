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

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParallelBlockWalCorrectnessTest {
    private static final int ALIGNMENT = 4096;
    private static final long CAPACITY = 4L * ALIGNMENT;
    @TempDir
    Path dir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void laterForcedBlockCannotPassAnEarlierFailure(boolean aio) throws Exception {
        Path path = dir.resolve("failure-" + aio);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(3L * ALIGNMENT + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            var allocator = new BlockRingAllocator(CAPACITY, ALIGNMENT);
            var gate = new ForceGate(io);
            var writer = new ParallelRingBlockWriter(gate, allocator, 3L * ALIGNMENT, CAPACITY,
                ALIGNMENT, 7, 8, 2, 8);
            try (var coordinator = new RingBlockCommitCoordinator(allocator, checkpoint, writer, io,
                3L * ALIGNMENT, new BlockRingRecovery.Scan(List.of(), -1), ALIGNMENT, ALIGNMENT, 7, 8)) {
                var first = coordinator.appendGroups(List.of(List.of(record(0, 1))));
                assertTrue(gate.earlierEntered.await(10, TimeUnit.SECONDS));
                var second = coordinator.appendGroups(List.of(List.of(record(1, 1))));
                assertTrue(gate.laterForced.await(10, TimeUnit.SECONDS));
                assertFalse(first.isDone());
                assertFalse(second.isDone());
                gate.release.countDown();
                assertThrows(ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS));
                assertThrows(ExecutionException.class, () -> second.get(10, TimeUnit.SECONDS));
            } finally {
                gate.release.countDown();
            }
            assertTrue(BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8).scan().blocks().isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fullyReclaimedRingCanDurablyReuseItsEntireCapacity(boolean aio) throws Exception {
        Path path = dir.resolve("rebase-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, 4 * ALIGNMENT, 0)) {
            wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            long reclaimed = 0;
            while (reclaimed == 0 && System.nanoTime() < deadline) {
                reclaimed = wal.reclaim((record, location) -> true, ALIGNMENT);
                if (reclaimed == 0) Thread.yield();
            }
            assertEquals(ALIGNMENT, reclaimed);
            assertEquals(CAPACITY, wal.reclaimedBeforeOffset());
            wal.append(record(1, 3 * ALIGNMENT)).get(10, TimeUnit.SECONDS);
            assertEquals(CAPACITY, wal.usedBytes());
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, 4 * ALIGNMENT, 0)) {
            List<Long> replay = new ArrayList<>();
            wal.replay((record, location) -> replay.add(record.firstOffset()));
            assertEquals(List.of(1L), replay);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointCorruptionAfterPhysicalReusePreservesAcknowledgedRecords(boolean aio) throws Exception {
        Path path = dir.resolve("checkpoint-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            for (int i = 0; i < 4; i++) wal.append(record(i, 1)).get(10, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            long reclaimed = 0;
            while (reclaimed == 0 && System.nanoTime() < deadline) {
                reclaimed = wal.reclaim((record, location) -> record.firstOffset() == 0, ALIGNMENT);
                if (reclaimed == 0) Thread.yield();
            }
            assertEquals(ALIGNMENT, reclaimed);
            wal.append(record(4, 1)).get(10, TimeUnit.SECONDS);
        }
        for (int slot = 0; slot < 4; slot++) {
            Path copy = dir.resolve("checkpoint-" + aio + "-" + slot);
            Files.copy(path, copy);
            try (FileChannel file = FileChannel.open(copy, StandardOpenOption.WRITE)) {
                file.write(ByteBuffer.wrap(new byte[] {0}), (long) (slot % 3) * ALIGNMENT);
                if (slot == 3) file.write(ByteBuffer.wrap(new byte[] {0}), ALIGNMENT);
                file.force(true);
            }
            try (WalIoBackend backend = backend(aio)) {
                if (slot == 3) {
                    assertThrows(IOException.class,
                        () -> CoordinatedRingWalAdapter.reopen(copy, backend, CAPACITY, ALIGNMENT, 0));
                } else {
                    try (var wal = CoordinatedRingWalAdapter.reopen(copy, backend, CAPACITY, ALIGNMENT, 0)) {
                        List<Long> replay = new ArrayList<>();
                        wal.replay((record, location) -> replay.add(record.firstOffset()));
                        assertEquals(List.of(1L, 2L, 3L, 4L), replay);
                    }
                }
            }
        }
    }

    private static WalRecord record(long offset, int bytes) {
        return WalRecord.data(1, 2, 0, 1, offset, offset, new byte[bytes]);
    }

    private static WalIoBackend backend(boolean aio) {
        return aio ? new AsynchronousFileChannelWalIoBackend() : new FileChannelWalIoBackend();
    }

    private static final class ForceGate implements WalIoBackend.Handle {
        private final WalIoBackend.Handle delegate;
        private final ThreadLocal<Boolean> earlier = ThreadLocal.withInitial(() -> false);
        private final CountDownLatch earlierEntered = new CountDownLatch(1);
        private final CountDownLatch laterForced = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private ForceGate(WalIoBackend.Handle delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() throws IOException {
            return delegate.size();
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            return delegate.read(dst, position);
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            earlier.set(position == 3L * ALIGNMENT);
            return delegate.write(src, position);
        }

        @Override
        public void truncate(long size) throws IOException {
            delegate.truncate(size);
        }

        @Override
        public void force() throws IOException {
            if (earlier.get()) {
                earlierEntered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IOException("force gate timed out");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException(e);
                }
                throw new IOException("injected earlier block force failure");
            }
            delegate.force();
            laterForced.countDown();
        }

        @Override
        public void seal() throws IOException {
            delegate.seal();
        }

        @Override
        public void close() { }
    }
}
