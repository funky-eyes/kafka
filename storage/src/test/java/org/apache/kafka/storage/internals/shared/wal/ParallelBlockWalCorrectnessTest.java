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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
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

    @Test
    void encodedBlockMatchesPreOptimizationFormat() throws Exception {
        var groups = List.of(
            new AlignedWalBlockCodec.Group(10, List.of(
                WalRecord.data(1, 2, 0, 1, 0, 0, new byte[] {1, 2, 3}), WalRecord.truncate(1, 2, 0, 1, 1))),
            new AlignedWalBlockCodec.Group(11, List.of(
                WalRecord.data(3, 4, 1, 2, 5, 6, new byte[] {4, 5, 6, 7}))));
        var encoded = AlignedWalBlockCodec.encodeWithLocations(4096, 7, 8, ALIGNMENT, groups);
        var sha256 = MessageDigest.getInstance("SHA-256");
        sha256.update(encoded.bytes());
        // Generated from the encoder in commit effd9f6, before this optimization.
        assertEquals("7a50bcc0d530d85efb7b9a3a76cf17b795c9b7308a0e3599291b5d4a35676e67",
            HexFormat.of().formatHex(sha256.digest()));
    }

    @Test
    void encodedLocationsMatchIndependentBlockDecoding() throws Exception {
        for (int sample = 1; sample <= 64; sample++) {
            List<AlignedWalBlockCodec.Group> groups = new ArrayList<>();
            for (int group = 0; group < 1 + sample % 5; group++) {
                List<WalRecord> records = new ArrayList<>();
                for (int record = 0; record < 1 + (sample + group) % 4; record++) {
                    records.add(record(group * 10L + record, 1 + (sample * 73 + group * 17 + record) % 1300));
                }
                if (group == 1) records.add(WalRecord.truncate(1, 2, 0, 1, 200));
                groups.add(new AlignedWalBlockCodec.Group(group, records));
            }
            long offset = (long) sample * ALIGNMENT;
            var encoded = AlignedWalBlockCodec.encodeWithLocations(offset, 7, 8, ALIGNMENT, groups);
            var decoded = AlignedWalBlockCodec.decode(encoded.bytes(), offset, 7, 8, ALIGNMENT);
            assertEquals(groups.size(), encoded.groups().size());
            assertEquals(groups.size(), decoded.groups().size());
            for (int group = 0; group < groups.size(); group++) {
                assertEquals(groups.get(group).records().size(), encoded.groups().get(group).size());
                for (int record = 0; record < groups.get(group).records().size(); record++) {
                    var location = encoded.groups().get(group).get(record);
                    var entry = decoded.groups().get(group).records().get(record);
                    assertEquals(entry.walOffset(), location.offset());
                    assertEquals(entry.length(), location.length());
                    assertEquals(groups.get(group).records().get(record).firstOffset(), entry.record().firstOffset());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void appendLocationsReadCorrectRecordsAfterReopen(boolean aio) throws Exception {
        Path path = dir.resolve("locations-" + aio);
        List<List<WalRecord>> groups = List.of(
            List.of(record(0, 1), record(1, 3000)),
            List.of(record(2, 2000)),
            List.of(record(3, 200), record(4, 1000)));
        List<List<WalAppendResult>> locations = new ArrayList<>();
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, 2 * ALIGNMENT, 10_000_000L)) {
            var futures = groups.stream().map(wal::appendBatch).toList();
            for (var future : futures) locations.add(future.get(10, TimeUnit.SECONDS));
            verifyLocations(wal, groups, locations);
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, 2 * ALIGNMENT, 0)) {
            verifyLocations(wal, groups, locations);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void appendLocationsRemainValidAcrossPhysicalReuse(boolean aio) throws Exception {
        Path path = dir.resolve("locations-reuse-" + aio);
        List<List<WalRecord>> groups = new ArrayList<>();
        List<List<WalAppendResult>> locations = new ArrayList<>();
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            for (int i = 0; i < 4; i++) {
                var group = List.of(record(i, i + 1));
                groups.add(group);
                locations.add(wal.appendBatch(group).get(10, TimeUnit.SECONDS));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            long reclaimed = 0;
            while (reclaimed == 0 && System.nanoTime() < deadline) {
                reclaimed = wal.reclaim((record, location) -> record.firstOffset() == 0, ALIGNMENT);
                if (reclaimed == 0) Thread.yield();
            }
            assertEquals(ALIGNMENT, reclaimed);
            groups.remove(0);
            locations.remove(0);
            var group = List.of(record(4, 5));
            groups.add(group);
            locations.add(wal.appendBatch(group).get(10, TimeUnit.SECONDS));
            verifyLocations(wal, groups, locations);
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0)) {
            verifyLocations(wal, groups, locations);
        }
    }

    private static void verifyLocations(CoordinatedRingWalAdapter wal, List<List<WalRecord>> groups,
                                        List<List<WalAppendResult>> locations) throws IOException {
        assertEquals(groups.size(), locations.size());
        for (int group = 0; group < groups.size(); group++) {
            assertEquals(groups.get(group).size(), locations.get(group).size());
            for (int i = 0; i < groups.get(group).size(); i++) {
                WalRecord expected = groups.get(group).get(i);
                WalAppendResult result = locations.get(group).get(i);
                WalRecord actual = wal.read(new WalLocation(result.offset(), result.length(),
                    expected.leaderEpoch(), expected.firstOffset(), expected.lastOffset()));
                assertEquals(expected.firstOffset(), actual.firstOffset());
                assertEquals(expected.payload(), actual.payload());
            }
        }
    }

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void corruptedCommittedPayloadFailsRecovery(boolean aio) throws Exception {
        Path path = dir.resolve("payload-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
            wal.append(record(1, 1)).get(10, TimeUnit.SECONDS);
        }
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
            long secondPayload = 4L * ALIGNMENT + AlignedWalBlockCodec.HEADER_BYTES + WalRecordCodec.HEADER_BYTES;
            file.write(ByteBuffer.wrap(new byte[] {42}), secondPayload);
            file.force(true);
        }
        try (WalIoBackend backend = backend(aio)) {
            assertThrows(IOException.class,
                () -> CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0));
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
