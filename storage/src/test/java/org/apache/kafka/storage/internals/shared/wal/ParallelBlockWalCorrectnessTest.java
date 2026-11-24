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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

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
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParallelBlockWalCorrectnessTest {
    private static final int ALIGNMENT = 4096;
    private static final long CAPACITY = 4L * ALIGNMENT;
    @TempDir
    Path dir;

    @Test
    void openTailCannotBeOvertakenOrReusedAfterSealing() throws Exception {
        var allocator = new BlockRingAllocator(CAPACITY, ALIGNMENT);
        var open = allocator.reserveOpen(ALIGNMENT);
        assertThrows(IOException.class, () -> allocator.reserveOpen(ALIGNMENT));
        assertThrows(IOException.class, () -> allocator.reserve(ALIGNMENT));
        allocator.growOpen(open, 2 * ALIGNMENT);
        assertEquals(2L * ALIGNMENT, allocator.usedBytes());
        var sealed = allocator.sealOpen(open);
        assertThrows(IOException.class, () -> allocator.growOpen(open, 2 * ALIGNMENT));
        var next = allocator.reserve(ALIGNMENT);
        assertEquals(sealed.end(), next.reservedStart());
    }

    @Test
    void batchRetirementWaitsForEachReaderBeforeReusingItsBlock() throws Exception {
        var allocator = new BlockRingAllocator(CAPACITY, ALIGNMENT);
        var first = allocator.reserve(ALIGNMENT);
        var second = allocator.reserve(ALIGNMENT);
        allocator.dataForced(first);
        allocator.dataForced(second);
        allocator.tailCheckpointForced(second.end());
        var firstPin = allocator.pin(first);
        var secondPin = allocator.pin(second);
        var retirements = allocator.prepareRetirements(List.of(first, second));
        assertFalse(retirements.get(0).readersDrained().isDone());
        assertFalse(retirements.get(1).readersDrained().isDone());
        assertThrows(IOException.class, () -> allocator.headCheckpointsForced(retirements.get(0)));
        firstPin.close();
        retirements.get(0).readersDrained().get(10, TimeUnit.SECONDS);
        assertFalse(retirements.get(1).readersDrained().isDone());
        secondPin.close();
        retirements.get(1).readersDrained().get(10, TimeUnit.SECONDS);
        allocator.headCheckpointsForced(retirements.get(0));
        allocator.headCheckpointsForced(retirements.get(1));
        assertEquals(0, allocator.usedBytes());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bankedCheckpointUsesOneForceAndCanRecoverEitherBank(boolean aio) throws Exception {
        Path path = dir.resolve("banked-" + aio);
        long fileBytes = CAPACITY + CoordinatedRingWalAdapter.BANKED_METADATA_BYTES;
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.createWithBankedCheckpoints(path, backend, CAPACITY, ALIGNMENT, 0, 8,
                 8L * ALIGNMENT)) {
            backend.countForces.set(true);
            wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
            assertEquals(2, backend.forces.get(), "a data force followed by one tail force");
            wal.append(record(1, 1)).get(10, TimeUnit.SECONDS);
            wal.append(record(2, 1)).get(10, TimeUnit.SECONDS);
        }
        assertEquals(fileBytes, Files.size(path));
        // Generation four is in slots 0..2. A crash during its inactive-bank
        // writes can leave all three torn while the three prior copies survive.
        Path interrupted = dir.resolve("banked-interrupted-" + aio);
        Files.copy(path, interrupted);
        corruptSlots(interrupted, 0, 1, 2);
        assertReplayByFileSize(interrupted, aio, fileBytes, List.of(0L, 1L));

        // After a successful shared force, losing two independent copies must
        // still recover the newest generation, including the acknowledged record.
        corruptSlots(path, 0, 1);
        assertReplayByFileSize(path, aio, fileBytes, List.of(0L, 1L, 2L));
        corruptSlots(path, 1, 2);
        assertReplayByFileSize(path, aio, fileBytes, List.of(0L, 1L, 2L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointCopiesRemainIdenticalWithPartialWrites(boolean aio) throws Exception {
        for (var layout : List.of(BlockRingCheckpoint.Layout.LEGACY, BlockRingCheckpoint.Layout.BANKED)) {
            Path path = dir.resolve("partial-checkpoint-" + layout + "-" + aio);
            try (var backend = new CountingBackend(aio); var io = backend.create(path)) {
                io.preallocate(CAPACITY + layout.metadataBytes(ALIGNMENT));
                var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
                byte[] initial = Files.readAllBytes(path);
                backend.maxWriteBytes = 113;
                backend.countWrites.set(true);
                backend.countForces.set(true);

                checkpoint.advanceTail(ALIGNMENT, 0);

                byte[] written = Files.readAllBytes(path);
                int firstNew = layout == BlockRingCheckpoint.Layout.LEGACY ? 2 : 0;
                int secondNew = layout == BlockRingCheckpoint.Layout.LEGACY ? 0 : 1;
                int old = layout == BlockRingCheckpoint.Layout.LEGACY ? 1 : 3;
                byte[] expected = Arrays.copyOfRange(written, firstNew * ALIGNMENT, (firstNew + 1) * ALIGNMENT);
                assertArrayEquals(expected, Arrays.copyOfRange(written, secondNew * ALIGNMENT,
                    (secondNew + 1) * ALIGNMENT));
                if (layout == BlockRingCheckpoint.Layout.BANKED) {
                    assertArrayEquals(expected, Arrays.copyOfRange(written, 2 * ALIGNMENT, 3 * ALIGNMENT));
                }
                assertArrayEquals(Arrays.copyOfRange(initial, old * ALIGNMENT, (old + 1) * ALIGNMENT),
                    Arrays.copyOfRange(written, old * ALIGNMENT, (old + 1) * ALIGNMENT));
                assertFalse(Arrays.equals(expected, Arrays.copyOfRange(initial, firstNew * ALIGNMENT,
                    (firstNew + 1) * ALIGNMENT)));
                assertTrue(backend.writes.get() > 3, "each copy must handle partial writes");
                assertEquals(layout == BlockRingCheckpoint.Layout.LEGACY ? 2 : 1, backend.forces.get());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowCheckpointRequiresPersistedBoundAndFailsClosedUntilReconciled(boolean aio) throws Exception {
        Path path = dir.resolve("window-checkpoint-" + aio);
        long fileBytes = CAPACITY + BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            assertEquals(BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT), checkpoint.metadataBytes());
            assertEquals(0, checkpoint.current().scanCeiling());
            assertThrows(IOException.class, () -> checkpoint.advanceTail(ALIGNMENT, 0));
            assertThrows(IOException.class, () -> checkpoint.extendScanCeiling(1));
            assertThrows(IOException.class, () -> checkpoint.extendScanCeiling(CAPACITY + ALIGNMENT));
            assertEquals(0, checkpoint.current().scanCeiling());

            checkpoint.extendScanCeiling(ALIGNMENT);
            assertEquals(ALIGNMENT, checkpoint.current().scanCeiling());
            assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));
            assertThrows(IOException.class, () -> BlockRingCheckpoint.openByFileSize(io, ALIGNMENT, fileBytes));

            ByteBuffer block = AlignedWalBlockCodec.encode(0, 7, 8, ALIGNMENT,
                List.of(new AlignedWalBlockCodec.Group(0, List.of(record(0, 1)))));
            long offset = checkpoint.metadataBytes();
            while (block.hasRemaining()) {
                int written = io.write(block, offset);
                assertTrue(written > 0);
                offset += written;
            }
            io.force();
            checkpoint.advanceTail(ALIGNMENT, 0);
            var reopened = BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8);
            assertEquals(ALIGNMENT, reopened.checkpoint().current().scanCeiling());
            assertEquals(1, reopened.scan().blocks().size());
            assertEquals(0, reopened.scan().blocks().get(0).groups().get(0).records().get(0).record().firstOffset());
            byte[] before = Files.readAllBytes(path);
            assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY));
            assertThrows(IOException.class, () -> BlockRingCheckpoint.openByFileSize(io, ALIGNMENT, fileBytes));
            assertArrayEquals(before, Files.readAllBytes(path), "broker format rejection must not mutate the WAL");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowRestartFencesAddressesAndGroupIdsAcrossRepeatedReopens(boolean aio) throws Exception {
        Path path = dir.resolve("window-restart-fence-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();
        }
        for (int restart = 0; restart < 2; restart++) {
            try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.reopen(path)) {
                byte[] before = Files.readAllBytes(path);
                var opened = BlockRingCheckpoint.openWindowForReservationFence(io, ALIGNMENT, CAPACITY, 7, 8);
                assertArrayEquals(before, Files.readAllBytes(path));
                var recovery = BlockRingAllocator.recoverWindow(opened.checkpoint(), opened.scan(), CAPACITY, ALIGNMENT);
                var allocator = recovery.allocator();
                assertThrows(IOException.class, () ->
                    BlockRingAllocator.recoverWindow(opened.checkpoint(), opened.scan(), CAPACITY, ALIGNMENT));
                long ceiling = (2L + restart) * ALIGNMENT;
                assertEquals(ceiling, allocator.usedBytes());
                assertEquals(ceiling, recovery.firstGroupId());
                assertThrows(IOException.class, () -> allocator.reserve(ALIGNMENT));
                assertThrows(IOException.class, () -> allocator.reserveWindow(ALIGNMENT, 1));
                assertThrows(IllegalArgumentException.class, () ->
                    new ParallelRingBlockWriter(io, allocator, dataStart, CAPACITY, ALIGNMENT, 7, 9, 1, 2));
                opened.checkpoint().extendScanCeiling(ceiling + ALIGNMENT);
                try (var writer = new ParallelRingBlockWriter(io, allocator, dataStart, CAPACITY, ALIGNMENT, 7, 8, 1, 2)) {
                    var submitted = writer.submitWindowed(ALIGNMENT, List.of(List.of(record(2, 1)), List.of(record(3, 1))));
                    assertEquals(ceiling, submitted.address().reservedStart());
                    assertEquals(ceiling, submitted.firstGroupId());
                    assertEquals(2, submitted.forced().get(10, TimeUnit.SECONDS).size());
                    assertEquals(0, allocator.contiguousForcedTail(), "the old hole remains quarantined");
                }
            }
        }
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.reopen(path)) {
            var opened = BlockRingCheckpoint.openWindowForReservationFence(io, ALIGNMENT, CAPACITY, 7, 8);
            var recovery = BlockRingAllocator.recoverWindow(opened.checkpoint(), opened.scan(), CAPACITY, ALIGNMENT);
            assertEquals(CAPACITY, recovery.allocator().usedBytes());
            assertThrows(IOException.class, () -> recovery.allocator().reserveWindow(ALIGNMENT, 1));
            assertThrows(IOException.class, () -> opened.checkpoint().extendScanCeiling(CAPACITY + ALIGNMENT));
            var snapshot = BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT, CAPACITY, 7, 8,
                ALIGNMENT, 4, 8);
            var candidates = snapshot.items().stream().filter(BlockRingRecovery.WindowCandidate.class::isInstance)
                .map(BlockRingRecovery.WindowCandidate.class::cast).toList();
            assertEquals(List.of(1L, 2L * ALIGNMENT, 3L * ALIGNMENT), candidates.stream()
                .map(candidate -> candidate.groups().get(0).id()).toList());
            assertEquals(List.of(2L * ALIGNMENT, 2L * ALIGNMENT + 1), candidates.get(1).groups().stream()
                .map(BlockRingRecovery.WindowGroup::id).toList());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowReservationStopsAfterCeilingForceFailure(boolean aio) throws Exception {
        Path path = dir.resolve("window-reservation-force-failure-" + aio);
        try (var backend = new CountingBackend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(CAPACITY + BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT));
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(ALIGNMENT);
            var opened = BlockRingCheckpoint.openWindowForReservationFence(io, ALIGNMENT, CAPACITY, 7, 8);
            var allocator = BlockRingAllocator.recoverWindow(opened.checkpoint(), opened.scan(), CAPACITY, ALIGNMENT).allocator();
            backend.countForces.set(true);
            backend.failAtForce = 1;
            assertThrows(IOException.class, () -> opened.checkpoint().extendScanCeiling(2L * ALIGNMENT));
            assertThrows(IOException.class, () -> allocator.reserveWindow(ALIGNMENT, 1));
            assertEquals(ALIGNMENT, allocator.usedBytes());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowRestartFencePreservesHoleAcrossPhysicalWrap(boolean aio) throws Exception {
        Path path = dir.resolve("window-restart-wrap-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(3L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, 0, 0, record(0, 1));
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();
            checkpoint.advanceTail(2L * ALIGNMENT, 1);
            checkpoint.advanceHead(ALIGNMENT, 0);
        }
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.reopen(path)) {
            var opened = BlockRingCheckpoint.openWindowForReservationFence(io, ALIGNMENT, CAPACITY, 7, 8);
            var recovery = BlockRingAllocator.recoverWindow(opened.checkpoint(), opened.scan(), CAPACITY, ALIGNMENT);
            var allocator = recovery.allocator();
            assertEquals(ALIGNMENT + 2L, recovery.firstGroupId());
            var retirement = allocator.prepareRetirements(allocator.committedAddresses()).get(0);
            retirement.readersDrained().get(10, TimeUnit.SECONDS);
            opened.checkpoint().advanceHead(2L * ALIGNMENT, 1);
            allocator.headCheckpointsForced(retirement);
            opened.checkpoint().extendScanCeiling(6L * ALIGNMENT);
            try (var writer = new ParallelRingBlockWriter(io, allocator, dataStart, CAPACITY, ALIGNMENT, 7, 8, 1, 2)) {
                var submitted = writer.submitWindowed(2 * ALIGNMENT, List.of(List.of(record(2, ALIGNMENT))));
                assertEquals(3L * ALIGNMENT, submitted.address().reservedStart());
                assertEquals(4L * ALIGNMENT, submitted.address().blockStart());
                assertEquals(ALIGNMENT, submitted.address().padding());
                assertEquals(0, submitted.address().physical());
                submitted.forced().get(10, TimeUnit.SECONDS);
                assertEquals(2L * ALIGNMENT, allocator.contiguousForcedTail());
            }
        }
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.reopen(path)) {
            var opened = BlockRingCheckpoint.openWindowForReservationFence(io, ALIGNMENT, CAPACITY, 7, 8);
            var recovery = BlockRingAllocator.recoverWindow(opened.checkpoint(), opened.scan(), CAPACITY, ALIGNMENT);
            assertEquals(CAPACITY, recovery.allocator().usedBytes());
            assertThrows(IOException.class, () -> recovery.allocator().reserveWindow(ALIGNMENT, 1));
            var snapshot = BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT, CAPACITY, 7, 8,
                2 * ALIGNMENT, 4, 2);
            assertTrue(snapshot.items().stream().filter(BlockRingRecovery.WindowCandidate.class::isInstance)
                .map(BlockRingRecovery.WindowCandidate.class::cast).anyMatch(candidate -> candidate.start() == 4L * ALIGNMENT));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedWindowCheckpointForceFencesAndCannotReopenAnUnreconciledWindow(boolean aio) throws Exception {
        Path path = dir.resolve("window-force-failure-" + aio);
        try (var backend = new CountingBackend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(CAPACITY + BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT));
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            backend.countForces.set(true);
            backend.failAtForce = 1;
            assertThrows(IOException.class, () -> checkpoint.extendScanCeiling(ALIGNMENT));
            assertThrows(IOException.class, checkpoint::current);
            assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void interruptedWindowCheckpointWritesNeverAdmitAnUnreconciledWindow(boolean aio) throws Exception {
        for (int failAt : List.of(1, 2, 3)) {
            Path path = dir.resolve("window-write-failure-" + aio + "-" + failAt);
            try (var backend = new CountingBackend(aio); WalIoBackend.Handle io = backend.create(path)) {
                io.preallocate(CAPACITY + BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT));
                var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
                backend.failAtWrite = failAt;
                backend.countWrites.set(true);
                assertThrows(IOException.class, () -> checkpoint.extendScanCeiling(ALIGNMENT));
                assertThrows(IOException.class, checkpoint::current);
                backend.countWrites.set(false);
                if (failAt == 1) {
                    assertEquals(0, BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8)
                        .checkpoint().current().scanCeiling());
                } else {
                    assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sparseWindowInspectionKeepsDamagedAndUnwrittenRangesUnclassified(boolean aio) throws Exception {
        Path path = dir.resolve("inspect-sparse-window-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 7, record(10, 1));
            writeInspectionBlock(io, dataStart, 3L * ALIGNMENT, 9, record(20, 1));
            assertEquals(1, io.write(ByteBuffer.wrap(new byte[] {42}), dataStart + 3L * ALIGNMENT + 70));
            io.force();

            List<String> events = new ArrayList<>();
            BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY, ALIGNMENT, ALIGNMENT, 0, 0, CAPACITY, 7, 8,
                inspectionEvents(events));
            assertEquals(List.of("unknown:0:4096", "block:4096:10", "unknown:8192:16384"), events);
            assertThrows(IOException.class, () -> BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY,
                ALIGNMENT, ALIGNMENT, 0, 0, CAPACITY + ALIGNMENT, 7, 8, inspectionEvents(events)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowInspectionRejectsStalePhysicalBlocksAcrossWrap(boolean aio) throws Exception {
        Path path = dir.resolve("inspect-wrap-window-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            writeInspectionBlock(io, dataStart, 3L * ALIGNMENT, 3, record(30, 1));
            writeInspectionBlock(io, dataStart, 4L * ALIGNMENT, 4, record(40, 1));
            List<String> events = new ArrayList<>();
            BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY, ALIGNMENT, ALIGNMENT, 2L * ALIGNMENT,
                3L * ALIGNMENT, 5L * ALIGNMENT, 7, 8, inspectionEvents(events));
            assertEquals(List.of("block:12288:30", "block:16384:40"), events);

            // The same physical sector contains a valid block from an older
            // logical cycle. It must not be treated as the new candidate.
            writeInspectionBlock(io, dataStart, 0, 1, record(1, 1));
            events.clear();
            BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY, ALIGNMENT, ALIGNMENT, 2L * ALIGNMENT,
                3L * ALIGNMENT, 5L * ALIGNMENT, 7, 8, inspectionEvents(events));
            assertEquals(List.of("block:12288:30", "unknown:16384:20480"), events);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowInspectionPropagatesIoFailureInsteadOfClassifyingItAsAHole(boolean aio) throws Exception {
        Path path = dir.resolve("inspect-read-failure-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (var backend = new CountingBackend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            writeInspectionBlock(io, dataStart, 0, 0, record(0, 1));
            backend.failAtRead = 2;
            backend.countReads.set(true);
            List<String> events = new ArrayList<>();
            assertThrows(IOException.class, () -> BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY,
                ALIGNMENT, ALIGNMENT, 0, 0, ALIGNMENT, 7, 8, inspectionEvents(events)));
            assertTrue(events.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowInspectionRejectsOversizedHeaderBeforeReadingItsClaimedBlock(boolean aio) throws Exception {
        Path path = dir.resolve("inspect-oversized-header-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (var backend = new CountingBackend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            ByteBuffer header = ByteBuffer.allocate(ALIGNMENT);
            header.putInt(0, AlignedWalBlockCodec.MAGIC);
            header.putInt(4, AlignedWalBlockCodec.VERSION);
            header.putInt(8, 2 * ALIGNMENT);
            header.putLong(16, 0);
            header.putLong(24, 7);
            header.putLong(32, 8);
            long position = dataStart;
            while (header.hasRemaining()) {
                int written = io.write(header, position);
                assertTrue(written > 0);
                position += written;
            }
            backend.countReads.set(true);
            List<String> events = new ArrayList<>();
            BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY, ALIGNMENT, ALIGNMENT,
                0, 0, ALIGNMENT, 7, 8, inspectionEvents(events));
            assertEquals(List.of("unknown:0:4096"), events);
            assertTrue(backend.reads.get() > 0);
            assertTrue(backend.maxReadRequest.get() <= ALIGNMENT,
                "an oversized length must not trigger a block read");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowInspectionFindsALaterForcedBlockWhileAnEarlierWriteIsBlocked(boolean aio) throws Exception {
        Path path = dir.resolve("inspect-out-of-order-force-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            var gate = new WriteOrderGate(io, dataStart);
            var allocator = new BlockRingAllocator(CAPACITY, ALIGNMENT);
            try (var writer = new ParallelRingBlockWriter(gate, allocator, dataStart, CAPACITY,
                ALIGNMENT, 7, 8, 2, 8)) {
                var first = writer.submitOwned(ALIGNMENT, offset -> AlignedWalBlockCodec.encode(offset, 7, 8,
                    ALIGNMENT, List.of(new AlignedWalBlockCodec.Group(0, List.of(record(0, 1))))));
                assertTrue(gate.earlierEntered.await(10, TimeUnit.SECONDS));
                WalRecord otherPartition = WalRecord.data(1, 2, 1, 1, 1, 1, new byte[] {1});
                var second = writer.submitOwned(ALIGNMENT, offset -> AlignedWalBlockCodec.encode(offset, 7, 8,
                    ALIGNMENT, List.of(new AlignedWalBlockCodec.Group(1, List.of(otherPartition)))));
                try {
                    second.forced().get(10, TimeUnit.SECONDS);
                    assertFalse(first.forced().isDone());
                    List<String> events = new ArrayList<>();
                    List<Integer> partitions = new ArrayList<>();
                    BlockRingRecovery.inspectWindow(io, dataStart, CAPACITY, ALIGNMENT, ALIGNMENT, 0, 0,
                        checkpoint.current().scanCeiling(), 7, 8, new BlockRingRecovery.WindowVisitor() {
                            @Override
                            public void candidate(long start, AlignedWalBlockCodec.DecodedBlock block) {
                                WalRecord recovered = block.groups().get(0).records().get(0).record();
                                events.add("block:" + start + ":" + recovered.firstOffset());
                                partitions.add(recovered.partition());
                            }

                            @Override
                            public void unclassified(long start, long end) {
                                events.add("unknown:" + start + ":" + end);
                            }
                        });
                    assertEquals(List.of("unknown:0:4096", "block:4096:1"), events);
                    assertEquals(List.of(1), partitions);
                    // The gate keeps the first writer from modifying the file during this controlled scan.
                    var snapshot = BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT, CAPACITY, 7, 8,
                        ALIGNMENT, 2, 1);
                    var a = new WalPartitionKey(1, 2, 0);
                    var b = new WalPartitionKey(1, 2, 1);
                    var analysis = WindowPartitionReconciler.analyze(snapshot,
                        Map.of(a, new WindowPartitionReconciler.Boundary(0, 1, 1),
                            b, new WindowPartitionReconciler.Boundary(1, 2, 1)));
                    assertEquals(1, analysis.partitions().get(b).contiguous().size());
                    assertEquals(2, analysis.partitions().get(b).nextOffset());
                    assertTrue(analysis.partitions().get(a).problems().contains(
                        WindowPartitionReconciler.Problem.REQUIRED_OFFSET_MISSING));
                    assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));
                } finally {
                    gate.release.countDown();
                }
                assertThrows(ExecutionException.class, () -> first.forced().get(10, TimeUnit.SECONDS));
            } finally {
                gate.release.countDown();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void checkpointBoundInspectionValidatesPrefixAndNeverRepairsOrPublishes(boolean aio) throws Exception {
        Path path = dir.resolve("checkpoint-bound-inspection-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();
            byte[] before = Files.readAllBytes(path);
            List<String> events = new ArrayList<>();
            BlockRingCheckpoint.inspectWindow(io, ALIGNMENT, CAPACITY, 7, 8, ALIGNMENT, inspectionEvents(events));
            assertEquals(List.of("unknown:0:4096", "block:4096:1"), events);
            assertArrayEquals(before, Files.readAllBytes(path), "inspection must not repair or mutate the WAL");
            assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));

            writeInspectionBlock(io, dataStart, 0, 0, record(0, 1));
            io.force();
            checkpoint.advanceTail(ALIGNMENT, 0);
            assertEquals(1, io.write(ByteBuffer.wrap(new byte[] {42}), dataStart + 70));
            io.force();
            events.clear();
            assertThrows(IOException.class, () -> BlockRingCheckpoint.inspectWindow(io, ALIGNMENT, CAPACITY,
                7, 8, ALIGNMENT, inspectionEvents(events)));
            assertTrue(events.isEmpty(), "a corrupt committed prefix must fail before any candidate is returned");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowCheckpointDoesNotFallBackPastAnUnreadableBank(boolean aio) throws Exception {
        Path path = dir.resolve("window-unreadable-bank-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();

            // Generation two is in slots 0..2. Two lost copies are tolerable
            // while at least one copy of each bank identifies the window.
            corruptSlots(path, 0, 1);
            List<String> events = new ArrayList<>();
            BlockRingCheckpoint.inspectWindow(io, ALIGNMENT, CAPACITY, 7, 8, ALIGNMENT, inspectionEvents(events));
            assertEquals(List.of("unknown:0:4096", "block:4096:1"), events);

            // If the entire newest bank is lost, accepting the older empty
            // window could hide a previously confirmed later block.
            corruptSlots(path, 2);
            events.clear();
            assertThrows(IOException.class, () -> BlockRingCheckpoint.inspectWindow(io, ALIGNMENT, CAPACITY,
                7, 8, ALIGNMENT, inspectionEvents(events)));
            assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));
            assertTrue(events.isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reopenedWindowCanBeInspectedWithoutOpeningAnUnreconciledWal(boolean aio) throws Exception {
        Path path = dir.resolve("window-inspect-after-reopen-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio)) {
            try (WalIoBackend.Handle io = backend.create(path)) {
                io.preallocate(dataStart + CAPACITY);
                var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
                checkpoint.extendScanCeiling(2L * ALIGNMENT);
                writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
                io.force();
            }
            try (WalIoBackend.Handle io = backend.reopen(path)) {
                List<String> events = new ArrayList<>();
                BlockRingCheckpoint.inspectWindow(io, ALIGNMENT, CAPACITY, 7, 8, ALIGNMENT, inspectionEvents(events));
                assertEquals(List.of("unknown:0:4096", "block:4096:1"), events);
                assertThrows(IOException.class, () -> BlockRingCheckpoint.open(io, ALIGNMENT, CAPACITY, 7, 8));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowSnapshotPreservesAtomicGroupsWithoutRetainingPayloads(boolean aio) throws Exception {
        Path path = dir.resolve("window-snapshot-groups-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            WalRecord first = WalRecord.data(1, 2, 0, 3, 5, 6, new byte[] {1, 2});
            WalRecord second = WalRecord.data(1, 2, 1, 4, 10, 10, new byte[] {3});
            WalRecord truncate = WalRecord.truncate(1, 2, 0, 5, 7);
            ByteBuffer bytes = AlignedWalBlockCodec.encode(ALIGNMENT, 7, 8, ALIGNMENT, List.of(
                new AlignedWalBlockCodec.Group(5, List.of(first, second)),
                new AlignedWalBlockCodec.Group(6, List.of(truncate))));
            long position = dataStart + ALIGNMENT;
            while (bytes.hasRemaining()) {
                int written = io.write(bytes, position);
                assertTrue(written > 0);
                position += written;
            }
            io.force();

            var snapshot = BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT, CAPACITY, 7, 8,
                ALIGNMENT, 2, 3);
            assertEquals(7, snapshot.identityHigh());
            assertEquals(8, snapshot.identityLow());
            assertEquals(2, snapshot.checkpointGeneration());
            assertEquals(0, snapshot.head());
            assertEquals(0, snapshot.tail());
            assertEquals(2L * ALIGNMENT, snapshot.ceiling());
            assertEquals(-1, snapshot.lastCommittedGroup());
            assertEquals(new BlockRingRecovery.WindowUnclassified(0, ALIGNMENT), snapshot.items().get(0));
            var candidate = (BlockRingRecovery.WindowCandidate) snapshot.items().get(1);
            assertEquals(ALIGNMENT, candidate.start());
            assertEquals(List.of(5L, 6L), candidate.groups().stream().map(BlockRingRecovery.WindowGroup::id).toList());
            var entries = candidate.groups().get(0).entries();
            assertEquals(List.of(new WalPartitionKey(1, 2, 0), new WalPartitionKey(1, 2, 1)),
                entries.stream().map(BlockRingRecovery.WindowEntry::partition).toList());
            assertEquals(3, entries.get(0).leaderEpoch());
            assertEquals(5, entries.get(0).firstOffset());
            assertEquals(6, entries.get(0).lastOffset());
            assertEquals(2, entries.get(0).payloadLength());
            assertEquals(WalRecordType.TRUNCATE, candidate.groups().get(1).entries().get(0).type());
            assertThrows(UnsupportedOperationException.class, () -> snapshot.items().clear());
            assertThrows(IOException.class, () -> BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT, CAPACITY,
                7, 8, ALIGNMENT, 1, 3));
            assertThrows(IOException.class, () -> BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT, CAPACITY,
                7, 8, ALIGNMENT, 2, 2));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedWindowSnapshotDoesNotReturnPartiallyScannedResults(boolean aio) throws Exception {
        Path path = dir.resolve("window-snapshot-read-failure-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (var backend = new CountingBackend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, 0, 0, record(0, 1));
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();
            backend.failReadAtPosition = dataStart + ALIGNMENT;
            assertThrows(IOException.class, () -> BlockRingCheckpoint.inspectWindowSnapshot(io, ALIGNMENT,
                CAPACITY, 7, 8, ALIGNMENT, 2, 2));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void windowInspectionRejectsCheckpointChangeDuringScan(boolean aio) throws Exception {
        Path path = dir.resolve("window-inspect-checkpoint-race-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(dataStart + CAPACITY);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, 0, 0, record(0, 1));
            io.force();
            var visitor = new BlockRingRecovery.WindowVisitor() {
                @Override
                public void candidate(long start, AlignedWalBlockCodec.DecodedBlock block) throws IOException {
                    checkpoint.extendScanCeiling(3L * ALIGNMENT);
                }

                @Override
                public void unclassified(long start, long end) { }
            };
            assertThrows(IOException.class, () -> BlockRingCheckpoint.inspectWindow(io, ALIGNMENT, CAPACITY,
                7, 8, ALIGNMENT, visitor));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void brokerAdapterRejectsWindowedWalWithoutChangingItOrLeakingOwnership(boolean aio) throws Exception {
        Path path = dir.resolve("window-adapter-rejection-" + aio);
        long fileBytes = CAPACITY + BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
        }
        byte[] before = Files.readAllBytes(path);
        try (WalIoBackend backend = backend(aio)) {
            for (int attempt = 0; attempt < 2; attempt++) {
                IOException failure = assertThrows(IOException.class, () ->
                    CoordinatedRingWalAdapter.reopenByFileSize(path, backend, fileBytes, 0, 8, 8L * ALIGNMENT));
                assertTrue(failure.getMessage().contains("not enabled"));
            }
        }
        assertArrayEquals(before, Files.readAllBytes(path));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void recoveryLeaseKeepsWindowReadOnlyAndExclusivelyOwned(boolean aio) throws Exception {
        Path path = dir.resolve("window-recovery-lease-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();
        }
        byte[] before = Files.readAllBytes(path);
        try (WalIoBackend backend = backend(aio)) {
            WindowRecoveryLease lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes,
                ALIGNMENT, 2, 1);
            try (lease) {
                var snapshot = lease.snapshot();
                assertEquals(7, snapshot.identityHigh());
                assertEquals(8, snapshot.identityLow());
                assertEquals(2L * ALIGNMENT, snapshot.ceiling());
                assertEquals(2, snapshot.items().size());
                assertThrows(IOException.class, () -> WindowRecoveryLease.open(path, backend, ALIGNMENT,
                    fileBytes, ALIGNMENT, 2, 1));
                assertThrows(IOException.class, () -> CoordinatedRingWalAdapter.reopenByFileSize(path, backend,
                    fileBytes, 0, 8, 8L * ALIGNMENT));
            }
            lease.close();
            assertThrows(IOException.class, lease::snapshot);
            // An incomplete scan must release both the reader and ownership.
            assertThrows(IOException.class, () -> WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes,
                ALIGNMENT, 1, 1));
            try (var reopened = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 2, 1)) {
                assertEquals(2, reopened.snapshot().items().size());
            }
        }
        assertArrayEquals(before, Files.readAllBytes(path));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void recoveryLeaseRejectsLegacyWalAndReleasesOwnership(boolean aio) throws Exception {
        Path path = dir.resolve("legacy-recovery-lease-" + aio);
        long fileBytes = CAPACITY + BlockRingCheckpoint.Layout.BANKED.metadataBytes(ALIGNMENT);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
        }
        byte[] before = Files.readAllBytes(path);
        try (WalIoBackend backend = backend(aio)) {
            assertThrows(IOException.class, () -> WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes,
                ALIGNMENT, 2, 1));
            try (var wal = CoordinatedRingWalAdapter.reopenByFileSize(path, backend, fileBytes, 0, 8,
                8L * ALIGNMENT)) {
                assertEquals(0, wal.usedBytes());
            }
        }
        assertArrayEquals(before, Files.readAllBytes(path));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void uncoveredReclaimHintAvoidsRereadsButNeverAuthorizesReclaim(boolean aio) throws Exception {
        Path path = dir.resolve("reclaim-negative-hint-" + aio);
        RemoteObjectIndex remote = new RemoteObjectIndex();
        var policy = new RemoteCoverageWalReclaimPolicy(remote);
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
            backend.countReads.set(true);
            assertEquals(0, wal.reclaim(policy, 1));
            int reads = backend.reads.get();
            assertTrue(reads > 0);
            assertEquals(0, wal.reclaim(policy, 1));
            assertEquals(reads, backend.reads.get(), "unchanged uncovered range should not reread a block");
            assertEquals(0, wal.reclaimedBeforeOffset());

            // Arbitrary policies must still receive the full, freshly decoded record.
            assertEquals(0, wal.reclaim((record, location) -> false, 1));
            assertTrue(backend.reads.get() > reads);
            assertEquals(0, wal.reclaim(policy, 1));
            int blockedReads = backend.reads.get();
            assertEquals(0, wal.reclaim(policy, 1));
            assertEquals(blockedReads, backend.reads.get());

            var partition = new SharedPartitionId(1, 2, 0);
            CRC32C checksum = new CRC32C();
            checksum.update(new byte[] {0}, 0, 1);
            var mismatch = new RemoteObjectIndex();
            mismatch.add(new SharedObjectMetadata(2, 1, 123, List.of(new SharedObjectRange(
                partition, new OffsetRange(0, 1), 1, 0, 1, checksum.getValue() + 1))));
            assertEquals(0, wal.reclaim(new RemoteCoverageWalReclaimPolicy(mismatch), 1));
            assertEquals(blockedReads, backend.reads.get(), "mismatched identity cannot unblock the negative hint");
            remote.add(new SharedObjectMetadata(1, 1, 123, List.of(new SharedObjectRange(
                partition, new OffsetRange(0, 1), 1, 0, 1, checksum.getValue()))));
            backend.failAtRead = backend.reads.get() + 1;
            assertThrows(IOException.class, () -> wal.reclaim(policy, 1));
            assertEquals(0, wal.reclaimedBeforeOffset(), "coverage alone cannot authorize reclaim");
            backend.failAtRead = 0;
            assertTrue(wal.reclaim(policy, 1) > 0);
            assertTrue(wal.reclaimedBeforeOffset() > 0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mismatchedRemoteBatchCannotReclaimParallelWalAcrossRestart(boolean aio) throws Exception {
        Path path = dir.resolve("reclaim-identity-restart-" + aio);
        var partition = new SharedPartitionId(1, 2, 0);
        CRC32C different = new CRC32C();
        different.update(new byte[] {1}, 0, 1);
        RemoteObjectIndex mismatched = new RemoteObjectIndex();
        mismatched.add(new SharedObjectMetadata(1, 1, 123, List.of(new SharedObjectRange(
            partition, new OffsetRange(0, 1), 1, 0, 1, different.getValue()))));
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.createWithBankedCheckpoints(
                 path, backend, CAPACITY, ALIGNMENT, 0, 8, 8L * ALIGNMENT)) {
            wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
            assertEquals(0, wal.reclaim(new RemoteCoverageWalReclaimPolicy(mismatched), 1));
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopenByFileSize(path, backend,
                 CAPACITY + CoordinatedRingWalAdapter.BANKED_METADATA_BYTES, 0, 8, 8L * ALIGNMENT)) {
            List<Long> recovered = new ArrayList<>();
            wal.replay((record, location) -> recovered.add(record.firstOffset()));
            assertEquals(List.of(0L), recovered);
            CRC32C correct = new CRC32C();
            correct.update(new byte[] {0}, 0, 1);
            RemoteObjectIndex matching = new RemoteObjectIndex();
            matching.add(new SharedObjectMetadata(2, 1, 123, List.of(new SharedObjectRange(
                partition, new OffsetRange(0, 1), 1, 0, 1, correct.getValue()))));
            assertTrue(wal.reclaim(new RemoteCoverageWalReclaimPolicy(matching), 1) > 0);
        }
        assertReplayByFileSize(path, aio, CAPACITY + CoordinatedRingWalAdapter.BANKED_METADATA_BYTES, List.of());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void remoteReclaimRequiresEveryRecordInAGroup(boolean aio) throws Exception {
        Path path = dir.resolve("reclaim-multiple-records-" + aio);
        var partition = new SharedPartitionId(1, 2, 0);
        CRC32C firstCrc = new CRC32C();
        firstCrc.update(new byte[1024], 0, 1024);
        byte[] secondPayload = new byte[1024];
        java.util.Arrays.fill(secondPayload, (byte) 1);
        CRC32C secondCrc = new CRC32C();
        secondCrc.update(secondPayload, 0, secondPayload.length);
        var first = new SharedObjectRange(partition, new OffsetRange(0, 1), 1, 0, 1024, firstCrc.getValue());
        var wrong = new SharedObjectRange(partition, new OffsetRange(1, 2), 1, 1024, 1024, firstCrc.getValue());
        var second = new SharedObjectRange(partition, new OffsetRange(1, 2), 1, 1024, 1024, secondCrc.getValue());
        var mismatched = new RemoteObjectIndex();
        mismatched.add(new SharedObjectMetadata(1, 2048, 0, List.of(first, wrong)));
        var matching = new RemoteObjectIndex();
        matching.add(new SharedObjectMetadata(2, 2048, 0, List.of(first, second)));
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.createWithBankedCheckpoints(
                 path, backend, CAPACITY, ALIGNMENT, 0, 8, 8L * ALIGNMENT)) {
            wal.appendBatch(List.of(record(0, 1024), WalRecord.data(1, 2, 0, 1, 1, 1, secondPayload)))
                .get(10, TimeUnit.SECONDS);
            assertEquals(0, wal.reclaim(new RemoteCoverageWalReclaimPolicy(mismatched), 1));
            assertEquals(0, wal.reclaimedBeforeOffset());
            assertTrue(wal.reclaim(new RemoteCoverageWalReclaimPolicy(matching), 1) > 0);
        }
        assertReplayByFileSize(path, aio, CAPACITY + CoordinatedRingWalAdapter.BANKED_METADATA_BYTES, List.of());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leasedWindowClassifiesPartitionsIndependentlyAcrossAHole(boolean aio) throws Exception {
        Path path = dir.resolve("window-partition-analysis-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        var a = new WalPartitionKey(1, 2, 0);
        var b = new WalPartitionKey(1, 2, 1);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(3L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, WalRecord.data(1, 2, 1, 3, 0, 0, new byte[] {1}));
            writeInspectionBlock(io, dataStart, 2L * ALIGNMENT, 2, WalRecord.data(1, 2, 0, 1, 1, 1, new byte[] {2}));
            io.force();
        }
        byte[] before = Files.readAllBytes(path);
        try (WalIoBackend backend = backend(aio)) {
            WindowRecoveryLease lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 3, 2);
            try (lease) {
                var analysis = lease.analyze(Map.of(a, new WindowPartitionReconciler.Boundary(0, 1, 1),
                    b, new WindowPartitionReconciler.Boundary(0, 1, 3)));
                assertEquals(7, analysis.identityHigh());
                assertEquals(8, analysis.identityLow());
                assertEquals(2, analysis.checkpointGeneration());
                assertEquals(CAPACITY, analysis.capacity());
                assertEquals(0, analysis.head());
                assertEquals(0, analysis.tail());
                assertEquals(3L * ALIGNMENT, analysis.ceiling());
                assertEquals(-1, analysis.lastCommittedGroup());
                assertEquals(List.of(new BlockRingRecovery.WindowUnclassified(0, ALIGNMENT)), analysis.unclassified());
                assertEquals(1, analysis.partitions().get(b).nextOffset());
                assertEquals(1, analysis.partitions().get(b).contiguous().size());
                assertEquals(ALIGNMENT, analysis.partitions().get(b).contiguous().get(0).blockStart());
                assertEquals(ALIGNMENT, analysis.partitions().get(b).contiguous().get(0).blockLength());
                assertTrue(analysis.partitions().get(b).problems().isEmpty());
                assertEquals(0, analysis.partitions().get(a).nextOffset());
                assertEquals(1, analysis.partitions().get(a).quarantined().size());
                assertEquals(Set.of(WindowPartitionReconciler.Problem.GAP_OR_OVERLAP,
                    WindowPartitionReconciler.Problem.REQUIRED_OFFSET_MISSING), analysis.partitions().get(a).problems());
                var bBoundary = new WindowPartitionReconciler.Boundary(0, 1, 3);
                var staged = lease.readContiguous(b, bBoundary, 1);
                assertEquals(1, staged.size());
                assertEquals(1, staged.get(0).size());
                assertEquals(1, staged.get(0).get(0).partition());
                assertEquals(1, staged.get(0).get(0).payload().get(0));
                assertThrows(IOException.class, () -> lease.readContiguous(b, bBoundary, 0));
                assertThrows(IOException.class, () -> lease.readContiguous(a,
                    new WindowPartitionReconciler.Boundary(0, 1, 1), 1024));
                assertThrows(IOException.class, () -> CoordinatedRingWalAdapter.reopenByFileSize(path, backend,
                    fileBytes, 0, 8, 8L * ALIGNMENT));
            }
            assertThrows(IOException.class, () -> lease.analyze(Map.of()));
            assertThrows(IOException.class, () -> lease.readContiguous(b,
                new WindowPartitionReconciler.Boundary(0, 1, 3), 1024));
        }
        assertArrayEquals(before, Files.readAllBytes(path));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leasedCandidateReadRejectsChangesAndKeepsOwnership(boolean aio) throws Exception {
        Path path = dir.resolve("window-candidate-read-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        WalRecord original = WalRecord.data(1, 2, 1, 3, 0, 0, new byte[] {1});
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, original);
            io.force();
            WindowRecoveryLease lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 2, 1);
            try (lease) {
                assertThrows(IOException.class, () -> RingWalOwnership.acquire(path));
                assertThrows(IOException.class, () -> lease.readCandidate(0));
                var block = lease.readCandidate(ALIGNMENT);
                assertEquals(1, block.groups().size());
                assertEquals(1, block.groups().get(0).id());
                var record = block.groups().get(0).records().get(0).record();
                assertEquals(1, record.partition());
                assertEquals(1, record.payload().get(0));

                assertEquals(1, io.write(ByteBuffer.wrap(new byte[] {42}), dataStart + ALIGNMENT + 70));
                io.force();
                assertThrows(IOException.class, () -> lease.readCandidate(ALIGNMENT));

                // A non-cooperating writer changes the payload but leaves the
                // partition, offsets and group identity identical and valid.
                writeInspectionBlock(io, dataStart, ALIGNMENT, 1,
                    WalRecord.data(1, 2, 1, 3, 0, 0, new byte[] {2}));
                io.force();
                assertThrows(IOException.class, () -> lease.readCandidate(ALIGNMENT));

                writeInspectionBlock(io, dataStart, ALIGNMENT, 1, original);
                io.force();
                assertEquals(1, lease.readCandidate(ALIGNMENT).groups().get(0).records().get(0).record().payload().get(0));
                checkpoint.extendScanCeiling(3L * ALIGNMENT);
                assertThrows(IOException.class, () -> lease.readCandidate(ALIGNMENT));
            }
            assertThrows(IOException.class, () -> lease.readCandidate(ALIGNMENT));
            try (var ownership = RingWalOwnership.acquire(path)) {
                assertNotNull(ownership);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partitionRecoveryDoesNotReturnAPartialPrefixWhenALaterCandidateChanges(boolean aio) throws Exception {
        Path path = dir.resolve("window-partition-stage-failure-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        var partition = new WalPartitionKey(1, 2, 1);
        var boundary = new WindowPartitionReconciler.Boundary(0, 2, 3);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(3L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, WalRecord.data(1, 2, 1, 3, 0, 0, new byte[] {1}));
            writeInspectionBlock(io, dataStart, 2L * ALIGNMENT, 2, WalRecord.data(1, 2, 1, 3, 1, 1, new byte[] {2}));
            io.force();
            try (var lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 3, 2)) {
                var staged = lease.readContiguous(partition, boundary, 2);
                assertEquals(2, staged.size());
                assertEquals(0, staged.get(0).get(0).firstOffset());
                assertEquals(1, staged.get(1).get(0).firstOffset());
                writeInspectionBlock(io, dataStart, 2L * ALIGNMENT, 2,
                    WalRecord.data(1, 2, 1, 3, 1, 1, new byte[] {3}));
                io.force();
                assertThrows(IOException.class, () -> lease.readContiguous(partition, boundary, 2));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partitionRecoveryPreservesGroupsWhenPartitionsShareABlock(boolean aio) throws Exception {
        Path path = dir.resolve("window-partition-shared-block-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        var partition = new WalPartitionKey(1, 2, 1);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            ByteBuffer block = AlignedWalBlockCodec.encode(ALIGNMENT, 7, 8, ALIGNMENT, List.of(
                new AlignedWalBlockCodec.Group(1, List.of(WalRecord.data(1, 2, 1, 3, 0, 0, new byte[] {1}))),
                new AlignedWalBlockCodec.Group(2, List.of(WalRecord.data(1, 2, 0, 4, 0, 0, new byte[] {9}))),
                new AlignedWalBlockCodec.Group(3, List.of(WalRecord.data(1, 2, 1, 3, 1, 1, new byte[] {2})))));
            long position = dataStart + ALIGNMENT;
            while (block.hasRemaining()) {
                int written = io.write(block, position);
                assertTrue(written > 0);
                position += written;
            }
            io.force();
            try (var lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 2, 3)) {
                var groups = lease.readContiguous(partition, new WindowPartitionReconciler.Boundary(0, 2, 3), 2);
                assertEquals(2, groups.size());
                assertEquals(0, groups.get(0).get(0).firstOffset());
                assertEquals(1, groups.get(1).get(0).firstOffset());
                assertEquals(1, groups.get(0).get(0).payload().get(0));
                assertEquals(2, groups.get(1).get(0).payload().get(0));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leasedWindowQuarantinesMixedGroupsEpochMismatchAndUnknownPartitions(boolean aio) throws Exception {
        Path path = dir.resolve("window-partition-quarantine-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        var a = new WalPartitionKey(1, 2, 0);
        var b = new WalPartitionKey(1, 2, 1);
        var c = new WalPartitionKey(1, 2, 2);
        var unknown = new WalPartitionKey(1, 2, 3);
        var control = new WalPartitionKey(1, 2, 4);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(ALIGNMENT);
            ByteBuffer bytes = AlignedWalBlockCodec.encode(0, 7, 8, ALIGNMENT, List.of(
                new AlignedWalBlockCodec.Group(0, List.of(record(0, 1), WalRecord.data(1, 2, 1, 1, 0, 0, new byte[] {1}))),
                new AlignedWalBlockCodec.Group(1, List.of(record(0, 1))),
                new AlignedWalBlockCodec.Group(2, List.of(WalRecord.data(1, 2, 2, 4, 0, 0, new byte[] {2}))),
                new AlignedWalBlockCodec.Group(3, List.of(WalRecord.data(1, 2, 3, 1, 0, 0, new byte[] {3}))),
                new AlignedWalBlockCodec.Group(4, List.of(WalRecord.truncate(1, 2, 4, 1, 0)))));
            long position = dataStart;
            while (bytes.hasRemaining()) {
                int written = io.write(bytes, position);
                assertTrue(written > 0);
                position += written;
            }
            io.force();
        }
        try (WalIoBackend backend = backend(aio);
             var lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 1, 6)) {
            var analysis = lease.analyze(Map.of(a, new WindowPartitionReconciler.Boundary(0, 1, 1),
                b, new WindowPartitionReconciler.Boundary(0, 1, 1),
                c, new WindowPartitionReconciler.Boundary(0, 1, 3),
                control, new WindowPartitionReconciler.Boundary(0, 1, 1)));
            assertTrue(analysis.partitions().get(a).problems().contains(WindowPartitionReconciler.Problem.UNSUPPORTED_ATOMIC_GROUP));
            assertTrue(analysis.partitions().get(b).problems().contains(WindowPartitionReconciler.Problem.UNSUPPORTED_ATOMIC_GROUP));
            assertEquals(2, analysis.partitions().get(a).quarantined().size());
            assertTrue(analysis.partitions().get(a).contiguous().isEmpty());
            assertTrue(analysis.partitions().get(c).problems().contains(WindowPartitionReconciler.Problem.EPOCH_MISMATCH));
            assertTrue(analysis.partitions().get(unknown).problems().contains(WindowPartitionReconciler.Problem.MISSING_BOUNDARY));
            assertTrue(analysis.partitions().get(control).problems().contains(WindowPartitionReconciler.Problem.CONTROL_RECORD));
            assertThrows(UnsupportedOperationException.class, () -> analysis.partitions().clear());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leasedWindowRejectsNonmonotonicPhysicalGroupIds(boolean aio) throws Exception {
        Path path = dir.resolve("window-group-order-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, 0, 2, record(0, 1));
            writeInspectionBlock(io, dataStart, ALIGNMENT, 1, record(1, 1));
            io.force();
        }
        try (WalIoBackend backend = backend(aio);
             var lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 2, 2)) {
            assertThrows(IOException.class, () -> lease.analyze(Map.of(new WalPartitionKey(1, 2, 0),
                new WindowPartitionReconciler.Boundary(0, 2, 1))));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leasedWindowDoesNotPartiallyAcceptAnAtomicGroup(boolean aio) throws Exception {
        Path path = dir.resolve("window-group-partial-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        var a = new WalPartitionKey(1, 2, 0);
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(ALIGNMENT);
            ByteBuffer bytes = AlignedWalBlockCodec.encode(0, 7, 8, ALIGNMENT, List.of(
                new AlignedWalBlockCodec.Group(0, List.of(record(0, 1), record(2, 1)))));
            long position = dataStart;
            while (bytes.hasRemaining()) {
                int written = io.write(bytes, position);
                assertTrue(written > 0);
                position += written;
            }
            io.force();
        }
        try (WalIoBackend backend = backend(aio);
             var lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 1, 2)) {
            var result = lease.analyze(Map.of(a, new WindowPartitionReconciler.Boundary(0, 1, 1))).partitions().get(a);
            assertEquals(0, result.nextOffset());
            assertTrue(result.contiguous().isEmpty());
            assertEquals(1, result.quarantined().size());
            assertTrue(result.problems().contains(WindowPartitionReconciler.Problem.GAP_OR_OVERLAP));
            assertTrue(result.problems().contains(WindowPartitionReconciler.Problem.REQUIRED_OFFSET_MISSING));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leasedWindowChecksGroupOrderAgainstCommittedPrefix(boolean aio) throws Exception {
        Path path = dir.resolve("window-prefix-group-order-" + aio);
        long dataStart = BlockRingCheckpoint.Layout.WINDOWED.metadataBytes(ALIGNMENT);
        long fileBytes = dataStart + CAPACITY;
        try (WalIoBackend backend = backend(aio); WalIoBackend.Handle io = backend.create(path)) {
            io.preallocate(fileBytes);
            var checkpoint = BlockRingCheckpoint.initialize(io, ALIGNMENT, CAPACITY, 7, 8);
            checkpoint.extendScanCeiling(2L * ALIGNMENT);
            writeInspectionBlock(io, dataStart, 0, 0, record(0, 1));
            io.force();
            checkpoint.advanceTail(ALIGNMENT, 0);
            writeInspectionBlock(io, dataStart, ALIGNMENT, 0, record(1, 1));
            io.force();
        }
        try (WalIoBackend backend = backend(aio);
             var lease = WindowRecoveryLease.open(path, backend, ALIGNMENT, fileBytes, ALIGNMENT, 1, 1)) {
            assertEquals(0, lease.snapshot().lastCommittedGroup());
            assertThrows(IOException.class, () -> lease.analyze(Map.of(new WalPartitionKey(1, 2, 0),
                new WindowPartitionReconciler.Boundary(1, 2, 1))));
        }
    }

    private static BlockRingRecovery.WindowVisitor inspectionEvents(List<String> events) {
        return new BlockRingRecovery.WindowVisitor() {
            @Override
            public void candidate(long start, AlignedWalBlockCodec.DecodedBlock block) {
                long first = block.groups().get(0).records().get(0).record().firstOffset();
                events.add("block:" + start + ":" + first);
            }

            @Override
            public void unclassified(long start, long end) {
                events.add("unknown:" + start + ":" + end);
            }
        };
    }

    private static void writeInspectionBlock(WalIoBackend.Handle io, long dataStart, long logicalOffset,
                                             long groupId, WalRecord record) throws IOException {
        ByteBuffer bytes = AlignedWalBlockCodec.encode(logicalOffset, 7, 8, ALIGNMENT,
            List.of(new AlignedWalBlockCodec.Group(groupId, List.of(record))));
        long position = dataStart + logicalOffset % CAPACITY;
        while (bytes.hasRemaining()) {
            int written = io.write(bytes, position);
            if (written <= 0) throw new IOException("Short inspection test write");
            position += written;
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void bankedHeadSurvivesReuseAndTwoLostNewestSlots(boolean aio) throws Exception {
        Path path = dir.resolve("banked-reuse-" + aio);
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.createWithBankedCheckpoints(path, backend, CAPACITY, ALIGNMENT, 0, 8,
                 8L * ALIGNMENT)) {
            for (int i = 0; i < 4; i++) wal.append(record(i, 1)).get(10, TimeUnit.SECONDS);
            backend.countForces.set(true);
            assertEquals(2L * ALIGNMENT, wal.reclaim((record, location) -> record.firstOffset() < 2, CAPACITY));
            assertEquals(1, backend.forces.get(), "the covered head prefix shares one force");
            wal.append(record(4, 1)).get(10, TimeUnit.SECONDS);
        }
        // Generation seven is in the second bank. Its newest tail includes
        // the write that physically reused the retired block.
        corruptSlots(path, 3, 4);
        assertReplayByFileSize(path, aio, CAPACITY + CoordinatedRingWalAdapter.BANKED_METADATA_BYTES, List.of(2L, 3L, 4L));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fileSizeOpeningPreservesLegacyCheckpointFormat(boolean aio) throws Exception {
        Path path = dir.resolve("legacy-by-file-size-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
        }
        long fileBytes = CAPACITY + CoordinatedRingWalAdapter.METADATA_BYTES;
        assertReplayByFileSize(path, aio, fileBytes, List.of(0L));
        assertEquals(fileBytes, Files.size(path));
    }

    private static void assertReplayByFileSize(Path path, boolean aio, long fileBytes, List<Long> expected)
        throws Exception {
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopenByFileSize(path, backend, fileBytes, 0, 8, 8L * ALIGNMENT)) {
            List<Long> recovered = new ArrayList<>();
            wal.replay((record, location) -> recovered.add(record.firstOffset()));
            assertEquals(expected, recovered);
        }
    }

    private static void corruptSlots(Path path, int... slots) throws IOException {
        try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
            for (int slot : slots) {
                file.write(ByteBuffer.wrap(new byte[] {0}), (long) slot * ALIGNMENT);
            }
            file.force(true);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reclaimsOnlyCoveredPrefixWithOneDurableHeadCheckpoint(boolean aio) throws Exception {
        Path path = dir.resolve("batch-reclaim-" + aio);
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            for (int i = 0; i < 4; i++) wal.append(record(i, 1)).get(10, TimeUnit.SECONDS);
            backend.countForces.set(true);
            assertEquals(3L * ALIGNMENT, wal.reclaim((record, location) -> record.firstOffset() < 3, CAPACITY));
            assertEquals(2, backend.forces.get(), "one head transition must force both checkpoint copies");
            assertEquals(ALIGNMENT, wal.usedBytes());
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0)) {
            List<Long> recovered = new ArrayList<>();
            wal.replay((record, location) -> recovered.add(record.firstOffset()));
            assertEquals(List.of(3L), recovered);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void activeBlockReservesImmediatelyAndBatchesSequentialAdmissions(boolean aio) throws Exception {
        Path path = dir.resolve("active-block-sequential-" + aio);
        List<SharedWal.AppendSubmission> admissions = new ArrayList<>();
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT,
                 TimeUnit.SECONDS.toNanos(30))) {
            for (int i = 0; i < 4; i++) {
                var admission = wal.submitBatch(List.of(record(i, 1)));
                assertTrue(admission.reserved().isDone(), "physical space is reserved during admission");
                admission.reserved().get(1, TimeUnit.SECONDS);
                assertFalse(admission.durable().isDone());
                admissions.add(admission);
            }
            assertEquals(ALIGNMENT, wal.usedBytes());
            wal.close();
            for (var admission : admissions) admission.durable().get(10, TimeUnit.SECONDS);
            assertEquals(1, wal.blockStats().blocks());
            assertEquals(4, wal.blockStats().groups());
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0)) {
            List<Long> replay = new ArrayList<>();
            wal.replay((record, location) -> replay.add(record.firstOffset()));
            assertEquals(List.of(0L, 1L, 2L, 3L), replay);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentAdmissionsShareReservedBlockWithoutLosingGroups(boolean aio) throws Exception {
        Path path = dir.resolve("active-block-concurrent-" + aio);
        var pool = Executors.newFixedThreadPool(16);
        var start = new CountDownLatch(1);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT,
                 TimeUnit.SECONDS.toNanos(30))) {
            List<java.util.concurrent.Future<SharedWal.AppendSubmission>> submitted = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                long offset = i;
                submitted.add(pool.submit(() -> {
                    start.await();
                    return wal.submitBatch(List.of(record(offset, 1)));
                }));
            }
            start.countDown();
            List<SharedWal.AppendSubmission> admissions = new ArrayList<>();
            for (var future : submitted) {
                var admission = future.get(10, TimeUnit.SECONDS);
                admission.reserved().get(1, TimeUnit.SECONDS);
                admissions.add(admission);
            }
            assertEquals(ALIGNMENT, wal.usedBytes());
            wal.close();
            for (var admission : admissions) admission.durable().get(10, TimeUnit.SECONDS);
            assertEquals(1, wal.blockStats().blocks());
            assertEquals(16, wal.blockStats().groups());
        } finally {
            pool.shutdownNow();
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0)) {
            Set<Long> recovered = new java.util.HashSet<>();
            wal.replay((record, location) -> assertTrue(recovered.add(record.firstOffset())));
            assertEquals(16, recovered.size());
            for (long i = 0; i < 16; i++) assertTrue(recovered.contains(i));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void activeBlockGrowsOnlyWithinTheAvailablePhysicalExtent(boolean aio) throws Exception {
        Path path = dir.resolve("active-block-grow-wrap-" + aio);
        List<SharedWal.AppendSubmission> later = new ArrayList<>();
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, 2 * ALIGNMENT,
                 TimeUnit.SECONDS.toNanos(30))) {
            var first = wal.submitBatch(List.of(record(0, 3000)));
            var second = wal.submitBatch(List.of(record(1, 7600)));
            var third = wal.submitBatch(List.of(record(2, 500)));
            first.durable().get(10, TimeUnit.SECONDS);
            second.durable().get(10, TimeUnit.SECONDS);
            third.reserved().get(1, TimeUnit.SECONDS);
            assertFalse(third.durable().isDone());
            long reclaimed = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reclaimed < 3L * ALIGNMENT && System.nanoTime() < deadline) {
                reclaimed += wal.reclaim((record, location) -> record.firstOffset() < 2, CAPACITY);
                if (reclaimed < 3L * ALIGNMENT) Thread.yield();
            }
            assertEquals(3L * ALIGNMENT, reclaimed);
            var fourth = wal.submitBatch(List.of(record(3, 5000)));
            fourth.reserved().get(1, TimeUnit.SECONDS);
            assertEquals(3L * ALIGNMENT, wal.usedBytes(), "the new 8 KiB block wraps after the open 4 KiB block");
            later.add(third);
            later.add(fourth);
            wal.close();
            for (var admission : later) admission.durable().get(10, TimeUnit.SECONDS);
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, 2 * ALIGNMENT, 0)) {
            List<Long> replay = new ArrayList<>();
            wal.replay((record, location) -> replay.add(record.firstOffset()));
            assertEquals(List.of(2L, 3L), replay);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reclaimDoesNotRebasePastAnAdmittedOpenBlock(boolean aio) throws Exception {
        Path path = dir.resolve("active-block-reclaim-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT,
                 TimeUnit.SECONDS.toNanos(30))) {
            var first = wal.submitBatch(List.of(record(0, 3000)));
            var second = wal.submitBatch(List.of(record(1, 3000)));
            first.reserved().get(1, TimeUnit.SECONDS);
            second.reserved().get(1, TimeUnit.SECONDS);
            first.durable().get(10, TimeUnit.SECONDS);
            assertFalse(second.durable().isDone());
            long reclaimed = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (reclaimed == 0 && System.nanoTime() < deadline) {
                reclaimed = wal.reclaim((record, location) -> true, ALIGNMENT);
                if (reclaimed == 0) Thread.yield();
            }
            assertEquals(ALIGNMENT, reclaimed);
            assertEquals(ALIGNMENT, wal.reclaimedBeforeOffset());
            assertEquals(ALIGNMENT, wal.usedBytes());
            wal.close();
            second.durable().get(10, TimeUnit.SECONDS);
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0)) {
            List<Long> replay = new ArrayList<>();
            wal.replay((record, location) -> replay.add(record.firstOffset()));
            assertEquals(List.of(1L), replay);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void earlierForceFailureFencesAdmittedOpenBlock(boolean aio) throws Exception {
        Path path = dir.resolve("active-block-force-failure-" + aio);
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT,
                 TimeUnit.SECONDS.toNanos(30))) {
            backend.failAtForce = 1;
            backend.countForces.set(true);
            var first = wal.submitBatch(List.of(record(0, 3000)));
            var second = wal.submitBatch(List.of(record(1, 3000)));
            first.reserved().get(1, TimeUnit.SECONDS);
            second.reserved().get(1, TimeUnit.SECONDS);
            assertThrows(ExecutionException.class, () -> first.durable().get(10, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> second.durable().get(10, TimeUnit.SECONDS));
            var rejected = wal.submitBatch(List.of(record(2, 1)));
            assertThrows(ExecutionException.class, () -> rejected.reserved().get(1, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void reservationFailsBeforeAcknowledgementWhenPhysicalRingIsFull(boolean aio) throws Exception {
        Path path = dir.resolve("reservation-full-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            for (int i = 0; i < 4; i++) {
                var submitted = wal.submitBatch(List.of(record(i, 1)));
                submitted.reserved().get(10, TimeUnit.SECONDS);
                submitted.durable().get(10, TimeUnit.SECONDS);
            }
            var rejected = wal.submitBatch(List.of(record(4, 1)));
            assertThrows(ExecutionException.class, () -> rejected.reserved().get(10, TimeUnit.SECONDS));
            assertThrows(ExecutionException.class, () -> rejected.durable().get(10, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cachedReadRespectsRealWalFailureFence(boolean aio) throws Exception {
        Path path = dir.resolve("cached-read-fence-" + aio);
        SharedPartitionId partition = new SharedPartitionId(1, 2, 0);
        try (var backend = new CountingBackend(aio);
             var engine = new SharedStorageEngine(
                 CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0))) {
            engine.appendOwnedBatchGroup(partition, List.of(
                new SharedStorageEngine.OwnedDataBatch(1, 0, 0, ByteBuffer.wrap(new byte[] {1}))))
                .get(10, TimeUnit.SECONDS);
            assertTrue(engine.readLocal(partition, 0).isPresent());
            assertTrue(engine.readLocal(partition, 0).isPresent());
            assertEquals(2, engine.localCacheHitCount(), "confirmed data is reused on the first fetch");
            assertEquals(0, engine.localCacheMissCount());
            backend.failAtForce = 1;
            backend.countForces.set(true);
            assertThrows(ExecutionException.class, () ->
                engine.appendData(partition, 1, 1, 1, ByteBuffer.wrap(new byte[] {2})).get(10, TimeUnit.SECONDS));
            assertThrows(IOException.class, () -> engine.readLocal(partition, 0),
                "a cache hit must not bypass a fenced WAL backend");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedBatchHeadForceFencesWritesAndKeepsUncoveredSuffix(boolean aio) throws Exception {
        for (int failAt : List.of(1, 2)) {
            Path path = dir.resolve("batch-head-failure-" + aio + "-" + failAt);
            try (var backend = new CountingBackend(aio);
                 var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
                for (int i = 0; i < 4; i++) wal.append(record(i, 1)).get(10, TimeUnit.SECONDS);
                backend.failAtForce = failAt;
                backend.countForces.set(true);
                assertThrows(IOException.class, () -> wal.reclaim((record, location) -> record.firstOffset() < 3,
                    CAPACITY));
                var rejected = assertThrows(ExecutionException.class,
                    () -> wal.append(record(4, 1)).get(10, TimeUnit.SECONDS));
                assertFalse(rejected.getCause() instanceof WalCapacityExceededException,
                    "the failed checkpoint must fence writes rather than merely fill the ring");
            }
            try (WalIoBackend backend = backend(aio);
                 var wal = CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0)) {
                List<Long> recovered = new ArrayList<>();
                wal.replay((record, location) -> recovered.add(record.firstOffset()));
                assertTrue(recovered.equals(List.of(0L, 1L, 2L, 3L)) || recovered.equals(List.of(3L)),
                    "only the fully covered prefix may be retired: " + recovered);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void coldFetchAndUploadReuseOneValidatedBlock(boolean aio) throws Exception {
        Path path = dir.resolve("cold-fetch-block-" + aio);
        var partition = new SharedPartitionId(1, 2, 0);
        try (WalIoBackend backend = backend(aio);
             var engine = new SharedStorageEngine(CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0))) {
            engine.appendOwnedBatchGroup(partition, List.of(
                new SharedStorageEngine.OwnedDataBatch(1, 0, 0, ByteBuffer.wrap(new byte[] {10})),
                new SharedStorageEngine.OwnedDataBatch(1, 1, 1, ByteBuffer.wrap(new byte[] {20}))))
                .get(10, TimeUnit.SECONDS);
        }
        try (var backend = new CountingBackend(aio);
             var engine = new SharedStorageEngine(CoordinatedRingWalAdapter.reopen(path, backend, CAPACITY, ALIGNMENT, 0))) {
            backend.countReads.set(true);
            var fetch = engine.readBatches(partition, 0, 10, false);
            assertEquals(2, fetch.batches().size());
            assertEquals(10, fetch.batches().get(0).get());
            assertEquals(20, fetch.batches().get(1).get());
            assertEquals(1, backend.reads.get(), "cold Fetch should decode the shared WAL block once");
            var candidates = engine.uploadCandidates(partition, 0, 2);
            assertEquals(2, engine.readUploadCandidates(candidates).size());
            assertEquals(1, backend.reads.get(), "upload should reuse confirmed Fetch batches");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void batchReadDecodesEachBlockOnceAndValidatesEveryLocation(boolean aio) throws Exception {
        Path path = dir.resolve("batched-block-read-" + aio);
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            var firstGroup = wal.appendBatch(List.of(record(0, 1), record(1, 1))).get(10, TimeUnit.SECONDS);
            var second = wal.append(record(2, 1)).get(10, TimeUnit.SECONDS);
            var first = new WalLocation(firstGroup.get(0).offset(), firstGroup.get(0).length(), 1, 0, 0);
            var next = new WalLocation(firstGroup.get(1).offset(), firstGroup.get(1).length(), 1, 1, 1);
            var last = new WalLocation(second.offset(), second.length(), 1, 2, 2);
            backend.countReads.set(true);
            var loaded = wal.readBatch(List.of(next, last, first, next));
            assertEquals(List.of(1L, 2L, 0L, 1L), loaded.stream().map(WalRecord::firstOffset).toList());
            assertEquals(2, backend.reads.get(), "two blocks should be read once each");
            var mismatched = new WalLocation(next.walOffset(), next.length(), 2, 1, 1);
            assertThrows(WalCorruptionException.class, () -> wal.readBatch(List.of(first, mismatched)));
            var unknown = new WalLocation(first.walOffset() + 1, first.length(), 1, 0, 0);
            assertThrows(IOException.class, () -> wal.readBatch(List.of(first, unknown)));
            backend.failAtRead = backend.reads.get() + 2;
            assertThrows(IOException.class, () -> wal.readBatch(List.of(first, last)));
            backend.failAtRead = 0;
            assertEquals(2L * ALIGNMENT, wal.reclaim((record, location) -> true, CAPACITY));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void batchReadRejectsCorruptLaterBlockWithoutReturningPartialResults(boolean aio) throws Exception {
        Path path = dir.resolve("batched-corrupt-read-" + aio);
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            var first = wal.append(record(0, 1)).get(10, TimeUnit.SECONDS);
            var second = wal.append(record(1, 1)).get(10, TimeUnit.SECONDS);
            var firstLocation = new WalLocation(first.offset(), first.length(), 1, 0, 0);
            var secondLocation = new WalLocation(second.offset(), second.length(), 1, 1, 1);
            try (FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE)) {
                long payload = 4L * ALIGNMENT + AlignedWalBlockCodec.HEADER_BYTES + WalRecordCodec.HEADER_BYTES;
                assertEquals(1, file.write(ByteBuffer.wrap(new byte[] {42}), payload));
                file.force(true);
            }
            assertThrows(IOException.class, () -> wal.readBatch(List.of(firstLocation, secondLocation)));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void batchReclaimWaitsForAnActualReaderBeforeForcingHead(boolean aio) throws Exception {
        Path path = dir.resolve("batch-reader-" + aio);
        var readerExecutor = Executors.newSingleThreadExecutor(task -> new Thread(task, "held-wal-reader"));
        var reclaimExecutor = Executors.newSingleThreadExecutor();
        try (var backend = new CountingBackend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, CAPACITY, ALIGNMENT, 0)) {
            WalAppendResult second = null;
            for (int i = 0; i < 4; i++) {
                var location = wal.append(record(i, 1)).get(10, TimeUnit.SECONDS);
                if (i == 1) second = location;
            }
            var location = new WalLocation(second.offset(), second.length(), 1, 1, 1, 1);
            backend.holdReader.set(true);
            var reader = readerExecutor.submit(() -> wal.readBatch(List.of(location, location)));
            assertTrue(backend.readerEntered.await(10, TimeUnit.SECONDS));
            backend.countForces.set(true);
            var reclaim = reclaimExecutor.submit(() -> wal.reclaim((record, ignored) -> record.firstOffset() < 3,
                CAPACITY));
            assertThrows(TimeoutException.class, () -> reclaim.get(200, TimeUnit.MILLISECONDS));
            assertEquals(0, backend.forces.get(), "head must not be forced while a reader holds a block");
            assertEquals(CAPACITY, wal.usedBytes());
            backend.releaseReader.countDown();
            assertEquals(List.of(1L, 1L), reader.get(10, TimeUnit.SECONDS).stream().map(WalRecord::firstOffset).toList());
            assertEquals(3L * ALIGNMENT, reclaim.get(10, TimeUnit.SECONDS));
            assertEquals(2, backend.forces.get());
        } finally {
            readerExecutor.shutdownNow();
            reclaimExecutor.shutdownNow();
        }
    }

    @Test
    void reclaimContinuesAcrossBoundedBatches() throws Exception {
        long capacity = 72L * ALIGNMENT;
        Path path = dir.resolve("bounded-reclaim");
        try (var backend = new CountingBackend(false);
             var wal = CoordinatedRingWalAdapter.create(path, backend, capacity, ALIGNMENT, 0)) {
            for (int i = 0; i < 70; i++) wal.append(record(i, 1)).get(10, TimeUnit.SECONDS);
            backend.countForces.set(true);
            assertEquals(69L * ALIGNMENT, wal.reclaim((record, location) -> record.firstOffset() < 69, capacity));
            assertEquals(4, backend.forces.get(), "69 blocks need two double-copy head checkpoints");
            assertEquals(ALIGNMENT, wal.usedBytes());
        }
        try (WalIoBackend backend = backend(false);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, capacity, ALIGNMENT, 0)) {
            List<Long> recovered = new ArrayList<>();
            wal.replay((record, location) -> recovered.add(record.firstOffset()));
            assertEquals(List.of(69L), recovered);
        }
    }

    @Test
    void blockRecordEncodingMatchesExistingCodecForDifferentPayloadBuffers() {
        List<WalRecord> records = new ArrayList<>();
        for (int size : List.of(1, 31, 1024, 4097, 16385)) {
            byte[] bytes = new byte[size + 6];
            for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) (i * 73 + size);
            ByteBuffer heap = ByteBuffer.wrap(bytes);
            heap.position(3).limit(size + 3);
            records.add(WalRecord.dataOwned(1, 2, 0, 7, size, size + 1L, heap));
            ByteBuffer direct = ByteBuffer.allocateDirect(size + 6);
            direct.put(bytes).position(3).limit(size + 3);
            records.add(WalRecord.dataOwned(1, 2, 0, 8, size, size + 1L, direct));
        }
        records.add(WalRecord.truncate(1, 2, 0, 9, 123));
        records.add(WalRecord.groupCommit(123, 2));
        for (WalRecord record : records) {
            var old = WalRecordCodec.encode(record);
            ByteBuffer expected = ByteBuffer.allocate(old.totalLength());
            expected.put(old.header()).put(old.payload()).flip();
            ByteBuffer output = ByteBuffer.allocateDirect(old.totalLength() + 13);
            output.position(7);
            assertEquals(old.totalLength(), WalRecordCodec.writeTo(output, record));
            assertEquals(7 + old.totalLength(), output.position());
            output.flip().position(7);
            assertEquals(expected, output.slice(), "block and existing WAL record encoding must be identical");
        }
    }

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
            var reclaimDecoded = AlignedWalBlockCodec.decodeForRemoteReclaim(encoded.bytes(), offset, 7, 8, ALIGNMENT);
            assertEquals(groups.size(), encoded.groups().size());
            assertEquals(groups.size(), decoded.groups().size());
            assertEquals(decoded.groups().size(), reclaimDecoded.groups().size());
            for (int group = 0; group < groups.size(); group++) {
                assertEquals(groups.get(group).records().size(), encoded.groups().get(group).size());
                for (int record = 0; record < groups.get(group).records().size(); record++) {
                    var location = encoded.groups().get(group).get(record);
                    var entry = decoded.groups().get(group).records().get(record);
                    assertEquals(entry.walOffset(), location.offset());
                    assertEquals(entry.length(), location.length());
                    assertEquals(groups.get(group).records().get(record).firstOffset(), entry.record().firstOffset());
                    var borrowed = reclaimDecoded.groups().get(group).records().get(record);
                    assertEquals(entry.walOffset(), borrowed.walOffset());
                    assertEquals(entry.length(), borrowed.length());
                    assertEquals(entry.record().type(), borrowed.record().type());
                    assertEquals(entry.record().firstOffset(), borrowed.record().firstOffset());
                    assertEquals(entry.record().lastOffset(), borrowed.record().lastOffset());
                    assertEquals(entry.record().payload(), borrowed.record().payload());
                }
            }
        }
    }

    @Test
    void remoteReclaimBorrowedDecoderStillChecksRecordChecksum() throws Exception {
        var encoded = AlignedWalBlockCodec.encodeWithLocations(4096, 7, 8, ALIGNMENT, List.of(
            new AlignedWalBlockCodec.Group(1, List.of(record(0, 32)))));
        ByteBuffer changed = ByteBuffer.allocate(encoded.bytes().remaining());
        changed.put(encoded.bytes()).flip();
        int payloadOffset = AlignedWalBlockCodec.HEADER_BYTES + WalRecordCodec.HEADER_BYTES;
        changed.put(payloadOffset, (byte) (changed.get(payloadOffset) ^ 1));
        CRC32C crc = new CRC32C();
        ByteBuffer first = changed.duplicate();
        first.limit(AlignedWalBlockCodec.CRC_OFFSET);
        crc.update(first);
        ByteBuffer second = changed.duplicate();
        second.position(AlignedWalBlockCodec.CRC_OFFSET + Integer.BYTES);
        crc.update(second);
        changed.putInt(AlignedWalBlockCodec.CRC_OFFSET, (int) crc.getValue());

        assertThrows(WalCorruptionException.class, () -> AlignedWalBlockCodec.decode(changed, 4096, 7, 8, ALIGNMENT));
        assertThrows(WalCorruptionException.class,
            () -> AlignedWalBlockCodec.decodeForRemoteReclaim(changed, 4096, 7, 8, ALIGNMENT));
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
        List<WalLocation> batchedLocations = new ArrayList<>();
        List<WalRecord> expectedRecords = new ArrayList<>();
        for (int group = 0; group < groups.size(); group++) {
            assertEquals(groups.get(group).size(), locations.get(group).size());
            for (int i = 0; i < groups.get(group).size(); i++) {
                WalRecord expected = groups.get(group).get(i);
                WalAppendResult result = locations.get(group).get(i);
                WalRecord actual = wal.read(new WalLocation(result.offset(), result.length(),
                    expected.leaderEpoch(), expected.firstOffset(), expected.lastOffset()));
                assertEquals(expected.firstOffset(), actual.firstOffset());
                assertEquals(expected.payload(), actual.payload());
                batchedLocations.add(new WalLocation(result.offset(), result.length(),
                    expected.leaderEpoch(), expected.firstOffset(), expected.lastOffset()));
                expectedRecords.add(expected);
            }
        }
        java.util.Collections.reverse(batchedLocations);
        java.util.Collections.reverse(expectedRecords);
        var batched = wal.readBatch(batchedLocations);
        for (int i = 0; i < batched.size(); i++) {
            assertEquals(expectedRecords.get(i).firstOffset(), batched.get(i).firstOffset());
            assertEquals(expectedRecords.get(i).payload(), batched.get(i).payload());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void finishedWritesShareTheNextDataForce(boolean aio) throws Exception {
        for (boolean failCohort : List.of(false, true)) {
            Path path = dir.resolve("grouped-data-force-" + aio + "-" + failCohort);
            try (var backend = new CountingBackend(aio); var io = backend.create(path)) {
                io.preallocate(CAPACITY);
                var allocator = new BlockRingAllocator(CAPACITY, ALIGNMENT);
                var queued = new CountDownLatch(1);
                var releaseEncoder = new CountDownLatch(1);
                backend.holdFirstForce = true;
                backend.failAtForce = failCohort ? 2 : 0;
                backend.countForces.set(true);
                try (var writer = new ParallelRingBlockWriter(io, allocator, 0, CAPACITY, ALIGNMENT, 7, 8, 2, 8)) {
                    try {
                        var first = writer.appendOwned(ALIGNMENT, offset -> ByteBuffer.allocate(ALIGNMENT));
                        assertTrue(backend.firstForceEntered.await(10, TimeUnit.SECONDS));
                        var second = writer.appendOwned(ALIGNMENT, offset -> ByteBuffer.allocate(ALIGNMENT));
                        var third = writer.appendOwned(ALIGNMENT, offset -> ByteBuffer.allocate(ALIGNMENT));
                        // With one worker in force(), this encoder can run only after the preceding two writes queued.
                        var fourth = writer.appendOwned(ALIGNMENT, offset -> {
                            queued.countDown();
                            try {
                                awaitGate(releaseEncoder);
                            } catch (IOException e) {
                                throw new java.io.UncheckedIOException(e);
                            }
                            return ByteBuffer.allocate(ALIGNMENT);
                        });
                        assertTrue(queued.await(10, TimeUnit.SECONDS));
                        assertFalse(first.isDone());
                        assertFalse(second.isDone());
                        assertFalse(third.isDone());
                        backend.releaseFirstForce.countDown();
                        first.get(10, TimeUnit.SECONDS);
                        if (failCohort) {
                            assertThrows(ExecutionException.class, () -> second.get(10, TimeUnit.SECONDS));
                            assertThrows(ExecutionException.class, () -> third.get(10, TimeUnit.SECONDS));
                        } else {
                            second.get(10, TimeUnit.SECONDS);
                            third.get(10, TimeUnit.SECONDS);
                            assertEquals(3L * ALIGNMENT, allocator.contiguousForcedTail());
                        }
                        assertEquals(2, backend.forces.get());
                        releaseEncoder.countDown();
                        if (failCohort) {
                            assertThrows(ExecutionException.class, () -> fourth.get(10, TimeUnit.SECONDS));
                        } else {
                            fourth.get(10, TimeUnit.SECONDS);
                        }
                    } finally {
                        backend.releaseFirstForce.countDown();
                        releaseEncoder.countDown();
                    }
                }
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
            var gate = new WriteOrderGate(io);
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

    private static final class CountingBackend implements WalIoBackend {
        private final WalIoBackend delegate;
        private final AtomicBoolean countForces = new AtomicBoolean();
        private final AtomicInteger forces = new AtomicInteger();
        private final AtomicBoolean countWrites = new AtomicBoolean();
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicBoolean countReads = new AtomicBoolean();
        private final AtomicInteger reads = new AtomicInteger();
        private final AtomicInteger maxReadRequest = new AtomicInteger();
        private volatile int failAtWrite;
        private volatile int failAtRead;
        private volatile long failReadAtPosition = -1;
        private volatile int failAtForce;
        private volatile int maxWriteBytes;
        private boolean holdFirstForce;
        private final CountDownLatch firstForceEntered = new CountDownLatch(1);
        private final CountDownLatch releaseFirstForce = new CountDownLatch(1);
        private final AtomicBoolean holdReader = new AtomicBoolean();
        private final CountDownLatch readerEntered = new CountDownLatch(1);
        private final CountDownLatch releaseReader = new CountDownLatch(1);

        private CountingBackend(boolean aio) {
            delegate = backend(aio);
        }

        @Override
        public Handle openRead(Path path) throws IOException {
            return wrap(delegate.openRead(path));
        }

        @Override
        public Handle reopen(Path path) throws IOException {
            return wrap(delegate.reopen(path));
        }

        @Override
        public Handle create(Path path) throws IOException {
            return wrap(delegate.create(path));
        }

        @Override
        public long size(Path path) throws IOException {
            return delegate.size(path);
        }

        @Override
        public boolean supportsPreallocation() {
            return delegate.supportsPreallocation();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private Handle wrap(Handle handle) {
            return new Handle() {
                @Override
                public long size() throws IOException {
                    return handle.size();
                }

                @Override
                public int read(ByteBuffer dst, long position) throws IOException {
                    if (position == failReadAtPosition) throw new IOException("injected WAL read failure");
                    if (countReads.get()) {
                        maxReadRequest.accumulateAndGet(dst.remaining(), Math::max);
                        if (reads.incrementAndGet() == failAtRead) throw new IOException("injected WAL read failure");
                    }
                    if (holdReader.get() && Thread.currentThread().getName().equals("held-wal-reader")) {
                        readerEntered.countDown();
                        try {
                            if (!releaseReader.await(10, TimeUnit.SECONDS))
                                throw new IOException("timed out waiting for reader release");
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("reader interrupted", e);
                        }
                    }
                    return handle.read(dst, position);
                }

                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    if (countWrites.get() && writes.incrementAndGet() == failAtWrite)
                        throw new IOException("injected WAL write failure");
                    int originalLimit = src.limit();
                    try {
                        if (maxWriteBytes > 0) src.limit(Math.min(originalLimit, src.position() + maxWriteBytes));
                        return handle.write(src, position);
                    } finally {
                        src.limit(originalLimit);
                    }
                }

                @Override
                public void truncate(long size) throws IOException {
                    handle.truncate(size);
                }

                @Override
                public void preallocate(long size) throws IOException {
                    handle.preallocate(size);
                }

                @Override
                public void force() throws IOException {
                    if (countForces.get()) {
                        int count = forces.incrementAndGet();
                        if (holdFirstForce && count == 1) {
                            firstForceEntered.countDown();
                            awaitGate(releaseFirstForce);
                        }
                        if (count == failAtForce) throw new IOException("injected WAL force failure");
                    }
                    handle.force();
                }

                @Override
                public void seal() throws IOException {
                    handle.seal();
                }

                @Override
                public void close() throws IOException {
                    handle.close();
                }
            };
        }
    }

    private static void awaitGate(CountDownLatch gate) throws IOException {
        try {
            if (!gate.await(10, TimeUnit.SECONDS)) throw new IOException("WAL I/O gate timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("WAL I/O gate interrupted", e);
        }
    }

    private static final class WriteOrderGate implements WalIoBackend.Handle {
        private final WalIoBackend.Handle delegate;
        private final long firstBlockPosition;
        private final CountDownLatch earlierEntered = new CountDownLatch(1);
        private final CountDownLatch laterForced = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

        private WriteOrderGate(WalIoBackend.Handle delegate) {
            this(delegate, 3L * ALIGNMENT);
        }

        private WriteOrderGate(WalIoBackend.Handle delegate, long firstBlockPosition) {
            this.delegate = delegate;
            this.firstBlockPosition = firstBlockPosition;
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
            if (position == firstBlockPosition) {
                earlierEntered.countDown();
                awaitGate(release);
                throw new IOException("injected earlier block write failure");
            }
            return delegate.write(src, position);
        }

        @Override
        public void truncate(long size) throws IOException {
            delegate.truncate(size);
        }

        @Override
        public void force() throws IOException {
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
