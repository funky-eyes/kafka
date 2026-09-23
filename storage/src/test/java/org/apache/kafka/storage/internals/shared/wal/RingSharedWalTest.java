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

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingSharedWalTest {
    private static final long DATA_CAPACITY = 256L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void reportsSuccessfulDurabilityBatchStats() throws Exception {
        Path path = tempDir.resolve("durability-stats.wal");
        long totalCapacity = RingWalLayout.DATA_START + 4096L;

        try (RingSharedWal wal = new RingSharedWal(path, totalCapacity)) {
            assertEquals(WalDurabilityStats.EMPTY, wal.durabilityStats());

            wal.appendBatch(List.of(dataRecord(0L, 32), dataRecord(1L, 32))).join();
            WalDurabilityStats first = wal.durabilityStats();
            assertEquals(1L, first.durabilityBatchCount());
            assertEquals(1L, first.durableAppendGroupCount());
            assertEquals(wal.usedBytes(), first.durableBytes());
            assertTrue(first.durabilityBarrierNanos() >= 0L);
            assertEquals(1L, first.maxGroupsPerDurabilityBatch());
            assertEquals(1L, first.singletonCoalesceWaitCount());
            assertEquals(0L, first.singletonCoalesceHitCount());
            assertTrue(first.singletonCoalesceWaitNanos() >= 0L);
            assertEquals(0L, first.appendInterArrivalCount());

            wal.appendBatch(List.of(dataRecord(2L, 32))).join();
            WalDurabilityStats second = wal.durabilityStats();
            assertEquals(2L, second.durabilityBatchCount());
            assertEquals(2L, second.durableAppendGroupCount());
            assertEquals(wal.usedBytes(), second.durableBytes());
            assertTrue(second.durabilityBarrierNanos() >= first.durabilityBarrierNanos());
            assertEquals(1L, second.maxGroupsPerDurabilityBatch());
            assertEquals(2L, second.singletonCoalesceWaitCount());
            assertEquals(0L, second.singletonCoalesceHitCount());
            assertTrue(second.singletonCoalesceWaitNanos() >= first.singletonCoalesceWaitNanos());
            assertEquals(1L, second.appendInterArrivalCount());
            assertTrue(second.appendInterArrivalNanos() >= 0L);
        }
    }

    @Test
    void coalescesNearConcurrentSingletonGroupsBehindOneDurabilityBarrier() throws Exception {
        Path path = tempDir.resolve("durability-coalescing.wal");
        long totalCapacity = RingWalLayout.DATA_START + 4096L;

        try (RingSharedWal wal = new RingSharedWal(
            path,
            totalCapacity,
            new FileChannelWalIoBackend(),
            TimeUnit.SECONDS.toMillis(30L),
            TimeUnit.MILLISECONDS.toNanos(50L)
        )) {
            CompletableFuture<List<WalAppendResult>> first = wal.appendBatch(List.of(dataRecord(0L, 32)));
            CompletableFuture<List<WalAppendResult>> second = wal.appendBatch(List.of(dataRecord(1L, 32)));

            assertEquals(1, first.get(10, TimeUnit.SECONDS).size());
            assertEquals(1, second.get(10, TimeUnit.SECONDS).size());

            WalDurabilityStats stats = wal.durabilityStats();
            assertEquals(1L, stats.durabilityBatchCount());
            assertEquals(2L, stats.durableAppendGroupCount());
            assertEquals(2L, stats.maxGroupsPerDurabilityBatch());
            assertTrue(stats.singletonCoalesceHitCount() <= stats.singletonCoalesceWaitCount());
            assertTrue(stats.singletonCoalesceWaitNanos() >= 0L);
            assertEquals(1L, stats.appendInterArrivalCount());
            assertTrue(stats.appendInterArrivalNanos() >= 0L);
        }
    }

    @Test
    void replaysAcrossWrapAndClearsStalePhysicalPadding() throws Exception {
        Path path = tempDir.resolve("shared-ring.wal");
        WalRecord first = dataRecord(0L, 10);
        WalRecord second = dataRecord(10L, 40);

        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY)) {
            List<WalAppendResult> firstResult = wal.appendBatch(List.of(first)).join();
            assertEquals(1, firstResult.size());
            assertEquals(130L, wal.usedBytes());
            assertEquals(130L, wal.reclaim((record, ignored) -> true));
            assertEquals(130L, wal.reclaimedBeforeOffset());
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer stale = ByteBuffer.allocate(26);
            while (stale.hasRemaining()) {
                stale.put((byte) 0x5a);
            }
            stale.flip();
            channel.write(stale, RingWalLayout.DATA_START + 230L);
            channel.force(false);
        }

        WalAppendResult secondResult;
        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY)) {
            secondResult = wal.appendBatch(List.of(second)).join().get(0);
            assertEquals(130L, secondResult.offset());
            assertEquals(100, secondResult.length());
            assertRecordEquals(second, wal.read(location(second, secondResult)));
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer padding = ByteBuffer.allocate(26);
            channel.read(padding, RingWalLayout.DATA_START + 230L);
            padding.flip();
            while (padding.hasRemaining()) {
                assertEquals(0, padding.get());
            }
        }

        try (RingSharedWal reopened = new RingSharedWal(path, TOTAL_CAPACITY)) {
            List<WalRecord> replayed = new ArrayList<>();
            reopened.replay((record, ignored) -> replayed.add(record));
            assertEquals(1, replayed.size());
            assertRecordEquals(second, replayed.get(0));
            assertRecordEquals(second, reopened.read(location(second, secondResult)));
            assertEquals(186L, reopened.usedBytes());
        }
    }

    @Test
    void reclaimStopsBeforeFirstUnsafeAppendGroup() throws Exception {
        Path path = tempDir.resolve("reclaim-ring.wal");
        long totalCapacity = RingWalLayout.DATA_START + 1024L;
        WalRecord first = dataRecord(0L, 8);
        WalRecord second = dataRecord(1L, 8);

        try (RingSharedWal wal = new RingSharedWal(path, totalCapacity)) {
            WalAppendResult firstResult = wal.appendBatch(List.of(first)).join().get(0);
            WalAppendResult secondResult = wal.appendBatch(List.of(second)).join().get(0);

            long reclaimed = wal.reclaim((record, ignored) -> record.firstOffset() == 0L, Long.MAX_VALUE);
            long firstGroupEnd = firstResult.offset() + firstResult.length() + WalRecordCodec.MIN_RECORD_BYTES;
            assertEquals(firstGroupEnd, reclaimed);
            assertEquals(firstGroupEnd, wal.reclaimedBeforeOffset());
            assertThrows(IllegalArgumentException.class, () -> wal.read(location(first, firstResult)));
            assertRecordEquals(second, wal.read(location(second, secondResult)));

            List<WalRecord> replayed = new ArrayList<>();
            wal.replay((record, ignored) -> replayed.add(record));
            assertEquals(1, replayed.size());
            assertRecordEquals(second, replayed.get(0));
        }
    }

    @Test
    void rejectsWholeAppendGroupWhenCommitCannotFit() throws Exception {
        Path path = tempDir.resolve("capacity-ring.wal");
        long totalCapacity = RingWalLayout.DATA_START + 180L;
        WalRecord record = dataRecord(0L, 80);

        try (RingSharedWal wal = new RingSharedWal(path, totalCapacity)) {
            CompletionException failure = assertThrows(
                CompletionException.class,
                () -> wal.appendBatch(List.of(record)).join()
            );
            assertInstanceOf(WalCapacityExceededException.class, failure.getCause());
            assertEquals(0L, wal.usedBytes());

            List<WalRecord> replayed = new ArrayList<>();
            wal.replay((replayedRecord, ignored) -> replayed.add(replayedRecord));
            assertEquals(List.of(), replayed);
        }
    }

    private static WalRecord dataRecord(long offset, int payloadBytes) {
        byte[] payload = new byte[payloadBytes];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (offset + i + 1);
        }
        return WalRecord.data(1L, 2L, 0, 3, offset, offset, payload);
    }

    private static WalLocation location(WalRecord record, WalAppendResult result) {
        return new WalLocation(
            result.offset(),
            result.length(),
            record.payload().remaining(),
            record.leaderEpoch(),
            record.firstOffset(),
            record.lastOffset()
        );
    }

    private static void assertRecordEquals(WalRecord expected, WalRecord actual) {
        assertEquals(expected.type(), actual.type());
        assertEquals(expected.topicIdHigh(), actual.topicIdHigh());
        assertEquals(expected.topicIdLow(), actual.topicIdLow());
        assertEquals(expected.partition(), actual.partition());
        assertEquals(expected.leaderEpoch(), actual.leaderEpoch());
        assertEquals(expected.firstOffset(), actual.firstOffset());
        assertEquals(expected.lastOffset(), actual.lastOffset());
        assertArrayEquals(bytes(expected.payload()), bytes(actual.payload()));
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
