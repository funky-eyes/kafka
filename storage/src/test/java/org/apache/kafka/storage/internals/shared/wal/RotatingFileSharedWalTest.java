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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RotatingFileSharedWalTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldReleaseCapacityAndContinueAppendingAfterSafePrefixReclaim() throws Exception {
        Path walDir = tempDir.resolve("rotating-capacity");
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, 512, 256)) {
            append(wal, 0, 100);
            append(wal, 1, 100);
            long beforeReclaim = wal.usedBytes();

            ExecutionException full = assertThrows(ExecutionException.class, () -> append(wal, 2, 100));
            assertTrue(full.getCause() instanceof WalCapacityExceededException);

            long reclaimed = wal.reclaim((record, ignored) -> record.lastOffset() == 0, 1L);
            assertTrue(reclaimed > 0, "a complete remotely safe prefix segment must be physically released");
            assertTrue(wal.usedBytes() < beforeReclaim);

            append(wal, 2, 100);
            assertTrue(wal.usedBytes() <= wal.capacityBytes());
        }

        List<Long> replayed = replayOffsets(walDir, 512, 256);
        assertEquals(List.of(1L, 2L), replayed);
    }

    @Test
    void shouldNeverDeletePartOfAnAppendGroupSpanningSegments() throws Exception {
        Path walDir = tempDir.resolve("rotating-group-boundary");
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, 1024, 200)) {
            wal.appendBatch(List.of(
                WalRecord.data(1, 2, 0, 1, 0, 0, new byte[100]),
                WalRecord.data(1, 2, 0, 1, 1, 1, new byte[100])
            )).get(10, TimeUnit.SECONDS);

            long before = wal.usedBytes();
            long reclaimed = wal.reclaim((record, ignored) -> record.lastOffset() == 0, 1L);
            assertEquals(0L, reclaimed,
                "one reclaimable record must not allow deletion of a physical prefix containing half an append group");
            assertEquals(before, wal.usedBytes());
        }

        assertEquals(List.of(0L, 1L), replayOffsets(walDir, 1024, 200));
    }

    @Test
    void shouldPassOnlyUserRecordsToReclaimPolicy() throws Exception {
        Path walDir = tempDir.resolve("rotating-policy");
        AtomicBoolean sawCommit = new AtomicBoolean();
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, 1024, 256)) {
            append(wal, 0, 50);
            long reclaimed = wal.reclaim((record, ignored) -> {
                if (record.type() == WalRecordType.GROUP_COMMIT) {
                    sawCommit.set(true);
                }
                return true;
            }, 1L);
            assertTrue(reclaimed > 0);
        }
        assertFalse(sawCommit.get(), "GROUP_COMMIT is an internal framing record, not a reclaim-policy input");
    }

    @Test
    void shouldKeepPhysicalPrefixWhenOldestGroupIsNotReclaimable() throws Exception {
        Path walDir = tempDir.resolve("rotating-prefix");
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, 1024, 256)) {
            append(wal, 0, 100);
            append(wal, 1, 100);
            long before = wal.usedBytes();

            long reclaimed = wal.reclaim((record, ignored) -> record.firstOffset() > 0, 1L);
            assertEquals(0L, reclaimed, "reclamation may never jump over an unsafe oldest group");
            assertEquals(before, wal.usedBytes());
        }
        assertEquals(List.of(0L, 1L), replayOffsets(walDir, 1024, 256));
    }

    @Test
    void shouldLeaveDirectoryAndReopenedWalConsistentAfterDeletingAllSafeSegments() throws Exception {
        Path walDir = tempDir.resolve("rotating-all");
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, 512, 256)) {
            append(wal, 0, 50);
            long reclaimed = wal.reclaim((record, ignored) -> true, Long.MAX_VALUE);
            assertTrue(reclaimed > 0);
            assertEquals(0L, wal.usedBytes());
            assertEquals(0L, segmentCount(walDir));

            append(wal, 1, 50);
            assertTrue(wal.usedBytes() > 0);
        }
        assertEquals(List.of(1L), replayOffsets(walDir, 512, 256));
    }

    @Test
    void shouldRetainRemoteCommittedWalBelowPressureHighWatermark() throws Exception {
        Path walDir = tempDir.resolve("rotating-retention-window");
        long before;
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, 4096, 512)) {
            append(wal, 0, 200);
            append(wal, 1, 200);
            before = wal.usedBytes();
            assertTrue(before < percentage(4096, RotatingFileSharedWal.DEFAULT_RECLAIM_HIGH_WATERMARK_PERCENT));

            assertEquals(0L, wal.reclaim((record, ignored) -> true),
                "remote commit alone must not eagerly discard the local recovery window");
            assertEquals(before, wal.usedBytes());
        }
        assertEquals(List.of(0L, 1L), replayOffsets(walDir, 4096, 512));
    }

    @Test
    void shouldReclaimOnlyEnoughSafePrefixToReturnToLowWatermark() throws Exception {
        Path walDir = tempDir.resolve("rotating-pressure-window");
        long capacity = 4096L;
        long segmentBytes = 512L;
        List<Long> appended = new ArrayList<>();
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, capacity, segmentBytes)) {
            long high = percentage(capacity, RotatingFileSharedWal.DEFAULT_RECLAIM_HIGH_WATERMARK_PERCENT);
            for (long offset = 0; wal.usedBytes() < high; offset++) {
                append(wal, offset, 200);
                appended.add(offset);
            }
            long before = wal.usedBytes();
            assertTrue(before >= high);

            long reclaimed = wal.reclaim((record, ignored) -> true);
            assertTrue(reclaimed > 0);
            long low = percentage(capacity, RotatingFileSharedWal.DEFAULT_RECLAIM_LOW_WATERMARK_PERCENT);
            assertTrue(wal.usedBytes() <= low,
                "bounded reclamation should create the requested recovery headroom");
            assertTrue(wal.usedBytes() > 0,
                "pressure reclamation must retain newer WAL instead of eagerly clearing every safe segment");
        }

        List<Long> replayed = replayOffsets(walDir, capacity, segmentBytes);
        assertFalse(replayed.isEmpty());
        assertTrue(replayed.get(0) > appended.get(0), "oldest safe WAL prefix should have rotated away");
        assertEquals(appended.get(appended.size() - 1), replayed.get(replayed.size() - 1));
    }

    private static void append(RotatingFileSharedWal wal, long offset, int payloadBytes) throws Exception {
        wal.append(WalRecord.data(1, 2, 0, 1, offset, offset, new byte[payloadBytes]))
            .get(10, TimeUnit.SECONDS);
    }

    private static List<Long> replayOffsets(Path walDir, long capacity, long segmentBytes) throws Exception {
        List<Long> offsets = new ArrayList<>();
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(walDir, capacity, segmentBytes)) {
            wal.replay((record, ignored) -> offsets.add(record.firstOffset()));
        }
        return List.copyOf(offsets);
    }

    private static long segmentCount(Path walDir) throws Exception {
        try (var files = Files.list(walDir)) {
            return files.filter(path -> path.getFileName().toString().startsWith("wal-"))
                .filter(path -> path.getFileName().toString().endsWith(".log"))
                .count();
        }
    }

    private static long percentage(long value, int percent) {
        return value * percent / 100L;
    }
}
