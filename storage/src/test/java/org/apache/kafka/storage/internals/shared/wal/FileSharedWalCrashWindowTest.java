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

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash-artifact gate for every persistence window around a WAL append group.
 *
 * <p>These tests deliberately mutate the durable tail to the byte layouts a process/power failure may leave behind.
 * They are distinct from the independent-process SIGKILL gate: their purpose is deterministic coverage of recovery's
 * byte-boundary state machine, including windows that are difficult to hit reliably with timing-based process kills.</p>
 */
class FileSharedWalCrashWindowTest {
    private static final long CAPACITY_BYTES = 1024 * 1024;
    private static final long SEGMENT_BYTES = 4096;

    @TempDir
    Path tempDir;

    @Test
    void incompleteFinalGroupIsNeverPartiallyVisibleAtAnyRecordOrCommitBoundary() throws Exception {
        Path pristine = tempDir.resolve("pristine");
        CandidateLayout layout = writePristineSingleSegmentWal(pristine);

        List<Long> crashCuts = List.of(
            layout.groupStart() + 1,
            layout.groupStart() + WalRecordCodec.PREFIX_BYTES - 1L,
            layout.groupStart() + WalRecordCodec.PREFIX_BYTES,
            layout.firstRecordEnd(),
            layout.firstRecordEnd() + 1,
            layout.secondRecordEnd(),
            layout.commitStart() + 1,
            layout.commitStart() + WalRecordCodec.PREFIX_BYTES - 1L,
            layout.commitStart() + WalRecordCodec.PREFIX_BYTES,
            layout.commitEnd() - 1
        );

        for (int i = 0; i < crashCuts.size(); i++) {
            long cut = crashCuts.get(i);
            Path caseDir = tempDir.resolve("cut-" + i);
            copyWal(pristine, caseDir);
            truncate(caseDir.resolve(layout.segmentFileName()), cut);

            assertRecoveredPrefixOnly(caseDir, layout.groupStart(), "cut=" + cut);
            assertAppendableAfterRecovery(caseDir, "cut=" + cut);
        }
    }

    @Test
    void completeCommitMarkerMakesWholeGroupVisible() throws Exception {
        Path walDir = tempDir.resolve("complete");
        CandidateLayout layout = writePristineSingleSegmentWal(walDir);
        truncate(walDir.resolve(layout.segmentFileName()), layout.commitEnd());

        List<WalRecord> replayed = replay(walDir, SEGMENT_BYTES);
        assertEquals(List.of(0L, 10L, 11L), dataOffsets(replayed));
    }

    @Test
    void partialCommitInLaterSegmentDropsWholeCrossSegmentGroup() throws Exception {
        Path pristine = tempDir.resolve("cross-segment-pristine");
        try (FileSharedWal wal = new FileSharedWal(pristine, CAPACITY_BYTES, 200)) {
            wal.appendBatch(List.of(
                data(30L, new byte[100]),
                data(31L, new byte[100])
            )).get(10, TimeUnit.SECONDS);
        }

        Path commitSegment = pristine.resolve("wal-00000000000000000002.log");
        assertTrue(Files.exists(commitSegment));
        long commitLength = Files.size(commitSegment);
        assertEquals(WalRecordCodec.HEADER_BYTES, commitLength);

        for (long cut : List.of(1L, (long) WalRecordCodec.PREFIX_BYTES, commitLength - 1L)) {
            Path caseDir = tempDir.resolve("cross-segment-cut-" + cut);
            copyWal(pristine, caseDir);
            truncate(caseDir.resolve(commitSegment.getFileName()), cut);

            List<WalRecord> replayed;
            try (FileSharedWal wal = new FileSharedWal(caseDir, CAPACITY_BYTES, 200)) {
                replayed = replay(wal);
            }
            assertTrue(replayed.isEmpty(), "partial cross-segment group became visible at cut=" + cut);
            assertEquals(0L, Files.size(caseDir.resolve("wal-00000000000000000000.log")));
            assertFalse(Files.exists(caseDir.resolve("wal-00000000000000000001.log")));
            assertFalse(Files.exists(caseDir.resolve("wal-00000000000000000002.log")));
        }
    }

    private CandidateLayout writePristineSingleSegmentWal(Path walDir) throws Exception {
        List<WalAppendResult> candidate;
        try (FileSharedWal wal = new FileSharedWal(walDir, CAPACITY_BYTES, SEGMENT_BYTES)) {
            wal.append(data(0L, new byte[] {1})).get(10, TimeUnit.SECONDS);
            candidate = wal.appendBatch(List.of(
                data(10L, new byte[20]),
                data(11L, new byte[20])
            )).get(10, TimeUnit.SECONDS);
        }

        assertEquals(2, candidate.size());
        WalAppendResult first = candidate.get(0);
        WalAppendResult second = candidate.get(1);
        assertEquals(first.segmentId(), second.segmentId());
        long firstRecordEnd = first.position() + first.length();
        assertEquals(firstRecordEnd, second.position());
        long secondRecordEnd = second.position() + second.length();
        long commitStart = secondRecordEnd;
        long commitEnd = commitStart + WalRecordCodec.HEADER_BYTES;
        Path segment = walDir.resolve(String.format("wal-%020d.log", first.segmentId()));
        assertEquals(commitEnd, Files.size(segment));
        return new CandidateLayout(
            segment.getFileName(),
            first.position(),
            firstRecordEnd,
            secondRecordEnd,
            commitStart,
            commitEnd
        );
    }

    private void assertRecoveredPrefixOnly(Path walDir, long expectedSize, String description) throws Exception {
        List<WalRecord> replayed;
        try (FileSharedWal wal = new FileSharedWal(walDir, CAPACITY_BYTES, SEGMENT_BYTES)) {
            replayed = replay(wal);
        }
        assertEquals(List.of(0L), dataOffsets(replayed), description);
        assertEquals(expectedSize, Files.size(walDir.resolve("wal-00000000000000000000.log")), description);
    }

    private void assertAppendableAfterRecovery(Path walDir, String description) throws Exception {
        try (FileSharedWal wal = new FileSharedWal(walDir, CAPACITY_BYTES, SEGMENT_BYTES)) {
            wal.append(data(20L, new byte[] {2})).get(10, TimeUnit.SECONDS);
        }
        assertEquals(List.of(0L, 20L), dataOffsets(replay(walDir, SEGMENT_BYTES)), description);
    }

    private static WalRecord data(long offset, byte[] payload) {
        return WalRecord.data(1L, 2L, 0, 3, offset, offset, payload);
    }

    private static List<WalRecord> replay(Path walDir, long segmentBytes) throws Exception {
        try (FileSharedWal wal = new FileSharedWal(walDir, CAPACITY_BYTES, segmentBytes)) {
            return replay(wal);
        }
    }

    private static List<WalRecord> replay(FileSharedWal wal) throws Exception {
        List<WalRecord> replayed = new ArrayList<>();
        wal.replay((record, ignored) -> replayed.add(record));
        return replayed;
    }

    private static List<Long> dataOffsets(List<WalRecord> records) {
        return records.stream()
            .filter(record -> record.type() == WalRecordType.DATA)
            .map(WalRecord::firstOffset)
            .toList();
    }

    private static void truncate(Path segment, long size) throws Exception {
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE)) {
            channel.truncate(size);
            channel.force(false);
        }
    }

    private static void copyWal(Path source, Path target) throws Exception {
        Files.createDirectories(target);
        try (Stream<Path> files = Files.list(source)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                Files.copy(file, target.resolve(file.getFileName()));
            }
        }
    }

    private record CandidateLayout(
        Path segmentFileName,
        long groupStart,
        long firstRecordEnd,
        long secondRecordEnd,
        long commitStart,
        long commitEnd
    ) {
    }
}
