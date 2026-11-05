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

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.RingSharedWal;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalAppendResult;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;
import org.apache.kafka.storage.internals.shared.wal.WalReplayConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Startup replay diagnostics must come from the WAL's recovered groups, never caller assertions. */
class StartupWalReplayEvidenceTest {
    private static final SharedPartitionId FIRST = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId SECOND = new SharedPartitionId(1L, 2L, 1);

    @TempDir
    Path tempDir;

    @Test
    void emptyBackendHasNoManufacturedReplayHorizon() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            assertEquals(StartupWalReplayEvidence.EMPTY, engine.startupWalReplayEvidence());
            assertTrue(engine.startupWalReplayEvidence().partition(FIRST).isEmpty());
            assertEquals(-1L, engine.startupWalReplayEvidence().highestWalEndExclusive());
        }
    }

    @Test
    void fileBackendReopenInventoriesOnlyCommittedDataAndTruncate() throws Exception {
        Path dir = tempDir.resolve("file-recovered");
        WalAppendResult truncate;
        try (FileSharedWal wal = new FileSharedWal(dir, 1024 * 1024, 4096)) {
            wal.appendBatch(List.of(data(FIRST, 0L), data(SECOND, 10L))).get(10L, TimeUnit.SECONDS);
            truncate = wal.append(WalRecord.truncate(1L, 2L, 0, 2, 0L)).get(10L, TimeUnit.SECONDS);
        }
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            StartupWalReplayEvidence evidence = engine.startupWalReplayEvidence();
            assertEquals(3L, evidence.replayedRecords());
            assertEquals(truncate.offset() + truncate.length(), evidence.highestWalEndExclusive());
            assertEquals(2L, evidence.partition(FIRST).orElseThrow().replayedRecords());
            assertEquals(1L, evidence.partition(FIRST).orElseThrow().dataRecords());
            assertEquals(1L, evidence.partition(FIRST).orElseThrow().truncateRecords());
            assertEquals(1L, evidence.partition(SECOND).orElseThrow().dataRecords());
            assertTrue(engine.localLocations(FIRST, 0L, 1L).isEmpty());
        }
    }

    @Test
    void ringBackendReopenInventoriesCheckpointedDataAndTruncate() throws Exception {
        Path path = tempDir.resolve("ring-recovered.wal");
        WalAppendResult truncate;
        try (RingSharedWal wal = new RingSharedWal(path, 1024L * 1024L)) {
            wal.appendBatch(List.of(data(FIRST, 0L), data(SECOND, 10L))).get(10L, TimeUnit.SECONDS);
            truncate = wal.append(WalRecord.truncate(1L, 2L, 0, 2, 0L)).get(10L, TimeUnit.SECONDS);
        }
        try (SharedStorageEngine engine = new SharedStorageEngine(new RingSharedWal(path, 1024L * 1024L))) {
            StartupWalReplayEvidence evidence = engine.startupWalReplayEvidence();
            assertEquals(3L, evidence.replayedRecords());
            assertEquals(truncate.offset() + truncate.length(), evidence.highestWalEndExclusive());
            assertEquals(1L, evidence.partition(FIRST).orElseThrow().truncateRecords());
            assertEquals(1L, evidence.partition(SECOND).orElseThrow().dataRecords());
            assertTrue(engine.localLocations(FIRST, 0L, 1L).isEmpty());
        }
    }

    @Test
    void subsequentLiveAppendCannotChangeFrozenStartupEvidence() throws Exception {
        Path dir = tempDir.resolve("file-live");
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            StartupWalReplayEvidence startup = engine.startupWalReplayEvidence();
            assertSame(StartupWalReplayEvidence.EMPTY, startup);
            engine.appendData(FIRST, 3, 0L, 0L, ByteBuffer.wrap(new byte[]{1})).join();
            assertSame(startup, engine.startupWalReplayEvidence());
            assertEquals(0L, startup.replayedRecords());
        }
        try (SharedStorageEngine restarted = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            assertEquals(1L, restarted.startupWalReplayEvidence().replayedRecords());
            assertEquals(1L, restarted.startupWalReplayEvidence().partition(FIRST).orElseThrow().dataRecords());
        }
    }

    @Test
    void malformedBackendReplayOrderClosesWalWithoutReturningEvidence() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        doAnswer(invocation -> {
            WalReplayConsumer replay = invocation.getArgument(0);
            replay.accept(data(FIRST, 0L), new WalAppendResult(200L, 61));
            replay.accept(data(FIRST, 1L), new WalAppendResult(100L, 61));
            return null;
        }).when(wal).replay(any(WalReplayConsumer.class));

        IllegalStateException invalid = assertThrows(
            IllegalStateException.class, () -> new SharedStorageEngine(wal)
        );
        assertTrue(invalid.getMessage().contains("replay regressed"));
        verify(wal).close();
    }

    @Test
    void missingFileGroupCommitCannotInventARecoveredRecord() throws Exception {
        Path dir = tempDir.resolve("file-incomplete-commit");
        try (FileSharedWal wal = new FileSharedWal(dir, 1024 * 1024, 200)) {
            wal.appendBatch(List.of(
                WalRecord.data(1L, 2L, 0, 1, 0L, 0L, new byte[100]),
                WalRecord.data(1L, 2L, 0, 1, 1L, 1L, new byte[100])
            )).get(10L, TimeUnit.SECONDS);
        }
        Path commitSegment = dir.resolve("wal-00000000000000000002.log");
        try (FileChannel channel = FileChannel.open(commitSegment, StandardOpenOption.WRITE)) {
            channel.truncate(0L);
            channel.force(false);
        }
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 200))) {
            assertEquals(StartupWalReplayEvidence.EMPTY, engine.startupWalReplayEvidence());
            assertTrue(engine.localLocations(FIRST, 0L, 2L).isEmpty());
        }
    }

    @Test
    void returnedPartitionSummaryIsAnImmutableDefensiveCopy() {
        StartupWalReplayEvidence.Builder builder = new StartupWalReplayEvidence.Builder();
        builder.accept(data(FIRST, 0L), new WalAppendResult(0L, 61));
        StartupWalReplayEvidence firstSnapshot = builder.snapshot();
        builder.accept(data(SECOND, 0L), new WalAppendResult(61L, 61));

        assertEquals(1L, firstSnapshot.replayedRecords());
        assertFalse(firstSnapshot.partition(FIRST).isEmpty());
        assertTrue(firstSnapshot.partition(SECOND).isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> firstSnapshot.partitions().clear());
        assertThrows(IllegalArgumentException.class, () ->
            new StartupWalReplayEvidence.PartitionSummary(2L, 1L, 0L, 61L));
    }

    private static WalRecord data(SharedPartitionId partition, long offset) {
        return WalRecord.data(partition.topicIdHigh(), partition.topicIdLow(),
            partition.partition(), 1, offset, offset, new byte[]{1});
    }
}
