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
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LocalWalDurabilityObservationTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId OTHER = new SharedPartitionId(1L, 2L, 1);

    @TempDir
    Path tempDir;

    @Test
    void pendingDataFutureDoesNotPublishAFalseDurabilityCompletion() throws Exception {
        CompletableFuture<WalAppendResult> backend = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(backend)) {
            var append = engine.appendData(PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1}));
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, engine.liveWalDurabilityObservation(PARTITION));
            assertEquals(-1L, engine.walAdmissionRevision(PARTITION));
            backend.complete(new WalAppendResult(1L, 61));
            append.join();
            var completed = engine.liveWalDurabilityObservation(PARTITION);
            assertEquals(1L, completed.dataBatches());
            assertEquals(0L, completed.dataGroups());
            assertEquals(62L, completed.highestSuccessfulWalEndExclusive());
            assertEquals(-1L, completed.highestTruncateWalEndExclusive());
        }
    }

    @Test
    void failedDurabilityFutureNeverCreatesALiveSuccessReceipt() throws Exception {
        CompletableFuture<WalAppendResult> backend = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(backend)) {
            var append = engine.appendOwnedData(PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1}));
            backend.completeExceptionally(new IOException("force failed"));
            assertThrows(CompletionException.class, append::join);
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, engine.liveWalDurabilityObservation(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void malformedDurableWalResultCannotCreateAReceiptOrIndex() throws Exception {
        CompletableFuture<WalAppendResult> backend = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(backend)) {
            var append = engine.appendData(PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1}));
            backend.complete(new WalAppendResult(1L, 62));
            assertThrows(CompletionException.class, append::join);
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, engine.liveWalDurabilityObservation(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void ownedDataGroupCountsOnlyAfterTheWholeDurableGroupPublishes() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = new CompletableFuture<>();
        SharedWal wal = mock(SharedWal.class);
        when(wal.appendBatch(anyList())).thenReturn(backend);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            var append = engine.appendOwnedBatchGroup(PARTITION, List.of(
                batch(0L, 9L), batch(10L, 19L)
            ));
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, engine.liveWalDurabilityObservation(PARTITION));
            backend.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(62L, 61)));
            assertEquals(2, append.join().size());
            var success = engine.liveWalDurabilityObservation(PARTITION);
            assertEquals(2L, success.dataBatches());
            assertEquals(1L, success.dataGroups());
            assertEquals(123L, success.highestSuccessfulWalEndExclusive());
            assertEquals(0L, success.truncates());
        }
    }

    @Test
    void rejectedPartialGroupResultsCannotClaimDurableDataCoverage() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = new CompletableFuture<>();
        SharedWal wal = mock(SharedWal.class);
        when(wal.appendBatch(anyList())).thenReturn(backend);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            var append = engine.appendOwnedBatchGroup(PARTITION, List.of(
                batch(0L, 9L), batch(10L, 19L)
            ));
            backend.complete(List.of(new WalAppendResult(1L, 61)));
            assertThrows(CompletionException.class, append::join);
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, engine.liveWalDurabilityObservation(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void truncateSuccessIsCountedOnlyAfterTruncateIndexPublication() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        CompletableFuture<WalAppendResult> barrier = new CompletableFuture<>();
        when(wal.append(any(WalRecord.class))).thenReturn(
            CompletableFuture.completedFuture(new WalAppendResult(1L, 61)), barrier
        );
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            engine.appendData(PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1})).join();
            var truncate = engine.truncate(PARTITION, 9, 0L);
            assertEquals(0L, engine.liveWalDurabilityObservation(PARTITION).truncates());
            barrier.complete(new WalAppendResult(62L, 60));
            truncate.join();
            var completed = engine.liveWalDurabilityObservation(PARTITION);
            assertEquals(1L, completed.dataBatches());
            assertEquals(1L, completed.truncates());
            assertEquals(122L, completed.highestTruncateWalEndExclusive());
            assertEquals(122L, completed.highestSuccessfulWalEndExclusive());
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void outOfOrderDurabilityCompletionsNeverMoveLogicalMaximumBackwards() {
        LocalWalDurabilityObservation observation = new LocalWalDurabilityObservation();
        observation.dataCompleted(PARTITION, new WalLocation(200L, 61, 8, 10L, 19L));
        var oldSnapshot = observation.snapshot(PARTITION);
        observation.dataCompleted(PARTITION, new WalLocation(100L, 61, 8, 0L, 9L));
        var newer = observation.snapshot(PARTITION);
        assertEquals(261L, oldSnapshot.highestSuccessfulWalEndExclusive());
        assertEquals(1L, oldSnapshot.dataBatches());
        assertEquals(261L, newer.highestSuccessfulWalEndExclusive());
        assertEquals(2L, newer.dataBatches());
    }

    @Test
    void anotherPartitionCannotInheritDurableCompletionCounts() {
        LocalWalDurabilityObservation observation = new LocalWalDurabilityObservation();
        observation.dataCompleted(PARTITION, new WalLocation(1L, 61, 8, 0L, 9L));
        assertEquals(1L, observation.snapshot(PARTITION).dataBatches());
        assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, observation.snapshot(OTHER));
    }

    @Test
    void restartTurnsPreviouslyCompletedWritesIntoReplayNotLiveCompletions() throws Exception {
        Path dir = tempDir.resolve("file-restart");
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            engine.appendData(PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1})).join();
            assertEquals(1L, engine.liveWalDurabilityObservation(PARTITION).dataBatches());
            assertEquals(0L, engine.startupWalReplayEvidence().replayedRecords());
        }
        try (SharedStorageEngine restarted = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            assertEquals(1L, restarted.startupWalReplayEvidence().replayedRecords());
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, restarted.liveWalDurabilityObservation(PARTITION));
        }
    }

    @Test
    void ringBackendDurableFuturePublishesLiveReceiptButNotStartupReplay() throws Exception {
        Path path = tempDir.resolve("live-ring.wal");
        try (SharedStorageEngine engine = new SharedStorageEngine(new RingSharedWal(path, 1024L * 1024L))) {
            var append = engine.appendOwnedData(PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1}));
            var indexed = append.join();
            assertEquals(indexed.walOffset() + indexed.length(),
                engine.liveWalDurabilityObservation(PARTITION).highestSuccessfulWalEndExclusive());
            assertEquals(0L, engine.startupWalReplayEvidence().replayedRecords());
        }
        try (SharedStorageEngine reopened = new SharedStorageEngine(new RingSharedWal(path, 1024L * 1024L))) {
            assertEquals(1L, reopened.startupWalReplayEvidence().replayedRecords());
            assertSame(LocalWalDurabilityObservation.Snapshot.EMPTY, reopened.liveWalDurabilityObservation(PARTITION));
        }
    }

    private static SharedStorageEngine.OwnedDataBatch batch(long first, long last) {
        return new SharedStorageEngine.OwnedDataBatch(8, first, last, ByteBuffer.wrap(new byte[]{1}));
    }

    private static SharedStorageEngine singleEngine(CompletableFuture<WalAppendResult> backend) throws IOException {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenReturn(backend);
        return new SharedStorageEngine(wal);
    }
}
