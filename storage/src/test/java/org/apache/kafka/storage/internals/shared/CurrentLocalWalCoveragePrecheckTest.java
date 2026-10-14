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

import static org.apache.kafka.storage.internals.shared.CurrentLocalWalCoveragePrecheck.Finding;
import static org.apache.kafka.storage.internals.shared.CurrentLocalWalCoveragePrecheck.assess;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CurrentLocalWalCoveragePrecheckTest {
    private static final SharedPartitionId FIRST = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId SECOND = new SharedPartitionId(1L, 2L, 1);

    @TempDir
    Path tempDir;

    @Test
    void contiguousKafkaBatchesMayHaveInterleavedPhysicalWalAddresses() {
        assertEquals(Finding.INDEXED_BATCH_WINDOW, assess(
            List.of(location(1L, 0L, 9L), location(150L, 10L, 19L)), 0L, 20L));
    }

    @Test
    void kafkaOffsetGapCannotBecomeALocalDurableWindow() {
        assertEquals(Finding.KAFKA_BATCH_GAP_OR_OVERLAP, assess(
            List.of(location(1L, 0L, 9L), location(62L, 20L, 29L)), 0L, 30L));
    }

    @Test
    void overlappingKafkaBatchesCannotBecomeALocalDurableWindow() {
        assertEquals(Finding.KAFKA_BATCH_GAP_OR_OVERLAP, assess(
            List.of(location(1L, 0L, 9L), location(62L, 9L, 19L)), 0L, 20L));
    }

    @Test
    void sourceLogStartInsideKafkaRecordBatchFailsClosed() {
        assertEquals(Finding.KAFKA_BATCH_GAP_OR_OVERLAP, assess(
            List.of(location(1L, 0L, 9L), location(62L, 10L, 19L)), 5L, 20L));
    }

    @Test
    void sourceHighWatermarkInsideKafkaRecordBatchFailsClosed() {
        assertEquals(Finding.SOURCE_END_SPLITS_BATCH, assess(
            List.of(location(1L, 0L, 9L), location(62L, 10L, 19L)), 0L, 15L));
    }

    @Test
    void missingTrailingKafkaRangeCannotBecomeACompleteWindow() {
        assertEquals(Finding.SOURCE_WINDOW_NOT_INDEXED, assess(
            List.of(location(1L, 0L, 9L)), 0L, 20L));
    }

    @Test
    void noLocalDataAndEmptyNativeWindowHaveDistinctFindings() {
        assertEquals(Finding.NO_INDEXED_DATA, assess(List.of(), 0L, 10L));
        assertEquals(Finding.EMPTY_SOURCE_WINDOW, assess(List.of(), 10L, 10L));
    }

    @Test
    void invalidKafkaSourceWindowAndNullLocationsAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> assess(List.of(), -1L, 10L));
        assertThrows(IllegalArgumentException.class, () -> assess(List.of(), 10L, 9L));
        assertThrows(NullPointerException.class, () -> assess(null, 0L, 1L));
    }

    @Test
    void unfinishedWalFutureCannotPopulateCurrentIndexedCoverage() throws Exception {
        CompletableFuture<WalAppendResult> backend = new CompletableFuture<>();
        try (SharedStorageEngine engine = mockedEngine(backend)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(FIRST, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1}));
            assertEquals(Finding.NO_INDEXED_DATA, engine.localDurableWindowCoverage(FIRST, 0L, 10L));
            backend.complete(new WalAppendResult(1L, 61));
            append.join();
            assertEquals(Finding.INDEXED_BATCH_WINDOW,
                engine.localDurableWindowCoverage(FIRST, 0L, 10L));
        }
    }

    @Test
    void failedWalFutureCannotPopulateCurrentIndexedCoverage() throws Exception {
        CompletableFuture<WalAppendResult> backend = new CompletableFuture<>();
        try (SharedStorageEngine engine = mockedEngine(backend)) {
            CompletableFuture<WalLocation> append =
                engine.appendOwnedData(FIRST, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1}));
            backend.completeExceptionally(new IOException("durability failed"));
            assertThrows(CompletionException.class, append::join);
            assertEquals(Finding.NO_INDEXED_DATA, engine.localDurableWindowCoverage(FIRST, 0L, 10L));
        }
    }

    @Test
    void recoveredAndNewDurableFileBatchesJointlyCoverTheKafkaWindow() throws Exception {
        Path dir = tempDir.resolve("file-combined");
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            engine.appendData(FIRST, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1})).join();
        }
        try (SharedStorageEngine restarted = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            assertEquals(1L, restarted.startupWalReplayEvidence().replayedRecords());
            assertEquals(Finding.SOURCE_WINDOW_NOT_INDEXED,
                restarted.localDurableWindowCoverage(FIRST, 0L, 20L));
            restarted.appendOwnedData(FIRST, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})).join();
            assertEquals(Finding.INDEXED_BATCH_WINDOW,
                restarted.localDurableWindowCoverage(FIRST, 0L, 20L));
            assertEquals(1L, restarted.startupWalReplayEvidence().replayedRecords());
            assertEquals(1L, restarted.liveWalDurabilityObservation(FIRST).dataBatches());
        }
    }

    @Test
    void ringWalCurrentWindowDoesNotBorrowAnotherPartitionData() throws Exception {
        Path path = tempDir.resolve("ring-mixed.wal");
        try (SharedStorageEngine engine = new SharedStorageEngine(new RingSharedWal(path, 1024L * 1024L))) {
            engine.appendData(FIRST, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1})).join();
            engine.appendData(SECOND, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})).join();
            assertEquals(Finding.INDEXED_BATCH_WINDOW, engine.localDurableWindowCoverage(FIRST, 0L, 10L));
            assertEquals(Finding.SOURCE_WINDOW_NOT_INDEXED,
                engine.localDurableWindowCoverage(FIRST, 0L, 20L));
            assertEquals(Finding.NO_INDEXED_DATA, engine.localDurableWindowCoverage(SECOND, 0L, 10L));
        }
    }

    @Test
    void durableTruncateRevokesCurrentIndexedDataCoverage() throws Exception {
        Path dir = tempDir.resolve("file-truncated");
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(dir, 1024 * 1024, 4096))) {
            engine.appendData(FIRST, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1})).join();
            assertEquals(Finding.INDEXED_BATCH_WINDOW, engine.localDurableWindowCoverage(FIRST, 0L, 10L));
            engine.truncate(FIRST, 9, 0L).join();
            assertEquals(Finding.NO_INDEXED_DATA, engine.localDurableWindowCoverage(FIRST, 0L, 10L));
        }
    }

    private static WalLocation location(long walPosition, long first, long last) {
        return new WalLocation(walPosition, 61, 8, first, last);
    }

    private static SharedStorageEngine mockedEngine(CompletableFuture<WalAppendResult> backend) throws IOException {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenReturn(backend);
        return new SharedStorageEngine(wal);
    }
}
