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
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalAppendResult;
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** A local WAL future is an index publication barrier, not KRaft retirement authority. */
class SharedStorageEngineWalDurabilityFenceTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);

    @Test
    void pendingWalBarrierKeepsEveryGroupBatchInvisible() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
            assertTrue(engine.storedBatches(PARTITION, 0L, 20L).isEmpty());
            assertTrue(engine.uploadCandidates(PARTITION, 0L, 20L).isEmpty());

            barrier.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(62L, 61)));
            assertEquals(2, append.join().size());
            assertEquals(List.of(0L, 10L),
                engine.localLocations(PARTITION, 0L, 20L).stream().map(WalLocation::firstOffset).toList());
        }
    }

    @Test
    void failedWalBarrierCannotPublishAnyIndexedGroupBatch() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.completeExceptionally(new IOException("WAL force failed"));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
            assertTrue(engine.uploadCandidates(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void partialWalBarrierResultCannotPublishIndexedPrefix() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(1L, 61)));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void nullWalBarrierResultCannotPublishIndexedPrefix() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(Arrays.asList(new WalAppendResult(1L, 61), null));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void duplicateWalGroupAddressesCannotPublishAnyIndexedBatch() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(1L, 61)));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void descendingWalGroupAddressesCannotPublishAnyIndexedBatch() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(62L, 61), new WalAppendResult(1L, 61)));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }


    @Test
    void firstWalGroupLengthMismatchDoesNotPublishIndexedPrefix() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(1L, 62), new WalAppendResult(63L, 61)));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void laterWalGroupLengthMismatchDoesNotPublishIndexedPrefix() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(62L, 62)));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
            assertTrue(engine.uploadCandidates(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void overlappingWalGroupAddressesCannotPublishAnyIndexedBatch() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(50L, 61)));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void walGroupOffsetOverflowCannotPublishAnyIndexedBatch() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(
                new WalAppendResult(1L, 61),
                new WalAppendResult(Long.MAX_VALUE - 20L, 61)
            ));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void gappedWalGroupAddressesRemainValid() throws Exception {
        CompletableFuture<List<WalAppendResult>> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(barrier)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            barrier.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(128L, 61)));
            assertEquals(2, append.join().size());
            assertEquals(List.of(0L, 10L),
                engine.localLocations(PARTITION, 0L, 20L).stream()
                    .map(WalLocation::firstOffset).toList());
        }
    }

    @Test
    void invalidSingleCopiedAppendResultNeverPublishesIndex() throws Exception {
        CompletableFuture<WalAppendResult> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(barrier)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            barrier.complete(new WalAppendResult(1L, 62));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void invalidSingleOwnedAppendResultNeverPublishesIndex() throws Exception {
        CompletableFuture<WalAppendResult> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(barrier)) {
            CompletableFuture<WalLocation> append =
                engine.appendOwnedData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            barrier.complete(new WalAppendResult(1L, 62));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void singleWalAddressOverflowCannotPublishIndex() throws Exception {
        CompletableFuture<WalAppendResult> barrier = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(barrier)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            barrier.complete(new WalAppendResult(Long.MAX_VALUE - 20L, 61));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void invalidTruncateResultCannotRemoveIndexedBytes() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        CompletableFuture<WalAppendResult> failedTruncate = new CompletableFuture<>();
        when(wal.append(any(WalRecord.class))).thenReturn(
            CompletableFuture.completedFuture(new WalAppendResult(1L, 61)), failedTruncate
        );
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1})).join();
            long before = engine.walMutationRevision(PARTITION);
            assertEquals(1, engine.localLocations(PARTITION, 0L, 10L).size());
            CompletableFuture<Void> truncate = engine.truncate(PARTITION, 4, 0L);
            failedTruncate.complete(new WalAppendResult(62L, 61));
            assertThrows(CompletionException.class, truncate::join);
            assertEquals(before, engine.walMutationRevision(PARTITION));
            assertEquals(1, engine.localLocations(PARTITION, 0L, 10L).size());
        }
    }


    @Test
    void duplicateKafkaBatchOffsetsNeverReachWalAdmission() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            CompletableFuture<List<WalLocation>> rejected = engine.appendOwnedBatchGroup(
                PARTITION, List.of(batch(0L, 9L), batch(0L, 9L))
            );
            assertThrows(CompletionException.class, rejected::join);
            verify(wal, never()).appendBatch(anyList());
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void overlappingKafkaBatchOffsetsNeverReachWalAdmission() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            CompletableFuture<List<WalLocation>> rejected = engine.appendOwnedBatchGroup(
                PARTITION, List.of(batch(0L, 9L), batch(9L, 19L))
            );
            assertThrows(CompletionException.class, rejected::join);
            verify(wal, never()).appendBatch(anyList());
            assertTrue(engine.localLocations(PARTITION, 0L, 20L).isEmpty());
        }
    }

    @Test
    void reversedKafkaBatchOffsetsNeverReachWalAdmission() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            CompletableFuture<List<WalLocation>> rejected = engine.appendOwnedBatchGroup(
                PARTITION, List.of(batch(20L, 29L), batch(0L, 9L))
            );
            assertThrows(CompletionException.class, rejected::join);
            verify(wal, never()).appendBatch(anyList());
            assertTrue(engine.localLocations(PARTITION, 0L, 30L).isEmpty());
        }
    }

    @Test
    void gappedKafkaBatchOffsetsCanStillCrossOneDurableWalBarrier() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        when(wal.appendBatch(anyList())).thenReturn(CompletableFuture.completedFuture(List.of(
            new WalAppendResult(1L, 61), new WalAppendResult(100L, 61)
        )));
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            List<WalLocation> durable = engine.appendOwnedBatchGroup(
                PARTITION, List.of(batch(0L, 9L), batch(20L, 29L))
            ).join();
            assertEquals(List.of(0L, 20L), durable.stream().map(WalLocation::firstOffset).toList());
            assertEquals(2, engine.localLocations(PARTITION, 0L, 30L).size());
            verify(wal).appendBatch(anyList());
        }
    }

    private static SharedStorageEngine.OwnedDataBatch batch(long first, long last) {
        return new SharedStorageEngine.OwnedDataBatch(3, first, last, ByteBuffer.wrap(new byte[] {1}));
    }

    private static SharedStorageEngine singleEngine(CompletableFuture<WalAppendResult> barrier) throws IOException {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenReturn(barrier);
        return new SharedStorageEngine(wal);
    }

    private static SharedStorageEngine engine(CompletableFuture<List<WalAppendResult>> barrier) throws IOException {
        SharedWal wal = mock(SharedWal.class);
        when(wal.appendBatch(anyList())).thenReturn(barrier);
        return new SharedStorageEngine(wal);
    }

    private static List<SharedStorageEngine.OwnedDataBatch> batches() {
        return List.of(
            new SharedStorageEngine.OwnedDataBatch(3, 0L, 9L, ByteBuffer.wrap(new byte[] {1})),
            new SharedStorageEngine.OwnedDataBatch(3, 10L, 19L, ByteBuffer.wrap(new byte[] {2}))
        );
    }
}
