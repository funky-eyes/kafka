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
import org.apache.kafka.storage.internals.shared.wal.WalReplayConsumer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
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


    @Test
    void nullSingleWalGroupResultCannotPublishIndex() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = CompletableFuture.completedFuture(null);
        try (SharedStorageEngine engine = defaultSingleAppendEngine(backend)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void emptySingleWalGroupResultCannotPublishIndex() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = CompletableFuture.completedFuture(List.of());
        try (SharedStorageEngine engine = defaultSingleAppendEngine(backend)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void extraSingleWalGroupResultCannotPublishIndex() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = CompletableFuture.completedFuture(
            List.of(new WalAppendResult(1L, 61), new WalAppendResult(62L, 61))
        );
        try (SharedStorageEngine engine = defaultSingleAppendEngine(backend)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void nullMemberSingleWalGroupResultCannotPublishIndex() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = CompletableFuture.completedFuture(
            Arrays.asList((WalAppendResult) null)
        );
        try (SharedStorageEngine engine = defaultSingleAppendEngine(backend)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertThrows(CompletionException.class, append::join);
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void validDefaultSingleWalAppendPublishesExactlyOneBatch() throws Exception {
        CompletableFuture<List<WalAppendResult>> backend = CompletableFuture.completedFuture(
            List.of(new WalAppendResult(1L, 61))
        );
        try (SharedStorageEngine engine = defaultSingleAppendEngine(backend)) {
            WalLocation result =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1})).join();
            assertEquals(1L, result.walOffset());
            assertEquals(1, engine.localLocations(PARTITION, 0L, 10L).size());
        }
    }

    @Test
    void failedColdReplayClosesWalAndPreservesOriginalIOException() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        IOException replayFailure = new IOException("replay failed");
        doThrow(replayFailure).when(wal).replay(any(WalReplayConsumer.class));

        IOException observed = assertThrows(IOException.class, () -> new SharedStorageEngine(wal));
        assertSame(replayFailure, observed);
        verify(wal).close();
    }

    @Test
    void failedColdReplaySuppressesWalCloseFailure() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        IllegalStateException replayFailure = new IllegalStateException("invalid replay entry");
        IOException closeFailure = new IOException("cannot release WAL");
        doThrow(replayFailure).when(wal).replay(any(WalReplayConsumer.class));
        doThrow(closeFailure).when(wal).close();

        IllegalStateException observed = assertThrows(
            IllegalStateException.class, () -> new SharedStorageEngine(wal));
        assertSame(replayFailure, observed);
        assertEquals(1, observed.getSuppressed().length);
        assertSame(closeFailure, observed.getSuppressed()[0]);
        verify(wal).close();
    }

    @Test
    void replayAndCloseReturningSameFailureCannotSelfSuppress() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        IOException replayFailure = new IOException("WAL failed and was already closed");
        doThrow(replayFailure).when(wal).replay(any(WalReplayConsumer.class));
        doThrow(replayFailure).when(wal).close();

        IOException observed = assertThrows(IOException.class, () -> new SharedStorageEngine(wal));
        assertSame(replayFailure, observed);
        assertEquals(0, observed.getSuppressed().length);
        verify(wal).close();
    }

    @Test
    void normalColdReplayTransfersWalOwnershipToEngine() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        SharedStorageEngine engine = new SharedStorageEngine(wal);
        verify(wal, never()).close();
        engine.close();
        verify(wal).close();
    }

    @Test
    void staleLaterWalGroupMemberCannotPublishAnEarlierPrefix() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenReturn(
            CompletableFuture.completedFuture(new WalAppendResult(300L, 61)));
        when(wal.appendBatch(anyList())).thenReturn(CompletableFuture.completedFuture(List.of(
            new WalAppendResult(100L, 61), new WalAppendResult(161L, 61))));
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            engine.appendData(PARTITION, 3, 10L, 19L, ByteBuffer.wrap(new byte[] {1})).join();
            long before = engine.walMutationRevision(PARTITION);
            CompletableFuture<List<WalLocation>> group =
                engine.appendOwnedBatchGroup(PARTITION, batches());
            assertThrows(CompletionException.class, group::join);
            assertEquals(1, engine.localLocations(PARTITION, 0L, 20L).size());
            assertEquals(10L, engine.localLocations(PARTITION, 0L, 20L).get(0).firstOffset());
            assertEquals(300L, engine.localLocations(PARTITION, 0L, 20L).get(0).walOffset());
            assertEquals(before, engine.walMutationRevision(PARTITION));
        }
    }

    @Test
    void equalLaterWalGroupAddressCannotPublishAnEarlierPrefix() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenReturn(
            CompletableFuture.completedFuture(new WalAppendResult(300L, 61)));
        when(wal.appendBatch(anyList())).thenReturn(CompletableFuture.completedFuture(List.of(
            new WalAppendResult(100L, 61), new WalAppendResult(300L, 61))));
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            engine.appendData(PARTITION, 3, 10L, 19L, ByteBuffer.wrap(new byte[] {1})).join();
            CompletableFuture<List<WalLocation>> group =
                engine.appendOwnedBatchGroup(PARTITION, batches());
            assertThrows(CompletionException.class, group::join);
            assertEquals(1, engine.localLocations(PARTITION, 0L, 20L).size());
            assertEquals(300L, engine.localLocations(PARTITION, 0L, 20L).get(0).walOffset());
        }
    }

    @Test
    void validReplacementGroupAdvancesOneMutationGeneration() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenReturn(
            CompletableFuture.completedFuture(new WalAppendResult(300L, 61)));
        when(wal.appendBatch(anyList())).thenReturn(CompletableFuture.completedFuture(List.of(
            new WalAppendResult(500L, 61), new WalAppendResult(561L, 61))));
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            engine.appendData(PARTITION, 3, 10L, 19L, ByteBuffer.wrap(new byte[] {1})).join();
            long before = engine.walMutationRevision(PARTITION);
            assertEquals(2, engine.appendOwnedBatchGroup(PARTITION, batches()).join().size());
            assertEquals(List.of(0L, 10L), engine.localLocations(PARTITION, 0L, 20L)
                .stream().map(WalLocation::firstOffset).toList());
            assertEquals(before + 1L, engine.walMutationRevision(PARTITION));
        }
    }

    @Test
    void walBackendCannotMutateSubmittedBatchMembership() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        when(wal.appendBatch(anyList())).thenAnswer(invocation -> {
            List<WalRecord> submitted = invocation.getArgument(0);
            assertThrows(UnsupportedOperationException.class, submitted::clear);
            return CompletableFuture.completedFuture(List.of(
                new WalAppendResult(100L, 61), new WalAppendResult(161L, 61)));
        });
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            assertEquals(2, engine.appendOwnedBatchGroup(PARTITION, batches()).join().size());
            assertEquals(2, engine.localLocations(PARTITION, 0L, 20L).size());
        }
    }


    @Test
    void pendingCopiedWalAppendMakesLocalAdmissionRevisionUnavailable() throws Exception {
        CompletableFuture<WalAppendResult> durable = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(durable)) {
            assertEquals(0L, engine.walAdmissionRevision(PARTITION));
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertEquals(-1L, engine.walAdmissionRevision(PARTITION));
            assertEquals(0L, engine.walMutationRevision(PARTITION));

            durable.complete(new WalAppendResult(1L, 61));
            append.join();
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
            assertEquals(0L, engine.walMutationRevision(PARTITION));
        }
    }

    @Test
    void ownedWalAppendFailureStillClosesItsAdmission() throws Exception {
        CompletableFuture<WalAppendResult> durable = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(durable)) {
            CompletableFuture<WalLocation> append =
                engine.appendOwnedData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertEquals(-1L, engine.walAdmissionRevision(PARTITION));
            durable.completeExceptionally(new IOException("WAL force failed"));
            assertThrows(CompletionException.class, append::join);
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void groupedWalAppendAdmissionCoversTheWholeDurableBarrier() throws Exception {
        CompletableFuture<List<WalAppendResult>> durable = new CompletableFuture<>();
        try (SharedStorageEngine engine = engine(durable)) {
            CompletableFuture<List<WalLocation>> append = engine.appendOwnedBatchGroup(PARTITION, batches());
            assertEquals(-1L, engine.walAdmissionRevision(PARTITION));
            durable.complete(List.of(new WalAppendResult(1L, 61), new WalAppendResult(62L, 61)));
            assertEquals(2, append.join().size());
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
        }
    }

    @Test
    void truncateAlsoTracksAdmissionThroughIndexPublication() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        CompletableFuture<WalAppendResult> durableTruncate = new CompletableFuture<>();
        when(wal.append(any(WalRecord.class))).thenReturn(
            CompletableFuture.completedFuture(new WalAppendResult(1L, 61)), durableTruncate
        );
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1})).join();
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
            CompletableFuture<Void> truncate = engine.truncate(PARTITION, 4, 0L);
            assertEquals(-1L, engine.walAdmissionRevision(PARTITION));
            durableTruncate.complete(new WalAppendResult(62L, 60));
            truncate.join();
            assertEquals(4L, engine.walAdmissionRevision(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void synchronousWalBackendFailureDoesNotStrandAdmission() throws Exception {
        SharedWal wal = mock(SharedWal.class);
        when(wal.append(any(WalRecord.class))).thenThrow(new IllegalStateException("WAL unavailable"));
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            assertThrows(IllegalStateException.class, () ->
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1})));
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    @Test
    void pendingWriteDoesNotFenceUnrelatedPartitionAdmission() throws Exception {
        CompletableFuture<WalAppendResult> durable = new CompletableFuture<>();
        SharedPartitionId independent = new SharedPartitionId(1L, 2L, 1);
        try (SharedStorageEngine engine = singleEngine(durable)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            assertEquals(-1L, engine.walAdmissionRevision(PARTITION));
            assertEquals(0L, engine.walAdmissionRevision(independent));
            durable.complete(new WalAppendResult(1L, 61));
            append.join();
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
            assertEquals(0L, engine.walAdmissionRevision(independent));
        }
    }

    @Test
    void invalidDurableWalResultAlsoCompletesAdmissionWithoutPublishingIndex() throws Exception {
        CompletableFuture<WalAppendResult> durable = new CompletableFuture<>();
        try (SharedStorageEngine engine = singleEngine(durable)) {
            CompletableFuture<WalLocation> append =
                engine.appendData(PARTITION, 3, 0L, 9L, ByteBuffer.wrap(new byte[] {1}));
            durable.complete(new WalAppendResult(1L, 62));
            assertThrows(CompletionException.class, append::join);
            assertEquals(2L, engine.walAdmissionRevision(PARTITION));
            assertTrue(engine.localLocations(PARTITION, 0L, 10L).isEmpty());
        }
    }

    private static SharedStorageEngine defaultSingleAppendEngine(
        CompletableFuture<List<WalAppendResult>> backend
    ) throws IOException {
        SharedWal wal = mock(SharedWal.class, CALLS_REAL_METHODS);
        when(wal.appendBatch(anyList())).thenReturn(backend);
        return new SharedStorageEngine(wal);
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
