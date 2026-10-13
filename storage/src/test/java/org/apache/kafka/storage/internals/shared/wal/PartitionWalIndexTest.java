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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionWalIndexTest {
    @Test
    void shouldInvalidateEntriesAtAndAfterTruncateOffset() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);

        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{1}), new WalAppendResult(1, 0, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 20, 29, new byte[]{2}), new WalAppendResult(1, 100, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30, 39, new byte[]{3}), new WalAppendResult(1, 200, 61));

        index.apply(WalRecord.truncate(1L, 2L, 3, 8, 25), new WalAppendResult(1, 300, 60));

        assertTrue(index.find(key, 10).isPresent());
        assertTrue(index.find(key, 19).isPresent());
        assertFalse(index.find(key, 20).isPresent());
        assertFalse(index.find(key, 25).isPresent());
        assertFalse(index.find(key, 30).isPresent());
    }

    @Test
    void shouldReplaceLogicalRangeWithNewGeneration() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(10L, 20L, 1);

        index.apply(WalRecord.data(10L, 20L, 1, 3, 100, 109, new byte[]{1}), new WalAppendResult(2, 0, 61));
        index.apply(WalRecord.truncate(10L, 20L, 1, 4, 100), new WalAppendResult(2, 100, 60));
        index.apply(WalRecord.data(10L, 20L, 1, 4, 100, 109, new byte[]{2}), new WalAppendResult(3, 0, 61));

        WalLocation location = index.find(key, 105).orElseThrow();
        assertEquals(3, location.segmentId());
        assertEquals(4, location.leaderEpoch());
    }

    @Test
    void mutationRevisionIgnoresTailAppendsButFencesDestructiveChanges() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);

        index.apply(WalRecord.data(1L, 2L, 3, 7, 0, 9, new byte[]{1}), new WalAppendResult(1, 0, 61));
        long afterFirstAppend = index.mutationRevision(key);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{2}), new WalAppendResult(1, 100, 61));
        assertEquals(afterFirstAppend, index.mutationRevision(key),
            "Ordinary tail appends must not invalidate active upload selection");

        index.apply(WalRecord.data(1L, 2L, 3, 8, 0, 9, new byte[]{3}), new WalAppendResult(1, 200, 61));
        long afterReplacement = index.mutationRevision(key);
        assertTrue(afterReplacement > afterFirstAppend);

        index.truncate(key, 10L);
        assertTrue(index.mutationRevision(key) > afterReplacement);
    }

    @Test
    void destructiveMutationHasNoStableRevisionWhileItIsInProgress() throws Exception {
        PartitionWalIndex.MutationGate gate = new PartitionWalIndex.MutationGate();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<Void> mutation = CompletableFuture.runAsync(() ->
            gate.destructiveChange(() -> {
                entered.countDown();
                try {
                    if (!release.await(5L, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out holding WAL mutation");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Interrupted WAL mutation", e);
                }
            })
        );
        try {
            assertTrue(entered.await(5L, TimeUnit.SECONDS));
            assertEquals(-1L, gate.observedRevision(),
                "An in-flight destructive mutation must not supply stable source evidence");
        } finally {
            release.countDown();
        }
        mutation.get(10L, TimeUnit.SECONDS);
        assertEquals(1L, gate.observedRevision());
    }

    @Test
    void failedDestructiveMutationRestoresReadableRevision() {
        PartitionWalIndex.MutationGate gate = new PartitionWalIndex.MutationGate();
        assertThrows(IllegalStateException.class, () ->
            gate.destructiveChange(() -> {
                throw new IllegalStateException("test mutation failure");
            })
        );
        assertEquals(1L, gate.observedRevision(),
            "A failed mutation must not leave the generation permanently odd");
    }

    @Test
    void reclamationFencesOnlyModifiedPartition() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey reclaimed = new WalPartitionKey(1L, 2L, 3);
        WalPartitionKey surviving = new WalPartitionKey(1L, 2L, 4);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0, 9, new byte[]{1}),
            new WalAppendResult(100L, 61));
        index.apply(WalRecord.data(1L, 2L, 4, 7, 0, 9, new byte[]{2}),
            new WalAppendResult(200L, 61));
        long before = index.mutationRevision(reclaimed);
        long survivorBefore = index.mutationRevision(surviving);

        index.removeBefore(150L);
        assertTrue(index.find(reclaimed, 0L).isEmpty());
        assertTrue(index.find(surviving, 0L).isPresent());
        assertTrue(index.mutationRevision(reclaimed) > before);
        assertEquals(survivorBefore, index.mutationRevision(surviving));
        long completed = index.mutationRevision(reclaimed);
        index.removeBefore(150L);
        assertEquals(completed, index.mutationRevision(reclaimed));
    }

    @Test
    void clearFencesEveryFormerPartitionAndKeepsGenerationAfterReplay() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey first = new WalPartitionKey(1L, 2L, 3);
        WalPartitionKey second = new WalPartitionKey(1L, 2L, 4);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0, 9, new byte[]{1}),
            new WalAppendResult(100L, 61));
        index.apply(WalRecord.data(1L, 2L, 4, 7, 0, 9, new byte[]{2}),
            new WalAppendResult(200L, 61));
        index.clear();
        assertTrue(index.find(first, 0L).isEmpty());
        assertTrue(index.find(second, 0L).isEmpty());
        assertEquals(1L, index.mutationRevision(first));
        assertEquals(1L, index.mutationRevision(second));

        index.apply(WalRecord.data(1L, 2L, 3, 8, 0, 9, new byte[]{3}),
            new WalAppendResult(300L, 61));
        assertEquals(1L, index.mutationRevision(first));
        assertTrue(index.find(first, 0L).isPresent());
    }

    @Test
    void emptyTruncateIsStillAGenerationFence() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        assertEquals(0L, index.mutationRevision(key));
        index.truncate(key, 100L);
        assertEquals(1L, index.mutationRevision(key));
        assertTrue(index.find(key, 100L).isEmpty());
    }


    @Test
    void malformedReplayPayloadLengthCannotCreateAnIndex() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        WalRecord record = WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1});

        assertThrows(IllegalArgumentException.class,
            () -> index.apply(record, new WalAppendResult(100L, 62)));
        assertEquals(0, index.partitionCount());
        assertEquals(0L, index.mutationRevision(key));
    }

    @Test
    void malformedReplayReplacementCannotRemovePreviousRange() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1}),
            new WalAppendResult(100L, 61));
        long priorRevision = index.mutationRevision(key);

        assertThrows(IllegalArgumentException.class, () ->
            index.apply(WalRecord.data(1L, 2L, 3, 8, 0L, 9L, new byte[]{2}),
                new WalAppendResult(200L, 62)));
        assertEquals(100L, index.find(key, 0L).orElseThrow().walOffset());
        assertEquals(priorRevision, index.mutationRevision(key));
    }

    @Test
    void internalGroupCommitMarkerCannotEnterUserPartitionIndex() {
        PartitionWalIndex index = new PartitionWalIndex();

        assertThrows(IllegalArgumentException.class, () ->
            index.apply(WalRecord.groupCommit(77L, 2), new WalAppendResult(100L, 60)));
        assertEquals(0, index.partitionCount());
    }

    @Test
    void overlappingPredecessorAndSuccessorAreEvictedAsWholeBatches() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1}),
            new WalAppendResult(100L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 10L, 19L, new byte[]{2}),
            new WalAppendResult(200L, 61));
        long before = index.mutationRevision(key);
        index.apply(WalRecord.data(1L, 2L, 3, 8, 5L, 14L, new byte[]{3}),
            new WalAppendResult(300L, 61));

        assertTrue(index.find(key, 0L).isEmpty());
        assertEquals(300L, index.find(key, 5L).orElseThrow().walOffset());
        assertEquals(300L, index.find(key, 10L).orElseThrow().walOffset());
        assertTrue(index.find(key, 15L).isEmpty());
        assertEquals(1, index.ranges(key).size());
        assertTrue(index.mutationRevision(key) > before);
    }

    @Test
    void oneReplacementEvictsEveryOverlappedSuccessorButKeepsLaterTail() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 10L, 19L, new byte[]{1}),
            new WalAppendResult(100L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 20L, 29L, new byte[]{2}),
            new WalAppendResult(200L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30L, 39L, new byte[]{3}),
            new WalAppendResult(300L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 40L, 49L, new byte[]{4}),
            new WalAppendResult(400L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 8, 5L, 34L, new byte[]{5}),
            new WalAppendResult(500L, 61));

        assertEquals(500L, index.find(key, 10L).orElseThrow().walOffset());
        assertEquals(500L, index.find(key, 30L).orElseThrow().walOffset());
        assertTrue(index.find(key, 35L).isEmpty());
        assertEquals(400L, index.find(key, 45L).orElseThrow().walOffset());
        assertEquals(2, index.ranges(key).size());
    }

    @Test
    void insertingIntoGapKeepsUnrelatedBatchesButFencesRevision() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1}),
            new WalAppendResult(100L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30L, 39L, new byte[]{2}),
            new WalAppendResult(200L, 61));
        long before = index.mutationRevision(key);

        index.apply(WalRecord.data(1L, 2L, 3, 8, 20L, 29L, new byte[]{3}),
            new WalAppendResult(300L, 61));
        assertEquals(100L, index.find(key, 5L).orElseThrow().walOffset());
        assertEquals(300L, index.find(key, 25L).orElseThrow().walOffset());
        assertEquals(200L, index.find(key, 35L).orElseThrow().walOffset());
        assertEquals(3, index.ranges(key).size());
        assertTrue(index.mutationRevision(key) > before);
    }


    @Test
    void malformedTruncateReplayDoesNotEvictExistingData() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1}),
            new WalAppendResult(100L, 61));
        long before = index.mutationRevision(key);

        assertThrows(IllegalArgumentException.class, () ->
            index.apply(WalRecord.truncate(1L, 2L, 3, 8, 0L),
                new WalAppendResult(200L, 61)));
        assertEquals(100L, index.find(key, 0L).orElseThrow().walOffset());
        assertEquals(before, index.mutationRevision(key));
    }

    @Test
    void overflowingTruncateReplayDoesNotAdvanceMutationRevision() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1}),
            new WalAppendResult(100L, 61));
        long before = index.mutationRevision(key);

        assertThrows(IllegalArgumentException.class, () ->
            index.apply(WalRecord.truncate(1L, 2L, 3, 8, 0L),
                new WalAppendResult(Long.MAX_VALUE - 20L, 60)));
        assertEquals(100L, index.find(key, 0L).orElseThrow().walOffset());
        assertEquals(before, index.mutationRevision(key));
    }

    @Test
    void overflowingDataReplayCannotPublishAnIndex() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        assertThrows(IllegalArgumentException.class, () ->
            index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[]{1}),
                new WalAppendResult(Long.MAX_VALUE - 20L, 61)));
        assertEquals(0, index.partitionCount());
        assertEquals(0L, index.mutationRevision(key));
    }

    @Test
    void directWalLocationConstructorRejectsOverflowedSpan() {
        assertThrows(IllegalArgumentException.class, () ->
            new WalLocation(Long.MAX_VALUE - 20L, 61, 7, 0L, 9L));
    }

    @Test
    void exactEndOfAddressSpaceRemainsValid() {
        WalLocation location = new WalLocation(Long.MAX_VALUE - 61L, 61, 7, 0L, 9L);
        assertEquals(Long.MAX_VALUE - 61L, location.walOffset());
        assertEquals(61, location.length());
    }

    @Test
    void staleOverlappingReplayCannotReplaceNewerWalLocation() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[] {1}),
            new WalAppendResult(200L, 61));
        long revision = index.mutationRevision(key);

        assertThrows(IllegalStateException.class, () ->
            index.apply(WalRecord.data(1L, 2L, 3, 8, 0L, 9L, new byte[] {2}),
                new WalAppendResult(100L, 61)));
        assertEquals(200L, index.find(key, 0L).orElseThrow().walOffset());
        assertEquals(revision, index.mutationRevision(key));
    }

    @Test
    void duplicatedWalAddressCannotReplaceItsPreviousRange() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[] {1}),
            new WalAppendResult(100L, 61));

        assertThrows(IllegalStateException.class, () ->
            index.apply(WalRecord.data(1L, 2L, 3, 8, 0L, 9L, new byte[] {2}),
                new WalAppendResult(100L, 61)));
        assertEquals(7, index.find(key, 0L).orElseThrow().leaderEpoch());
        assertEquals(0L, index.mutationRevision(key));
    }

    @Test
    void staleOverlappingSuccessorCannotPartiallyEvictPredecessor() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 0L, 9L, new byte[] {1}),
            new WalAppendResult(100L, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 10L, 19L, new byte[] {2}),
            new WalAppendResult(400L, 61));

        assertThrows(IllegalStateException.class, () ->
            index.apply(WalRecord.data(1L, 2L, 3, 8, 5L, 14L, new byte[] {3}),
                new WalAppendResult(300L, 61)));
        assertEquals(100L, index.find(key, 5L).orElseThrow().walOffset());
        assertEquals(400L, index.find(key, 14L).orElseThrow().walOffset());
        assertEquals(2, index.ranges(key).size());
        assertEquals(0L, index.mutationRevision(key));
    }

    @Test
    void shouldWalkPartitionRangesWithoutMaterializingBacklog() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);

        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{1}), new WalAppendResult(1, 0, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30, 39, new byte[]{2}), new WalAppendResult(1, 100, 61));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 40, 49, new byte[]{3}), new WalAppendResult(1, 200, 61));

        WalLocation containing = index.rangeAtOrAfter(key, 15L).orElseThrow();
        assertEquals(10L, containing.firstOffset());

        WalLocation afterGap = index.rangeAtOrAfter(key, 20L).orElseThrow();
        assertEquals(30L, afterGap.firstOffset());

        WalLocation next = index.rangeAfter(key, 30L).orElseThrow();
        assertEquals(40L, next.firstOffset());
        assertTrue(index.rangeAfter(key, 40L).isEmpty());
    }
}
