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

        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{1}), new WalAppendResult(1, 0, 100));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 20, 29, new byte[]{2}), new WalAppendResult(1, 100, 100));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30, 39, new byte[]{3}), new WalAppendResult(1, 200, 100));

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

        index.apply(WalRecord.data(10L, 20L, 1, 3, 100, 109, new byte[]{1}), new WalAppendResult(2, 0, 100));
        index.apply(WalRecord.truncate(10L, 20L, 1, 4, 100), new WalAppendResult(2, 100, 60));
        index.apply(WalRecord.data(10L, 20L, 1, 4, 100, 109, new byte[]{2}), new WalAppendResult(3, 0, 100));

        WalLocation location = index.find(key, 105).orElseThrow();
        assertEquals(3, location.segmentId());
        assertEquals(4, location.leaderEpoch());
    }

    @Test
    void mutationRevisionIgnoresTailAppendsButFencesDestructiveChanges() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);

        index.apply(WalRecord.data(1L, 2L, 3, 7, 0, 9, new byte[]{1}), new WalAppendResult(1, 0, 100));
        long afterFirstAppend = index.mutationRevision(key);
        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{2}), new WalAppendResult(1, 100, 100));
        assertEquals(afterFirstAppend, index.mutationRevision(key),
            "Ordinary tail appends must not invalidate active upload selection");

        index.apply(WalRecord.data(1L, 2L, 3, 8, 0, 9, new byte[]{3}), new WalAppendResult(1, 200, 100));
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
            new WalAppendResult(100L, 100));
        index.apply(WalRecord.data(1L, 2L, 4, 7, 0, 9, new byte[]{2}),
            new WalAppendResult(200L, 100));
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
            new WalAppendResult(100L, 100));
        index.apply(WalRecord.data(1L, 2L, 4, 7, 0, 9, new byte[]{2}),
            new WalAppendResult(200L, 100));
        index.clear();
        assertTrue(index.find(first, 0L).isEmpty());
        assertTrue(index.find(second, 0L).isEmpty());
        assertEquals(1L, index.mutationRevision(first));
        assertEquals(1L, index.mutationRevision(second));

        index.apply(WalRecord.data(1L, 2L, 3, 8, 0, 9, new byte[]{3}),
            new WalAppendResult(300L, 100));
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
    void shouldWalkPartitionRangesWithoutMaterializingBacklog() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);

        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{1}), new WalAppendResult(1, 0, 100));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30, 39, new byte[]{2}), new WalAppendResult(1, 100, 100));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 40, 49, new byte[]{3}), new WalAppendResult(1, 200, 100));

        WalLocation containing = index.rangeAtOrAfter(key, 15L).orElseThrow();
        assertEquals(10L, containing.firstOffset());

        WalLocation afterGap = index.rangeAtOrAfter(key, 20L).orElseThrow();
        assertEquals(30L, afterGap.firstOffset());

        WalLocation next = index.rangeAfter(key, 30L).orElseThrow();
        assertEquals(40L, next.firstOffset());
        assertTrue(index.rangeAfter(key, 40L).isEmpty());
    }
}
