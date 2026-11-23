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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.wal.WalCapacityExceededException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionDurableOffsetTrackerTest {
    @Test
    void publishesOnlyTheContiguousDurableReservationPrefix() {
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 10L, Runnable::run, 10, 100L);
        var first = tracker.reserve(1L, 10L, 12L, 1L);
        var second = tracker.reserve(1L, 12L, 15L, 1L);
        CompletableFuture<Void> firstWal = new CompletableFuture<>();
        CompletableFuture<Void> secondWal = new CompletableFuture<>();
        first.bind(firstWal);
        second.bind(secondWal);
        CompletableFuture<Void> duplicate = tracker.awaitOffset(1L, 15L);
        secondWal.complete(null);
        assertEquals(10L, tracker.confirmedOffset());
        assertFalse(first.confirmed().isDone());
        assertFalse(second.confirmed().isDone());
        assertFalse(duplicate.isDone());
        firstWal.complete(null);
        assertEquals(15L, tracker.confirmedOffset());
        assertTrue(first.confirmed().isDone());
        assertTrue(second.confirmed().isDone());
        assertTrue(duplicate.isDone());
        duplicate.join();
    }

    @Test
    void exactTailAndInteriorOffsetWaitForTheirOwnOrderedReservations() {
        var tracker = new PartitionDurableOffsetTracker(1L, 0L, Runnable::run, 3, 3L);
        var first = tracker.reserve(1L, 0L, 2L, 1L);
        var second = tracker.reserve(1L, 2L, 5L, 1L);
        var third = tracker.reserve(1L, 5L, 9L, 1L);
        var firstWal = new CompletableFuture<Void>();
        var secondWal = new CompletableFuture<Void>();
        var thirdWal = new CompletableFuture<Void>();
        first.bind(firstWal);
        second.bind(secondWal);
        third.bind(thirdWal);
        CompletableFuture<Void> middle = tracker.awaitOffset(1L, 4L);
        CompletableFuture<Void> tail = tracker.awaitOffset(1L, 9L);
        CompletableFuture<Void> cancelled = tracker.awaitOffset(1L, 9L);
        assertTrue(cancelled.cancel(false));
        assertThrows(CompletionException.class, () -> tracker.awaitOffset(1L, 10L).join());

        secondWal.complete(null);
        assertFalse(middle.isDone());
        assertFalse(tail.isDone());
        firstWal.complete(null);
        assertTrue(middle.isDone());
        middle.join();
        assertFalse(tail.isDone());
        tracker.fence(new IOException("leadership lost"));
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, tail::join).getCause());
        assertFalse(thirdWal.isCancelled());
        thirdWal.complete(null);
    }

    @Test
    void failureFencesAllPendingAndResetIgnoresOldCompletions() {
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(3L, 0L, Runnable::run, 10, 100L);
        var first = tracker.reserve(3L, 0L, 1L, 1L);
        var second = tracker.reserve(3L, 1L, 2L, 1L);
        CompletableFuture<Void> firstWal = new CompletableFuture<>();
        CompletableFuture<Void> secondWal = new CompletableFuture<>();
        first.bind(firstWal);
        second.bind(secondWal);
        secondWal.completeExceptionally(new IOException("force failure"));
        assertEquals(0L, tracker.confirmedOffset());
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> first.confirmed().join()).getCause());
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> second.confirmed().join()).getCause());
        assertThrows(IllegalStateException.class, () -> tracker.reserve(3L, 2L, 3L, 1L));
        assertEquals(1, tracker.inFlightWrites());
        assertThrows(IllegalStateException.class, () -> tracker.reset(4L, 5L, new IOException("new leader")));
        firstWal.complete(null);
        assertEquals(0, tracker.inFlightWrites());
        tracker.reset(4L, 5L, new IOException("new leader"));
        var next = tracker.reserve(4L, 5L, 6L, 1L);
        CompletableFuture<Void> nextWal = new CompletableFuture<>();
        next.bind(nextWal);
        assertEquals(5L, tracker.confirmedOffset());
        assertFalse(next.confirmed().isDone());
        nextWal.complete(null);
        assertEquals(6L, tracker.confirmedOffset());
        next.confirmed().join();
    }

    @Test
    void cancellationAndMalformedOffsetsCannotAdvanceOrCancelDurability() {
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(8L, 20L, Runnable::run, 10, 100L);
        var first = tracker.reserve(8L, 20L, 22L, 1L);
        CompletableFuture<Void> durable = new CompletableFuture<>();
        first.bind(durable);
        CompletableFuture<Void> cancelled = first.confirmed();
        assertTrue(cancelled.cancel(false));
        assertFalse(durable.isCancelled());
        assertThrows(IllegalArgumentException.class, () -> tracker.reserve(8L, 21L, 23L, 1L));
        assertThrows(IllegalStateException.class, () -> tracker.reserve(7L, 22L, 23L, 1L));
        assertThrows(IllegalStateException.class, () -> first.bind(new CompletableFuture<>()));
        assertThrows(CompletionException.class, () -> tracker.awaitOffset(8L, 23L).join());
        durable.complete(null);
        assertEquals(22L, tracker.confirmedOffset());
        tracker.awaitOffset(8L, 22L).join();
    }

    @Test
    void unexplainedOffsetGapCannotBecomeConfirmed() {
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 20L, Runnable::run, 10, 100L);
        assertThrows(IllegalArgumentException.class, () -> tracker.reserve(1L, 22L, 24L, 1L));
        assertEquals(20L, tracker.confirmedOffset());
    }

    @Test
    void eitherFailureFencesImmediatelyWithoutWaitingForTheOtherSignal() throws Exception {
        for (boolean failWal : new boolean[] {false, true}) {
            PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 0L, Runnable::run, 10, 100L);
            var first = tracker.reserve(1L, 0L, 1L, 1L);
            var second = tracker.reserve(1L, 1L, 2L, 1L);
            CompletableFuture<Void> wal = new CompletableFuture<>();
            CompletableFuture<Void> state = new CompletableFuture<>();
            CompletableFuture<Void> laterWal = new CompletableFuture<>();
            first.bind(wal, state);
            second.bind(laterWal, CompletableFuture.completedFuture(null));
            laterWal.complete(null);
            (failWal ? wal : state).completeExceptionally(new IOException("failed"));
            assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> first.confirmed().join()).getCause());
            assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> second.confirmed().join()).getCause());
            assertEquals(0L, tracker.confirmedOffset());
            assertThrows(IllegalStateException.class, () -> tracker.reserve(1L, 2L, 3L, 1L));
            if (!failWal) {
                assertFalse(tracker.awaitSettled(1, TimeUnit.MILLISECONDS));
                wal.complete(null);
            }
            assertTrue(tracker.awaitSettled(1, TimeUnit.SECONDS));
            tracker.reset(2L, 0L, new IOException("new leader"));
            if (failWal) state.complete(null);
            assertEquals(0L, tracker.confirmedOffset());
        }
    }

    @Test
    void leadershipFenceRejectsSuccessStillQueuedForNotification() {
        java.util.List<Runnable> notifications = new java.util.ArrayList<>();
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 0L, notifications::add, 10, 100L);
        var reservation = tracker.reserve(1L, 0L, 1L, 1L);
        var wal = new CompletableFuture<Void>();
        reservation.bind(wal);
        wal.complete(null);
        assertEquals(1L, tracker.confirmedOffset());
        assertFalse(reservation.confirmed().isDone());
        tracker.fence(new IOException("leadership lost before delivery"));
        notifications.forEach(Runnable::run);
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> reservation.confirmed().join()).getCause());
    }

    @Test
    void walFailureRejectsUndeliveredSuccessNotifications() {
        java.util.List<Runnable> notifications = new java.util.ArrayList<>();
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 0L, notifications::add, 10, 100L);
        var first = tracker.reserve(1L, 0L, 1L, 1L);
        var second = tracker.reserve(1L, 1L, 2L, 1L);
        var firstWal = new CompletableFuture<Void>();
        var secondWal = new CompletableFuture<Void>();
        first.bind(firstWal);
        second.bind(secondWal);
        firstWal.complete(null);
        secondWal.completeExceptionally(new IOException("later WAL failure"));
        notifications.forEach(Runnable::run);
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> first.confirmed().join()).getCause());
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, () -> second.confirmed().join()).getCause());
        assertEquals(1L, tracker.confirmedOffset());
    }

    @Test
    void aBlockedNotificationDoesNotBlockWalAndPreservesCompletionOrder() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch firstRelease = new CountDownLatch(1);
        try {
            PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 0L, executor, 2, 2L);
            var first = tracker.reserve(1L, 0L, 1L, 1L);
            var second = tracker.reserve(1L, 1L, 2L, 1L);
            CompletableFuture<Void> firstWal = new CompletableFuture<>();
            CompletableFuture<Void> secondWal = new CompletableFuture<>();
            first.bind(firstWal);
            second.bind(secondWal);
            first.confirmed().thenRun(() -> {
                firstEntered.countDown();
                try {
                    if (!firstRelease.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("notification did not unblock");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            });
            CompletableFuture<Void> secondConfirmed = second.confirmed();
            firstWal.complete(null);
            assertTrue(firstEntered.await(10, TimeUnit.SECONDS));
            secondWal.complete(null);
            assertFalse(secondConfirmed.isDone());
            assertThrows(WalCapacityExceededException.class, () -> tracker.reserve(1L, 2L, 3L, 1L));
            assertEquals(2L, tracker.confirmedOffset());
            firstRelease.countDown();
            secondConfirmed.get(10, TimeUnit.SECONDS);
        } finally {
            firstRelease.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void leadershipResetFailsWaitersWithoutCancellingIo() throws Exception {
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 0L, Runnable::run, 10, 100L);
        var reservation = tracker.reserve(1L, 0L, 1L, 1L);
        CompletableFuture<Void> durable = new CompletableFuture<>();
        reservation.bind(durable);
        CompletableFuture<Void> waiting = reservation.confirmed();
        tracker.fence(new IOException("fenced"));
        assertFalse(durable.isCancelled());
        assertInstanceOf(IOException.class, assertThrows(CompletionException.class, waiting::join).getCause());
        assertThrows(IllegalStateException.class, () -> tracker.reset(2L, 0L, new IOException("new leader")));
        assertFalse(tracker.awaitSettled(1L, TimeUnit.MILLISECONDS));
        durable.complete(null);
        assertTrue(tracker.awaitSettled(1L, TimeUnit.SECONDS));
        assertEquals(0, tracker.inFlightWrites());
        tracker.reset(2L, 0L, new IOException("new leader"));
        assertEquals(0L, tracker.confirmedOffset());
        assertThrows(CompletionException.class, () -> tracker.awaitOffset(1L, 1L).join());
    }
}
