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

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test-only experimental model of an ordered, exclusive Kafka offset frontier.
 * It is not connected to Broker reads, leadership transitions or acknowledgements.
 * Callers reserve under the partition append lock before submitting the WAL
 * group, and bind the returned reservation even if submission fails. This class
 * is not a replay frontier: its initial offset must be established by recovery.
 */
final class PartitionDurableOffsetTracker {
    private final Executor notificationExecutor;
    private final int maxOutstandingGroups;
    private final long maxOutstandingBytes;
    private int outstandingGroups;
    private int inFlightWrites;
    private long outstandingBytes;
    private final ArrayDeque<Reservation> pending = new ArrayDeque<>();
    private final ArrayDeque<Notification> notifications = new ArrayDeque<>();
    private boolean drainingNotifications;
    private long generation;
    private long confirmedOffset;
    private long reservedOffset;
    private Throwable failure;

    PartitionDurableOffsetTracker(long generation, long confirmedOffset, Executor notificationExecutor,
                                  int maxOutstandingGroups, long maxOutstandingBytes) {
        if (generation < 0L || confirmedOffset < 0L || maxOutstandingGroups <= 0 || maxOutstandingBytes <= 0L) {
            throw new IllegalArgumentException("invalid generation, confirmed offset or admission limit");
        }
        this.notificationExecutor = Objects.requireNonNull(notificationExecutor, "notificationExecutor");
        this.maxOutstandingGroups = maxOutstandingGroups;
        this.maxOutstandingBytes = maxOutstandingBytes;
        this.generation = generation;
        this.confirmedOffset = confirmedOffset;
        this.reservedOffset = confirmedOffset;
    }

    synchronized Reservation reserve(long expectedGeneration, long firstOffset, long endOffset, long admittedBytes) {
        if (failure != null) {
            throw new IllegalStateException("partition durability tracker is fenced", failure);
        }
        if (expectedGeneration != generation) {
            throw new IllegalStateException("stale partition generation");
        }
        if (firstOffset != reservedOffset || endOffset <= firstOffset || admittedBytes <= 0L) {
            throw new IllegalArgumentException("partition offsets or admitted byte count are invalid");
        }
        if (outstandingGroups >= maxOutstandingGroups || admittedBytes > maxOutstandingBytes ||
            outstandingBytes > maxOutstandingBytes - admittedBytes) {
            throw new WalCapacityExceededException("partition durability admission limit reached");
        }
        Reservation reservation = new Reservation(this, generation, endOffset, admittedBytes);
        outstandingGroups++;
        inFlightWrites++;
        outstandingBytes += admittedBytes;
        pending.addLast(reservation);
        reservedOffset = endOffset;
        return reservation;
    }

    synchronized long confirmedOffset() {
        return confirmedOffset;
    }

    synchronized CompletableFuture<Void> awaitOffset(long expectedGeneration, long endOffset) {
        if (expectedGeneration != generation) {
            return CompletableFuture.failedFuture(new IllegalStateException("stale partition generation"));
        }
        if (endOffset <= confirmedOffset) {
            return CompletableFuture.completedFuture(null);
        }
        if (failure != null) {
            return CompletableFuture.failedFuture(failure);
        }
        for (Reservation reservation : pending) {
            if (endOffset <= reservation.endOffset) {
                return reservation.confirmed.thenApply(ignored -> null);
            }
        }
        return CompletableFuture.failedFuture(new IllegalArgumentException("offset has not been reserved"));
    }

    /**
     * Fence confirmation immediately, but leave all I/O in flight. Callers must
     * wait for it to settle before truncating or resetting the generation.
     */
    void fence(Throwable reason) {
        Objects.requireNonNull(reason, "reason");
        boolean drain;
        synchronized (this) {
            if (failure != null) {
                return;
            }
            failure = reason;
            while (!pending.isEmpty()) {
                Reservation entry = pending.removeFirst();
                entry.pending = false;
                notifications.addLast(new Notification(entry, reason));
            }
            notifyAll();
            drain = startNotificationDrain();
        }
        if (drain) {
            scheduleNotificationDrain();
        }
    }

    synchronized int inFlightWrites() {
        return inFlightWrites;
    }

    /** Wait for submitted WAL operations, not for user notification callbacks. */
    synchronized boolean awaitSettled(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0L) {
            throw new IllegalArgumentException("negative timeout");
        }
        long timeoutNanos = unit.toNanos(timeout);
        long remaining = timeoutNanos;
        long started = System.nanoTime();
        while (inFlightWrites != 0) {
            if (remaining <= 0L) {
                return false;
            }
            TimeUnit.NANOSECONDS.timedWait(this, remaining);
            long elapsed = System.nanoTime() - started;
            remaining = elapsed < 0L || elapsed >= timeoutNanos ? 0L : timeoutNanos - elapsed;
        }
        return true;
    }

    /** Start a new generation only after every prior WAL operation has settled. */
    void reset(long newGeneration, long recoveredOffset, Throwable reason) {
        Objects.requireNonNull(reason, "reason");
        boolean drain;
        synchronized (this) {
            if (newGeneration <= generation || recoveredOffset < 0L) {
                throw new IllegalArgumentException("invalid new generation or recovered offset");
            }
            if (inFlightWrites != 0) {
                throw new IllegalStateException("prior WAL writes must settle before resetting the partition");
            }
            for (Reservation reservation : pending) {
                reservation.pending = false;
                notifications.addLast(new Notification(reservation, reason));
            }
            pending.clear();
            generation = newGeneration;
            confirmedOffset = recoveredOffset;
            reservedOffset = recoveredOffset;
            failure = null;
            drain = startNotificationDrain();
        }
        if (drain) {
            scheduleNotificationDrain();
        }
    }

    private void complete(Reservation reservation, boolean walCompletion, Throwable error) {
        Throwable completionError = error instanceof CompletionException && error.getCause() != null
            ? error.getCause() : error;
        boolean drain;
        synchronized (this) {
            if (!reservation.markSettled(walCompletion)) return;
            if (walCompletion) {
                inFlightWrites--;
                notifyAll();
            }
            if (reservation.generation != generation || failure != null || !reservation.pending) {
                return;
            }
            applyCompletion(reservation, completionError);
            drain = startNotificationDrain();
        }
        if (drain) {
            scheduleNotificationDrain();
        }
    }

    // Called with the tracker lock held.
    private void applyCompletion(Reservation reservation, Throwable completionError) {
        if (completionError != null) {
            failure = completionError;
            while (!pending.isEmpty()) {
                Reservation entry = pending.removeFirst();
                entry.pending = false;
                notifications.addLast(new Notification(entry, completionError));
            }
        } else if (reservation.walSettled && reservation.publicationSettled) {
            reservation.durable = true;
            while (!pending.isEmpty() && pending.peekFirst().durable) {
                Reservation head = pending.removeFirst();
                head.pending = false;
                confirmedOffset = head.endOffset;
                notifications.addLast(new Notification(head, null));
            }
        }
    }

    // Called only under the tracker lock. Exactly one caller drains callbacks,
    // but callbacks themselves execute outside that lock to avoid deadlocks.
    private boolean startNotificationDrain() {
        if (drainingNotifications || notifications.isEmpty()) {
            return false;
        }
        drainingNotifications = true;
        return true;
    }

    private void scheduleNotificationDrain() {
        try {
            notificationExecutor.execute(this::drainNotifications);
        } catch (RuntimeException rejected) {
            // Preserve completion even during shutdown or executor saturation.
            // The caller may block on this exceptional fallback path.
            drainNotifications();
        }
    }

    private void drainNotifications() {
        while (true) {
            Notification notification;
            synchronized (this) {
                notification = notifications.pollFirst();
                if (notification == null) {
                    drainingNotifications = false;
                    return;
                }
            }
            try {
                if (notification.error() == null) {
                    notification.reservation().confirmed.complete(null);
                } else {
                    notification.reservation().confirmed.completeExceptionally(notification.error());
                }
            } finally {
                synchronized (this) {
                    outstandingGroups--;
                    outstandingBytes -= notification.reservation().admittedBytes;
                }
            }
        }
    }

    private record Notification(Reservation reservation, Throwable error) {
    }

    static final class Reservation {
        private final PartitionDurableOffsetTracker owner;
        private final long generation;
        private final long endOffset;
        private final long admittedBytes;
        private final AtomicBoolean bound = new AtomicBoolean();
        private final CompletableFuture<Void> confirmed = new CompletableFuture<>();
        private boolean durable;
        private boolean walSettled;
        private boolean publicationSettled;
        private boolean pending = true;

        private Reservation(PartitionDurableOffsetTracker owner, long generation, long endOffset, long admittedBytes) {
            this.owner = owner;
            this.generation = generation;
            this.endOffset = endOffset;
            this.admittedBytes = admittedBytes;
        }

        // Called with the owner lock held.
        private boolean markSettled(boolean walCompletion) {
            if (walCompletion) {
                if (walSettled) return false;
                walSettled = true;
            } else {
                if (publicationSettled) return false;
                publicationSettled = true;
            }
            return true;
        }

        void bind(CompletableFuture<?> durability) {
            bind(durability, CompletableFuture.completedFuture(null));
        }

        void bind(CompletableFuture<?> durability, CompletableFuture<?> publication) {
            Objects.requireNonNull(durability, "durability");
            Objects.requireNonNull(publication, "publication");
            if (!bound.compareAndSet(false, true)) {
                throw new IllegalStateException("reservation is already bound");
            }
            durability.whenComplete((ignored, error) -> owner.complete(this, true, error));
            publication.whenComplete((ignored, error) -> owner.complete(this, false, error));
        }

        CompletableFuture<Void> confirmed() {
            // Caller cancellation cannot cancel the internal durable confirmation.
            return confirmed.thenApply(ignored -> null);
        }
    }
}
