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
package org.apache.kafka.storage.internals.shared.object;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/** Periodically claims and re-deletes orphan physical objects without overlapping cleanup passes. */
public final class OrphanCleanupScheduler implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(OrphanCleanupScheduler.class);
    private static final long CLOSE_WAIT_LOG_INTERVAL_SECONDS = 30L;

    private final OrphanObjectCleaner cleaner;
    private final LongSupplier currentTimeMsSupplier;
    private final long orphanGraceMs;
    private final AtomicBoolean cleanupInProgress = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();
    private final Object cleanupDrainMonitor = new Object();

    private ScheduledExecutorService executor;

    public OrphanCleanupScheduler(
        OrphanObjectCleaner cleaner,
        LongSupplier currentTimeMsSupplier,
        long orphanGraceMs
    ) {
        this.cleaner = Objects.requireNonNull(cleaner, "cleaner");
        this.currentTimeMsSupplier = Objects.requireNonNull(currentTimeMsSupplier, "currentTimeMsSupplier");
        if (orphanGraceMs <= 0) {
            throw new IllegalArgumentException("orphanGraceMs must be positive");
        }
        this.orphanGraceMs = orphanGraceMs;
    }

    public synchronized void start(long intervalMs) {
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("intervalMs must be positive");
        }
        if (closed.get()) {
            throw new IllegalStateException("Orphan cleanup scheduler is closed");
        }
        if (executor != null) {
            throw new IllegalStateException("Orphan cleanup scheduler is already started");
        }
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "kafka-shared-storage-orphan-cleaner");
            thread.setDaemon(true);
            return thread;
        });
        LOG.info(
            "Started shared orphan cleanup scheduler with intervalMs={} and orphanGraceMs={}",
            intervalMs,
            orphanGraceMs
        );
        executor.scheduleWithFixedDelay(this::runScheduledCleanup, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    public CompletableFuture<Integer> tryCleanOnce() {
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Orphan cleanup scheduler is closed"));
        }
        if (!cleanupInProgress.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(0);
        }
        if (closed.get()) {
            releaseCleanupSlot();
            return CompletableFuture.failedFuture(new IllegalStateException("Orphan cleanup scheduler is closed"));
        }

        final long cutoff;
        try {
            cutoff = Math.max(0L, Math.subtractExact(currentTimeMsSupplier.getAsLong(), orphanGraceMs));
        } catch (ArithmeticException e) {
            releaseCleanupSlot();
            return CompletableFuture.failedFuture(e);
        }

        final CompletableFuture<Integer> result;
        try {
            result = cleaner.clean(cutoff);
        } catch (RuntimeException e) {
            releaseCleanupSlot();
            return CompletableFuture.failedFuture(e);
        }
        return result.whenComplete((deleted, error) -> {
            try {
                if (error == null) {
                    lastFailure.set(null);
                    if (deleted != null && deleted > 0) {
                        LOG.info("Shared orphan cleanup deleted {} physical object(s)", deleted);
                    }
                } else {
                    lastFailure.set(error);
                    LOG.warn("Shared orphan cleanup failed", error);
                }
            } finally {
                releaseCleanupSlot();
            }
        });
    }

    public Optional<Throwable> lastFailure() {
        return Optional.ofNullable(lastFailure.get());
    }

    private void releaseCleanupSlot() {
        synchronized (cleanupDrainMonitor) {
            cleanupInProgress.set(false);
            cleanupDrainMonitor.notifyAll();
        }
    }

    private void runScheduledCleanup() {
        tryCleanOnce().whenComplete((ignored, error) -> {
            if (error != null) {
                lastFailure.set(error);
            }
        });
    }

    /**
     * Stops accepting and scheduling new cleanup passes without waiting for an already-started asynchronous delete.
     *
     * <p>The extension closes the metadata store between this phase and {@link #close()} so a cleanup waiting for a
     * metadata claim cannot deadlock broker shutdown.</p>
     *
     * @return true if the caller was interrupted while waiting for the scheduler executor to stop
     */
    public synchronized boolean stop() {
        if (!closed.compareAndSet(false, true)) {
            return false;
        }
        ScheduledExecutorService executorToStop = executor;
        if (executorToStop != null) {
            executorToStop.shutdownNow();
            executor = null;
        }
        return awaitExecutorStop(executorToStop);
    }

    @Override
    public void close() {
        boolean interrupted = stop();
        interrupted |= awaitCleanupDrain();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean awaitExecutorStop(ScheduledExecutorService executorToStop) {
        if (executorToStop == null) {
            return false;
        }
        boolean interrupted = false;
        while (!executorToStop.isTerminated()) {
            try {
                if (!executorToStop.awaitTermination(CLOSE_WAIT_LOG_INTERVAL_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("Shared orphan cleanup scheduler executor is still running during close; interrupting it again");
                    executorToStop.shutdownNow();
                }
            } catch (InterruptedException e) {
                interrupted = true;
                executorToStop.shutdownNow();
            }
        }
        return interrupted;
    }

    private boolean awaitCleanupDrain() {
        boolean interrupted = false;
        synchronized (cleanupDrainMonitor) {
            while (cleanupInProgress.get()) {
                try {
                    TimeUnit.SECONDS.timedWait(cleanupDrainMonitor, CLOSE_WAIT_LOG_INTERVAL_SECONDS);
                    if (cleanupInProgress.get()) {
                        LOG.warn("Still draining an in-flight orphan cleanup during scheduler close");
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        return interrupted;
    }
}
