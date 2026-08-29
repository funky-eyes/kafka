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

    private final OrphanObjectCleaner cleaner;
    private final LongSupplier currentTimeMsSupplier;
    private final long orphanGraceMs;
    private final AtomicBoolean cleanupInProgress = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

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

        final long cutoff;
        try {
            cutoff = Math.max(0L, Math.subtractExact(currentTimeMsSupplier.getAsLong(), orphanGraceMs));
        } catch (ArithmeticException e) {
            cleanupInProgress.set(false);
            return CompletableFuture.failedFuture(e);
        }

        final CompletableFuture<Integer> result;
        try {
            result = cleaner.clean(cutoff);
        } catch (RuntimeException e) {
            cleanupInProgress.set(false);
            return CompletableFuture.failedFuture(e);
        }
        return result.whenComplete((deleted, error) -> {
            if (error == null) {
                lastFailure.set(null);
                if (deleted != null && deleted > 0) {
                    LOG.info("Shared orphan cleanup deleted {} physical object(s)", deleted);
                }
            } else {
                lastFailure.set(error);
                LOG.warn("Shared orphan cleanup failed", error);
            }
            cleanupInProgress.set(false);
        });
    }

    public Optional<Throwable> lastFailure() {
        return Optional.ofNullable(lastFailure.get());
    }

    private void runScheduledCleanup() {
        tryCleanOnce().whenComplete((ignored, error) -> {
            if (error != null) {
                lastFailure.set(error);
            }
        });
    }

    @Override
    public synchronized void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }
}
