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

import org.apache.kafka.storage.internals.shared.metadata.InMemoryObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.ObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrphanCleanupSchedulerTest {
    @Test
    void closeWaitsForInFlightCleanupBeforeReturning() throws Exception {
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(105L, 1L).get();
        CompletableFuture<Void> blockedDelete = new CompletableFuture<>();
        ObjectStore objects = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return blockedDelete;
            }
        };
        OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata),
            () -> 10_000L,
            1_000L
        );

        CompletableFuture<Integer> cleanup = scheduler.tryCleanOnce();
        assertFalse(scheduler.stop(), "Stopping a healthy scheduler should not report interruption");
        assertFalse(cleanup.isDone(), "First shutdown phase must not abandon the in-flight cleanup");

        CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(scheduler::close);
        assertThrows(
            java.util.concurrent.TimeoutException.class,
            () -> closeFuture.get(200, TimeUnit.MILLISECONDS),
            "Scheduler close must wait for the in-flight orphan delete"
        );

        blockedDelete.complete(null);
        closeFuture.get(10, TimeUnit.SECONDS);
        assertEquals(1, cleanup.get(10, TimeUnit.SECONDS));
    }

    @Test
    void transientClockRuntimeFailureReleasesCleanupSlotAndAllowsGracefulClose() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(301L, 1L).get(10, TimeUnit.SECONDS);
        objects.put(301L, ByteBuffer.wrap(new byte[] {1})).get(10, TimeUnit.SECONDS);
        AtomicInteger samples = new AtomicInteger();
        IllegalStateException clockFailure = new IllegalStateException("time source unavailable");

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata),
            () -> {
                if (samples.getAndIncrement() == 0) {
                    throw clockFailure;
                }
                return 10_000L;
            },
            1_000L
        )) {
            ExecutionException first = assertThrows(
                ExecutionException.class,
                () -> scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS)
            );
            assertSame(clockFailure, first.getCause());
            assertSame(clockFailure, scheduler.lastFailure().orElseThrow());
            assertTrue(objects.contains(301L));
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS),
                "The clock failure must not leave cleanup permanently BUSY");
            assertFalse(objects.contains(301L));
            assertTrue(scheduler.lastFailure().isEmpty());
        }
    }

    @Test
    void synchronousMetadataInventoryFailureReportsAndReleasesCleanupSlot() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore delegate = new InMemoryObjectMetadataStore();
        delegate.prepare(302L, 1L).get(10, TimeUnit.SECONDS);
        objects.put(302L, ByteBuffer.wrap(new byte[] {2})).get(10, TimeUnit.SECONDS);
        IllegalStateException metadataFailure = new IllegalStateException("metadata replay unavailable");
        AtomicInteger scans = new AtomicInteger();
        ObjectMetadataStore metadata = new ObjectMetadataStore() {
            @Override
            public CompletableFuture<Void> prepare(long objectId, long createdTimeMs) {
                return delegate.prepare(objectId, createdTimeMs);
            }

            @Override
            public CompletableFuture<Void> commit(SharedObjectMetadata object) {
                return delegate.commit(object);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return delegate.delete(objectId);
            }

            @Override
            public CompletableFuture<Boolean> claimCleanup(long objectId, long createdTimeMs) {
                return delegate.claimCleanup(objectId, createdTimeMs);
            }

            @Override
            public CompletableFuture<Void> completeCleanup(long objectId) {
                return delegate.completeCleanup(objectId);
            }

            @Override
            public List<SharedObjectMetadata> committedObjects() {
                return delegate.committedObjects();
            }

            @Override
            public List<PreparedObject> preparedObjects() {
                return delegate.preparedObjects();
            }

            @Override
            public List<PreparedObject> cleanupClaimedObjects() {
                if (scans.getAndIncrement() == 0) {
                    throw metadataFailure;
                }
                return delegate.cleanupClaimedObjects();
            }
        };

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata), () -> 10_000L, 1_000L
        )) {
            ExecutionException first = assertThrows(
                ExecutionException.class,
                () -> scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS)
            );
            assertSame(metadataFailure, first.getCause());
            assertSame(metadataFailure, scheduler.lastFailure().orElseThrow());
            assertTrue(objects.contains(302L));
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(objects.contains(302L));
            assertTrue(scheduler.lastFailure().isEmpty());
        }
    }

    @Test
    void clockBeforeGraceCannotDeletePreparedAtEpochZero() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(401L, 0L).get(10, TimeUnit.SECONDS);
        objects.put(401L, ByteBuffer.wrap(new byte[] {1})).get(10, TimeUnit.SECONDS);
        AtomicLong clock = new AtomicLong(999L);

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata), clock::get, 1_000L
        )) {
            assertEquals(0, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertTrue(objects.contains(401L), "PREPARED at zero must receive the full grace");
            assertTrue(metadata.isPrepared(401L));
            assertFalse(metadata.isCleanupClaimed(401L));

            clock.set(1_000L);
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(objects.contains(401L));
            assertTrue(metadata.isCleanupClaimed(401L));
        }
    }

    @Test
    void beforeGraceStillRetriesDurableClaimWithoutClaimingPreparedObjects() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(402L, 0L).get(10, TimeUnit.SECONDS);
        metadata.prepare(403L, 0L).get(10, TimeUnit.SECONDS);
        objects.put(402L, ByteBuffer.wrap(new byte[] {2})).get(10, TimeUnit.SECONDS);
        objects.put(403L, ByteBuffer.wrap(new byte[] {3})).get(10, TimeUnit.SECONDS);
        assertTrue(metadata.claimCleanup(402L, 0L).get(10, TimeUnit.SECONDS));

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata), () -> 500L, 1_000L
        )) {
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(objects.contains(402L));
            assertTrue(objects.contains(403L));
            assertTrue(metadata.isPrepared(403L));

            // A delayed PUT after the first DELETE must be deleted again
            // under the same persisted CLAIMED fence, even before grace.
            objects.put(402L, ByteBuffer.wrap(new byte[] {4})).get(10, TimeUnit.SECONDS);
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(objects.contains(402L));
            assertTrue(objects.contains(403L));
            assertFalse(metadata.isCleanupClaimed(403L));
        }
    }

    @Test
    void negativeClockFailsClosedAndReleasesAdmissionForRetry() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(404L, 0L).get(10, TimeUnit.SECONDS);
        objects.put(404L, ByteBuffer.wrap(new byte[] {5})).get(10, TimeUnit.SECONDS);
        AtomicLong clock = new AtomicLong(-1L);

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata), clock::get, 1_000L
        )) {
            ExecutionException failure = assertThrows(
                ExecutionException.class, () -> scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS)
            );
            assertTrue(failure.getCause().getMessage().contains("non-negative"));
            assertTrue(scheduler.lastFailure().isPresent());
            assertTrue(objects.contains(404L));
            assertFalse(metadata.isCleanupClaimed(404L));

            clock.set(1_000L);
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(objects.contains(404L));
            assertTrue(scheduler.lastFailure().isEmpty());
        }
    }

    @Test
    void appliesGraceCutoffBeforeClaimingPreparedObjects() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(101L, 8_999L).get();
        objects.put(101L, ByteBuffer.wrap(new byte[] {1})).get();
        metadata.prepare(102L, 9_001L).get();
        objects.put(102L, ByteBuffer.wrap(new byte[] {2})).get();
        AtomicLong now = new AtomicLong(10_000L);

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata),
            now::get,
            1_000L
        )) {
            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(objects.contains(101L));
            assertTrue(metadata.isCleanupClaimed(101L));
            assertTrue(objects.contains(102L));
            assertTrue(metadata.isPrepared(102L));
        }
    }

    @Test
    void overlappingPassReturnsImmediatelyWithoutRunningSecondDelete() throws Exception {
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(103L, 1L).get();
        CompletableFuture<Void> blockedDelete = new CompletableFuture<>();
        ObjectStore objects = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return blockedDelete;
            }
        };

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata),
            () -> 10_000L,
            1_000L
        )) {
            CompletableFuture<Integer> first = scheduler.tryCleanOnce();
            assertEquals(0, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(first.isDone());
            blockedDelete.complete(null);
            assertEquals(1, first.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void failureIsReportedAndLaterPassCanRetryClaimedDelete() throws Exception {
        InMemoryObjectStore delegate = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        metadata.prepare(104L, 1L).get();
        delegate.put(104L, ByteBuffer.wrap(new byte[] {4})).get();
        AtomicLong attempts = new AtomicLong();
        ObjectStore objects = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return delegate.put(objectId, data);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return delegate.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                if (attempts.incrementAndGet() == 1L) {
                    return CompletableFuture.failedFuture(new RuntimeException("injected cleanup failure"));
                }
                return delegate.delete(objectId);
            }
        };

        try (OrphanCleanupScheduler scheduler = new OrphanCleanupScheduler(
            new OrphanObjectCleaner(objects, metadata),
            () -> 10_000L,
            1_000L
        )) {
            try {
                scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS);
            } catch (Exception expected) {
                // The scheduler must release its overlap gate even when the asynchronous delete fails.
            }
            assertTrue(scheduler.lastFailure().isPresent());
            assertTrue(metadata.isCleanupClaimed(104L));

            assertEquals(1, scheduler.tryCleanOnce().get(10, TimeUnit.SECONDS));
            assertFalse(delegate.contains(104L));
            assertTrue(scheduler.lastFailure().isEmpty());
        }
    }
}
