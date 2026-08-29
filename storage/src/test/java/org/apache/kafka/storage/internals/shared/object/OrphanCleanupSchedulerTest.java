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
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrphanCleanupSchedulerTest {
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
