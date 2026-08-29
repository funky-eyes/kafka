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
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrphanObjectCleanerTest {
    @Test
    void committedObjectIsNeverClaimedOrDeleted() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        long objectId = 101L;
        metadata.prepare(objectId, 100L).get();
        objects.put(objectId, ByteBuffer.wrap(new byte[] {1})).get();
        metadata.commit(metadata(objectId)).get();

        int deleted = new OrphanObjectCleaner(objects, metadata).clean(1_000L).get();

        assertEquals(0, deleted);
        assertTrue(objects.contains(objectId));
        assertTrue(metadata.isCommitted(objectId));
    }

    @Test
    void cleanupClaimAndCommitRaceHasExactlyOneWinner() throws Exception {
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        long objectId = 102L;
        long createdTimeMs = 200L;
        metadata.prepare(objectId, createdTimeMs).get();

        CountDownLatch start = new CountDownLatch(1);
        CompletableFuture<Boolean> cleanup = CompletableFuture.supplyAsync(() -> {
            await(start);
            return metadata.claimCleanup(objectId, createdTimeMs).join();
        });
        CompletableFuture<Boolean> commit = CompletableFuture.supplyAsync(() -> {
            await(start);
            try {
                metadata.commit(metadata(objectId)).join();
                return true;
            } catch (RuntimeException expected) {
                return false;
            }
        });

        start.countDown();
        boolean cleanupWon = cleanup.get(10, TimeUnit.SECONDS);
        boolean commitWon = commit.get(10, TimeUnit.SECONDS);

        assertTrue(cleanupWon ^ commitWon, "exactly one conditional transition must win");
        assertEquals(commitWon, metadata.isCommitted(objectId));
        assertEquals(cleanupWon, metadata.isCleanupClaimed(objectId));
    }

    @Test
    void failedPhysicalDeleteRetainsClaimForRetry() throws Exception {
        InMemoryObjectStore delegate = new InMemoryObjectStore();
        AtomicBoolean failFirstDelete = new AtomicBoolean(true);
        ObjectStore flakyObjects = new ObjectStore() {
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
                if (failFirstDelete.compareAndSet(true, false)) {
                    return CompletableFuture.failedFuture(new RuntimeException("injected delete failure"));
                }
                return delegate.delete(objectId);
            }
        };
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        long objectId = 103L;
        metadata.prepare(objectId, 300L).get();
        flakyObjects.put(objectId, ByteBuffer.wrap(new byte[] {3})).get();
        OrphanObjectCleaner cleaner = new OrphanObjectCleaner(flakyObjects, metadata);

        ExecutionException failure = assertThrows(ExecutionException.class, () -> cleaner.clean(1_000L).get());
        assertTrue(failure.getCause().getMessage().contains("delete failure"));
        assertTrue(metadata.isCleanupClaimed(objectId));
        assertTrue(delegate.contains(objectId));

        assertEquals(1, cleaner.clean(1_000L).get());
        assertFalse(delegate.contains(objectId));
        assertTrue(metadata.isCleanupClaimed(objectId));
    }

    @Test
    void latePutAfterSuccessfulDeleteIsRemovedByLaterPass() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        long objectId = 104L;
        metadata.prepare(objectId, 400L).get();
        objects.put(objectId, ByteBuffer.wrap(new byte[] {4})).get();
        OrphanObjectCleaner cleaner = new OrphanObjectCleaner(objects, metadata);

        assertEquals(1, cleaner.clean(1_000L).get());
        assertFalse(objects.contains(objectId));
        assertTrue(metadata.isCleanupClaimed(objectId));

        // Models an S3 PUT that had already escaped the crashed uploader and became visible after the first DELETE.
        objects.put(objectId, ByteBuffer.wrap(new byte[] {5})).get();
        assertTrue(objects.contains(objectId));

        assertEquals(1, cleaner.clean(1_000L).get());
        assertFalse(objects.contains(objectId));
        assertTrue(metadata.isCleanupClaimed(objectId));
    }

    @Test
    void activePreparedUploadIsNeverClaimed() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        ActiveObjectUploads activeUploads = new ActiveObjectUploads();
        long objectId = 105L;
        metadata.prepare(objectId, 500L).get();
        objects.put(objectId, ByteBuffer.wrap(new byte[] {6})).get();
        activeUploads.begin(objectId);

        OrphanObjectCleaner cleaner = new OrphanObjectCleaner(objects, metadata, activeUploads);
        assertEquals(0, cleaner.clean(10_000L).get());
        assertTrue(objects.contains(objectId));
        assertTrue(metadata.isPrepared(objectId));

        activeUploads.end(objectId);
        assertEquals(1, cleaner.clean(10_000L).get());
        assertFalse(objects.contains(objectId));
        assertTrue(metadata.isCleanupClaimed(objectId));
    }

    @Test
    void youngPreparedObjectIsNotClaimed() throws Exception {
        InMemoryObjectStore objects = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadata = new InMemoryObjectMetadataStore();
        long objectId = 106L;
        metadata.prepare(objectId, 2_000L).get();
        objects.put(objectId, ByteBuffer.wrap(new byte[] {7})).get();

        assertEquals(0, new OrphanObjectCleaner(objects, metadata).clean(1_999L).get());
        assertTrue(objects.contains(objectId));
        assertTrue(metadata.isPrepared(objectId));
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for race start");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static SharedObjectMetadata metadata(long objectId) {
        SharedObjectRange range = new SharedObjectRange(
            new SharedPartitionId(1L, 2L, 0),
            new OffsetRange(0L, 10L),
            1,
            0L,
            10,
            17L
        );
        return new SharedObjectMetadata(objectId, 10L, 23L, List.of(range));
    }
}
