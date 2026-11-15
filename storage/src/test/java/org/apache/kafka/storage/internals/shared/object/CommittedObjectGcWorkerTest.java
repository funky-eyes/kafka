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

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simulated KRaft grants are NOT a production delete authority. Test-only
 * permissions exercise the future executor's local safety and retry contract.
 */
class CommittedObjectGcWorkerTest {
    private static final long OBJECT_ID = 501L;

    @Test
    void distinctKRaftGrantsAndDrainedReadersShareOnePhysicalDeleteBatch() {
        TestContext ctx = new TestContext();
        for (long id : List.of(501L, 502L, 503L)) {
            ctx.put(id);
            ctx.authority.add(id);
        }
        ctx.fence.fence(501L);
        ctx.fence.fence(502L);
        assertEquals(new CommittedObjectGcWorker.SweepStats(3, 1, 2), ctx.worker.sweep().join());
        assertEquals(List.of(List.of(501L, 502L)), ctx.store.batches);
        assertFalse(ctx.store.backing.contains(501L));
        assertFalse(ctx.store.backing.contains(502L));
        assertTrue(ctx.store.backing.contains(503L));
        assertEquals(2, ctx.authority.completions.get());
        assertTrue(ctx.authority.ready.containsKey(503L));
    }

    @Test
    void partialPhysicalBatchFailureKeepsEveryKRaftGrantRetryable() {
        TestContext ctx = new TestContext();
        for (long id : List.of(501L, 502L)) {
            ctx.put(id);
            ctx.authority.add(id);
            ctx.fence.fence(id);
        }
        // Model S3 partially deleting the second key while the first fails.
        ctx.store.failNext.set(true);
        assertThrows(CompletionException.class, () -> ctx.worker.sweep().join());
        assertEquals(0, ctx.authority.completions.get());
        assertTrue(ctx.authority.ready.containsKey(501L));
        assertTrue(ctx.authority.ready.containsKey(502L));

        // Both are retried, including whichever physical DELETE succeeded
        // before the first HTTP/batch failure. DELETE is idempotent.
        assertEquals(new CommittedObjectGcWorker.SweepStats(2, 0, 2), ctx.worker.sweep().join());
        assertEquals(List.of(List.of(501L, 502L), List.of(501L, 502L)), ctx.store.batches);
        assertFalse(ctx.store.backing.contains(501L));
        assertFalse(ctx.store.backing.contains(502L));
        assertEquals(2, ctx.authority.completions.get());
    }

    @Test
    void noDurableDeleteReadyGrantNeverIssuesRemoteDelete() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.fence.fence(OBJECT_ID);

        assertEquals(new CommittedObjectGcWorker.SweepStats(0, 0, 0), ctx.worker.sweep().join());
        assertTrue(ctx.store.backing.contains(OBJECT_ID));
        assertEquals(0, ctx.store.deletes.get());
    }

    @Test
    void aGrantWithoutGlobalOrLocalQuiescenceNeverDeletes() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.authority.add(OBJECT_ID);
        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 1, 0), ctx.worker.sweep().join());

        ctx.fence.fence(OBJECT_ID);
        ctx.authority.permit = false;
        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 1, 0), ctx.worker.sweep().join());
        assertTrue(ctx.store.backing.contains(OBJECT_ID));
        assertEquals(0, ctx.store.deletes.get());
    }

    @Test
    void logicalCacheReadAndPhysicalRangeGetMustBothDrainBeforeDelete() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.authority.add(OBJECT_ID);
        ActiveCommittedObjectReads.ReadLease cache = ctx.reads.tryAcquireLogicalRead(OBJECT_ID).orElseThrow();
        ActiveCommittedObjectReads.ReadLease physical = ctx.reads.tryAcquire(OBJECT_ID).orElseThrow();
        ctx.fence.fence(OBJECT_ID);

        cache.close();
        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 1, 0), ctx.worker.sweep().join());
        assertTrue(ctx.store.backing.contains(OBJECT_ID));
        physical.close();
        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 0, 1), ctx.worker.sweep().join());
        assertFalse(ctx.store.backing.contains(OBJECT_ID));
        assertEquals(1, ctx.authority.completions.get());
    }

    @Test
    void unfinishedUploadMustDrainBeforePhysicalDelete() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.authority.add(OBJECT_ID);
        ActiveObjectUploads.UploadLease upload = ctx.uploads.beginLease(OBJECT_ID);
        ctx.fence.fence(OBJECT_ID);

        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 1, 0), ctx.worker.sweep().join());
        assertTrue(ctx.store.backing.contains(OBJECT_ID));
        upload.close();
        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 0, 1), ctx.worker.sweep().join());
        assertFalse(ctx.store.backing.contains(OBJECT_ID));
    }

    @Test
    void failedRemoteDeleteLeavesDurableGrantRetryable() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.authority.add(OBJECT_ID);
        ctx.fence.fence(OBJECT_ID);
        ctx.store.failNext.set(true);

        assertThrows(CompletionException.class, () -> ctx.worker.sweep().join());
        assertTrue(ctx.store.backing.contains(OBJECT_ID));
        assertTrue(ctx.authority.ready.containsKey(OBJECT_ID));
        assertEquals(0, ctx.authority.completions.get());

        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 0, 1), ctx.worker.sweep().join());
        assertFalse(ctx.store.backing.contains(OBJECT_ID));
        assertFalse(ctx.authority.ready.containsKey(OBJECT_ID));
    }

    @Test
    void successfulDeleteWithFailedKRaftCompletionMustRetryDelete() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.authority.add(OBJECT_ID);
        ctx.fence.fence(OBJECT_ID);
        ctx.authority.failCompletion.set(true);

        assertThrows(CompletionException.class, () -> ctx.worker.sweep().join());
        assertFalse(ctx.store.backing.contains(OBJECT_ID));
        assertTrue(ctx.authority.ready.containsKey(OBJECT_ID));
        assertEquals(0, ctx.authority.completions.get());

        assertEquals(new CommittedObjectGcWorker.SweepStats(1, 0, 1), ctx.worker.sweep().join());
        assertEquals(2, ctx.store.deletes.get());
        assertEquals(1, ctx.authority.completions.get());
        assertFalse(ctx.authority.ready.containsKey(OBJECT_ID));
    }

    @Test
    void concurrentSweepOrCallerCancellationCannotReleaseInFlightOwnership() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.authority.add(OBJECT_ID);
        ctx.fence.fence(OBJECT_ID);
        CompletableFuture<Void> deferredDelete = new CompletableFuture<>();
        ctx.store.blockedDelete = deferredDelete;

        CompletableFuture<CommittedObjectGcWorker.SweepStats> caller = ctx.worker.sweep();
        assertEquals(1, ctx.store.deletes.get());
        assertTrue(caller.cancel(false));
        assertThrows(CompletionException.class, () -> ctx.worker.sweep().join());

        deferredDelete.complete(null);
        // The physical I/O owns the sweep even though the caller was cancelled.
        assertEquals(1, ctx.authority.completions.get());
        assertEquals(new CommittedObjectGcWorker.SweepStats(0, 0, 0), ctx.worker.sweep().join());
    }

    @Test
    void duplicateAndOversizedAuthorityBatchesAreRejectedBeforeAnyDelete() {
        TestContext ctx = new TestContext();
        ctx.put(OBJECT_ID);
        ctx.fence.fence(OBJECT_ID);

        CommittedObjectGcWorker.DeleteReady original = ready(OBJECT_ID);
        ctx.authority.override = List.of(original, new CommittedObjectGcWorker.DeleteReady(
            original.descriptor(), original.retirementGeneration() + 1L, original.readyRecordOffset()));
        assertThrows(CompletionException.class, () -> ctx.worker.sweep().join());
        assertEquals(0, ctx.store.deletes.get());

        List<CommittedObjectGcWorker.DeleteReady> overflow = new ArrayList<>();
        for (int i = 0; i <= CommittedObjectGcWorker.MAX_DELETE_BATCH; i++) {
            overflow.add(ready(1000L + i));
        }
        ctx.authority.override = overflow;
        assertThrows(CompletionException.class, () -> ctx.worker.sweep().join());
        assertTrue(ctx.store.backing.contains(OBJECT_ID));
        assertEquals(0, ctx.store.deletes.get());
    }

    private static CommittedObjectGcWorker.DeleteReady ready(long objectId) {
        SharedObjectRange range = new SharedObjectRange(
            new SharedPartitionId(1L, 2L, 0),
            new OffsetRange(0L, 10L),
            1, 0L, 10, 17L
        );
        SharedObjectMetadata descriptor = new SharedObjectMetadata(
            objectId, 10L, 23L, List.of(range)
        );
        return new CommittedObjectGcWorker.DeleteReady(descriptor, 4L, 99L);
    }

    private static final class TestContext {
        private final TestStore store = new TestStore();
        private final TestAuthority authority = new TestAuthority();
        private final ActiveCommittedObjectReads reads = new ActiveCommittedObjectReads();
        private final ActiveObjectUploads uploads = new ActiveObjectUploads();
        private final LocalCommittedObjectIoFence fence = new LocalCommittedObjectIoFence(reads, uploads);
        private final CommittedObjectGcWorker worker = new CommittedObjectGcWorker(store, authority, fence);

        private void put(long id) {
            store.put(id, ByteBuffer.wrap(new byte[] {1, 2, 3})).join();
        }
    }

    private static final class TestAuthority implements CommittedObjectGcWorker.DeleteAuthority {
        private final Map<Long, CommittedObjectGcWorker.DeleteReady> ready = new LinkedHashMap<>();
        private final AtomicBoolean failCompletion = new AtomicBoolean();
        private final AtomicInteger completions = new AtomicInteger();
        private List<CommittedObjectGcWorker.DeleteReady> override;
        private boolean permit = true;

        private void add(long id) {
            ready.put(id, CommittedObjectGcWorkerTest.ready(id));
        }

        @Override
        public CompletableFuture<List<CommittedObjectGcWorker.DeleteReady>> readyDeletes(int maxBatch) {
            return CompletableFuture.completedFuture(
                override == null ? List.copyOf(ready.values()) : List.copyOf(override)
            );
        }

        @Override
        public CompletableFuture<Boolean> stillDeleteReady(CommittedObjectGcWorker.DeleteReady candidate) {
            return CompletableFuture.completedFuture(
                permit && candidate.equals(ready.get(candidate.objectId()))
            );
        }

        @Override
        public CompletableFuture<Void> recordDeleted(CommittedObjectGcWorker.DeleteReady candidate) {
            if (failCompletion.compareAndSet(true, false)) {
                return CompletableFuture.failedFuture(new IllegalStateException("injected KRaft commit timeout"));
            }
            if (!candidate.equals(ready.remove(candidate.objectId()))) {
                return CompletableFuture.failedFuture(new IllegalStateException("stale controller grant"));
            }
            completions.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class TestStore implements ObjectStore {
        private final InMemoryObjectStore backing = new InMemoryObjectStore();
        private final AtomicInteger deletes = new AtomicInteger();
        private final List<List<Long>> batches = new ArrayList<>();
        private final AtomicBoolean failNext = new AtomicBoolean();
        private volatile CompletableFuture<Void> blockedDelete;

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            return backing.put(objectId, data);
        }

        @Override
        public CompletableFuture<ByteBuffer> rangeRead(long id, long pos, int len) {
            return backing.rangeRead(id, pos, len);
        }

        @Override
        public CompletableFuture<Void> deleteBatch(List<Long> ids) {
            batches.add(List.copyOf(ids));
            return ObjectStore.super.deleteBatch(ids);
        }

        @Override
        public CompletableFuture<Void> delete(long id) {
            deletes.incrementAndGet();
            if (failNext.compareAndSet(true, false)) {
                return CompletableFuture.failedFuture(new IllegalStateException("injected MinIO DELETE failure"));
            }
            CompletableFuture<Void> release = blockedDelete;
            return release == null ? backing.delete(id) : release.thenCompose(ignored -> backing.delete(id));
        }
    }
}
