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
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamObjectDataBlockCacheTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(91, 92, 0);
    private static final OffsetRange OFFSETS = new OffsetRange(0, 10);

    @Test
    void shouldCoalesceAndReuseVerifiedDataBlockLoad() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(256);
        RemoteObjectIndex.RangeReference reference = reference(1);
        StreamObjectFormat.DataBlockIndexEntry block = block(16, 80, 101);
        StreamObjectDataBlockReader.DataBlockSnapshot snapshot = snapshot();
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> pending = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();

        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> first = cache.get(
            reference,
            block,
            () -> {
                loads.incrementAndGet();
                return pending;
            }
        );
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> second = cache.get(
            reference,
            block,
            () -> {
                loads.incrementAndGet();
                return CompletableFuture.completedFuture(snapshot);
            }
        );

        assertNotSame(first, second, "Coalescing must not share a caller cancellation handle");
        assertEquals(1, loads.get());
        pending.complete(snapshot);
        assertSame(snapshot, first.get(10, TimeUnit.SECONDS));
        assertSame(snapshot, second.get(10, TimeUnit.SECONDS));
        assertSame(snapshot, cache.get(
            reference,
            block,
            () -> {
                loads.incrementAndGet();
                return CompletableFuture.completedFuture(snapshot);
            }
        ).get(10, TimeUnit.SECONDS));
        assertEquals(1, loads.get());
        assertEquals(1, cache.size());
        assertEquals(80, cache.cachedBytes());
    }

    @Test
    void cancelledDataBlockWaiterCannotPoisonSharedVerifiedBlock() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(256);
        RemoteObjectIndex.RangeReference ref = reference(5L);
        StreamObjectFormat.DataBlockIndexEntry dataBlock = block(16, 80, 501L);
        StreamObjectDataBlockReader.DataBlockSnapshot snapshot = snapshot();
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> pending = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> abandoned = cache.get(
            ref, dataBlock, () -> {
                loads.incrementAndGet();
                return pending;
            }
        );
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> survivor = cache.get(
            ref, dataBlock, () -> {
                loads.incrementAndGet();
                return CompletableFuture.completedFuture(snapshot);
            }
        );
        assertNotSame(abandoned, survivor);
        assertEquals(1, loads.get());
        assertTrue(abandoned.cancel(true));
        assertFalse(pending.isCancelled(), "One reader cannot cancel the underlying shared DataBlock GET");
        assertFalse(survivor.isDone());
        assertEquals(80L, cache.cachedBytes());

        pending.complete(snapshot);
        assertTrue(abandoned.isCancelled());
        assertSame(snapshot, survivor.get(10, TimeUnit.SECONDS));
        assertSame(snapshot, cache.get(ref, dataBlock, () -> completedLoad(loads))
            .get(10, TimeUnit.SECONDS));
        assertEquals(1, loads.get(), "One cancelled waiter must not evict the reusable verified block");
        assertEquals(1, cache.size());
        assertEquals(80L, cache.cachedBytes());
    }

    @Test
    void noCacheStillKeepsCancelledWaiterSeparateFromSourceLoad() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(0);
        RemoteObjectIndex.RangeReference ref = reference(6L);
        StreamObjectFormat.DataBlockIndexEntry dataBlock = block(16, 80, 601L);
        StreamObjectDataBlockReader.DataBlockSnapshot snapshot = snapshot();
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> transport = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> cancelled = cache.get(
            ref, dataBlock, () -> {
                loads.incrementAndGet();
                return transport;
            }
        );
        assertTrue(cancelled.cancel(true));
        assertFalse(transport.isCancelled(), "Zero-budget cache bypass must not cancel a physical block load");
        transport.complete(snapshot);
        assertTrue(cancelled.isCancelled());
        assertSame(snapshot, transport.join());
        assertEquals(0, cache.size());
        StreamObjectDataBlockReader.DataBlockSnapshot reloaded =
            cache.get(ref, dataBlock, () -> completedLoad(loads)).get(10, TimeUnit.SECONDS);
        assertNotSame(snapshot, reloaded, "A zero-budget cache must load a fresh DataBlock snapshot");
        assertEquals(snapshot, reloaded, "Both independently loaded snapshots must have the same verified content");
        assertEquals(2, loads.get(), "Cache bypass still launches a fresh load for the next caller");
        assertEquals(0L, cache.cachedBytes(), "Disabled caching must retain no byte budget");
    }

    @Test
    void failedSharedDataBlockMustEvictEvenWhenOneWaiterWasCancelled() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(256);
        RemoteObjectIndex.RangeReference ref = reference(7L);
        StreamObjectFormat.DataBlockIndexEntry dataBlock = block(16, 80, 701L);
        StreamObjectDataBlockReader.DataBlockSnapshot recovered = snapshot();
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> pending = new CompletableFuture<>();
        AtomicInteger loads = new AtomicInteger();

        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> cancelled = cache.get(
            ref, dataBlock, () -> {
                loads.incrementAndGet();
                return pending;
            }
        );
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> survivor = cache.get(
            ref, dataBlock, () -> completedLoad(loads)
        );
        assertTrue(cancelled.cancel(true));
        IOException failure = new IOException("retryable physical DataBlock failure");
        pending.completeExceptionally(failure);
        ExecutionException propagated = assertThrows(
            ExecutionException.class, () -> survivor.get(10, TimeUnit.SECONDS)
        );
        assertSame(failure, propagated.getCause());
        assertEquals(0, cache.size(), "Failed shared load must still be evicted");
        assertEquals(0L, cache.cachedBytes());
        assertSame(recovered, cache.get(ref, dataBlock, () -> {
            loads.incrementAndGet();
            return CompletableFuture.completedFuture(recovered);
        }).get(10, TimeUnit.SECONDS));
        assertEquals(2, loads.get());
        assertEquals(1, cache.size());
    }

    @Test
    void shouldEvictLeastRecentlyUsedBlocksByByteBudget() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(150);
        RemoteObjectIndex.RangeReference reference = reference(2);
        StreamObjectFormat.DataBlockIndexEntry firstBlock = block(16, 80, 201);
        StreamObjectFormat.DataBlockIndexEntry secondBlock = block(96, 80, 202);
        AtomicInteger firstLoads = new AtomicInteger();
        AtomicInteger secondLoads = new AtomicInteger();

        cache.get(reference, firstBlock, () -> completedLoad(firstLoads)).get(10, TimeUnit.SECONDS);
        cache.get(reference, secondBlock, () -> completedLoad(secondLoads)).get(10, TimeUnit.SECONDS);
        cache.get(reference, firstBlock, () -> completedLoad(firstLoads)).get(10, TimeUnit.SECONDS);

        assertEquals(2, firstLoads.get());
        assertEquals(1, secondLoads.get());
        assertEquals(1, cache.size());
        assertEquals(80, cache.cachedBytes());
    }

    @Test
    void shouldAllowDisablingDataBlockCache() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(0);
        RemoteObjectIndex.RangeReference reference = reference(3);
        StreamObjectFormat.DataBlockIndexEntry block = block(16, 80, 301);
        AtomicInteger loads = new AtomicInteger();

        cache.get(reference, block, () -> completedLoad(loads)).get(10, TimeUnit.SECONDS);
        cache.get(reference, block, () -> completedLoad(loads)).get(10, TimeUnit.SECONDS);

        assertEquals(2, loads.get());
        assertEquals(0, cache.size());
        assertEquals(0, cache.cachedBytes());
    }

    @Test
    void shouldNotRetainFailedOrOversizedLoads() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(64);
        RemoteObjectIndex.RangeReference reference = reference(4);
        StreamObjectFormat.DataBlockIndexEntry oversized = block(16, 80, 401);
        AtomicInteger oversizedLoads = new AtomicInteger();

        cache.get(reference, oversized, () -> completedLoad(oversizedLoads)).get(10, TimeUnit.SECONDS);
        cache.get(reference, oversized, () -> completedLoad(oversizedLoads)).get(10, TimeUnit.SECONDS);
        assertEquals(2, oversizedLoads.get());
        assertEquals(0, cache.size());
        assertEquals(0, cache.cachedBytes());

        StreamObjectFormat.DataBlockIndexEntry failedBlock = block(16, 64, 402);
        AtomicInteger failedLoads = new AtomicInteger();
        IOException failure = new IOException("temporary block read failure");
        for (int i = 0; i < 2; i++) {
            ExecutionException error = assertThrows(
                ExecutionException.class,
                () -> cache.get(reference, failedBlock, () -> {
                    failedLoads.incrementAndGet();
                    return CompletableFuture.failedFuture(failure);
                }).get(10, TimeUnit.SECONDS)
            );
            assertSame(failure, error.getCause());
        }
        assertEquals(2, failedLoads.get());
        assertEquals(0, cache.size());
        assertEquals(0, cache.cachedBytes());
    }

    private static CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> completedLoad(
        AtomicInteger loads
    ) {
        loads.incrementAndGet();
        return CompletableFuture.completedFuture(snapshot());
    }

    private static RemoteObjectIndex.RangeReference reference(long objectId) {
        SharedObjectRange range = new SharedObjectRange(
            PARTITION,
            OFFSETS,
            4,
            72,
            8,
            11
        );
        return new RemoteObjectIndex.RangeReference(objectId, 4096, objectId + 1000, range);
    }

    private static StreamObjectFormat.DataBlockIndexEntry block(
        long position,
        int length,
        long checksum
    ) {
        return new StreamObjectFormat.DataBlockIndexEntry(
            PARTITION,
            4,
            OFFSETS,
            position,
            length,
            1,
            checksum
        );
    }

    private static StreamObjectDataBlockReader.DataBlockSnapshot snapshot() {
        return new StreamObjectDataBlockReader.DataBlockSnapshot(
            PARTITION,
            4,
            OFFSETS,
            List.of()
        );
    }
}
