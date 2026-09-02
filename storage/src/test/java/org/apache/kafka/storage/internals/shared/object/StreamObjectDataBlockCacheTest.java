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
import static org.junit.jupiter.api.Assertions.assertSame;
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

        assertSame(first, second);
        assertEquals(1, loads.get());
        pending.complete(snapshot);
        assertSame(snapshot, first.get(10, TimeUnit.SECONDS));
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
    void shouldNotRetainFailedOrOversizedLoads() throws Exception {
        StreamObjectDataBlockCache cache = new StreamObjectDataBlockCache(64);
        RemoteObjectIndex.RangeReference reference = reference(3);
        StreamObjectFormat.DataBlockIndexEntry oversized = block(16, 80, 301);
        AtomicInteger oversizedLoads = new AtomicInteger();

        cache.get(reference, oversized, () -> completedLoad(oversizedLoads)).get(10, TimeUnit.SECONDS);
        cache.get(reference, oversized, () -> completedLoad(oversizedLoads)).get(10, TimeUnit.SECONDS);
        assertEquals(2, oversizedLoads.get());
        assertEquals(0, cache.size());
        assertEquals(0, cache.cachedBytes());

        StreamObjectFormat.DataBlockIndexEntry failedBlock = block(16, 64, 302);
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
