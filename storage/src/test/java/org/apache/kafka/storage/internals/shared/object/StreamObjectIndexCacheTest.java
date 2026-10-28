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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamObjectIndexCacheTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(51, 61, 0);

    @TempDir
    Path tempDir;

    @Test
    void shouldCoalesceAndReuseIndexLoadForSameImmutableDescriptor() throws Exception {
        PackedObject packed = packObject(920);
        SharedObjectMetadata metadata = packed.metadata();
        CountingObjectStore store = new CountingObjectStore();
        store.install(metadata.objectId(), packed.bytes());
        StreamObjectIndexReader loader = new StreamObjectIndexReader(store);
        StreamObjectIndexCache cache = new StreamObjectIndexCache(4);
        RemoteObjectIndex.RangeReference reference = reference(metadata);

        CompletableFuture<java.util.Optional<StreamObjectIndexReader.IndexSnapshot>> first = cache.get(reference, loader);
        CompletableFuture<java.util.Optional<StreamObjectIndexReader.IndexSnapshot>> second = cache.get(reference, loader);

        assertSame(first, second);
        first.get(10, TimeUnit.SECONDS);
        assertEquals(3, store.readCount());
        cache.get(reference, loader).get(10, TimeUnit.SECONDS);
        assertEquals(3, store.readCount());
        assertEquals(1, cache.size());
    }

    @Test
    void shouldEvictLeastRecentlyUsedDescriptor() throws Exception {
        PackedObject firstPacked = packObject(921);
        PackedObject secondPacked = packObject(922);
        CountingObjectStore store = new CountingObjectStore();
        store.install(firstPacked.metadata().objectId(), firstPacked.bytes());
        store.install(secondPacked.metadata().objectId(), secondPacked.bytes());
        StreamObjectIndexReader loader = new StreamObjectIndexReader(store);
        StreamObjectIndexCache cache = new StreamObjectIndexCache(1);
        RemoteObjectIndex.RangeReference first = reference(firstPacked.metadata());
        RemoteObjectIndex.RangeReference second = reference(secondPacked.metadata());

        cache.get(first, loader).get(10, TimeUnit.SECONDS);
        cache.get(second, loader).get(10, TimeUnit.SECONDS);
        cache.get(first, loader).get(10, TimeUnit.SECONDS);

        assertEquals(9, store.readCount());
        assertEquals(1, cache.size());
    }

    @Test
    void shouldAllowDisablingIndexCache() throws Exception {
        PackedObject packed = packObject(923);
        SharedObjectMetadata metadata = packed.metadata();
        CountingObjectStore store = new CountingObjectStore();
        store.install(metadata.objectId(), packed.bytes());
        StreamObjectIndexReader loader = new StreamObjectIndexReader(store);
        StreamObjectIndexCache cache = new StreamObjectIndexCache(0);
        RemoteObjectIndex.RangeReference reference = reference(metadata);

        cache.get(reference, loader).get(10, TimeUnit.SECONDS);
        cache.get(reference, loader).get(10, TimeUnit.SECONDS);

        assertEquals(6, store.readCount());
        assertEquals(0, cache.size());
    }

    @Test
    void shouldNotRetainLegacyDescriptorMiss() throws Exception {
        PackedObject packed = packObject(924);
        CountingObjectStore store = new CountingObjectStore();
        store.install(packed.metadata().objectId(), packed.bytes());
        StreamObjectIndexReader loader = new StreamObjectIndexReader(store);
        StreamObjectIndexCache cache = new StreamObjectIndexCache(4);
        RemoteObjectIndex.RangeReference legacy = new RemoteObjectIndex.RangeReference(
            packed.metadata().objectId(),
            packed.metadata().ranges().get(0)
        );

        cache.get(legacy, loader).get(10, TimeUnit.SECONDS);
        cache.get(legacy, loader).get(10, TimeUnit.SECONDS);

        assertEquals(0, store.readCount());
        assertEquals(0, cache.size());
    }

    @Test
    void blockingRangeGetCannotHoldTheSharedIndexCacheMonitor() throws Exception {
        PackedObject slow = packObject(925L);
        PackedObject fast = packObject(926L);
        CountingObjectStore slowDelegate = new CountingObjectStore();
        slowDelegate.install(slow.metadata().objectId(), slow.bytes());
        CountingObjectStore fastStore = new CountingObjectStore();
        fastStore.install(fast.metadata().objectId(), fast.bytes());

        CountDownLatch firstHeaderEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstHeader = new CountDownLatch(1);
        AtomicInteger slowHeaderCalls = new AtomicInteger();
        ObjectStore blockingStore = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (position == 0L) {
                    slowHeaderCalls.incrementAndGet();
                    firstHeaderEntered.countDown();
                    try {
                        if (!releaseFirstHeader.await(10L, TimeUnit.SECONDS)) {
                            return CompletableFuture.failedFuture(
                                new IllegalStateException("Timed out releasing first Header GET"));
                        }
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return CompletableFuture.failedFuture(interrupted);
                    }
                }
                return slowDelegate.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        StreamObjectIndexCache cache = new StreamObjectIndexCache(4);
        StreamObjectIndexReader slowLoader = new StreamObjectIndexReader(blockingStore);
        StreamObjectIndexReader fastLoader = new StreamObjectIndexReader(fastStore);
        RemoteObjectIndex.RangeReference slowRef = reference(slow.metadata());
        RemoteObjectIndex.RangeReference fastRef = reference(fast.metadata());
        ExecutorService workers = Executors.newFixedThreadPool(3);
        try {
            Future<CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>>> first =
                workers.submit(() -> cache.get(slowRef, slowLoader));
            assertTrue(firstHeaderEntered.await(5L, TimeUnit.SECONDS));
            Future<CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>>> duplicate =
                workers.submit(() -> cache.get(slowRef, slowLoader));
            CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> same =
                duplicate.get(5L, TimeUnit.SECONDS);
            assertFalse(same.isDone(), "Concurrent same-object load must share its pending Future");

            Future<Optional<StreamObjectIndexReader.IndexSnapshot>> other =
                workers.submit(() -> cache.get(fastRef, fastLoader).get(5L, TimeUnit.SECONDS));
            assertTrue(other.get(5L, TimeUnit.SECONDS).isPresent(),
                "Independent object must not block behind another object's Range GET");
            assertEquals(3, fastStore.readCount());
            assertEquals(1, slowHeaderCalls.get(), "Same object must initiate only one Header GET");
            assertFalse(same.isDone());

            releaseFirstHeader.countDown();
            assertSame(same, first.get(5L, TimeUnit.SECONDS));
            assertTrue(same.get(5L, TimeUnit.SECONDS).isPresent());
            assertEquals(3, slowDelegate.readCount());
            assertEquals(2, cache.size());
        } finally {
            releaseFirstHeader.countDown();
            workers.shutdownNow();
        }
    }

    @Test
    void synchronouslyThrowingObjectStoreDoesNotPoisonIndexCache() throws Exception {
        PackedObject packed = packObject(927L);
        CountingObjectStore delegate = new CountingObjectStore();
        delegate.install(packed.metadata().objectId(), packed.bytes());
        AtomicInteger requests = new AtomicInteger();
        IllegalStateException injected = new IllegalStateException("synchronous Range GET rejected");
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (requests.getAndIncrement() == 0) {
                    throw injected;
                }
                return delegate.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        StreamObjectIndexCache cache = new StreamObjectIndexCache(4);
        StreamObjectIndexReader loader = new StreamObjectIndexReader(store);
        RemoteObjectIndex.RangeReference ref = reference(packed.metadata());

        ExecutionException first = assertThrows(ExecutionException.class,
            () -> cache.get(ref, loader).get(5L, TimeUnit.SECONDS));
        assertSame(injected, first.getCause());
        assertEquals(0, cache.size(), "A failed synchronous load must not remain cached");
        assertTrue(cache.get(ref, loader).get(5L, TimeUnit.SECONDS).isPresent());
        assertEquals(3, delegate.readCount());
        assertEquals(1, cache.size());
    }

    private PackedObject packObject(long objectId) throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("index-cache-" + objectId), 1024 * 1024, 4096))) {
            engine.appendData(PARTITION, 4, 0, 9, ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))
                .get(10, TimeUnit.SECONDS);
            return new SharedObjectPacker().pack(
                objectId,
                engine.uploadCandidates(PARTITION, 0, 10),
                engine
            );
        }
    }

    private static RemoteObjectIndex.RangeReference reference(SharedObjectMetadata metadata) {
        return new RemoteObjectIndex.RangeReference(
            metadata.objectId(),
            metadata.objectSize(),
            metadata.objectChecksum(),
            metadata.ranges().get(0)
        );
    }

    private static final class CountingObjectStore implements ObjectStore {
        private final Map<Long, ByteBuffer> objects = new HashMap<>();
        private int readCount;

        private synchronized void install(long objectId, ByteBuffer bytes) {
            ByteBuffer copy = ByteBuffer.allocate(bytes.remaining());
            copy.put(bytes.duplicate());
            copy.flip();
            objects.put(objectId, copy.asReadOnlyBuffer());
        }

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("put not used"));
        }

        @Override
        public synchronized CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
            ByteBuffer object = objects.get(objectId);
            if (object == null) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("unknown object " + objectId));
            }
            if (position < 0 || length < 0 || position > object.limit() - (long) length) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("invalid range"));
            }
            readCount++;
            ByteBuffer range = object.duplicate();
            range.position(Math.toIntExact(position));
            range.limit(Math.toIntExact(position + length));
            return CompletableFuture.completedFuture(range.slice().asReadOnlyBuffer());
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("delete not used"));
        }

        private synchronized int readCount() {
            return readCount;
        }
    }
}
