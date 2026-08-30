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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.InMemoryObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.ObjectStore;
import org.apache.kafka.storage.internals.shared.object.SharedObjectPacker;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUploadParallelismTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);

    @TempDir
    Path tempDir;

    @Test
    void uploadsReservedDisjointWalRangesConcurrentlyAndCommitsOutOfOrder() throws Exception {
        ControlledObjectStore objectStore = new ControlledObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("parallel")) {
            append(engine, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, 10L, 19L, new byte[] {4, 5, 6});
            SharedCommitProgress progress = leaderProgress(0L, 20L);
            AtomicLong objectIds = new AtomicLong(100L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );

            try (SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                objectIds::getAndIncrement,
                () -> 1_000L,
                3L,
                1_000L,
                70,
                2
            )) {
                CompletableFuture<Optional<SharedObjectMetadata>> first = scheduler.tryUploadOnce();
                CompletableFuture<Optional<SharedObjectMetadata>> second = scheduler.tryUploadOnce();

                assertEquals(2, objectStore.pendingPutCount());
                assertTrue(objectStore.isPending(100L));
                assertTrue(objectStore.isPending(101L));
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());

                objectStore.completePut(101L);
                SharedObjectMetadata secondMetadata = second.get(10, TimeUnit.SECONDS).orElseThrow();
                assertEquals(101L, secondMetadata.objectId());
                assertEquals(new OffsetRange(10L, 20L), secondMetadata.ranges().get(0).offsets());
                assertTrue(metadataStore.isCommitted(101L));
                assertTrue(engine.remoteIndex().coverage(PARTITION).covers(new OffsetRange(10L, 20L)));
                assertFalse(engine.remoteIndex().coverage(PARTITION).covers(new OffsetRange(0L, 10L)));

                objectStore.completePut(100L);
                SharedObjectMetadata firstMetadata = first.get(10, TimeUnit.SECONDS).orElseThrow();
                assertEquals(100L, firstMetadata.objectId());
                assertEquals(new OffsetRange(0L, 10L), firstMetadata.ranges().get(0).offsets());
                assertTrue(metadataStore.isCommitted(100L));
                assertTrue(engine.remoteIndex().coverage(PARTITION).covers(new OffsetRange(0L, 20L)));
            }
        }
    }

    @Test
    void releasesWalReservationAfterFailedPutSoRangeCanRetry() throws Exception {
        ControlledObjectStore objectStore = new ControlledObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("retry")) {
            append(engine, 0L, 9L, new byte[] {1, 2, 3});
            SharedCommitProgress progress = leaderProgress(0L, 10L);
            AtomicLong objectIds = new AtomicLong(100L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );

            try (SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                objectIds::getAndIncrement,
                () -> 1_000L,
                3L,
                1_000L,
                70,
                2
            )) {
                CompletableFuture<Optional<SharedObjectMetadata>> first = scheduler.tryUploadOnce();
                assertTrue(objectStore.isPending(100L));
                objectStore.failPut(100L, new IllegalStateException("simulated S3 PUT failure"));

                CompletionException failure = assertThrows(CompletionException.class, first::join);
                assertTrue(failure.getCause() instanceof IllegalStateException);
                assertTrue(metadataStore.isPrepared(100L));

                CompletableFuture<Optional<SharedObjectMetadata>> retry = scheduler.tryUploadOnce();
                assertTrue(objectStore.isPending(101L));
                objectStore.completePut(101L);
                SharedObjectMetadata retried = retry.get(10, TimeUnit.SECONDS).orElseThrow();

                assertEquals(101L, retried.objectId());
                assertEquals(new OffsetRange(0L, 10L), retried.ranges().get(0).offsets());
                assertTrue(metadataStore.isCommitted(101L));
                assertTrue(engine.remoteIndex().coverage(PARTITION).covers(new OffsetRange(0L, 10L)));
            }
        }
    }

    private SharedStorageEngine engine(String name) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), 1024 * 1024, 4096));
    }

    private static SharedCommitProgress leaderProgress(long logStartOffset, long highWatermark) {
        SharedCommitProgress progress = new SharedCommitProgress();
        progress.onLogLoaded(PARTITION, logStartOffset);
        progress.onHighWatermarkUpdated(PARTITION, highWatermark);
        progress.onLeader(PARTITION);
        return progress;
    }

    private static void append(
        SharedStorageEngine engine,
        long firstOffset,
        long lastOffset,
        byte[] payload
    ) throws Exception {
        engine.appendData(
            PARTITION,
            3,
            firstOffset,
            lastOffset,
            ByteBuffer.wrap(payload)
        ).get(10, TimeUnit.SECONDS);
    }

    private static final class ControlledObjectStore implements ObjectStore {
        private final Map<Long, PendingPut> pending = new ConcurrentHashMap<>();
        private final Map<Long, byte[]> stored = new ConcurrentHashMap<>();

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            ByteBuffer source = data.duplicate();
            byte[] bytes = new byte[source.remaining()];
            source.get(bytes);
            PendingPut previous = pending.putIfAbsent(objectId, new PendingPut(bytes));
            if (previous != null || stored.containsKey(objectId)) {
                return CompletableFuture.failedFuture(new IllegalStateException("Object already exists: " + objectId));
            }
            return pending.get(objectId).future();
        }

        @Override
        public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
            byte[] bytes = stored.get(objectId);
            if (bytes == null) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown object: " + objectId));
            }
            if (position < 0 || length < 0 || position > bytes.length || length > bytes.length - position) {
                return CompletableFuture.failedFuture(new IndexOutOfBoundsException());
            }
            return CompletableFuture.completedFuture(
                ByteBuffer.wrap(bytes, Math.toIntExact(position), length).slice().asReadOnlyBuffer()
            );
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            pending.remove(objectId);
            stored.remove(objectId);
            return CompletableFuture.completedFuture(null);
        }

        int pendingPutCount() {
            return pending.size();
        }

        boolean isPending(long objectId) {
            return pending.containsKey(objectId);
        }

        void completePut(long objectId) {
            PendingPut put = pending.remove(objectId);
            if (put == null) {
                throw new IllegalStateException("No pending PUT for object " + objectId);
            }
            stored.put(objectId, put.bytes());
            put.future().complete(null);
        }

        void failPut(long objectId, Throwable failure) {
            PendingPut put = pending.remove(objectId);
            if (put == null) {
                throw new IllegalStateException("No pending PUT for object " + objectId);
            }
            put.future().completeExceptionally(failure);
        }

        private record PendingPut(byte[] bytes, CompletableFuture<Void> future) {
            private PendingPut(byte[] bytes) {
                this(bytes, new CompletableFuture<>());
            }
        }
    }
}
