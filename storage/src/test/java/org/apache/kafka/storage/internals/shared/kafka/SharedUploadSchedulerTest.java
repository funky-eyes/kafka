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
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.InMemoryObjectStore;
import org.apache.kafka.storage.internals.shared.object.ObjectStore;
import org.apache.kafka.storage.internals.shared.object.SharedObjectPacker;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUploadSchedulerTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId P1 = new SharedPartitionId(3L, 4L, 1);

    @TempDir
    Path tempDir;

    @Test
    void closeWaitsForInFlightUploadBeforeReturning() throws Exception {
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        CountDownLatch putStarted = new CountDownLatch(1);
        CompletableFuture<Void> blockedPut = new CompletableFuture<>();
        ObjectStore objectStore = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                putStarted.countDown();
                return blockedPut;
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.completedFuture(null);
            }
        };

        try (SharedStorageEngine engine = engine("close-drain")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            SharedCommitProgress progress = leaderProgress(P0, 0L, 10L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                () -> 100L,
                () -> 1_000L,
                1024L
            );

            CompletableFuture<Optional<SharedObjectMetadata>> upload = scheduler.tryUploadOnce();
            assertTrue(putStarted.await(10, TimeUnit.SECONDS), "Object PUT did not start");

            assertFalse(scheduler.stop(), "Stopping a healthy scheduler should not report interruption");
            assertEquals(1, scheduler.uploadsInProgress(),
                "First shutdown phase must leave the in-flight upload available for an explicit drain");

            CompletableFuture<Void> closeFuture = CompletableFuture.runAsync(scheduler::close);
            assertThrows(
                java.util.concurrent.TimeoutException.class,
                () -> closeFuture.get(200, TimeUnit.MILLISECONDS),
                "Scheduler close must wait for the in-flight upload"
            );

            blockedPut.complete(null);
            closeFuture.get(10, TimeUnit.SECONDS);
            assertTrue(upload.get(10, TimeUnit.SECONDS).isPresent());
            assertEquals(0, scheduler.uploadsInProgress());
        }
    }

    @Test
    void neverUploadsWalBatchAtOrBeyondKafkaHighWatermark() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("hw-boundary")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, P0, 10L, 19L, new byte[] {4, 5});

            SharedCommitProgress progress = leaderProgress(P0, 0L, 10L);
            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 1024L)) {
                Optional<SharedObjectMetadata> result = scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS);

                assertTrue(result.isPresent());
                assertEquals(1, result.get().ranges().size());
                assertEquals(new OffsetRange(0L, 10L), result.get().ranges().get(0).offsets());
                assertTrue(engine.remoteIndex().coverage(P0).covers(new OffsetRange(0L, 10L)));
                assertFalse(engine.remoteIndex().coverage(P0).covers(new OffsetRange(10L, 20L)));
                assertEquals(1, engine.uploadCandidates(P0, 0L, 20L).size());
            }
        }
    }

    @Test
    void followerAndUnknownReplicaNeverUploadCommittedWal() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("follower-gate")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onHighWatermarkUpdated(P0, 10L);

            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 1024L)) {
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());

                progress.onFollower(P0);
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
                assertFalse(engine.remoteIndex().coverage(P0).covers(new OffsetRange(0L, 10L)));
            }
        }
    }

    @Test
    void leadershipTransitionEnablesAndDemotionStopsNewUploads() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("role-transition")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, P0, 10L, 19L, new byte[] {4, 5, 6});
            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onHighWatermarkUpdated(P0, 10L);
            progress.onFollower(P0);

            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 3L)) {
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());

                progress.onLeader(P0);
                Optional<SharedObjectMetadata> firstUpload = scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS);
                assertTrue(firstUpload.isPresent());
                assertTrue(engine.remoteIndex().coverage(P0).covers(new OffsetRange(0L, 10L)));

                progress.onHighWatermarkUpdated(P0, 20L);
                progress.onFollower(P0);
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
                assertFalse(engine.remoteIndex().coverage(P0).covers(new OffsetRange(10L, 20L)));
            }
        }
    }

    @Test
    void packsCommittedBatchesAcrossLeaderPartitionsInPhysicalWalOrder() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("cross-partition")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, P1, 20L, 29L, new byte[] {4, 5, 6, 7});

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onLogLoaded(P1, 20L);
            progress.onHighWatermarkUpdated(P0, 10L);
            progress.onHighWatermarkUpdated(P1, 30L);
            progress.onLeader(P0);
            progress.onLeader(P1);
            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 1024L)) {
                SharedObjectMetadata metadata = scheduler.tryUploadOnce()
                    .get(10, TimeUnit.SECONDS)
                    .orElseThrow();

                assertEquals(2, metadata.ranges().size());
                assertEquals(P0, metadata.ranges().get(0).partition());
                assertEquals(P1, metadata.ranges().get(1).partition());
                assertTrue(engine.remoteIndex().coverage(P0).covers(new OffsetRange(0L, 10L)));
                assertTrue(engine.remoteIndex().coverage(P1).covers(new OffsetRange(20L, 30L)));
                assertTrue(metadataStore.isCommitted(metadata.objectId()));
                assertTrue(objectStore.contains(metadata.objectId()));
            }
        }
    }

    @Test
    void mergesMultipleCandidatesAcrossPartitionsInPhysicalWalOrder() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("cross-partition-k-way")) {
            append(engine, P0, 0L, 9L, new byte[] {1});
            append(engine, P1, 20L, 29L, new byte[] {2});
            append(engine, P0, 10L, 19L, new byte[] {3});
            append(engine, P1, 30L, 39L, new byte[] {4});

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onLogLoaded(P1, 20L);
            progress.onHighWatermarkUpdated(P0, 20L);
            progress.onHighWatermarkUpdated(P1, 40L);
            progress.onLeader(P0);
            progress.onLeader(P1);

            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 1024L)) {
                List<SharedStorageEngine.UploadCandidate> selected = scheduler.selectCandidates();

                assertEquals(4, selected.size());
                assertEquals(
                    List.of(P0, P1, P0, P1),
                    selected.stream().map(SharedStorageEngine.UploadCandidate::partition).toList()
                );
                assertEquals(
                    List.of(
                        new OffsetRange(0L, 10L),
                        new OffsetRange(20L, 30L),
                        new OffsetRange(10L, 20L),
                        new OffsetRange(30L, 40L)
                    ),
                    selected.stream().map(SharedStorageEngine.UploadCandidate::offsets).toList()
                );
                assertTrue(
                    java.util.stream.IntStream.range(1, selected.size())
                        .allMatch(index ->
                            selected.get(index - 1).location().walOffset() <
                                selected.get(index).location().walOffset())
                );
            }
        }
    }

    @Test
    void concurrentUploadSelectionSkipsReservedHeadInPhysicalWalOrder() throws Exception {
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        CountDownLatch firstPutStarted = new CountDownLatch(1);
        CompletableFuture<Void> firstPut = new CompletableFuture<>();
        AtomicInteger putCalls = new AtomicInteger();
        ObjectStore objectStore = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                if (putCalls.getAndIncrement() == 0) {
                    firstPutStarted.countDown();
                    return firstPut;
                }
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.completedFuture(null);
            }
        };

        try (SharedStorageEngine engine = engine("reserved-k-way")) {
            append(engine, P0, 0L, 9L, new byte[] {1});
            append(engine, P1, 20L, 29L, new byte[] {2});
            append(engine, P0, 10L, 19L, new byte[] {3});

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onLogLoaded(P1, 20L);
            progress.onHighWatermarkUpdated(P0, 20L);
            progress.onHighWatermarkUpdated(P1, 30L);
            progress.onLeader(P0);
            progress.onLeader(P1);

            AtomicLong objectIds = new AtomicLong(100L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                objectIds::getAndIncrement,
                () -> 1_000L,
                1L,
                SharedUploadScheduler.DEFAULT_MAX_LINGER_MS,
                SharedUploadScheduler.DEFAULT_WAL_PRESSURE_PERCENT,
                2
            );
            try {
                CompletableFuture<Optional<SharedObjectMetadata>> firstUpload = scheduler.tryUploadOnce();
                assertTrue(firstPutStarted.await(10, TimeUnit.SECONDS), "First object PUT did not start");
                assertEquals(1, scheduler.reservedCandidateCount());
                assertEquals(1, scheduler.uploadsInProgress());

                SharedObjectMetadata second = scheduler.tryUploadOnce()
                    .get(10, TimeUnit.SECONDS)
                    .orElseThrow();
                assertEquals(1, second.ranges().size());
                assertEquals(P1, second.ranges().get(0).partition());
                assertEquals(new OffsetRange(20L, 30L), second.ranges().get(0).offsets());

                firstPut.complete(null);
                SharedObjectMetadata first = firstUpload.get(10, TimeUnit.SECONDS).orElseThrow();
                assertEquals(P0, first.ranges().get(0).partition());
                assertEquals(new OffsetRange(0L, 10L), first.ranges().get(0).offsets());
                assertEquals(0, scheduler.reservedCandidateCount());
                assertEquals(0, scheduler.uploadsInProgress());
            } finally {
                firstPut.complete(null);
                scheduler.close();
            }
        }
    }

    @Test
    void completingOlderInflightUploadDoesNotResetNextCandidateLingerAge() throws Exception {
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        CountDownLatch firstPutStarted = new CountDownLatch(1);
        CompletableFuture<Void> firstPut = new CompletableFuture<>();
        AtomicInteger putCalls = new AtomicInteger();
        ObjectStore objectStore = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                if (putCalls.getAndIncrement() == 0) {
                    firstPutStarted.countDown();
                    return firstPut;
                }
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.completedFuture(null);
            }
        };

        try (SharedStorageEngine engine = engine("linger-inflight")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3, 4, 5, 6});
            append(engine, P0, 10L, 19L, new byte[] {7, 8, 9, 10, 11, 12});
            SharedCommitProgress progress = leaderProgress(P0, 0L, 20L);

            AtomicLong nowMs = new AtomicLong();
            AtomicLong objectIds = new AtomicLong(100L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                objectIds::getAndIncrement,
                nowMs::get,
                10L,
                100L,
                100,
                2
            );
            try {
                assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());

                nowMs.set(100L);
                CompletableFuture<Optional<SharedObjectMetadata>> firstUpload =
                    scheduler.tryScheduledUploadOnce();
                assertTrue(firstPutStarted.await(10, TimeUnit.SECONDS), "First linger-triggered PUT did not start");

                // With A reserved, B becomes the new pending head at t=100 but has not lingered long enough yet.
                assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());

                firstPut.complete(null);
                SharedObjectMetadata first = firstUpload.get(10, TimeUnit.SECONDS).orElseThrow();
                assertEquals(new OffsetRange(0L, 10L), first.ranges().get(0).offsets());

                // Completing A must not clear B's pending timestamp. B has now aged 100ms and must upload.
                nowMs.set(200L);
                SharedObjectMetadata second = scheduler.tryScheduledUploadOnce()
                    .get(10, TimeUnit.SECONDS)
                    .orElseThrow();
                assertEquals(new OffsetRange(10L, 20L), second.ranges().get(0).offsets());
            } finally {
                firstPut.complete(null);
                scheduler.close();
            }
        }
    }

    @Test
    void permitsSingleOversizedBatchSoUploadCannotStallForever() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("oversized")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

            SharedCommitProgress progress = leaderProgress(P0, 0L, 10L);
            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 4L)) {
                SharedObjectMetadata metadata = scheduler.tryUploadOnce()
                    .get(10, TimeUnit.SECONDS)
                    .orElseThrow();

                assertEquals(1, metadata.ranges().size());
                assertEquals(new OffsetRange(0L, 10L), metadata.ranges().get(0).offsets());
            }
        }
    }

    @Test
    void reconcilesFailedCandidateAgainstCurrentValidityInsteadOfBoundedScan() throws Exception {
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        ObjectStore failingStore = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new IllegalStateException("simulated PUT failure"));
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.completedFuture(null);
            }
        };

        try (SharedStorageEngine engine = engine("failure-reconcile")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, P0, 10L, 19L, new byte[] {4, 5, 6});
            SharedCommitProgress progress = leaderProgress(P0, 0L, 20L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                failingStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            try (SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                () -> 100L,
                () -> 1_000L,
                3L
            )) {
                assertThrows(CompletionException.class, () -> scheduler.tryUploadOnce().join());
                assertTrue(scheduler.uploadFailurePresent());

                SharedStorageEngine.UploadCandidate failed =
                    engine.uploadCandidates(P0, 0L, 20L).get(0);
                engine.commitRemoteObject(new SharedObjectMetadata(
                    200L,
                    failed.location().payloadLength(),
                    123L,
                    List.of(new SharedObjectRange(
                        P0,
                        failed.offsets(),
                        failed.location().leaderEpoch(),
                        0,
                        failed.location().payloadLength(),
                        123L
                    ))
                ));

                // Selection now starts at the later backlog item. Reconciliation must still inspect the failed key
                // directly rather than preserving or clearing it based on membership in this bounded selection.
                scheduler.selectCandidates();
                assertFalse(scheduler.uploadFailurePresent());
                assertEquals(
                    new OffsetRange(10L, 20L),
                    scheduler.selectCandidates().get(0).offsets()
                );
            }
        }
    }

    @Test
    void clearsFailedCandidateWhenPartitionLosesLeadership() throws Exception {
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        ObjectStore failingStore = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new IllegalStateException("simulated PUT failure"));
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.completedFuture(null);
            }
        };

        try (SharedStorageEngine engine = engine("failure-demotion")) {
            append(engine, P0, 0L, 9L, new byte[] {1});
            SharedCommitProgress progress = leaderProgress(P0, 0L, 10L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                failingStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            try (SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                () -> 100L,
                () -> 1_000L,
                1024L
            )) {
                assertThrows(CompletionException.class, () -> scheduler.tryUploadOnce().join());
                assertTrue(scheduler.uploadFailurePresent());

                progress.onFollower(P0);
                assertTrue(scheduler.selectCandidates().isEmpty());
                assertFalse(scheduler.uploadFailurePresent());
            }
        }
    }

    @Test
    void releasesSingleFlightGuardWhenObjectIdSupplierFailsSynchronously() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("id-supplier-failure")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            SharedCommitProgress progress = leaderProgress(P0, 0L, 10L);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            AtomicLong attempts = new AtomicLong();
            try (SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                () -> {
                    if (attempts.getAndIncrement() == 0L) {
                        throw new IllegalStateException("simulated allocator failure");
                    }
                    return 101L;
                },
                () -> 1_000L,
                1024L
            )) {
                CompletionException failure = assertThrows(
                    CompletionException.class,
                    () -> scheduler.tryUploadOnce().join()
                );
                assertTrue(failure.getCause() instanceof IllegalStateException);
                assertTrue(scheduler.lastFailure().isPresent());

                Optional<SharedObjectMetadata> retry = scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS);
                assertTrue(retry.isPresent());
                assertEquals(101L, retry.get().objectId());
                assertFalse(scheduler.lastFailure().isPresent());
            }
        }
    }

    private static SharedCommitProgress leaderProgress(
        SharedPartitionId partition,
        long logStartOffset,
        long highWatermark
    ) {
        SharedCommitProgress progress = new SharedCommitProgress();
        progress.onLogLoaded(partition, logStartOffset);
        progress.onHighWatermarkUpdated(partition, highWatermark);
        progress.onLeader(partition);
        return progress;
    }

    private SharedStorageEngine engine(String name) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), 1024 * 1024, 4096));
    }

    private static SharedUploadScheduler scheduler(
        SharedStorageEngine engine,
        SharedCommitProgress progress,
        InMemoryObjectStore objectStore,
        InMemoryObjectMetadataStore metadataStore,
        long targetObjectBytes
    ) {
        AtomicLong objectIds = new AtomicLong(100L);
        SharedObjectUploader uploader = new SharedObjectUploader(
            objectStore,
            metadataStore,
            new SharedObjectPacker(),
            engine
        );
        return new SharedUploadScheduler(
            engine,
            progress,
            uploader,
            objectIds::getAndIncrement,
            () -> 1_000L,
            targetObjectBytes
        );
    }

    private static void append(
        SharedStorageEngine engine,
        SharedPartitionId partition,
        long firstOffset,
        long lastOffset,
        byte[] payload
    ) throws Exception {
        engine.appendData(
            partition,
            3,
            firstOffset,
            lastOffset,
            ByteBuffer.wrap(payload)
        ).get(10, TimeUnit.SECONDS);
    }
}
