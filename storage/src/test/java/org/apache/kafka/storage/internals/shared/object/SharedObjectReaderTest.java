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
import org.apache.kafka.storage.internals.shared.metadata.LocalRemoteObjectCheckpoint;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedObjectReaderTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(71, 81, 0);
    private static final SharedPartitionId OTHER_PARTITION = new SharedPartitionId(71, 81, 1);

    @TempDir
    Path tempDir;

    @Test
    void shouldUseIndexedDataBlockAndIgnoreMetadataPhysicalPosition() throws Exception {
        PackedObject packed = packObject(950);
        SharedObjectMetadata metadata = packed.metadata();
        List<SharedObjectRange> logicalRangesWithWrongPhysicalPosition = metadata.ranges().stream()
            .map(range -> new SharedObjectRange(
                range.partition(),
                range.offsets(),
                range.leaderEpoch(),
                0,
                range.objectLength(),
                range.checksum()
            ))
            .toList();
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(new SharedObjectMetadata(
            metadata.objectId(),
            metadata.objectSize(),
            metadata.objectChecksum(),
            logicalRangesWithWrongPhysicalPosition
        ));
        RecordingObjectStore store = new RecordingObjectStore(metadata.objectId(), packed.bytes());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4);

        assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(
            reader.read(PARTITION, 7).get(10, TimeUnit.SECONDS).orElseThrow()
        ));
        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(
            reader.read(PARTITION, 2).get(10, TimeUnit.SECONDS).orElseThrow()
        ));

        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(packed.bytes(), footer).get(0);
        assertEquals(List.of(
            new Read(0, StreamObjectFormat.OBJECT_HEADER_BYTES),
            new Read(metadata.objectSize() - StreamObjectFormat.FOOTER_BYTES, StreamObjectFormat.FOOTER_BYTES),
            new Read(footer.indexPosition(), footer.indexLength()),
            new Read(block.blockPosition(), block.blockLength())
        ), store.reads());
        assertTrue(store.reads().stream().noneMatch(
            read -> read.position() == 0 && read.length() == logicalRangesWithWrongPhysicalPosition.get(1).objectLength()
        ));
    }

    @Test
    void shouldFallbackToLegacyDirectRangeWithoutIndexReads() throws Exception {
        PackedObject packed = packObject(951);
        SharedObjectMetadata metadata = packed.metadata();
        SharedObjectRange target = metadata.ranges().get(1);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.restore(List.of(new RemoteObjectIndex.RangeReference(metadata.objectId(), target)));
        RecordingObjectStore store = new RecordingObjectStore(metadata.objectId(), packed.bytes());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4);

        assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(
            reader.read(PARTITION, 7).get(10, TimeUnit.SECONDS).orElseThrow()
        ));
        assertEquals(List.of(new Read(target.objectPosition(), target.objectLength())), store.reads());
    }

    @Test
    void differentlyPackedCommittedCopySurvivesCheckpointAndMetadataReplay() throws Exception {
        PackedObject canonical;
        PackedObject alternate;
        try (SharedStorageEngine engine = new SharedStorageEngine(new FileSharedWal(
            tempDir.resolve("different-packed-committed-copies"), 1024 * 1024, 4096
        ))) {
            append(engine, 0L, 4L, new byte[] {1, 2, 3, 4, 5});
            append(engine, 5L, 9L, new byte[] {6, 7, 8, 9});
            engine.appendData(OTHER_PARTITION, 8, 0L, 1L, ByteBuffer.wrap(new byte[] {10, 11, 12}))
                .get(10L, TimeUnit.SECONDS);

            List<SharedStorageEngine.UploadCandidate> canonicalBatches =
                engine.uploadCandidates(PARTITION, 0L, 10L);
            canonical = new SharedObjectPacker().pack(980L, canonicalBatches, engine);

            // A second replica can pack an unrelated partition before the
            // same immutable Kafka batches, shifting the physical KSO2
            // block and batch positions without changing logical content.
            List<SharedStorageEngine.UploadCandidate> differentlyPacked = new ArrayList<>();
            differentlyPacked.addAll(engine.uploadCandidates(OTHER_PARTITION, 0L, 2L));
            differentlyPacked.addAll(canonicalBatches);
            alternate = new SharedObjectPacker().pack(981L, differentlyPacked, engine);
        }

        SharedObjectRange original = canonical.metadata().ranges().stream()
            .filter(range -> range.partition().equals(PARTITION) && range.offsets().startOffset() == 0L)
            .findFirst().orElseThrow();
        SharedObjectRange relocated = alternate.metadata().ranges().stream()
            .filter(range -> range.partition().equals(PARTITION) && range.offsets().startOffset() == 0L)
            .findFirst().orElseThrow();
        assertEquals(original.offsets(), relocated.offsets());
        assertEquals(original.leaderEpoch(), relocated.leaderEpoch());
        assertEquals(original.checksum(), relocated.checksum());
        assertEquals(original.objectLength(), relocated.objectLength());
        assertTrue(original.objectPosition() != relocated.objectPosition());
        assertTrue(canonical.metadata().objectSize() < alternate.metadata().objectSize());

        Path checkpointDir = tempDir.resolve("different-packing-checkpoint");
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(checkpointDir);
        checkpoint.add(canonical.metadata());
        checkpoint.add(alternate.metadata());

        RemoteObjectIndex recovered = new RemoteObjectIndex();
        recovered.restore(new LocalRemoteObjectCheckpoint(checkpointDir).references());
        RemoteObjectIndex.RangeReference missingCanonical =
            recovered.find(PARTITION, 2L).orElseThrow();
        assertEquals(980L, missingCanonical.objectId());
        assertTrue(recovered.findEquivalentAlternative(missingCanonical).isEmpty(),
            "The local checkpoint persists only the canonical copy until authoritative metadata replay");
        recovered.add(alternate.metadata());
        assertEquals(981L, recovered.findEquivalentAlternative(missingCanonical)
            .orElseThrow().objectId());

        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(981L, alternate.bytes()).get(10L, TimeUnit.SECONDS);
        SharedObjectReader reader = new SharedObjectReader(store, recovered, 4);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertArrayEquals(new byte[] {6, 7, 8, 9},
            toArray(reader.read(PARTITION, 7L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertArrayEquals(new byte[] {10, 11, 12},
            toArray(reader.read(OTHER_PARTITION, 0L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertFalse(store.contains(980L),
            "Recovery used the independently packed COMMITTED copy without manufacturing canonical bytes");
    }

    @Test
    void shouldReadEquivalentCommittedObjectWhenCanonicalPhysicalCopyIsMissing() throws Exception {
        PackedObject canonical = packObject(960L);
        PackedObject alternative = packObject(961L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(alternative.metadata());
        index.add(canonical.metadata());
        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(alternative.metadata().objectId(), alternative.bytes()).get(10, TimeUnit.SECONDS);

        SharedObjectReader reader = new SharedObjectReader(store, index, 4);
        assertEquals(canonical.metadata().objectId(),
            index.find(PARTITION, 2L).orElseThrow().objectId());
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10, TimeUnit.SECONDS).orElseThrow()));
    }

    @Test
    void failedCanonicalHeaderMustFallbackWhileItsFooterReadIsStillPending() throws Exception {
        PackedObject canonical = packObject(972L);
        PackedObject alternate = packObject(973L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternate.metadata());
        InMemoryObjectStore backingStore = new InMemoryObjectStore();
        backingStore.put(alternate.metadata().objectId(), alternate.bytes())
            .get(10L, TimeUnit.SECONDS);

        CompletableFuture<ByteBuffer> pendingFooter = new CompletableFuture<>();
        AtomicInteger alternateGets = new AtomicInteger();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == canonical.metadata().objectId()) {
                    if (position == 0L) {
                        return CompletableFuture.failedFuture(new IOException("Canonical header unavailable"));
                    }
                    return pendingFooter;
                }
                if (objectId == alternate.metadata().objectId()) {
                    alternateGets.incrementAndGet();
                    return backingStore.rangeRead(objectId, position, length);
                }
                return CompletableFuture.failedFuture(new IOException("Unknown physical object"));
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        ByteBuffer recovered = new SharedObjectReader(store, index, 4)
            .read(PARTITION, 2L).get(2L, TimeUnit.SECONDS).orElseThrow();
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(recovered));
        assertTrue(alternateGets.get() > 0, "An independently COMMITTED fallback must be used");
        assertFalse(pendingFooter.isDone(), "Fallback must not await a stalled primary footer GET");
    }

    @Test
    void shouldVerifyEquivalentCommittedFallbackAfterCanonicalCopyCorruption() throws Exception {
        PackedObject canonical = packObject(962L);
        PackedObject alternative = packObject(963L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternative.metadata());
        ByteBuffer corrupted = writableCopy(canonical.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(corrupted);
        StreamObjectFormat.DataBlockIndexEntry block =
            StreamObjectFormat.readIndex(corrupted, footer).get(0);
        int payloadPosition = Math.toIntExact(
            block.blockPosition() + StreamObjectFormat.DATA_BLOCK_HEADER_BYTES
                + StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES
        );
        corrupted.put(payloadPosition, (byte) (corrupted.get(payloadPosition) ^ 0x44));

        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(canonical.metadata().objectId(), corrupted).get(10, TimeUnit.SECONDS);
        store.put(alternative.metadata().objectId(), alternative.bytes()).get(10, TimeUnit.SECONDS);
        SharedObjectReader reader = new SharedObjectReader(store, index, 4);

        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10, TimeUnit.SECONDS).orElseThrow()));
        assertTrue(store.contains(canonical.metadata().objectId()),
            "A corrupted object must not be deleted by read fallback");
    }

    @Test
    void shouldPreserveBothPhysicalFailuresWhenNeitherCommittedCopyIsAvailable() throws Exception {
        PackedObject canonical = packObject(964L);
        PackedObject alternative = packObject(965L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternative.metadata());
        SharedObjectReader reader = new SharedObjectReader(new InMemoryObjectStore(), index, 4);

        ExecutionException error = assertThrows(ExecutionException.class,
            () -> reader.read(PARTITION, 2L).get(10, TimeUnit.SECONDS));
        assertTrue(error.getCause().getSuppressed().length > 0,
            "Both failures must remain diagnosable; fallback cannot manufacture data");
    }

    @Test
    void sharedStoreFailureMustNotAccumulateSuppressedErrorsAcrossReaders() throws Exception {
        PackedObject canonical = packObject(966L);
        PackedObject alternative = packObject(967L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternative.metadata());
        IOException sharedFailure = new IOException("shared backend connection failed");
        ObjectStore failing = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.failedFuture(sharedFailure);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        SharedObjectReader reader = new SharedObjectReader(failing, index, 4);
        for (int attempt = 0; attempt < 3; attempt++) {
            ExecutionException error = assertThrows(ExecutionException.class,
                () -> reader.read(PARTITION, 2L).get(10, TimeUnit.SECONDS));
            assertTrue(error.getCause() instanceof IOException);
            assertSame(sharedFailure, error.getCause().getCause());
            assertEquals(0, sharedFailure.getSuppressed().length,
                "Reusing a failed future must not mutate shared exception state");
        }
    }

    @Test
    void cancelledPrimaryReadMustNotStartAnAlternatePhysicalRead() throws Exception {
        PackedObject canonical = packObject(970L);
        PackedObject alternative = packObject(971L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternative.metadata());

        AtomicInteger primaryReads = new AtomicInteger();
        AtomicInteger alternativeReads = new AtomicInteger();
        CompletableFuture<ByteBuffer> cancelledRead = new CompletableFuture<>();
        cancelledRead.cancel(false);
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == canonical.metadata().objectId()) {
                    primaryReads.incrementAndGet();
                } else if (objectId == alternative.metadata().objectId()) {
                    alternativeReads.incrementAndGet();
                } else {
                    throw new IllegalArgumentException("Unexpected object " + objectId);
                }
                return cancelledRead;
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        SharedObjectReader reader = new SharedObjectReader(store, index, 4);
        Throwable error = reader.read(PARTITION, 2L)
            .handle((result, failure) -> failure).get(10L, TimeUnit.SECONDS);
        assertTrue(hasCause(error, CancellationException.class));
        // A KSO2 index load issues Header and Footer range GETs
        // concurrently. Cancellation must never start fallback object I/O.
        assertEquals(2, primaryReads.get());
        assertEquals(0, alternativeReads.get(),
            "Cancellation must not trigger an alternate COMMITTED object GET");
        assertTrue(index.findEquivalentAlternative(
            index.find(PARTITION, 2L).orElseThrow()).isPresent(),
            "No fallback I/O must be caused by cancellation even if a COMMITTED copy exists");
    }

    @Test
    void shouldSurfaceIndexedDataBlockCorruption() throws Exception {
        PackedObject packed = packObject(952);
        SharedObjectMetadata metadata = packed.metadata();
        ByteBuffer corrupted = writableCopy(packed.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(corrupted);
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(corrupted, footer).get(0);
        int payloadPosition = Math.toIntExact(
            block.blockPosition() + StreamObjectFormat.DATA_BLOCK_HEADER_BYTES +
                StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES
        );
        corrupted.put(payloadPosition, (byte) (corrupted.get(payloadPosition) ^ 0x44));

        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(metadata);
        RecordingObjectStore store = new RecordingObjectStore(metadata.objectId(), corrupted);
        SharedObjectReader reader = new SharedObjectReader(store, index, 4);

        ExecutionException error = assertThrows(
            ExecutionException.class,
            () -> reader.read(PARTITION, 2).get(10, TimeUnit.SECONDS)
        );
        assertTrue(hasCause(error, RemoteObjectCorruptionException.class));
    }

    @Test
    void shouldPreserveObjectStoreTransportFailureAndRetryDataBlockLoad() throws Exception {
        PackedObject packed = packObject(953);
        SharedObjectMetadata metadata = packed.metadata();
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(metadata);
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(packed.bytes(), footer).get(0);
        IOException transportFailure = new IOException("temporary object-store failure");
        RetryingBlockObjectStore store = new RetryingBlockObjectStore(
            metadata.objectId(),
            packed.bytes(),
            block,
            transportFailure
        );
        SharedObjectReader reader = new SharedObjectReader(store, index, 4);

        ExecutionException error = assertThrows(
            ExecutionException.class,
            () -> reader.read(PARTITION, 2).get(10, TimeUnit.SECONDS)
        );
        assertSame(transportFailure, rootCause(error));
        assertFalse(hasCause(error, RemoteObjectCorruptionException.class));

        assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(
            reader.read(PARTITION, 2).get(10, TimeUnit.SECONDS).orElseThrow()
        ));
        assertEquals(2, store.blockReadCount());
    }

    private PackedObject packObject(long objectId) throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("shared-object-reader-" + objectId), 1024 * 1024, 4096))) {
            append(engine, 0, 4, new byte[]{1, 2, 3, 4, 5});
            append(engine, 5, 9, new byte[]{6, 7, 8, 9});
            return new SharedObjectPacker().pack(
                objectId,
                engine.uploadCandidates(PARTITION, 0, 10),
                engine
            );
        }
    }

    private static void append(SharedStorageEngine engine, long firstOffset, long lastOffset, byte[] payload)
        throws Exception {
        engine.appendData(PARTITION, 8, firstOffset, lastOffset, ByteBuffer.wrap(payload))
            .get(10, TimeUnit.SECONDS);
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        Throwable current = error;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable rootCause(Throwable error) {
        Throwable current = error;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    private static ByteBuffer writableCopy(ByteBuffer source) {
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source.duplicate());
        copy.flip();
        return copy;
    }

    private static byte[] toArray(ByteBuffer source) {
        ByteBuffer data = source.duplicate();
        byte[] result = new byte[data.remaining()];
        data.get(result);
        return result;
    }

    private static final class RecordingObjectStore implements ObjectStore {
        private final long objectId;
        private final ByteBuffer object;
        private final List<Read> reads = new ArrayList<>();

        private RecordingObjectStore(long objectId, ByteBuffer object) {
            this.objectId = objectId;
            this.object = copy(object);
        }

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("put not used"));
        }

        @Override
        public synchronized CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
            if (objectId != this.objectId) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("unknown object " + objectId));
            }
            if (position < 0 || length < 0 || position > object.limit() - (long) length) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("invalid range"));
            }
            reads.add(new Read(position, length));
            return CompletableFuture.completedFuture(slice(object, position, length));
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("delete not used"));
        }

        private synchronized List<Read> reads() {
            return List.copyOf(reads);
        }
    }

    private static final class RetryingBlockObjectStore implements ObjectStore {
        private final long objectId;
        private final ByteBuffer object;
        private final StreamObjectFormat.DataBlockIndexEntry block;
        private final IOException firstBlockFailure;
        private int blockReadCount;

        private RetryingBlockObjectStore(
            long objectId,
            ByteBuffer object,
            StreamObjectFormat.DataBlockIndexEntry block,
            IOException firstBlockFailure
        ) {
            this.objectId = objectId;
            this.object = copy(object);
            this.block = block;
            this.firstBlockFailure = firstBlockFailure;
        }

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("put not used"));
        }

        @Override
        public synchronized CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
            if (objectId != this.objectId) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("unknown object " + objectId));
            }
            if (position == block.blockPosition() && length == block.blockLength()) {
                blockReadCount++;
                if (blockReadCount == 1) {
                    return CompletableFuture.failedFuture(firstBlockFailure);
                }
            }
            return CompletableFuture.completedFuture(slice(object, position, length));
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("delete not used"));
        }

        private synchronized int blockReadCount() {
            return blockReadCount;
        }
    }

    private static ByteBuffer copy(ByteBuffer source) {
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source.duplicate());
        copy.flip();
        return copy.asReadOnlyBuffer();
    }

    private static ByteBuffer slice(ByteBuffer source, long position, int length) {
        if (position < 0 || length < 0 || position > source.limit() - (long) length) {
            throw new IllegalArgumentException("invalid range");
        }
        ByteBuffer range = source.duplicate();
        range.position(Math.toIntExact(position));
        range.limit(Math.toIntExact(position + length));
        return range.slice().asReadOnlyBuffer();
    }

    private record Read(long position, int length) {
    }
}
