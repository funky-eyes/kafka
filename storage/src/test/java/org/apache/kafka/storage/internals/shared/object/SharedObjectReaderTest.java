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
import org.apache.kafka.storage.internals.shared.metadata.RemoteMetadataConflictException;
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
import java.util.Optional;
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
    void fetchMergesRemoteAndLocalBatchesInOffsetOrder() throws Exception {
        PackedObject packed = packObject(1979);
        RecordingObjectStore store = new RecordingObjectStore(packed.metadata().objectId(), packed.bytes());
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("mixed-remote-local-fetch"), 1024 * 1024, 4096))) {
            engine.commitRemoteObject(packed.metadata());
            engine.installRemoteReader(new SharedObjectReader(store, engine.remoteIndex(), 0, 0));
            append(engine, 5, 9, new byte[] {9, 8, 7, 6});
            var result = engine.readBatches(PARTITION, 0, 1024, false);
            assertEquals(2, result.batches().size());
            assertEquals(9, result.sizeInBytes());
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(result.batches().get(0)));
            assertArrayEquals(new byte[] {9, 8, 7, 6}, toArray(result.batches().get(1)));

            var insideRemote = engine.readBatches(PARTITION, 3, 5, false);
            assertEquals(0, insideRemote.firstBatchOffset());
            assertEquals(1, insideRemote.batches().size());
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(insideRemote.batches().get(0)));
            var incomplete = engine.readBatches(PARTITION, 3, 4, false);
            assertTrue(incomplete.firstBatchIncomplete());
            assertEquals(0, incomplete.firstBatchOffset());
            assertTrue(incomplete.batches().isEmpty());
            var atLeastOne = engine.readBatches(PARTITION, 3, 0, true);
            assertEquals(1, atLeastOne.batches().size());
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(atLeastOne.batches().get(0)));
            var localOnly = engine.readBatches(PARTITION, 5, 4, false);
            assertEquals(5, localOnly.firstBatchOffset());
            assertArrayEquals(new byte[] {9, 8, 7, 6}, toArray(localOnly.batches().get(0)));
            assertTrue(engine.readBatches(PARTITION, 10, 1024, false).batches().isEmpty());
            assertTrue(engine.readBatches(PARTITION, Long.MAX_VALUE, 1024, false).batches().isEmpty());
        }
    }

    @Test
    void fetchDoesNotSubstituteDifferentLocalRangeWithEqualLength() throws Exception {
        PackedObject packed = packObject(1978);
        RecordingObjectStore store = new RecordingObjectStore(packed.metadata().objectId(), packed.bytes());
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("overlap-remote-local-fetch"), 1024 * 1024, 4096))) {
            engine.commitRemoteObject(packed.metadata());
            engine.installRemoteReader(new SharedObjectReader(store, engine.remoteIndex(), 0, 0));
            append(engine, 0, 2, new byte[] {9, 8, 7, 6, 5});
            var result = engine.readBatches(PARTITION, 3, 5, false);
            assertEquals(0, result.firstBatchOffset());
            assertEquals(1, result.batches().size());
            assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(result.batches().get(0)));
        }
    }

    @Test
    void exactRemoteReadRejectsDifferentRangeEpochOrLengthBeforeObjectIo() throws Exception {
        PackedObject packed = packObject(1977);
        RecordingObjectStore store = new RecordingObjectStore(packed.metadata().objectId(), packed.bytes());
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0);
        assertThrows(ExecutionException.class, () -> reader.readExact(PARTITION, 0, 3, 8, 5, null).get());
        assertThrows(ExecutionException.class, () -> reader.readExact(PARTITION, 0, 4, 9, 5, null).get());
        assertThrows(ExecutionException.class, () -> reader.readExact(PARTITION, 0, 4, 8, 4, null).get());
        assertTrue(store.reads().isEmpty(), "mismatched metadata must fail before any object read");
        var valid = reader.readExact(PARTITION, 0, 4, 8, 5, null).get().orElseThrow();
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(valid));
    }

    @Test
    void fetchReusesVerifiedBlockOnlyWithinOneRequestWhenSharedCacheIsDisabled() throws Exception {
        PackedObject packed = packObject(1980);
        RecordingObjectStore store = new RecordingObjectStore(packed.metadata().objectId(), packed.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(packed.bytes(), footer).get(0);
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("request-cache-fetch"), 1024 * 1024, 4096))) {
            engine.commitRemoteObject(packed.metadata());
            engine.installRemoteReader(new SharedObjectReader(store, engine.remoteIndex(), 0, 0));
            SharedStorageEngine.BatchReadResult first = engine.readBatches(PARTITION, 0, 1024, false);
            assertEquals(2, first.batches().size());
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(first.batches().get(0)));
            assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(first.batches().get(1)));
            assertEquals(1, store.reads().stream().filter(r -> r.position() == block.blockPosition() &&
                r.length() == block.blockLength()).count());
            assertEquals(4, store.reads().size(), "one index and one data block GET per Fetch");
            SharedStorageEngine.BatchReadResult second = engine.readBatches(PARTITION, 0, 1024, false);
            assertEquals(2, second.batches().size());
            assertEquals(2, store.reads().stream().filter(r -> r.position() == block.blockPosition() &&
                r.length() == block.blockLength()).count(), "no request cache survives a Fetch");
            assertEquals(8, store.reads().size(), "index reuse also ends when Fetch ends");
        }
    }

    @Test
    void requestReuseHasGlobalNonblockingSlotLimitAndCloseIsIdempotent() throws Exception {
        PackedObject packed = packObject(1981);
        RecordingObjectStore store = new RecordingObjectStore(packed.metadata().objectId(), packed.bytes());
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4, 0);
        List<SharedObjectReader.ReadSession> sessions = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            sessions.add(reader.openReadSession());
        }
        assertEquals(0, reader.availableRequestCacheSlots());
        try {
            SharedObjectReader.ReadSession overflow = sessions.get(8);
            reader.read(PARTITION, 2, overflow).get(10, TimeUnit.SECONDS);
            reader.read(PARTITION, 7, overflow).get(10, TimeUnit.SECONDS);
            StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
            StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(packed.bytes(), footer).get(0);
            assertEquals(2, store.reads().stream().filter(r -> r.position() == block.blockPosition() &&
                r.length() == block.blockLength()).count(), "overflow requests bypass reuse without blocking");
        } finally {
            sessions.forEach(SharedObjectReader.ReadSession::close);
            sessions.forEach(SharedObjectReader.ReadSession::close);
        }
        assertEquals(8, reader.availableRequestCacheSlots());
    }

    @Test
    void cancelledWaitingFetchKeepsSlotUntilPhysicalReadCompletes() throws Exception {
        PackedObject packed = packObject(1982);
        RecordingObjectStore backing = new RecordingObjectStore(packed.metadata().objectId(), packed.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(packed.bytes(), footer).get(0);
        CompletableFuture<ByteBuffer> pending = new CompletableFuture<>();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long id, ByteBuffer data) {
                return backing.put(id, data);
            }
            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long id, long position, int length) {
                if (position == block.blockPosition() && length == block.blockLength()) {
                    return pending;
                }
                return backing.rangeRead(id, position, length);
            }
            @Override
            public CompletableFuture<Void> delete(long id) {
                return backing.delete(id);
            }
        };
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4, 0);
        SharedObjectReader.ReadSession session = reader.openReadSession();
        CompletableFuture<Optional<ByteBuffer>> waiting = reader.read(PARTITION, 2, session);
        assertTrue(waiting.cancel(false));
        session.close();
        assertEquals(7, reader.availableRequestCacheSlots());
        pending.complete(slice(packed.bytes(), block.blockPosition(), block.blockLength()));
        assertEquals(8, reader.availableRequestCacheSlots());
    }

    @Test
    void requestCacheSeparatesDifferentObjectIdentities() throws Exception {
        PackedObject first;
        PackedObject second;
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("request-cache-two-objects"), 1024 * 1024, 4096))) {
            append(engine, 0, 4, new byte[]{1, 2, 3, 4, 5});
            append(engine, 5, 9, new byte[]{6, 7, 8, 9});
            engine.appendData(OTHER_PARTITION, 8, 0, 4, ByteBuffer.wrap(new byte[]{10}))
                .get(10, TimeUnit.SECONDS);
            engine.appendData(OTHER_PARTITION, 8, 5, 9, ByteBuffer.wrap(new byte[]{11}))
                .get(10, TimeUnit.SECONDS);
            SharedObjectPacker packer = new SharedObjectPacker();
            first = packer.pack(1983, engine.uploadCandidates(PARTITION, 0, 10), engine);
            second = packer.pack(1984, engine.uploadCandidates(OTHER_PARTITION, 0, 10), engine);
        }
        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(1983, first.bytes()).get(10, TimeUnit.SECONDS);
        backing.put(1984, second.bytes()).get(10, TimeUnit.SECONDS);
        AtomicInteger gets = new AtomicInteger();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long id, ByteBuffer data) {
                return backing.put(id, data);
            }
            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long id, long position, int length) {
                gets.incrementAndGet();
                return backing.rangeRead(id, position, length);
            }
            @Override
            public CompletableFuture<Void> delete(long id) {
                return backing.delete(id);
            }
        };
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(first.metadata());
        index.add(second.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{10}, toArray(reader.read(OTHER_PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(reader.read(PARTITION, 7, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{11}, toArray(reader.read(OTHER_PARTITION, 7, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertEquals(8, gets.get(), "two distinct indices and data blocks are each loaded once");
        }
    }

    @Test
    void requestCacheDoesNotReuseCorruptPrimaryAndValidatesFallback() throws Exception {
        PackedObject primary = packObject(1985);
        PackedObject alternate = packObject(1986);
        ByteBuffer corrupt = writableCopy(primary.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(corrupt);
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(corrupt, footer).get(0);
        int position = Math.toIntExact(block.blockPosition() + StreamObjectFormat.DATA_BLOCK_HEADER_BYTES +
            StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES);
        corrupt.put(position, (byte) (corrupt.get(position) ^ 0x44));
        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(1985, corrupt).get(10, TimeUnit.SECONDS);
        backing.put(1986, alternate.bytes()).get(10, TimeUnit.SECONDS);
        AtomicInteger primaryDataGets = new AtomicInteger();
        AtomicInteger alternateDataGets = new AtomicInteger();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long id, ByteBuffer data) {
                return backing.put(id, data);
            }
            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long id, long pos, int length) {
                if (pos == block.blockPosition() && length == block.blockLength()) {
                    (id == 1985 ? primaryDataGets : alternateDataGets).incrementAndGet();
                }
                return backing.rangeRead(id, pos, length);
            }
            @Override
            public CompletableFuture<Void> delete(long id) {
                return backing.delete(id);
            }
        };
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(primary.metadata());
        index.add(alternate.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(reader.read(PARTITION, 7, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertEquals(2, primaryDataGets.get(), "failed primary loads must not be retained");
            assertEquals(1, alternateDataGets.get(), "the validated fallback block is reused");
        }
        assertEquals(8, reader.availableRequestCacheSlots());
    }

    @Test
    void requestCacheSeparatesBlocksOfSameObject() throws Exception {
        PackedObject packed;
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("request-cache-two-blocks"), 1024 * 1024, 4096))) {
            append(engine, 0, 4, new byte[]{1, 2, 3, 4, 5});
            engine.appendData(OTHER_PARTITION, 8, 0, 4, ByteBuffer.wrap(new byte[]{10}))
                .get(10, TimeUnit.SECONDS);
            List<SharedStorageEngine.UploadCandidate> batches = new ArrayList<>();
            batches.addAll(engine.uploadCandidates(PARTITION, 0, 5));
            batches.addAll(engine.uploadCandidates(OTHER_PARTITION, 0, 5));
            packed = new SharedObjectPacker().pack(1987, batches, engine);
        }
        RecordingObjectStore store = new RecordingObjectStore(1987, packed.bytes());
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 4, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{10}, toArray(reader.read(OTHER_PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertEquals(5, store.reads().size(), "one index and two distinct data blocks");
        }
    }

    @Test
    void oversizedRequestIndexIsNotRetained() throws Exception {
        PackedObject packed;
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("request-cache-large-index"), 2 * 1024 * 1024, 4096))) {
            for (int i = 0; i < 1024; i++) {
                engine.appendData(PARTITION, 8, i, i, ByteBuffer.wrap(new byte[]{42})).get(10, TimeUnit.SECONDS);
            }
            packed = new SharedObjectPacker(StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES)
                .pack(1988, engine.uploadCandidates(PARTITION, 0, 1024), engine);
        }
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(packed.bytes());
        assertTrue(footer.indexLength() > 64 * 1024);
        RecordingObjectStore store = new RecordingObjectStore(1988, packed.bytes());
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            assertArrayEquals(new byte[]{42}, toArray(reader.read(PARTITION, 0, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{42}, toArray(reader.read(PARTITION, 0, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertEquals(7, store.reads().size(), "oversize index is read twice; data block is reused");
        }
    }

    @Test
    void corruptIndexIsNotRetainedWithinRequest() throws Exception {
        PackedObject packed = packObject(1989);
        ByteBuffer corrupt = writableCopy(packed.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(corrupt);
        int position = Math.toIntExact(footer.indexPosition() + StreamObjectFormat.INDEX_HEADER_BYTES);
        corrupt.put(position, (byte) (corrupt.get(position) ^ 0x44));
        RecordingObjectStore store = new RecordingObjectStore(1989, corrupt);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            for (int i = 0; i < 2; i++) {
                ExecutionException error = assertThrows(ExecutionException.class,
                    () -> reader.read(PARTITION, 2, session).get(10, TimeUnit.SECONDS));
                assertTrue(hasCause(error, RemoteObjectCorruptionException.class));
            }
            assertEquals(6, store.reads().size(), "failed object index is fetched and verified on each attempt");
        }
    }

    @Test
    void conflictingObjectReplacementIsRejectedWithoutChangingRequestView() throws Exception {
        PackedObject oldObject = packObject(1990);
        PackedObject newObject;
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("request-cache-replace"), 1024 * 1024, 4096))) {
            append(engine, 0, 4, new byte[]{9, 9, 9, 9, 9});
            append(engine, 5, 9, new byte[]{8, 8, 8, 8});
            newObject = new SharedObjectPacker().pack(1991, engine.uploadCandidates(PARTITION, 0, 10), engine);
        }
        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(1990, oldObject.bytes()).get(10, TimeUnit.SECONDS);
        store.put(1991, newObject.bytes()).get(10, TimeUnit.SECONDS);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(oldObject.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            SharedObjectMetadata object = newObject.metadata();
            assertThrows(RemoteMetadataConflictException.class, () -> index.restore(object.ranges().stream()
                .map(range -> new RemoteObjectIndex.RangeReference(
                    object.objectId(), object.objectSize(), object.objectChecksum(), range)).toList()));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
        }
    }

    @Test
    void tailObjectPublicationChangesRevisionWithoutReusingAnotherObjectsIndex() throws Exception {
        PackedObject oldObject = packObject(1992);
        PackedObject tailObject;
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("request-cache-tail-publication"), 1024 * 1024, 4096))) {
            append(engine, 10, 14, new byte[]{7, 7, 7});
            append(engine, 15, 19, new byte[]{8, 8, 8});
            tailObject = new SharedObjectPacker().pack(1993, engine.uploadCandidates(PARTITION, 10, 20), engine);
        }
        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(1992, oldObject.bytes()).get(10, TimeUnit.SECONDS);
        store.put(1993, tailObject.bytes()).get(10, TimeUnit.SECONDS);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(oldObject.metadata());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0);
        try (SharedObjectReader.ReadSession session = reader.openReadSession()) {
            long before = index.revision(PARTITION);
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            index.add(tailObject.metadata());
            assertTrue(index.revision(PARTITION) > before);
            assertArrayEquals(new byte[]{7, 7, 7}, toArray(reader.read(PARTITION, 12, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(reader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
        }
    }

    @Test
    void requestSessionCannotCrossReadersOrHideCorruptPhysicalCopy() throws Exception {
        PackedObject packed = packObject(1994);
        ByteBuffer corrupt = writableCopy(packed.bytes());
        StreamObjectFormat.Footer footer = StreamObjectFormat.readFooter(corrupt);
        StreamObjectFormat.DataBlockIndexEntry block = StreamObjectFormat.readIndex(corrupt, footer).get(0);
        int payload = Math.toIntExact(block.blockPosition() + StreamObjectFormat.DATA_BLOCK_HEADER_BYTES +
            StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES);
        corrupt.put(payload, (byte) (corrupt.get(payload) ^ 0x44));

        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        RecordingObjectStore healthyStore = new RecordingObjectStore(1994, packed.bytes());
        RecordingObjectStore corruptStore = new RecordingObjectStore(1994, corrupt);
        SharedObjectReader healthyReader = new SharedObjectReader(healthyStore, index, 0, 0);
        SharedObjectReader corruptReader = new SharedObjectReader(corruptStore, index, 0, 0);
        try (SharedObjectReader.ReadSession session = healthyReader.openReadSession()) {
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5}, toArray(healthyReader.read(PARTITION, 2, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
            ExecutionException foreign = assertThrows(ExecutionException.class,
                () -> corruptReader.read(PARTITION, 2, session).get(10, TimeUnit.SECONDS));
            assertTrue(hasCause(foreign, IllegalArgumentException.class));
            assertTrue(corruptStore.reads().isEmpty(), "foreign session is rejected before any object I/O");
            try (SharedObjectReader.ReadSession ownSession = corruptReader.openReadSession()) {
                ExecutionException damaged = assertThrows(ExecutionException.class,
                    () -> corruptReader.read(PARTITION, 2, ownSession).get(10, TimeUnit.SECONDS));
                assertTrue(hasCause(damaged, RemoteObjectCorruptionException.class));
                assertTrue(corruptStore.reads().size() > 0, "own session must validate its physical copy");
            }
            assertArrayEquals(new byte[]{6, 7, 8, 9}, toArray(healthyReader.read(PARTITION, 7, session)
                .get(10, TimeUnit.SECONDS).orElseThrow()));
        }
    }

    @Test
    void closedRequestSessionIsRejectedBeforeRemoteLookup() throws Exception {
        PackedObject packed = packObject(1995);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(packed.metadata());
        RecordingObjectStore store = new RecordingObjectStore(1995, packed.bytes());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0);
        SharedObjectReader.ReadSession session = reader.openReadSession();
        session.close();
        ExecutionException closed = assertThrows(ExecutionException.class,
            () -> reader.read(PARTITION, 2, session).get(10, TimeUnit.SECONDS));
        assertTrue(hasCause(closed, IllegalStateException.class));
        assertTrue(store.reads().isEmpty());
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
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.readExact(PARTITION, 0L, 4L, 8, 5, null).get(10, TimeUnit.SECONDS).orElseThrow()));
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
    void cancelledAlternateReadAfterPrimaryFailureRemainsCancellation() throws Exception {
        PackedObject canonical = packObject(972L);
        PackedObject alternative = packObject(973L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternative.metadata());

        CompletableFuture<ByteBuffer> pendingAlternate = new CompletableFuture<>();
        AtomicInteger alternateReads = new AtomicInteger();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == canonical.metadata().objectId()) {
                    return CompletableFuture.failedFuture(new IOException("canonical object unavailable"));
                }
                if (objectId == alternative.metadata().objectId()) {
                    alternateReads.incrementAndGet();
                    return pendingAlternate;
                }
                throw new IllegalArgumentException("Unexpected object " + objectId);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        var read = new SharedObjectReader(store, index, 4).read(PARTITION, 2L);
        assertTrue(alternateReads.get() > 0, "Primary failure must attempt the COMMITTED alternate");
        assertFalse(read.isDone(), "Pending alternate I/O is not a physical failure");
        pendingAlternate.cancel(false);
        Throwable failure = read.handle((ignored, error) -> error).get(10L, TimeUnit.SECONDS);
        assertTrue(hasCause(failure, CancellationException.class));
        assertFalse(hasCause(failure, IOException.class),
            "Alternate cancellation must not report two unreadable COMMITTED copies");
    }

    @Test
    void synchronousIndexedPrimaryFailureFallsBackWithIndexCacheDisabled() throws Exception {
        PackedObject canonical = packObject(980L);
        PackedObject alternative = packObject(981L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(canonical.metadata());
        index.add(alternative.metadata());

        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(alternative.metadata().objectId(), alternative.bytes()).get(10L, TimeUnit.SECONDS);
        AtomicInteger primaryAttempts = new AtomicInteger();
        ObjectStore store = synchronouslyFailingPrimary(canonical.metadata().objectId(), backing, primaryAttempts);

        SharedObjectReader reader = new SharedObjectReader(store, index, 0);
        ByteBuffer restored = reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow();
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(restored));
        assertEquals(1, primaryAttempts.get(), "A failed KSO2 Header GET must initiate one fallback");
    }

    @Test
    void synchronousLegacyDirectPrimaryFailureFallsBackToCommittedCopy() throws Exception {
        PackedObject canonical = packObject(982L);
        PackedObject alternative = packObject(983L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.restore(List.of(
            new RemoteObjectIndex.RangeReference(
                canonical.metadata().objectId(), canonical.metadata().ranges().get(0)),
            new RemoteObjectIndex.RangeReference(
                alternative.metadata().objectId(), alternative.metadata().ranges().get(0))
        ));

        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(alternative.metadata().objectId(), alternative.bytes()).get(10L, TimeUnit.SECONDS);
        AtomicInteger primaryAttempts = new AtomicInteger();
        ObjectStore store = synchronouslyFailingPrimary(canonical.metadata().objectId(), backing, primaryAttempts);

        SharedObjectReader reader = new SharedObjectReader(store, index, 0);
        ByteBuffer restored = reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow();
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5}, toArray(restored));
        assertEquals(1, primaryAttempts.get(), "Legacy direct Range GET must use the alternate after failure");
    }

    @Test
    void pendingHeaderFailureKeepsActualFooterTransportPinned() throws Exception {
        PackedObject primary = packObject(991L);
        PackedObject secondary = packObject(992L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(primary.metadata());
        index.add(secondary.metadata());

        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(secondary.metadata().objectId(), secondary.bytes()).get(10L, TimeUnit.SECONDS);
        CompletableFuture<ByteBuffer> pendingFooter = new CompletableFuture<>();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == primary.metadata().objectId()) {
                    return position == 0L
                        ? CompletableFuture.failedFuture(new IOException("Primary KSO2 header unavailable"))
                        : pendingFooter;
                }
                return backing.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        SharedObjectReader reader = new SharedObjectReader(store, index, 4, 0L, pins);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertFalse(pendingFooter.isDone(), "The failed header must not wait for its still-running footer GET");
        assertEquals(1L, pins.inFlightRangeGets(primary.metadata().objectId()),
            "The actual footer GET must stay pinned even though the fallback read has completed");

        pins.fence(primary.metadata().objectId());
        assertFalse(pins.locallyDrainedAfterFence(primary.metadata().objectId()));
        pendingFooter.completeExceptionally(new IOException("Original footer GET finally terminated"));
        assertEquals(0L, pins.inFlightRangeGets(primary.metadata().objectId()));
        assertTrue(pins.locallyDrainedAfterFence(primary.metadata().objectId()));
    }

    @Test
    void locallyFencedPrimaryFallsBackWithoutIssuingAnotherGet() throws Exception {
        PackedObject primary = packObject(993L);
        PackedObject secondary = packObject(994L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.restore(List.of(
            new RemoteObjectIndex.RangeReference(
                primary.metadata().objectId(), primary.metadata().ranges().get(0)),
            new RemoteObjectIndex.RangeReference(
                secondary.metadata().objectId(), secondary.metadata().ranges().get(0))
        ));
        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(secondary.metadata().objectId(), secondary.bytes()).get(10L, TimeUnit.SECONDS);
        AtomicInteger primaryGets = new AtomicInteger();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == primary.metadata().objectId()) {
                    primaryGets.incrementAndGet();
                    return CompletableFuture.failedFuture(new IOException("Forbidden primary Range GET"));
                }
                return backing.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        pins.fence(primary.metadata().objectId());
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0L, pins);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertEquals(0, primaryGets.get(), "Locally fenced physical objects must never issue another Range GET");
        assertTrue(pins.locallyDrainedAfterFence(primary.metadata().objectId()));
        assertFalse(pins.locallyDrainedAfterFence(secondary.metadata().objectId()),
            "An unfenced physical object cannot claim a retirement certificate");
    }

    @Test
    void cancelledCallerFutureCannotUnpinPendingTransportGet() throws Exception {
        PackedObject packed = packObject(995L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.restore(List.of(new RemoteObjectIndex.RangeReference(
            packed.metadata().objectId(), packed.metadata().ranges().get(0)
        )));
        CompletableFuture<ByteBuffer> pendingGet = new CompletableFuture<>();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return pendingGet;
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        SharedObjectReader reader = new SharedObjectReader(store, index, 0, 0L, pins);
        CompletableFuture<Optional<ByteBuffer>> cancelled = reader.read(PARTITION, 2L);
        assertEquals(1L, pins.inFlightRangeGets(packed.metadata().objectId()));
        assertEquals(1L, pins.inFlightLogicalReads(packed.metadata().objectId()));
        assertTrue(cancelled.cancel(true));
        assertFalse(pendingGet.isDone(), "Caller cancellation must not complete the S3 transport Future");
        assertEquals(1L, pins.inFlightRangeGets(packed.metadata().objectId()));
        assertEquals(1L, pins.inFlightLogicalReads(packed.metadata().objectId()));

        pins.fence(packed.metadata().objectId());
        assertFalse(pins.locallyDrainedAfterFence(packed.metadata().objectId()));
        pendingGet.complete(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));
        assertEquals(0L, pins.inFlightRangeGets(packed.metadata().objectId()));
        assertEquals(0L, pins.inFlightLogicalReads(packed.metadata().objectId()));
        assertTrue(pins.locallyDrainedAfterFence(packed.metadata().objectId()));
        assertTrue(cancelled.isCancelled());
    }

    @Test
    void warmCachedObjectCannotBypassLocalFenceAndMustUseAnotherCommittedCopy() throws Exception {
        PackedObject primary = packObject(996L);
        PackedObject alternative = packObject(997L);
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(primary.metadata());
        index.add(alternative.metadata());
        InMemoryObjectStore backing = new InMemoryObjectStore();
        backing.put(primary.metadata().objectId(), primary.bytes()).get(10L, TimeUnit.SECONDS);
        backing.put(alternative.metadata().objectId(), alternative.bytes()).get(10L, TimeUnit.SECONDS);
        AtomicInteger primaryGets = new AtomicInteger();
        AtomicInteger alternateGets = new AtomicInteger();
        ObjectStore counting = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == primary.metadata().objectId()) {
                    primaryGets.incrementAndGet();
                } else if (objectId == alternative.metadata().objectId()) {
                    alternateGets.incrementAndGet();
                }
                return backing.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        SharedObjectReader reader = new SharedObjectReader(counting, index, 4, 1024 * 1024L, pins);
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertTrue(primaryGets.get() > 0);
        assertEquals(0, alternateGets.get());
        assertEquals(0L, pins.inFlightLogicalReads(primary.metadata().objectId()));
        int physicalBeforeFence = primaryGets.get();
        pins.fence(primary.metadata().objectId());
        assertTrue(pins.locallyDrainedAfterFence(primary.metadata().objectId()));

        assertArrayEquals(new byte[] {1, 2, 3, 4, 5},
            toArray(reader.read(PARTITION, 2L).get(10L, TimeUnit.SECONDS).orElseThrow()));
        assertEquals(physicalBeforeFence, primaryGets.get(),
            "A fully cached, fenced physical object must not serve another logical read");
        assertTrue(alternateGets.get() > 0, "Fallback must read an independently COMMITTED physical object");
        assertTrue(pins.locallyDrainedAfterFence(primary.metadata().objectId()));
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

    private static ObjectStore synchronouslyFailingPrimary(
        long primaryObjectId,
        InMemoryObjectStore backing,
        AtomicInteger primaryAttempts
    ) {
        return new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                if (objectId == primaryObjectId) {
                    primaryAttempts.incrementAndGet();
                    throw new IllegalStateException("Synchronous primary object-store transport failure");
                }
                return backing.rangeRead(objectId, position, length);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
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
