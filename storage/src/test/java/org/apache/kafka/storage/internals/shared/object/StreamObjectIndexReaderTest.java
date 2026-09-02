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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamObjectIndexReaderTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(7, 8, 0);

    @TempDir
    Path tempDir;

    @Test
    void shouldLoadHeaderFooterAndIndexWithBoundedRangeReads() throws Exception {
        PackedObject packed = packObject(800);
        SharedObjectMetadata metadata = packed.metadata();
        RecordingObjectStore store = new RecordingObjectStore(metadata.objectId(), packed.bytes());
        StreamObjectIndexReader reader = new StreamObjectIndexReader(store);
        RemoteObjectIndex.RangeReference reference = new RemoteObjectIndex.RangeReference(
            metadata.objectId(),
            metadata.objectSize(),
            metadata.objectChecksum(),
            metadata.ranges().get(0)
        );

        Optional<StreamObjectIndexReader.IndexSnapshot> result = reader.load(reference)
            .get(10, TimeUnit.SECONDS);

        StreamObjectIndexReader.IndexSnapshot snapshot = result.orElseThrow();
        assertEquals(2, snapshot.entries().size());
        assertEquals(List.of(
            new Read(0, StreamObjectFormat.OBJECT_HEADER_BYTES),
            new Read(metadata.objectSize() - StreamObjectFormat.FOOTER_BYTES, StreamObjectFormat.FOOTER_BYTES),
            new Read(snapshot.footer().indexPosition(), snapshot.footer().indexLength())
        ), store.reads());
        assertFalse(store.reads().stream().anyMatch(read -> read.length() == metadata.objectSize()));
    }

    @Test
    void shouldLeaveLegacyReferenceOnDirectRangeFallbackWithoutRemoteReads() throws Exception {
        PackedObject packed = packObject(801);
        SharedObjectMetadata metadata = packed.metadata();
        RecordingObjectStore store = new RecordingObjectStore(metadata.objectId(), packed.bytes());
        StreamObjectIndexReader reader = new StreamObjectIndexReader(store);
        RemoteObjectIndex.RangeReference legacy =
            new RemoteObjectIndex.RangeReference(metadata.objectId(), metadata.ranges().get(0));

        assertEquals(Optional.empty(), reader.load(legacy).get(10, TimeUnit.SECONDS));
        assertEquals(List.of(), store.reads());
    }

    @Test
    void shouldRejectObjectWhoseHeaderIdDoesNotMatchMetadataReference() throws Exception {
        PackedObject packed = packObject(802);
        SharedObjectMetadata metadata = packed.metadata();
        long incorrectObjectId = 999;
        RecordingObjectStore store = new RecordingObjectStore(incorrectObjectId, packed.bytes());
        StreamObjectIndexReader reader = new StreamObjectIndexReader(store);
        RemoteObjectIndex.RangeReference reference = new RemoteObjectIndex.RangeReference(
            incorrectObjectId,
            metadata.objectSize(),
            metadata.objectChecksum(),
            metadata.ranges().get(0)
        );

        ExecutionException error = assertThrows(
            ExecutionException.class,
            () -> reader.load(reference).get(10, TimeUnit.SECONDS)
        );
        assertInstanceOf(RemoteObjectCorruptionException.class, error.getCause());
    }

    private PackedObject packObject(long objectId) throws Exception {
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("index-reader-" + objectId), 1024 * 1024, 4096))) {
            append(engine, 0, 9, new byte[32]);
            append(engine, 10, 19, new byte[32]);
            return new SharedObjectPacker(56).pack(
                objectId,
                engine.uploadCandidates(PARTITION, 0, 20),
                engine
            );
        }
    }

    private static void append(SharedStorageEngine engine, long firstOffset, long lastOffset, byte[] payload)
        throws Exception {
        engine.appendData(PARTITION, 3, firstOffset, lastOffset, ByteBuffer.wrap(payload))
            .get(10, TimeUnit.SECONDS);
    }

    private static final class RecordingObjectStore implements ObjectStore {
        private final long objectId;
        private final ByteBuffer object;
        private final List<Read> reads = new ArrayList<>();

        private RecordingObjectStore(long objectId, ByteBuffer object) {
            this.objectId = objectId;
            ByteBuffer copy = ByteBuffer.allocate(object.remaining());
            copy.put(object.duplicate());
            copy.flip();
            this.object = copy.asReadOnlyBuffer();
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
            ByteBuffer range = object.duplicate();
            range.position(Math.toIntExact(position));
            range.limit(Math.toIntExact(position + length));
            return CompletableFuture.completedFuture(range.slice().asReadOnlyBuffer());
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("delete not used"));
        }

        private synchronized List<Read> reads() {
            return List.copyOf(reads);
        }
    }

    private record Read(long position, int length) {
    }
}
