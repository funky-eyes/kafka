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

import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.CRC32C;

public final class SharedObjectReader {
    static final int DEFAULT_INDEX_CACHE_ENTRIES = 1_024;

    private final ObjectStore objectStore;
    private final RemoteObjectIndex remoteIndex;
    private final StreamObjectIndexReader indexReader;
    private final StreamObjectIndexCache indexCache;

    public SharedObjectReader(ObjectStore objectStore, RemoteObjectIndex remoteIndex) {
        this(objectStore, remoteIndex, DEFAULT_INDEX_CACHE_ENTRIES);
    }

    SharedObjectReader(ObjectStore objectStore, RemoteObjectIndex remoteIndex, int indexCacheEntries) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.remoteIndex = Objects.requireNonNull(remoteIndex, "remoteIndex");
        this.indexReader = new StreamObjectIndexReader(objectStore);
        this.indexCache = new StreamObjectIndexCache(indexCacheEntries);
    }

    /** Returns the complete Kafka RecordBatch containing {@code offset}, if it is remotely covered. */
    public CompletableFuture<Optional<ByteBuffer>> read(SharedPartitionId partition, long offset) {
        Objects.requireNonNull(partition, "partition");
        Optional<RemoteObjectIndex.RangeReference> reference = remoteIndex.find(partition, offset);
        if (reference.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        RemoteObjectIndex.RangeReference ref = reference.get();
        return ref.hasObjectDescriptor() ? readIndexed(ref) : readDirect(ref);
    }

    private CompletableFuture<Optional<ByteBuffer>> readIndexed(RemoteObjectIndex.RangeReference reference) {
        return indexCache.get(reference, indexReader).thenCompose(snapshot -> {
            if (snapshot.isEmpty()) {
                return readDirect(reference);
            }
            StreamObjectFormat.DataBlockIndexEntry block;
            try {
                block = findIndexedBlock(snapshot.get(), reference.range());
            } catch (IOException e) {
                return CompletableFuture.failedFuture(corruption(reference, e));
            }
            return readDataBlock(reference, block);
        });
    }

    private CompletableFuture<Optional<ByteBuffer>> readDataBlock(
        RemoteObjectIndex.RangeReference reference,
        StreamObjectFormat.DataBlockIndexEntry block
    ) {
        return objectStore.rangeRead(reference.objectId(), block.blockPosition(), block.blockLength())
            .thenApply(bytes -> {
                if (bytes.remaining() != block.blockLength()) {
                    throw new CompletionException(corruption(
                        reference,
                        new IOException(
                            "Remote DataBlock length mismatch: expected=" + block.blockLength() +
                                ", actual=" + bytes.remaining()
                        )
                    ));
                }
                try {
                    StreamObjectDataBlockReader.DataBlockSnapshot snapshot =
                        StreamObjectDataBlockReader.read(bytes, block);
                    return Optional.of(snapshot.batch(reference.range()));
                } catch (IOException e) {
                    throw new CompletionException(corruption(reference, e));
                }
            });
    }

    private CompletableFuture<Optional<ByteBuffer>> readDirect(RemoteObjectIndex.RangeReference reference) {
        SharedObjectRange range = reference.range();
        return objectStore.rangeRead(reference.objectId(), range.objectPosition(), range.objectLength())
            .thenApply(bytes -> {
                if (bytes.remaining() != range.objectLength()) {
                    throw new CompletionException(corruption(
                        reference,
                        new IOException(
                            "Remote batch length mismatch: expected=" + range.objectLength() +
                                ", actual=" + bytes.remaining()
                        )
                    ));
                }
                long actualChecksum = crc32c(bytes);
                if (actualChecksum != range.checksum()) {
                    throw new CompletionException(corruption(
                        reference,
                        new IOException(
                            "Remote batch checksum mismatch: expected=" + range.checksum() +
                                ", actual=" + actualChecksum
                        )
                    ));
                }
                return Optional.of(bytes.asReadOnlyBuffer());
            });
    }

    private static StreamObjectFormat.DataBlockIndexEntry findIndexedBlock(
        StreamObjectIndexReader.IndexSnapshot snapshot,
        SharedObjectRange range
    ) throws IOException {
        StreamObjectFormat.DataBlockIndexEntry match = null;
        for (StreamObjectFormat.DataBlockIndexEntry entry : snapshot.entries()) {
            if (!entry.partition().equals(range.partition()) || entry.leaderEpoch() != range.leaderEpoch()) {
                continue;
            }
            if (entry.offsets().startOffset() <= range.offsets().startOffset() &&
                range.offsets().endOffset() <= entry.offsets().endOffset()) {
                if (match != null) {
                    throw new IOException("Remote batch maps to multiple indexed DataBlocks");
                }
                match = entry;
            }
        }
        if (match == null) {
            throw new IOException("Remote batch is not covered by the stream object index");
        }
        return match;
    }

    private static RemoteObjectCorruptionException corruption(
        RemoteObjectIndex.RangeReference reference,
        Throwable cause
    ) {
        if (cause instanceof RemoteObjectCorruptionException corruption) {
            return corruption;
        }
        return new RemoteObjectCorruptionException(
            "Remote stream object corruption: object=" + reference.objectId() +
                ", partition=" + reference.range().partition() +
                ", offsets=" + reference.range().offsets() +
                ": " + cause.getMessage(),
            cause
        );
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }
}
