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
import java.util.concurrent.CancellationException;
import java.util.zip.CRC32C;

public final class SharedObjectReader {
    static final int DEFAULT_INDEX_CACHE_ENTRIES = 1_024;
    static final long DEFAULT_DATA_BLOCK_CACHE_BYTES = 64L * 1024 * 1024;

    private final ObjectStore objectStore;
    private final RemoteObjectIndex remoteIndex;
    private final StreamObjectIndexReader indexReader;
    private final StreamObjectIndexCache indexCache;
    private final StreamObjectDataBlockCache dataBlockCache;

    public SharedObjectReader(ObjectStore objectStore, RemoteObjectIndex remoteIndex) {
        this(
            objectStore,
            remoteIndex,
            DEFAULT_INDEX_CACHE_ENTRIES,
            DEFAULT_DATA_BLOCK_CACHE_BYTES
        );
    }

    SharedObjectReader(ObjectStore objectStore, RemoteObjectIndex remoteIndex, int indexCacheEntries) {
        this(objectStore, remoteIndex, indexCacheEntries, DEFAULT_DATA_BLOCK_CACHE_BYTES);
    }

    public SharedObjectReader(
        ObjectStore objectStore,
        RemoteObjectIndex remoteIndex,
        int indexCacheEntries,
        long dataBlockCacheBytes
    ) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.remoteIndex = Objects.requireNonNull(remoteIndex, "remoteIndex");
        this.indexReader = new StreamObjectIndexReader(objectStore);
        this.indexCache = new StreamObjectIndexCache(indexCacheEntries);
        this.dataBlockCache = new StreamObjectDataBlockCache(dataBlockCacheBytes);
    }

    /**
     * Reads the complete Kafka RecordBatch containing {@code offset}.
     * A failed physical COMMITTED copy can be retried from one equivalent
     * copy, with independent index and CRC validation. This does not
     * promote a physical copy, affect Kafka ISR/HW, or permit object GC.
     */
    public CompletableFuture<Optional<ByteBuffer>> read(SharedPartitionId partition, long offset) {
        Objects.requireNonNull(partition, "partition");
        Optional<RemoteObjectIndex.RangeReference> reference = remoteIndex.find(partition, offset);
        if (reference.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        RemoteObjectIndex.RangeReference ref = reference.get();
        return readReference(ref).exceptionallyCompose(firstFailure -> {
            if (unwrapCompletion(firstFailure) instanceof CancellationException) {
                // Cancellation is a caller/lifecycle signal, not evidence of
                // physical object loss. Never start a second S3 read.
                return CompletableFuture.failedFuture(firstFailure);
            }
            Optional<RemoteObjectIndex.RangeReference> fallback = remoteIndex.findEquivalentAlternative(ref);
            if (fallback.isEmpty()) {
                return CompletableFuture.failedFuture(firstFailure);
            }
            RemoteObjectIndex.RangeReference alternate = fallback.get();
            return readReference(alternate).handle((bytes, secondFailure) -> {
                if (secondFailure != null) {
                    // Cancellation of alternate I/O is a lifecycle signal,
                    // not evidence that both COMMITTED copies are unreadable.
                    Throwable cause = unwrapCompletion(secondFailure);
                    if (cause instanceof CancellationException) {
                        throw new CompletionException(cause);
                    }
                    throw bothPhysicalCopiesFailed(ref, alternate, firstFailure, secondFailure);
                }
                return bytes;
            });
        });
    }

    private static CompletionException bothPhysicalCopiesFailed(
        RemoteObjectIndex.RangeReference primaryReference,
        RemoteObjectIndex.RangeReference alternateReference,
        Throwable firstFailure,
        Throwable secondFailure
    ) {
        Throwable primary = unwrapCompletion(firstFailure);
        Throwable secondary = unwrapCompletion(secondFailure);
        // A cached in-flight I/O failure can be shared by concurrent reads.
        // Never add suppressed exceptions to that shared Throwable: doing so
        // leaks diagnostic references across readers and failed attempts.
        IOException combined = new IOException(
            "Both COMMITTED physical copies unreadable for partition="
                + primaryReference.range().partition() + ", offsets="
                + primaryReference.range().offsets() + ", primaryObject="
                + primaryReference.objectId() + ", alternateObject="
                + alternateReference.objectId(),
            primary
        );
        if (primary != secondary) {
            combined.addSuppressed(secondary);
        }
        return new CompletionException(combined);
    }

    private static Throwable unwrapCompletion(Throwable error) {
        return error instanceof CompletionException && error.getCause() != null
            ? error.getCause() : error;
    }

    private CompletableFuture<Optional<ByteBuffer>> readReference(
        RemoteObjectIndex.RangeReference reference
    ) {
        return reference.hasObjectDescriptor() ? readIndexed(reference) : readDirect(reference);
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
        return dataBlockCache.get(
            reference,
            block,
            () -> loadDataBlock(reference, block)
        ).thenApply(snapshot -> {
            try {
                return Optional.of(snapshot.batch(reference.range()));
            } catch (IOException e) {
                throw new CompletionException(corruption(reference, e));
            }
        });
    }

    private CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> loadDataBlock(
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
                    return StreamObjectDataBlockReader.read(bytes, block);
                } catch (IOException e) {
                    throw new CompletionException(corruption(reference, e));
                }
            });
    }

    private CompletableFuture<Optional<ByteBuffer>> readDirect(RemoteObjectIndex.RangeReference reference) {
        SharedObjectRange range = reference.range();
        CompletableFuture<ByteBuffer> bytesFuture;
        try {
            bytesFuture = Objects.requireNonNull(
                objectStore.rangeRead(reference.objectId(), range.objectPosition(), range.objectLength()),
                "ObjectStore returned null range future"
            );
        } catch (RuntimeException failure) {
            // Legacy/direct reads must not throw before the asynchronous
            // failure handler can select the other COMMITTED physical copy.
            return CompletableFuture.failedFuture(failure);
        }
        return bytesFuture.thenApply(bytes -> {
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
