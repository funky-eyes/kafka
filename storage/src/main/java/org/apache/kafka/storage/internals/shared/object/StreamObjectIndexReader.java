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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Reads the immutable KSO2 object descriptor with bounded range requests.
 *
 * <p>This reader deliberately does not fetch a DataBlock. It is the metadata-resolution layer used before a cold
 * read selects a block. Legacy checkpoint references without an object descriptor return {@link Optional#empty()}
 * so callers can continue using the existing per-batch physical reference during an online checkpoint migration.</p>
 */
final class StreamObjectIndexReader {
    private final ObjectStore objectStore;

    StreamObjectIndexReader(ObjectStore objectStore) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
    }

    CompletableFuture<Optional<IndexSnapshot>> load(RemoteObjectIndex.RangeReference reference) {
        Objects.requireNonNull(reference, "reference");
        if (!reference.hasObjectDescriptor()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        if (reference.objectSize() <
            StreamObjectFormat.OBJECT_HEADER_BYTES + StreamObjectFormat.INDEX_HEADER_BYTES +
                StreamObjectFormat.FOOTER_BYTES) {
            return CompletableFuture.failedFuture(new RemoteObjectCorruptionException(
                "Remote stream object is too short: object=" + reference.objectId() +
                    ", size=" + reference.objectSize()
            ));
        }

        long footerPosition = reference.objectSize() - StreamObjectFormat.FOOTER_BYTES;
        CompletableFuture<ByteBuffer> header = rangeReadExact(
            reference.objectId(),
            0,
            StreamObjectFormat.OBJECT_HEADER_BYTES
        );
        CompletableFuture<ByteBuffer> footer = rangeReadExact(
            reference.objectId(),
            footerPosition,
            StreamObjectFormat.FOOTER_BYTES
        );

        return header.thenCombine(footer, (headerBytes, footerBytes) -> {
            try {
                long headerObjectId = StreamObjectFormat.readObjectId(headerBytes);
                if (headerObjectId != reference.objectId()) {
                    throw new RemoteObjectCorruptionException(
                        "Remote stream object ID mismatch: expected=" + reference.objectId() +
                            ", actual=" + headerObjectId
                    );
                }
                return StreamObjectFormat.readFooterTail(footerBytes, reference.objectSize());
            } catch (IOException e) {
                throw new CompletionException(corruption(reference, e));
            }
        }).thenCompose(parsedFooter -> rangeReadExact(
            reference.objectId(),
            parsedFooter.indexPosition(),
            parsedFooter.indexLength()
        ).thenApply(indexBytes -> {
            try {
                List<StreamObjectFormat.DataBlockIndexEntry> entries =
                    StreamObjectFormat.readIndexBlock(indexBytes, parsedFooter);
                return Optional.of(new IndexSnapshot(parsedFooter, entries));
            } catch (IOException e) {
                throw new CompletionException(corruption(reference, e));
            }
        }));
    }

    private CompletableFuture<ByteBuffer> rangeReadExact(long objectId, long position, int length) {
        return objectStore.rangeRead(objectId, position, length).thenApply(bytes -> {
            if (bytes.remaining() != length) {
                throw new CompletionException(new RemoteObjectCorruptionException(
                    "Remote range length mismatch: object=" + objectId +
                        ", position=" + position + ", expected=" + length +
                        ", actual=" + bytes.remaining()
                ));
            }
            return bytes.asReadOnlyBuffer();
        });
    }

    private static RemoteObjectCorruptionException corruption(
        RemoteObjectIndex.RangeReference reference,
        IOException cause
    ) {
        if (cause instanceof RemoteObjectCorruptionException corruption) {
            return corruption;
        }
        return new RemoteObjectCorruptionException(
            "Invalid remote stream object index metadata: object=" + reference.objectId(),
            cause
        );
    }

    record IndexSnapshot(
        StreamObjectFormat.Footer footer,
        List<StreamObjectFormat.DataBlockIndexEntry> entries
    ) {
        IndexSnapshot {
            Objects.requireNonNull(footer, "footer");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries"));
        }
    }
}
