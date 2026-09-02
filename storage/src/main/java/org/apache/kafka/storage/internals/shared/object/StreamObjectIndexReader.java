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
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.CRC32C;

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
            return verifyLegacyDirectObject(reference, new IOException(
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

        return header.thenCombine(footer, HeaderAndFooter::new).thenCompose(parts -> {
            final long headerObjectId;
            try {
                headerObjectId = StreamObjectFormat.readObjectId(parts.header());
            } catch (IOException e) {
                if (hasStreamObjectMagic(parts.header())) {
                    return CompletableFuture.failedFuture(corruption(reference, e));
                }
                return verifyLegacyDirectObject(reference, e);
            }
            if (headerObjectId != reference.objectId()) {
                return CompletableFuture.failedFuture(new RemoteObjectCorruptionException(
                    "Remote stream object ID mismatch: expected=" + reference.objectId() +
                        ", actual=" + headerObjectId
                ));
            }

            final StreamObjectFormat.Footer parsedFooter;
            try {
                parsedFooter = StreamObjectFormat.readFooterTail(parts.footer(), reference.objectSize());
            } catch (IOException e) {
                return CompletableFuture.failedFuture(corruption(reference, e));
            }
            return rangeReadExact(
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
            });
        });
    }

    /**
     * Descriptor fields pre-date KSO2, so objectSize/objectChecksum alone cannot prove that an object has a KSO2
     * header. A descriptor-backed legacy object is accepted only after its complete bytes match the authoritative
     * object checksum. This keeps migration compatibility without turning malformed KSO2 metadata into a direct-read
     * fallback that could hide remote corruption.
     */
    private CompletableFuture<Optional<IndexSnapshot>> verifyLegacyDirectObject(
        RemoteObjectIndex.RangeReference reference,
        IOException streamFormatFailure
    ) {
        if (reference.objectSize() > Integer.MAX_VALUE) {
            return CompletableFuture.failedFuture(corruption(reference, new IOException(
                "Cannot checksum-verify descriptor-backed legacy object larger than Java ByteBuffer limit: size=" +
                    reference.objectSize(),
                streamFormatFailure
            )));
        }
        int objectSize = Math.toIntExact(reference.objectSize());
        return objectStore.rangeRead(reference.objectId(), 0, objectSize).thenApply(bytes -> {
            if (bytes.remaining() != objectSize) {
                throw new CompletionException(corruption(reference, new IOException(
                    "Descriptor-backed legacy object length mismatch: expected=" + objectSize +
                        ", actual=" + bytes.remaining(),
                    streamFormatFailure
                )));
            }
            if (hasStreamObjectMagic(bytes)) {
                throw new CompletionException(corruption(reference, streamFormatFailure));
            }
            long actualChecksum = crc32c(bytes);
            if (actualChecksum != reference.objectChecksum()) {
                throw new CompletionException(corruption(reference, new IOException(
                    "Remote object is neither valid KSO2 nor checksum-valid legacy direct data: expected checksum=" +
                        reference.objectChecksum() + ", actual=" + actualChecksum,
                    streamFormatFailure
                )));
            }
            return Optional.empty();
        });
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

    private static boolean hasStreamObjectMagic(ByteBuffer bytes) {
        if (bytes.remaining() < Integer.BYTES) {
            return false;
        }
        return bytes.duplicate().order(ByteOrder.BIG_ENDIAN).getInt() == StreamObjectFormat.OBJECT_MAGIC;
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
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

    private record HeaderAndFooter(ByteBuffer header, ByteBuffer footer) {
        private HeaderAndFooter {
            Objects.requireNonNull(header, "header");
            Objects.requireNonNull(footer, "footer");
        }
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
