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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public interface ObjectStore extends AutoCloseable {
    CompletableFuture<Void> put(long objectId, ByteBuffer data);

    /**
     * Publishes one immutable object from ordered parts.
     *
     * <p>The default implementation preserves compatibility for stores without native multipart support by joining
     * the parts and delegating to {@link #put(long, ByteBuffer)}. Production stores should override this method when
     * they can upload parts without materializing the complete object in heap.</p>
     */
    default CompletableFuture<Void> put(long objectId, List<ByteBuffer> parts) {
        Objects.requireNonNull(parts, "parts");
        if (parts.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("parts must not be empty"));
        }
        long totalBytes = 0L;
        for (ByteBuffer part : parts) {
            Objects.requireNonNull(part, "part");
            totalBytes = Math.addExact(totalBytes, part.remaining());
        }
        if (totalBytes > Integer.MAX_VALUE) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "ObjectStore without multipart support cannot join object larger than Integer.MAX_VALUE bytes"));
        }
        ByteBuffer joined = ByteBuffer.allocate(Math.toIntExact(totalBytes));
        for (ByteBuffer part : parts) {
            joined.put(part.duplicate());
        }
        joined.flip();
        return put(objectId, joined.asReadOnlyBuffer());
    }

    /**
     * Publishes a lazily produced immutable object with a known exact serialized size.
     *
     * <p>Stores with native multipart support should override this overload. Knowing the exact size lets them decide
     * whether the current part is final without pulling a second part into memory.</p>
     */
    default CompletableFuture<Void> put(long objectId, long objectSize, PartSource source) {
        if (objectSize <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectSize must be positive"));
        }
        return drainAndPut(objectId, objectSize, source);
    }

    /**
     * Publishes one immutable object by pulling ordered parts lazily from {@code source}.
     *
     * <p>The compatibility implementation drains the source before delegating to the list API. S3 and other remote
     * stores should override this method so serialization, WAL reads and network upload stay pipelined and bounded by
     * the part size rather than the complete object size.</p>
     */
    default CompletableFuture<Void> put(long objectId, PartSource source) {
        return drainAndPut(objectId, -1L, source);
    }

    private CompletableFuture<Void> drainAndPut(long objectId, long expectedSize, PartSource source) {
        Objects.requireNonNull(source, "source");
        List<ByteBuffer> parts = new ArrayList<>();
        long totalBytes = 0L;
        try (source) {
            ByteBuffer part;
            while ((part = source.nextPart()) != null) {
                if (!part.hasRemaining()) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "Object part source returned an empty part"));
                }
                totalBytes = Math.addExact(totalBytes, part.remaining());
                parts.add(part.asReadOnlyBuffer());
            }
        } catch (IOException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        if (expectedSize >= 0 && totalBytes != expectedSize) {
            return CompletableFuture.failedFuture(new IllegalArgumentException(
                "Object part source size mismatch: expected=" + expectedSize + ", actual=" + totalBytes));
        }
        return put(objectId, parts);
    }

    /**
     * Revalidate provider-side conditions before releasing the last local
     * recoverable WAL bytes for objects already committed remotely.
     *
     * <p>Only implementations relying on externally mutable Bucket policy
     * should override this hook. In-memory/test stores without remote
     * expiration semantics retain a no-op. A successful check is advisory:
     * policy can change afterward, so it is never a physical GC grant.</p>
     *
     * @throws IllegalStateException if reclaim would be unsafe
     */
    default void requireSafeForWalReclaim() {
    }

    CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length);

    CompletableFuture<Void> delete(long objectId);

    /**
     * Idempotently remove a bounded set of independently cleanup-fenced objects.
     *
     * <p>Only the caller can grant deletion authority. The default retains
     * compatibility with other storage providers by fanning out deletes;
     * S3 implementations may use one native multi-object API call. A partial
     * failure MUST fail the whole batch; durable claims let the caller retry
     * all objects safely without assuming atomic deletion. On provider failure,
     * the batch waits for all previously returned DELETE futures, including
     * when a provider throws synchronously while starting a later DELETE.
     * A provider must return a future for every operation it starts.</p>
     */
    default CompletableFuture<Void> deleteBatch(List<Long> objectIds) {
        List<Long> requested = List.copyOf(Objects.requireNonNull(objectIds, "objectIds"));
        if (requested.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        List<CompletableFuture<Void>> deletions = new ArrayList<>(requested.size());
        for (long objectId : requested) {
            try {
                deletions.add(Objects.requireNonNull(delete(objectId), "DELETE future"));
            } catch (RuntimeException failure) {
                // Already-started operations can outlive a synchronous error.
                // Wait for them before allowing the caller to retry the batch.
                deletions.add(CompletableFuture.failedFuture(failure));
                break;
            }
        }
        return CompletableFuture.allOf(deletions.toArray(CompletableFuture[]::new));
    }

    /** Pull-based ordered object byte source. {@code null} marks end-of-object. */
    interface PartSource extends AutoCloseable {
        ByteBuffer nextPart() throws IOException;

        @Override
        default void close() {
        }
    }

    @Override
    default void close() throws Exception {
    }
}
