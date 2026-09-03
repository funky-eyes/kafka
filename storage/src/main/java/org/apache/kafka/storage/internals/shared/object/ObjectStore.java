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
     * Publishes one immutable object by pulling ordered parts lazily from {@code source}.
     *
     * <p>The compatibility implementation drains the source before delegating to the list API. S3 and other remote
     * stores should override this method so serialization, WAL reads and network upload stay pipelined and bounded by
     * the part size rather than the complete object size.</p>
     */
    default CompletableFuture<Void> put(long objectId, PartSource source) {
        Objects.requireNonNull(source, "source");
        List<ByteBuffer> parts = new ArrayList<>();
        try (source) {
            ByteBuffer part;
            while ((part = source.nextPart()) != null) {
                if (!part.hasRemaining()) {
                    return CompletableFuture.failedFuture(new IllegalArgumentException(
                        "Object part source returned an empty part"));
                }
                parts.add(part.asReadOnlyBuffer());
            }
        } catch (IOException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return put(objectId, parts);
    }

    CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length);

    CompletableFuture<Void> delete(long objectId);

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
