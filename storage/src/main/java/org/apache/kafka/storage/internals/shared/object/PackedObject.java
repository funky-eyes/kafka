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

import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/** A packed immutable object represented as ordered upload parts plus its committed metadata. */
public record PackedObject(List<ByteBuffer> parts, SharedObjectMetadata metadata) {
    public PackedObject {
        Objects.requireNonNull(parts, "parts");
        Objects.requireNonNull(metadata, "metadata");
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("parts must not be empty");
        }
        parts = parts.stream()
            .map(part -> Objects.requireNonNull(part, "part").asReadOnlyBuffer())
            .toList();
    }

    public PackedObject(ByteBuffer bytes, SharedObjectMetadata metadata) {
        this(List.of(Objects.requireNonNull(bytes, "bytes")), metadata);
    }

    /** Compatibility view for callers that still require one contiguous buffer. */
    public ByteBuffer bytes() {
        if (parts.size() == 1) {
            return parts.get(0).asReadOnlyBuffer();
        }
        long totalBytes = parts.stream().mapToLong(ByteBuffer::remaining).sum();
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("Multipart object is too large for one Java ByteBuffer");
        }
        ByteBuffer joined = ByteBuffer.allocate(Math.toIntExact(totalBytes));
        parts.forEach(part -> joined.put(part.duplicate()));
        joined.flip();
        return joined.asReadOnlyBuffer();
    }
}
