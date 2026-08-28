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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryObjectStore implements ObjectStore {
    private final ConcurrentHashMap<Long, byte[]> objects = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        if (data == null) {
            return CompletableFuture.failedFuture(new NullPointerException("data"));
        }
        ByteBuffer source = data.duplicate();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        byte[] previous = objects.putIfAbsent(objectId, bytes);
        if (previous != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Object already exists: " + objectId));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
        byte[] bytes = objects.get(objectId);
        if (bytes == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown object: " + objectId));
        }
        if (position < 0 || length < 0 || position > bytes.length || length > bytes.length - position) {
            return CompletableFuture.failedFuture(new IndexOutOfBoundsException(
                "Invalid range position=" + position + ", length=" + length + ", objectSize=" + bytes.length));
        }
        return CompletableFuture.completedFuture(
            ByteBuffer.wrap(bytes, Math.toIntExact(position), length).slice().asReadOnlyBuffer());
    }

    @Override
    public CompletableFuture<Void> delete(long objectId) {
        objects.remove(objectId);
        return CompletableFuture.completedFuture(null);
    }

    public boolean contains(long objectId) {
        return objects.containsKey(objectId);
    }
}
