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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Byte-bounded LRU of verified immutable KSO2 DataBlocks.
 *
 * <p>Loads for the same immutable object/block descriptor are coalesced. Failed loads are removed immediately so a
 * transient object-store failure or corruption result cannot poison later reads. Entries larger than the complete
 * cache budget bypass the cache rather than evicting all useful blocks. A zero-byte budget disables caching.</p>
 */
final class StreamObjectDataBlockCache {
    private final long maxBytes;
    private final LinkedHashMap<Key, Entry> entries = new LinkedHashMap<>(16, 0.75f, true);
    private long cachedBytes;

    StreamObjectDataBlockCache(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must not be negative");
        }
        this.maxBytes = maxBytes;
    }

    CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> get(
        RemoteObjectIndex.RangeReference reference,
        StreamObjectFormat.DataBlockIndexEntry block,
        Supplier<CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot>> loader
    ) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(block, "block");
        Objects.requireNonNull(loader, "loader");
        if (maxBytes == 0 || block.blockLength() > maxBytes) {
            return load(loader);
        }

        Key key = Key.from(reference, block);
        Entry installed;
        synchronized (this) {
            Entry existing = entries.get(key);
            if (existing != null) {
                return existing.future();
            }
            installed = new Entry(new CompletableFuture<>(), block.blockLength());
            entries.put(key, installed);
            cachedBytes = Math.addExact(cachedBytes, installed.bytes());
            evictIfNeeded();
        }

        Entry shared = installed;
        shared.future().whenComplete((ignored, error) -> {
            if (error != null) {
                removeFailed(key, shared);
            }
        });

        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> loaded;
        try {
            loaded = Objects.requireNonNull(loader.get(), "loader returned null future");
        } catch (Throwable error) {
            shared.future().completeExceptionally(error);
            return shared.future();
        }
        loaded.whenComplete((snapshot, error) -> {
            if (error != null) {
                shared.future().completeExceptionally(error);
            } else if (snapshot == null) {
                shared.future().completeExceptionally(new NullPointerException("loader returned null snapshot"));
            } else {
                shared.future().complete(snapshot);
            }
        });
        return shared.future();
    }

    synchronized int size() {
        return entries.size();
    }

    synchronized long cachedBytes() {
        return cachedBytes;
    }

    private static CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> load(
        Supplier<CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot>> loader
    ) {
        try {
            return Objects.requireNonNull(loader.get(), "loader returned null future");
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    private synchronized void removeFailed(Key key, Entry expected) {
        Entry current = entries.get(key);
        if (current == expected) {
            entries.remove(key);
            cachedBytes -= expected.bytes();
        }
    }

    private void evictIfNeeded() {
        while (cachedBytes > maxBytes) {
            Iterator<Map.Entry<Key, Entry>> iterator = entries.entrySet().iterator();
            Map.Entry<Key, Entry> eldest = iterator.next();
            cachedBytes -= eldest.getValue().bytes();
            iterator.remove();
        }
    }

    private record Entry(
        CompletableFuture<StreamObjectDataBlockReader.DataBlockSnapshot> future,
        int bytes
    ) {
        private Entry {
            Objects.requireNonNull(future, "future");
        }
    }

    private record Key(
        long objectId,
        long objectSize,
        long objectChecksum,
        StreamObjectFormat.DataBlockIndexEntry block
    ) {
        private static Key from(
            RemoteObjectIndex.RangeReference reference,
            StreamObjectFormat.DataBlockIndexEntry block
        ) {
            return new Key(
                reference.objectId(),
                reference.objectSize(),
                reference.objectChecksum(),
                block
            );
        }
    }
}
