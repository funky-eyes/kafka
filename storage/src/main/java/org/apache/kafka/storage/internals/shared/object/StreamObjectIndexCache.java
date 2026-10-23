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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Bounded LRU of immutable object-index loads. Failed loads are never retained.
 * Slow physical object-store calls execute outside the per-cache publication monitor.
 */
final class StreamObjectIndexCache {
    private final int maxEntries;
    private final LinkedHashMap<Key, CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>>> entries =
        new LinkedHashMap<>(16, 0.75f, true);

    StreamObjectIndexCache(int maxEntries) {
        if (maxEntries < 0) {
            throw new IllegalArgumentException("maxEntries must not be negative");
        }
        this.maxEntries = maxEntries;
    }

    CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> get(
        RemoteObjectIndex.RangeReference reference,
        StreamObjectIndexReader loader
    ) {
        Objects.requireNonNull(reference, "reference");
        Objects.requireNonNull(loader, "loader");
        if (maxEntries == 0 || !reference.hasObjectDescriptor()) {
            // Cache bypass must still honor the asynchronous failure contract,
            // otherwise synchronous transport errors skip COMMITTED fallback.
            return loadOnce(reference, loader);
        }

        Key key = Key.from(reference);
        CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> installed = new CompletableFuture<>();
        synchronized (this) {
            CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> existing = entries.get(key);
            if (existing != null) {
                return existing;
            }
            // Publish the coalescing Future while holding the short cache
            // lock, but never invoke a potentially blocking Range GET here.
            entries.put(key, installed);
            evictIfNeeded();
        }

        installed.whenComplete((ignored, error) -> {
            if (error != null) {
                synchronized (StreamObjectIndexCache.this) {
                    entries.remove(key, installed);
                }
            }
        });
        CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> loaded =
            loadOnce(reference, loader);
        loaded.whenComplete((snapshot, error) -> {
            if (error != null) {
                installed.completeExceptionally(error);
            } else if (snapshot == null) {
                installed.completeExceptionally(new NullPointerException("index loader returned null snapshot"));
            } else {
                installed.complete(snapshot);
            }
        });
        return installed;
    }

    private static CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> loadOnce(
        RemoteObjectIndex.RangeReference reference,
        StreamObjectIndexReader loader
    ) {
        try {
            return Objects.requireNonNull(loader.load(reference), "index loader returned null future");
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    synchronized int size() {
        return entries.size();
    }

    private void evictIfNeeded() {
        while (entries.size() > maxEntries) {
            Iterator<Map.Entry<Key, CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>>>> iterator =
                entries.entrySet().iterator();
            iterator.next();
            iterator.remove();
        }
    }

    private record Key(long objectId, long objectSize, long objectChecksum) {
        private static Key from(RemoteObjectIndex.RangeReference reference) {
            return new Key(reference.objectId(), reference.objectSize(), reference.objectChecksum());
        }
    }
}
