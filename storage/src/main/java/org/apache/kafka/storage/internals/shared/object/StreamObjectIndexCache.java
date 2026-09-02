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

/** Bounded LRU of immutable object-index loads. Failed loads are never retained. */
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
            return loader.load(reference);
        }

        Key key = Key.from(reference);
        CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> result;
        synchronized (this) {
            result = entries.get(key);
            if (result != null) {
                return result;
            }
            result = loader.load(reference);
            entries.put(key, result);
            evictIfNeeded();
        }

        CompletableFuture<Optional<StreamObjectIndexReader.IndexSnapshot>> installed = result;
        result.whenComplete((ignored, error) -> {
            if (error != null) {
                synchronized (StreamObjectIndexCache.this) {
                    entries.remove(key, installed);
                }
            }
        });
        return result;
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
