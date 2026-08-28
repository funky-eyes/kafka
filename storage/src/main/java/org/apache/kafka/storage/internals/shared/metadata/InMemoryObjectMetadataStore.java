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
package org.apache.kafka.storage.internals.shared.metadata;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryObjectMetadataStore implements ObjectMetadataStore {
    private final ConcurrentHashMap<Long, Entry> entries = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> prepare(long objectId, long createdTimeMs) {
        if (objectId < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be non-negative"));
        }
        Entry existing = entries.putIfAbsent(objectId, new Entry(createdTimeMs, null));
        if (existing != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Object already prepared: " + objectId));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> commit(SharedObjectMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        try {
            Entry updated = entries.compute(metadata.objectId(), (id, existing) -> {
                if (existing == null) {
                    throw new IllegalStateException("Object was not prepared: " + id);
                }
                if (existing.metadata != null && !existing.metadata.equals(metadata)) {
                    throw new RemoteMetadataConflictException("Object commit changed immutable metadata: " + id);
                }
                return new Entry(existing.createdTimeMs, metadata);
            });
            if (updated == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                    "Unable to commit object " + metadata.objectId()));
            }
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> delete(long objectId) {
        entries.remove(objectId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public List<SharedObjectMetadata> committedObjects() {
        return entries.entrySet().stream()
            .filter(entry -> entry.getValue().metadata != null)
            .sorted(Comparator.comparingLong(java.util.Map.Entry::getKey))
            .map(entry -> entry.getValue().metadata)
            .toList();
    }

    public boolean isPrepared(long objectId) {
        return entries.containsKey(objectId);
    }

    public boolean isCommitted(long objectId) {
        Entry entry = entries.get(objectId);
        return entry != null && entry.metadata != null;
    }

    private record Entry(long createdTimeMs, SharedObjectMetadata metadata) {
    }
}
