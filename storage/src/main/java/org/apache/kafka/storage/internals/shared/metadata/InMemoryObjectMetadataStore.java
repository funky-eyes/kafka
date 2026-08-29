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
import java.util.concurrent.atomic.AtomicBoolean;

public final class InMemoryObjectMetadataStore implements ObjectMetadataStore {
    private final ConcurrentHashMap<Long, Entry> entries = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<Void> prepare(long objectId, long createdTimeMs) {
        if (objectId <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be positive"));
        }
        if (createdTimeMs < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("createdTimeMs must be non-negative"));
        }
        Entry existing = entries.putIfAbsent(objectId, Entry.prepared(createdTimeMs));
        if (existing != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Object already exists: " + objectId));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> commit(SharedObjectMetadata metadata) {
        Objects.requireNonNull(metadata, "metadata");
        try {
            entries.compute(metadata.objectId(), (id, existing) -> {
                if (existing == null) {
                    throw new IllegalStateException("Object was not prepared: " + id);
                }
                if (existing.state == EntryState.CLEANUP_CLAIMED || existing.state == EntryState.DELETED) {
                    throw new IllegalStateException("Object is fenced from commit by orphan cleanup: " + id);
                }
                if (existing.state == EntryState.COMMITTED) {
                    if (!existing.metadata.equals(metadata)) {
                        throw new RemoteMetadataConflictException("Object commit changed immutable metadata: " + id);
                    }
                    return existing;
                }
                return Entry.committed(existing.createdTimeMs, metadata);
            });
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
    public CompletableFuture<Boolean> claimCleanup(long objectId, long expectedCreatedTimeMs) {
        if (objectId <= 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("objectId must be positive"));
        }
        if (expectedCreatedTimeMs < 0) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("expectedCreatedTimeMs must be non-negative"));
        }
        AtomicBoolean claimed = new AtomicBoolean(false);
        entries.computeIfPresent(objectId, (id, existing) -> {
            if (existing.state == EntryState.PREPARED && existing.createdTimeMs == expectedCreatedTimeMs) {
                claimed.set(true);
                return Entry.cleanupClaimed(existing.createdTimeMs);
            }
            return existing;
        });
        return CompletableFuture.completedFuture(claimed.get());
    }

    @Override
    public CompletableFuture<Void> completeCleanup(long objectId) {
        try {
            entries.compute(objectId, (id, existing) -> {
                if (existing == null) {
                    throw new IllegalStateException("Object cleanup was not claimed: " + id);
                }
                if (existing.state == EntryState.DELETED) {
                    return existing;
                }
                if (existing.state != EntryState.CLEANUP_CLAIMED) {
                    throw new IllegalStateException("Object cleanup is not claimed: " + id);
                }
                return Entry.deleted(existing.createdTimeMs);
            });
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public List<SharedObjectMetadata> committedObjects() {
        return entries.entrySet().stream()
            .filter(entry -> entry.getValue().state == EntryState.COMMITTED)
            .sorted(Comparator.comparingLong(java.util.Map.Entry::getKey))
            .map(entry -> entry.getValue().metadata)
            .toList();
    }

    @Override
    public List<PreparedObject> preparedObjects() {
        return objectsInState(EntryState.PREPARED);
    }

    @Override
    public List<PreparedObject> cleanupClaimedObjects() {
        return objectsInState(EntryState.CLEANUP_CLAIMED);
    }

    public boolean isPrepared(long objectId) {
        Entry entry = entries.get(objectId);
        return entry != null && entry.state == EntryState.PREPARED;
    }

    public boolean isCommitted(long objectId) {
        Entry entry = entries.get(objectId);
        return entry != null && entry.state == EntryState.COMMITTED;
    }

    public boolean isCleanupClaimed(long objectId) {
        Entry entry = entries.get(objectId);
        return entry != null && entry.state == EntryState.CLEANUP_CLAIMED;
    }

    public boolean isDeleted(long objectId) {
        Entry entry = entries.get(objectId);
        return entry != null && entry.state == EntryState.DELETED;
    }

    private List<PreparedObject> objectsInState(EntryState state) {
        return entries.entrySet().stream()
            .filter(entry -> entry.getValue().state == state)
            .sorted(Comparator.comparingLong(java.util.Map.Entry::getKey))
            .map(entry -> new PreparedObject(entry.getKey(), entry.getValue().createdTimeMs))
            .toList();
    }

    private enum EntryState {
        PREPARED,
        COMMITTED,
        CLEANUP_CLAIMED,
        DELETED
    }

    private record Entry(long createdTimeMs, SharedObjectMetadata metadata, EntryState state) {
        private static Entry prepared(long createdTimeMs) {
            return new Entry(createdTimeMs, null, EntryState.PREPARED);
        }

        private static Entry committed(long createdTimeMs, SharedObjectMetadata metadata) {
            return new Entry(createdTimeMs, Objects.requireNonNull(metadata, "metadata"), EntryState.COMMITTED);
        }

        private static Entry cleanupClaimed(long createdTimeMs) {
            return new Entry(createdTimeMs, null, EntryState.CLEANUP_CLAIMED);
        }

        private static Entry deleted(long createdTimeMs) {
            return new Entry(createdTimeMs, null, EntryState.DELETED);
        }
    }
}
