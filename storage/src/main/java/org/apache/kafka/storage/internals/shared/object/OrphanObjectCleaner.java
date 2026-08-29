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

import org.apache.kafka.storage.internals.shared.metadata.ObjectMetadataStore;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Deletes physical objects that were PUT but never reached authoritative COMMITTED metadata.
 *
 * <p>Physical deletion is never based on a PREPARED snapshot alone. The cleaner first obtains an authoritative cleanup
 * fence from {@link ObjectMetadataStore#claimCleanup(long, long)}. If a concurrent COMMIT is ordered first, the claim
 * loses and the object is left untouched. If the claim is ordered first, all delayed COMMIT attempts are fenced before
 * the physical delete starts.</p>
 *
 * <p>A failed physical delete leaves the claim in place and is retried on a later pass. A crash or metadata failure
 * after a successful physical delete but before {@code completeCleanup} is also safe: object deletion is idempotent and
 * the durable CLAIMED state remains discoverable for retry.</p>
 */
public final class OrphanObjectCleaner {
    private final ObjectStore objectStore;
    private final ObjectMetadataStore metadataStore;

    public OrphanObjectCleaner(ObjectStore objectStore, ObjectMetadataStore metadataStore) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
    }

    /**
     * Cleans PREPARED objects created at or before {@code cutoffCreatedTimeMs}, plus all previously claimed cleanups.
     * The returned count is the number of physical delete/finalize sequences completed by this pass.
     */
    public CompletableFuture<Integer> clean(long cutoffCreatedTimeMs) {
        if (cutoffCreatedTimeMs < 0) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("cutoffCreatedTimeMs must be non-negative"));
        }

        Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimed = new LinkedHashMap<>();
        for (ObjectMetadataStore.PreparedObject object : metadataStore.cleanupClaimedObjects()) {
            alreadyClaimed.put(object.objectId(), object);
        }

        Map<Long, ObjectMetadataStore.PreparedObject> candidates = new LinkedHashMap<>();
        for (ObjectMetadataStore.PreparedObject object : metadataStore.preparedObjects()) {
            if (object.createdTimeMs() <= cutoffCreatedTimeMs && !alreadyClaimed.containsKey(object.objectId())) {
                candidates.put(object.objectId(), object);
            }
        }

        CompletableFuture<Integer> result = CompletableFuture.completedFuture(0);
        for (ObjectMetadataStore.PreparedObject claimed : alreadyClaimed.values()) {
            result = result.thenCompose(count -> deleteAndFinalize(claimed.objectId()).thenApply(ignored -> count + 1));
        }
        for (ObjectMetadataStore.PreparedObject candidate : candidates.values()) {
            result = result.thenCompose(count -> claimAndDelete(candidate).thenApply(deleted -> count + (deleted ? 1 : 0)));
        }
        return result;
    }

    private CompletableFuture<Boolean> claimAndDelete(ObjectMetadataStore.PreparedObject candidate) {
        return metadataStore.claimCleanup(candidate.objectId(), candidate.createdTimeMs())
            .thenCompose(claimed -> {
                if (!claimed) {
                    return CompletableFuture.completedFuture(false);
                }
                return deleteAndFinalize(candidate.objectId()).thenApply(ignored -> true);
            });
    }

    private CompletableFuture<Void> deleteAndFinalize(long objectId) {
        return objectStore.delete(objectId)
            .thenCompose(ignored -> metadataStore.completeCleanup(objectId));
    }
}
