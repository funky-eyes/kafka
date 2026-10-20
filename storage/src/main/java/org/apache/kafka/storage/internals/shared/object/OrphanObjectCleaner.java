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
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Deletes physical objects that are no longer needed by the logical remote read view.
 *
 * <p>Physical deletion is never based on a PREPARED snapshot alone. The cleaner first obtains an authoritative cleanup
 * fence from {@link ObjectMetadataStore#claimCleanup(long, long)}. If a concurrent COMMIT is ordered first, the claim
 * loses and the object is left untouched. If the claim is ordered first, all delayed COMMIT attempts are fenced before
 * the physical delete starts.</p>
 *
 * <p>CLAIMED is intentionally kept durable after a successful PREPARED-object delete. A remote PUT that was already in
 * flight when a broker crashed may become visible after the first DELETE. Keeping the claim discoverable makes later
 * passes issue the idempotent DELETE again, so an arbitrarily late physical write cannot turn into a permanent orphan.</p>
 *
 * <p>With Kafka replication, each broker has its own {@link RemoteObjectIndex}.
 * Equivalent COMMITTED copies can be published in different orders, and a
 * locally unreferenced copy may still be actively read by another replica.
 * A local reference lookup never authorizes COMMITTED physical deletion.
 * That requires durable Controller-owned retirement and cross-replica reader
 * and upload fencing. PREPARED orphan cleanup remains separately fenced by
 * the authoritative metadata store.</p>
 */
public final class OrphanObjectCleaner {
    private final ObjectStore objectStore;
    private final ObjectMetadataStore metadataStore;
    private final ActiveObjectUploads activeUploads;

    public OrphanObjectCleaner(ObjectStore objectStore, ObjectMetadataStore metadataStore) {
        this(objectStore, metadataStore, new ActiveObjectUploads(), null);
    }

    public OrphanObjectCleaner(
        ObjectStore objectStore,
        ObjectMetadataStore metadataStore,
        ActiveObjectUploads activeUploads
    ) {
        this(objectStore, metadataStore, activeUploads, null);
    }

    public OrphanObjectCleaner(
        ObjectStore objectStore,
        ObjectMetadataStore metadataStore,
        ActiveObjectUploads activeUploads,
        RemoteObjectIndex remoteIndex
    ) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.activeUploads = Objects.requireNonNull(activeUploads, "activeUploads");
        // Kept for source/binary compatibility. A broker-local read index
        // cannot establish globally safe COMMITTED object retirement.
    }

    /**
     * Cleans PREPARED objects created at or before {@code cutoffCreatedTimeMs}
     * and retries previously claimed PREPARED cleanups. COMMITTED objects are
     * never physically deleted based on one broker's local reference index.
     * The returned count is the number of completed PREPARED physical deletes.
     */
    public CompletableFuture<Integer> clean(long cutoffCreatedTimeMs) {
        if (cutoffCreatedTimeMs < 0) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("cutoffCreatedTimeMs must be non-negative"));
        }

        Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimed = alreadyClaimedObjects();
        Map<Long, ObjectMetadataStore.PreparedObject> candidates =
            preparedCleanupCandidates(cutoffCreatedTimeMs, alreadyClaimed);

        CompletableFuture<Integer> result = CompletableFuture.completedFuture(0);
        result = deleteAlreadyClaimed(result, alreadyClaimed);
        result = claimAndDeleteCandidates(result, candidates);
        return result;
    }

    private Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimedObjects() {
        Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimed = new LinkedHashMap<>();
        for (ObjectMetadataStore.PreparedObject object : metadataStore.cleanupClaimedObjects()) {
            if (!activeUploads.contains(object.objectId())) {
                alreadyClaimed.put(object.objectId(), object);
            }
        }
        return alreadyClaimed;
    }

    private Map<Long, ObjectMetadataStore.PreparedObject> preparedCleanupCandidates(
        long cutoffCreatedTimeMs,
        Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimed
    ) {
        Map<Long, ObjectMetadataStore.PreparedObject> candidates = new LinkedHashMap<>();
        for (ObjectMetadataStore.PreparedObject object : metadataStore.preparedObjects()) {
            if (object.createdTimeMs() <= cutoffCreatedTimeMs
                && !activeUploads.contains(object.objectId())
                && !alreadyClaimed.containsKey(object.objectId())) {
                candidates.put(object.objectId(), object);
            }
        }
        return candidates;
    }

    private CompletableFuture<Integer> deleteAlreadyClaimed(
        CompletableFuture<Integer> result,
        Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimed
    ) {
        for (ObjectMetadataStore.PreparedObject claimed : alreadyClaimed.values()) {
            result = result.thenCompose(count -> deleteClaimed(claimed.objectId()).thenApply(ignored -> count + 1));
        }
        return result;
    }

    private CompletableFuture<Integer> claimAndDeleteCandidates(
        CompletableFuture<Integer> result,
        Map<Long, ObjectMetadataStore.PreparedObject> candidates
    ) {
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
                return deleteClaimed(candidate.objectId()).thenApply(ignored -> true);
            });
    }

    private CompletableFuture<Void> deleteClaimed(long objectId) {
        return objectStore.delete(objectId);
    }

}
