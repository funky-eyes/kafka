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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

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
    // Multiple objects are independent after durable per-object CLAIM. Limit
    // concurrent S3 RPCs and avoid one serialized Future chain per physical ID.
    private static final int MAX_PARALLEL_OBJECTS = 8;
    private final Set<Long> inFlightObjects = ConcurrentHashMap.newKeySet();
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
        result = processBatches(result, alreadyClaimed.values(), false);
        return processBatches(result, candidates.values(), true);
    }

    /**
     * Retry durable PREPARED cleanup claims even if a clock has not yet
     * advanced through the grace interval. A past CLAIMED fence has already
     * won the COMMIT race, so the idempotent S3 DELETE still must be retried
     * after delayed in-flight PUTs; do not admit fresh PREPARED candidates.
     */
    CompletableFuture<Integer> cleanClaimedOnly() {
        Map<Long, ObjectMetadataStore.PreparedObject> alreadyClaimed = alreadyClaimedObjects();
        return processBatches(CompletableFuture.completedFuture(0), alreadyClaimed.values(), false);
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

    /**
     * At most eight objects per pass have an outstanding CLAIM or S3 DELETE.
     * Claims are independently fenced and async, then all successful claims
     * are sent in one provider-native deleteBatch call when supported.
     * A failed bulk response leaves EVERY CLAIM durable and retryable.
     */
    private CompletableFuture<Integer> processBatches(
        CompletableFuture<Integer> previous,
        Collection<ObjectMetadataStore.PreparedObject> objects,
        boolean requireClaim
    ) {
        List<ObjectMetadataStore.PreparedObject> candidates = new ArrayList<>(objects);
        for (int start = 0; start < candidates.size(); start += MAX_PARALLEL_OBJECTS) {
            int until = Math.min(start + MAX_PARALLEL_OBJECTS, candidates.size());
            List<ObjectMetadataStore.PreparedObject> batch = List.copyOf(candidates.subList(start, until));
            previous = previous.thenCompose(completed -> processBatch(batch, requireClaim)
                .thenApply(deleted -> completed + deleted));
        }
        return previous;
    }

    private CompletableFuture<Integer> processBatch(
        List<ObjectMetadataStore.PreparedObject> batch, boolean requireClaim
    ) {
        List<Long> admitted = new ArrayList<>(batch.size());
        List<CompletableFuture<Boolean>> authorized = new ArrayList<>(batch.size());
        for (ObjectMetadataStore.PreparedObject candidate : batch) {
            if (inFlightObjects.add(candidate.objectId())) {
                admitted.add(candidate.objectId());
                authorized.add(authorizeDeletion(candidate, requireClaim));
            }
        }
        CompletableFuture<?>[] claims = authorized.toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(claims)
            .thenCompose(ignored -> deleteAuthorizedBatch(admitted, authorized))
            .whenComplete((ignored, error) -> admitted.forEach(inFlightObjects::remove));
    }

    private CompletableFuture<Boolean> authorizeDeletion(
        ObjectMetadataStore.PreparedObject candidate, boolean requireClaim
    ) {
        if (!requireClaim) {
            // A persistent, replay-confirmed CLAIM is the S3 delete grant.
            return CompletableFuture.completedFuture(true);
        }
        try {
            return Objects.requireNonNull(
                metadataStore.claimCleanup(candidate.objectId(), candidate.createdTimeMs()),
                "Object metadata claim returned null");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    private CompletableFuture<Integer> deleteAuthorizedBatch(
        List<Long> admitted, List<CompletableFuture<Boolean>> authorized
    ) {
        List<Long> winners = new ArrayList<>(admitted.size());
        for (int index = 0; index < admitted.size(); index++) {
            // The allOf barrier has already completed every CLAIM.
            if (authorized.get(index).join()) {
                winners.add(admitted.get(index));
            }
        }
        if (winners.isEmpty()) {
            return CompletableFuture.completedFuture(0);
        }
        return objectStore.deleteBatch(winners).thenApply(ignored -> winners.size());
    }

}
