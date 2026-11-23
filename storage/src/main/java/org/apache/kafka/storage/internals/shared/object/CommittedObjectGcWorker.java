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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded, fail-closed executor for physical deletion of retired COMMITTED objects.
 *
 * <p>NOT a production entry point. No real KRaft-backed {@link DeleteAuthority}
 * exists yet and this package-private worker is deliberately not wired into
 * the broker or any scheduled task. Neither a local metadata image nor a
 * process-local read fence can mint a physical deletion grant.</p>
 *
 * <p>Only a durable Controller DELETE_READY transition, after fencing every
 * reference and every Broker incarnation, can supply a candidate to the
 * future implementation of DeleteAuthority. Every sweep revalidates its
 * immutable object descriptor and retirement generation before DELETE.
 * DELETE and its KRaft completion record deliberately remain separate:
 * when either operation fails or has an uncertain outcome, the authoritative
 * work item must stay retryable. Retrying an idempotent remote DELETE is safe,
 * but only if old uploads can never recreate an object after the terminal
 * retirement barrier.</p>
 */
final class CommittedObjectGcWorker {
    static final int MAX_DELETE_BATCH = 64;

    /**
     * A production implementation must obtain its candidates exclusively from
     * committed KRaft DELETE_READY records, not from caller-supplied IDs or a
     * broker-local reference scan. The permit remains irrevocable after ready.
     */
    interface DeleteAuthority {
        CompletableFuture<List<DeleteReady>> readyDeletes(int maxBatch);

        /** Validate immutable descriptor, generation, ownership and committed KRaft grant again. */
        CompletableFuture<Boolean> stillDeleteReady(DeleteReady candidate);

        /**
         * Verify an immutable bounded group. A production Controller adapter
         * may override this with one KRaft snapshot / RPC. The default
         * reuses independent per-object checks concurrently, with a maximum
         * fan-out of MAX_DELETE_BATCH. Any failed Future aborts ALL deletes.
         */
        default CompletableFuture<List<DeleteReady>> stillDeleteReadyBatch(List<DeleteReady> candidates) {
            Objects.requireNonNull(candidates, "candidates");
            if (candidates.size() > MAX_DELETE_BATCH) {
                return CompletableFuture.failedFuture(new IllegalArgumentException(
                    "Too many KRaft object retirement candidates for one grant check"));
            }
            List<CompletableFuture<Boolean>> checks = new ArrayList<>(candidates.size());
            for (DeleteReady candidate : candidates) {
                checks.add(Objects.requireNonNull(stillDeleteReady(candidate), "stillDeleteReady future"));
            }
            return CompletableFuture.allOf(checks.toArray(CompletableFuture[]::new))
                .thenApply(ignored -> {
                    List<DeleteReady> approved = new ArrayList<>();
                    for (int index = 0; index < candidates.size(); index++) {
                        if (Boolean.TRUE.equals(checks.get(index).join())) {
                            approved.add(candidates.get(index));
                        }
                    }
                    return List.copyOf(approved);
                });
        }

        /** Complete only after the DELETE_RECORDED KRaft transition has committed. */
        CompletableFuture<Void> recordDeleted(DeleteReady candidate);

        /**
         * Durably acknowledge a bounded set only AFTER their physical DELETE
         * batch succeeds. A Controller adapter may override this method with
         * one atomic KRaft write; until then, preserve the original serial
         * per-object completion and its retry semantics.
         *
         * <p>Failure or an uncertain commit must leave unacknowledged objects
         * discoverable through durable DELETE_READY on retry. The default may
         * have committed an earlier prefix; a future adapter must revalidate
         * each candidate against current KRaft state before retrying.</p>
         */
        default CompletableFuture<Void> recordDeletedBatch(List<DeleteReady> candidates) {
            List<DeleteReady> requested = List.copyOf(Objects.requireNonNull(candidates, "candidates"));
            if (requested.size() > MAX_DELETE_BATCH) {
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unbounded KRaft physical delete completion batch"));
            }
            CompletableFuture<Void> recorded = CompletableFuture.completedFuture(null);
            for (DeleteReady candidate : requested) {
                recorded = recorded.thenCompose(ignored -> Objects.requireNonNull(
                    recordDeleted(candidate), "recordDeleted future"));
            }
            return recorded;
        }
    }

    record DeleteReady(SharedObjectMetadata descriptor, long retirementGeneration, long readyRecordOffset) {
        DeleteReady {
            Objects.requireNonNull(descriptor, "descriptor");
            if (retirementGeneration <= 0L || readyRecordOffset < 0L) {
                throw new IllegalArgumentException("Physical delete requires a committed retirement generation");
            }
        }

        long objectId() {
            return descriptor.objectId();
        }
    }

    record SweepStats(int observed, int deferred, int deleted) {
        SweepStats {
            if (observed < 0 || deferred < 0 || deleted < 0 || deleted + deferred > observed) {
                throw new IllegalArgumentException("Invalid COMMITTED object GC sweep counts");
            }
        }
    }

    private final ObjectStore objects;
    private final DeleteAuthority authority;
    private final LocalCommittedObjectIoFence localFence;
    private final AtomicBoolean sweeping = new AtomicBoolean();

    CommittedObjectGcWorker(
        ObjectStore objects,
        DeleteAuthority authority,
        LocalCommittedObjectIoFence localFence
    ) {
        this.objects = Objects.requireNonNull(objects, "objects");
        this.authority = Objects.requireNonNull(authority, "authority");
        this.localFence = Objects.requireNonNull(localFence, "localFence");
    }

    /** One bounded sweep. Caller owns scheduling and backoff, not this worker. */
    CompletableFuture<SweepStats> sweep() {
        if (!sweeping.compareAndSet(false, true)) {
            return CompletableFuture.failedFuture(new IllegalStateException("COMMITTED GC sweep already in progress"));
        }
        CompletableFuture<SweepStats> pending;
        try {
            pending = Objects.requireNonNull(authority.readyDeletes(MAX_DELETE_BATCH), "readyDeletes future")
                .thenCompose(this::deleteCandidates);
        } catch (RuntimeException e) {
            sweeping.set(false);
            return CompletableFuture.failedFuture(e);
        }
        // Do not release the one-sweep fence on cancellation of the caller's
        // dependent future: the actual remote DELETE may still be in flight.
        pending.whenComplete((ignored, error) -> sweeping.set(false));
        return pending.thenApply(stats -> stats);
    }

    /**
     * KRaft grants remain per-object and are revalidated independently.
     * Only already-fenced, still-authorized objects enter a bulk S3 request.
     * The KRaft completion records remain durable/retryable; a future
     * Controller adapter can persist their completion as one atomic batch.
     */
    private CompletableFuture<SweepStats> deleteCandidates(List<DeleteReady> ready) {
        verifyCandidateBatch(ready);
        List<DeleteReady> locallyDrained = ready.stream()
            // A local fence is necessary but NEVER a cross-Broker GC grant.
            .filter(candidate -> localFence.locallyDrainedAfterFence(candidate.objectId()))
            .toList();
        return Objects.requireNonNull(
            authority.stillDeleteReadyBatch(locallyDrained), "stillDeleteReadyBatch future"
        ).thenCompose(approved -> {
            // A buggy or stale Controller adapter must not inject a different
            // descriptor, generation or object that was not locally drained.
            verifyApprovedSubset(locallyDrained, approved);
            return deleteApprovedBatch(ready.size(), approved);
        });
    }

    static void verifyApprovedSubset(List<DeleteReady> candidates, List<DeleteReady> approved) {
        Objects.requireNonNull(approved, "approved");
        Set<DeleteReady> eligible = new HashSet<>(candidates);
        Set<Long> unique = new HashSet<>();
        for (DeleteReady candidate : approved) {
            if (candidate == null || !eligible.contains(candidate) || !unique.add(candidate.objectId())) {
                throw new IllegalStateException("KRaft authority returned an unverified GC candidate");
            }
        }
    }

    private CompletableFuture<SweepStats> deleteApprovedBatch(int observed, List<DeleteReady> approved) {
        if (approved.isEmpty()) {
            return CompletableFuture.completedFuture(new SweepStats(observed, observed, 0));
        }
        List<Long> ids = approved.stream().map(DeleteReady::objectId).toList();
        return Objects.requireNonNull(objects.deleteBatch(ids), "ObjectStore DELETE batch future")
            .thenCompose(ignored -> Objects.requireNonNull(
                authority.recordDeletedBatch(approved), "recordDeletedBatch future"
            ).thenApply(done -> new SweepStats(
                observed, observed - approved.size(), approved.size())));
    }

    private static void verifyCandidateBatch(List<DeleteReady> ready) {
        Objects.requireNonNull(ready, "ready");
        if (ready.size() > MAX_DELETE_BATCH) {
            throw new IllegalStateException("KRaft deletion authority exceeded bounded batch capacity");
        }
        Set<Long> unique = new HashSet<>();
        for (DeleteReady candidate : ready) {
            Objects.requireNonNull(candidate, "ready candidate");
            if (!unique.add(candidate.objectId())) {
                throw new IllegalStateException("Conflicting or duplicate object deletion candidates");
            }
        }
    }


}
