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

        /** Complete only after the DELETE_RECORDED KRaft transition has committed. */
        CompletableFuture<Void> recordDeleted(DeleteReady candidate);
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
     * The KRaft completion records remain individually durable/retryable.
     */
    private CompletableFuture<SweepStats> deleteCandidates(List<DeleteReady> ready) {
        verifyCandidateBatch(ready);
        CompletableFuture<List<DeleteReady>> approved = CompletableFuture.completedFuture(new ArrayList<>());
        for (DeleteReady candidate : ready) {
            // A local fence is necessary, never sufficient cross-Broker proof.
            if (!localFence.locallyDrainedAfterFence(candidate.objectId())) {
                continue;
            }
            approved = approved.thenCompose(winners -> Objects.requireNonNull(
                authority.stillDeleteReady(candidate), "stillDeleteReady future"
            ).thenApply(permitted -> {
                if (Boolean.TRUE.equals(permitted)) {
                    winners.add(candidate);
                }
                return winners;
            }));
        }
        return approved.thenCompose(winners -> deleteApprovedBatch(ready.size(), winners));
    }

    private CompletableFuture<SweepStats> deleteApprovedBatch(int observed, List<DeleteReady> approved) {
        if (approved.isEmpty()) {
            return CompletableFuture.completedFuture(new SweepStats(observed, observed, 0));
        }
        List<Long> ids = approved.stream().map(DeleteReady::objectId).toList();
        return Objects.requireNonNull(objects.deleteBatch(ids), "ObjectStore DELETE batch future")
            .thenCompose(ignored -> {
                CompletableFuture<Void> recorded = CompletableFuture.completedFuture(null);
                for (DeleteReady candidate : approved) {
                    recorded = recorded.thenCompose(done -> Objects.requireNonNull(
                        authority.recordDeleted(candidate), "recordDeleted future"));
                }
                return recorded.thenApply(done -> new SweepStats(
                    observed, observed - approved.size(), approved.size()));
            });
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
