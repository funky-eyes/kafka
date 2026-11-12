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
package org.apache.kafka.storage.internals.shared.wal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * In-memory logical index from Kafka partition offsets to broker-wide logical WAL addresses.
 * TRUNCATE records invalidate old entries without exposing or depending on the WAL backend's physical allocation.
 */
public final class PartitionWalIndex {
    private final ConcurrentHashMap<WalPartitionKey, ConcurrentNavigableMap<Long, WalLocation>> locations =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<WalPartitionKey, MutationGate> mutationRevisions = new ConcurrentHashMap<>();

    public void apply(WalRecord record, WalAppendResult appendResult) {
        // Internal commit markers belong to the physical WAL protocol, not
        // to user partitions. A replay callback must never index one as DATA.
        if (record.type() != WalRecordType.DATA && record.type() != WalRecordType.TRUNCATE) {
            throw new IllegalArgumentException("Unsupported record type in partition WAL index: " + record.type());
        }
        WalPartitionKey key = WalPartitionKey.of(record);
        // Cold replay does not pass through SharedStorageEngine's append
        // future: validate both DATA and header-only TRUNCATE before changing
        // any indexed range or its mutation revision.
        WalLocation location = new WalLocation(
            appendResult.offset(),
            appendResult.length(),
            record.payload().remaining(),
            record.leaderEpoch(),
            record.firstOffset(),
            record.lastOffset()
        );
        if (record.type() == WalRecordType.TRUNCATE) {
            truncate(key, record.truncateOffset());
            return;
        }
        MutationGate gate = gate(key);
        synchronized (gate) {
            ConcurrentNavigableMap<Long, WalLocation> partitionLocations =
                locations.computeIfAbsent(key, ignored -> new ConcurrentSkipListMap<>());
            Map.Entry<Long, WalLocation> floor = partitionLocations.floorEntry(record.firstOffset());
            Map.Entry<Long, WalLocation> ceiling = partitionLocations.ceilingEntry(record.firstOffset());
            boolean destructiveMutation =
                (floor != null && floor.getValue().lastOffset() >= record.firstOffset()) ||
                    ceiling != null;
            if (destructiveMutation) {
                // Replayed WAL records must be visited in durable logical order.
                // Refuse a stale overlapping record before evicting current DATA.
                requireNewerOverlap(partitionLocations, floor, location);
                gate.destructiveChange(() -> replaceOverlappingRanges(partitionLocations, location));
            } else {
                // A pure tail append cannot invalidate a prior source LogStart observation.
                partitionLocations.put(record.firstOffset(), location);
            }
        }
    }

    /**
     * Validate the complete durable DATA group against one partition-index
     * snapshot before publishing any member. All records already crossed the
     * backend WAL durability barrier; this only guards index publication.
     *
     * <p>Do not acquire the engine's WAL maintenance lock here: File/Ring WAL
     * futures can complete while the WAL writer still holds its I/O lock.</p>
     */
    public List<WalLocation> applyDataGroup(List<WalRecord> records, List<WalAppendResult> results) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(results, "results");
        if (records.isEmpty() || records.size() != results.size()) {
            throw new IllegalArgumentException("DATA WAL group has invalid cardinality");
        }
        WalPartitionKey key = WalPartitionKey.of(Objects.requireNonNull(records.get(0), "first record"));
        MutationGate gate = gate(key);
        synchronized (gate) {
            ConcurrentNavigableMap<Long, WalLocation> current = locations.get(key);
            ValidatedDataGroup validated = validateDataGroup(records, results, key, current);
            publishValidatedDataGroup(key, gate, validated);
            return List.copyOf(validated.locations());
        }
    }

    private static ValidatedDataGroup validateDataGroup(
        List<WalRecord> records,
        List<WalAppendResult> results,
        WalPartitionKey key,
        ConcurrentNavigableMap<Long, WalLocation> current
    ) {
        List<WalLocation> candidates = new ArrayList<>(records.size());
        long previousLastOffset = -1L;
        long previousWalEnd = -1L;
        long indexedLastOffset = current == null || current.isEmpty()
            ? -1L : current.lastEntry().getValue().lastOffset();
        boolean destructive = false;
        for (int i = 0; i < records.size(); i++) {
            WalLocation candidate = validatedDataMember(records.get(i), results.get(i), key);
            if (candidate.firstOffset() <= previousLastOffset || candidate.walOffset() < previousWalEnd) {
                throw new IllegalArgumentException("WAL group offsets overlap or move backwards");
            }
            previousLastOffset = candidate.lastOffset();
            previousWalEnd = Math.addExact(candidate.walOffset(), candidate.length());
            // Always inspect the current member, including after finding
            // an earlier destructive range. A late stale member fails the group.
            boolean replaces = requiresIndexMutation(current, indexedLastOffset, candidate);
            destructive |= replaces;
            candidates.add(candidate);
        }
        return new ValidatedDataGroup(candidates, destructive);
    }

    private static WalLocation validatedDataMember(WalRecord record, WalAppendResult result, WalPartitionKey key) {
        Objects.requireNonNull(record, "DATA record");
        Objects.requireNonNull(result, "WAL result");
        if (record.type() != WalRecordType.DATA || !key.equals(WalPartitionKey.of(record))) {
            throw new IllegalArgumentException("WAL group must contain DATA from one partition");
        }
        return new WalLocation(
            result.offset(), result.length(), record.payload().remaining(),
            record.leaderEpoch(), record.firstOffset(), record.lastOffset()
        );
    }

    private static boolean requiresIndexMutation(
        ConcurrentNavigableMap<Long, WalLocation> current,
        long indexedLastOffset,
        WalLocation candidate
    ) {
        // Avoid floor/ceiling searches for the overwhelmingly common pure
        // tail append, preserving the shared-storage producer hot path.
        if (current == null || candidate.firstOffset() > indexedLastOffset) {
            return false;
        }
        Map.Entry<Long, WalLocation> floor = current.floorEntry(candidate.firstOffset());
        Map.Entry<Long, WalLocation> ceiling = current.ceilingEntry(candidate.firstOffset());
        if ((floor == null || floor.getValue().lastOffset() < candidate.firstOffset()) && ceiling == null) {
            return false;
        }
        requireNewerOverlap(current, floor, candidate);
        return true;
    }

    private void publishValidatedDataGroup(WalPartitionKey key, MutationGate gate, ValidatedDataGroup validated) {
        ConcurrentNavigableMap<Long, WalLocation> partitionLocations =
            locations.computeIfAbsent(key, ignored -> new ConcurrentSkipListMap<>());
        List<WalLocation> candidates = validated.locations();
        if (validated.destructive()) {
            // One odd/even generation spans the entire replacement group.
            gate.destructiveChange(() -> candidates.forEach(
                location -> replaceOverlappingRanges(partitionLocations, location)));
        } else {
            // Tail publication does not invalidate earlier source observations.
            candidates.forEach(location -> partitionLocations.put(location.firstOffset(), location));
        }
    }

    private record ValidatedDataGroup(List<WalLocation> locations, boolean destructive) {
    }

    private static void replaceOverlappingRanges(
        ConcurrentNavigableMap<Long, WalLocation> indexed,
        WalLocation candidate
    ) {
        Map.Entry<Long, WalLocation> floor = indexed.floorEntry(candidate.firstOffset());
        if (floor != null && floor.getValue().lastOffset() >= candidate.firstOffset()) {
            indexed.remove(floor.getKey(), floor.getValue());
        }
        indexed.subMap(candidate.firstOffset(), true, candidate.lastOffset(), true).clear();
        indexed.put(candidate.firstOffset(), candidate);
    }

    private static void requireNewerOverlap(
        ConcurrentNavigableMap<Long, WalLocation> indexed,
        Map.Entry<Long, WalLocation> predecessor,
        WalLocation proposed
    ) {
        if (predecessor != null && predecessor.getValue().lastOffset() >= proposed.firstOffset()
            && predecessor.getValue().walOffset() >= proposed.walOffset()) {
            throw new IllegalStateException("Overlapping WAL replay regressed its logical address");
        }
        for (WalLocation previous : indexed.subMap(
            proposed.firstOffset(), true, proposed.lastOffset(), true
        ).values()) {
            if (previous.walOffset() >= proposed.walOffset()) {
                throw new IllegalStateException("Overlapping WAL replay regressed its logical address");
            }
        }
    }

    public Optional<WalLocation> find(WalPartitionKey key, long offset) {
        ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
        if (partitionLocations == null) {
            return Optional.empty();
        }
        Map.Entry<Long, WalLocation> floor = partitionLocations.floorEntry(offset);
        if (floor != null && floor.getValue().contains(offset)) {
            return Optional.of(floor.getValue());
        }
        Map.Entry<Long, WalLocation> ceiling = partitionLocations.ceilingEntry(offset);
        if (ceiling != null && ceiling.getValue().contains(offset)) {
            return Optional.of(ceiling.getValue());
        }
        return Optional.empty();
    }

    /**
     * Returns the first logical range that contains {@code offset}, or the next range after a gap.
     *
     * <p>This is the bounded-memory cursor primitive used by shared-object scheduling. Unlike {@link #ranges}, it does
     * not materialize the partition's complete WAL backlog.</p>
     */
    public Optional<WalLocation> rangeAtOrAfter(WalPartitionKey key, long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
        ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
        if (partitionLocations == null) {
            return Optional.empty();
        }
        Map.Entry<Long, WalLocation> floor = partitionLocations.floorEntry(offset);
        if (floor != null && floor.getValue().lastOffset() >= offset) {
            return Optional.of(floor.getValue());
        }
        Map.Entry<Long, WalLocation> ceiling = partitionLocations.ceilingEntry(offset);
        return ceiling == null ? Optional.empty() : Optional.of(ceiling.getValue());
    }

    /**
     * Returns the next logical range after the supplied Kafka batch start offset without copying later ranges.
     */
    public Optional<WalLocation> rangeAfter(WalPartitionKey key, long firstOffset) {
        if (firstOffset < 0) {
            throw new IllegalArgumentException("firstOffset must be non-negative");
        }
        ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
        if (partitionLocations == null) {
            return Optional.empty();
        }
        Map.Entry<Long, WalLocation> higher = partitionLocations.higherEntry(firstOffset);
        return higher == null ? Optional.empty() : Optional.of(higher.getValue());
    }

    public List<WalLocation> ranges(WalPartitionKey key) {
        ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
        if (partitionLocations == null) {
            return List.of();
        }
        return List.copyOf(new ArrayList<>(partitionLocations.values()));
    }

    public void truncate(WalPartitionKey key, long truncateOffset) {
        if (truncateOffset < 0) {
            throw new IllegalArgumentException("truncateOffset must be non-negative");
        }
        MutationGate gate = gate(key);
        synchronized (gate) {
            // A TRUNCATE fences readers even when no indexed range currently overlaps it.
            gate.destructiveChange(() -> {
                ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
                if (partitionLocations == null) {
                    return;
                }
                partitionLocations.tailMap(truncateOffset, true).clear();
                Map.Entry<Long, WalLocation> floor = partitionLocations.floorEntry(truncateOffset);
                if (floor != null && floor.getValue().lastOffset() >= truncateOffset) {
                    partitionLocations.remove(floor.getKey(), floor.getValue());
                }
                if (partitionLocations.isEmpty()) {
                    locations.remove(key, partitionLocations);
                }
            });
        }
    }

    /**
     * Removes entries whose logical WAL address is below the backend's exclusive reclamation watermark.
     *
     * <p>The compare-and-remove form protects a concurrently re-appended Kafka range: a newer location installed for
     * the same logical Kafka offset is retained unless its own WAL address is also below the watermark.</p>
     */
    public void removeBefore(long walOffsetExclusive) {
        if (walOffsetExclusive < 0) {
            return;
        }
        for (WalPartitionKey key : locations.keySet()) {
            MutationGate gate = gate(key);
            synchronized (gate) {
                ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
                if (partitionLocations == null) {
                    continue;
                }
                // Do not invalidate an unrelated partition whose WAL entries survive reclamation.
                boolean changed = partitionLocations.values().stream()
                    .anyMatch(location -> location.walOffset() < walOffsetExclusive);
                if (!changed) {
                    continue;
                }
                gate.destructiveChange(() -> {
                    for (Map.Entry<Long, WalLocation> entry : partitionLocations.entrySet()) {
                        WalLocation location = entry.getValue();
                        if (location.walOffset() < walOffsetExclusive) {
                            partitionLocations.remove(entry.getKey(), location);
                        }
                    }
                    if (partitionLocations.isEmpty()) {
                        locations.remove(key, partitionLocations);
                    }
                });
            }
        }
    }

    /** Backend-only bridge while the rotating-file WAL still reports an extent watermark. */
    void removeSegmentsThrough(long segmentId) {
        if (segmentId < 0) {
            return;
        }
        removeBefore(WalAppendResult.firstOffsetAfterExtent(segmentId));
    }

    /** Clears indexed WAL addresses before replaying the surviving recovery window. */
    public void clear() {
        for (WalPartitionKey key : locations.keySet()) {
            MutationGate gate = gate(key);
            synchronized (gate) {
                ConcurrentNavigableMap<Long, WalLocation> partitionLocations = locations.get(key);
                if (partitionLocations != null) {
                    gate.destructiveChange(() -> {
                        partitionLocations.clear();
                        locations.remove(key, partitionLocations);
                    });
                }
            }
        }
    }

    /**
     * Returns -1 while the partition's index is being destructively mutated.
     * A stable generation never grants WAL fsync, Controller CAS or COMMITTED GC authority.
     * Ordinary tail appends do not advance this revision.
     */
    public long mutationRevision(WalPartitionKey key) {
        MutationGate gate = mutationRevisions.get(key);
        return gate == null ? 0L : gate.observedRevision();
    }

    /**
     * Separately observes WAL admissions and completions, including tail appends
     * that deliberately leave the destructive mutation revision unchanged.
     * -1 means a write is in flight or the observation raced a transition.
     *
     * <p>This is a broker-local advisory fence, NOT fsync or KRaft authority.</p>
     */
    public long walAdmissionRevision(WalPartitionKey key) {
        Objects.requireNonNull(key, "key");
        MutationGate gate = mutationRevisions.get(key);
        return gate == null ? 0L : gate.observedAdmissionRevision();
    }

    /**
     * Tracks the WAL future AND its index-publication continuation. Completion
     * never grabs the WAL maintenance lock or the index's monitor; File/Ring WAL
     * may invoke this callback while its writer I/O lock is still held.
     */
    public <T> CompletableFuture<T> trackDurableWrite(
        WalPartitionKey key,
        Supplier<CompletableFuture<T>> operation
    ) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(operation, "operation");
        MutationGate gate = gate(key);
        gate.beginAdmission();
        try {
            return Objects.requireNonNull(operation.get(), "WAL write future")
                .whenComplete((ignored, error) -> gate.completeAdmission());
        } catch (RuntimeException | Error failure) {
            gate.completeAdmission();
            throw failure;
        }
    }

    private MutationGate gate(WalPartitionKey key) {
        return mutationRevisions.computeIfAbsent(key, ignored -> new MutationGate());
    }

    /**
     * Single-partition seqlock. Writers hold this object's monitor while deciding
     * whether an update is destructive. Read-only probes never acquire that monitor:
     * odd sequence values fail closed rather than blocking behind a WAL update.
     */
    static final class MutationGate {
        private final AtomicLong sequence = new AtomicLong();
        // One CAS represents BOTH the monotonic generation and the pending
        // count. Separate atomics leave a window in which a source probe can
        // observe zero pending writes with the previous generation.
        // 16 low bits permit up to 65,535 concurrent WAL writes/partition.
        private static final long PENDING_MASK = 0xffffL;
        private static final long GENERATION_STEP = 1L << 16;
        private final AtomicLong admissionState = new AtomicLong();

        void beginAdmission() {
            while (true) {
                long prior = admissionState.get();
                long pending = prior & PENDING_MASK;
                // Reserve capacity to complete every accepted write without
                // wrapping the generation. Exhaustion fails closed.
                long reserved = (pending + 2L) * GENERATION_STEP + 1L;
                if (prior < 0L || pending == PENDING_MASK || prior > Long.MAX_VALUE - reserved) {
                    throw new IllegalStateException("Local WAL admission generation capacity exhausted");
                }
                if (admissionState.compareAndSet(prior, prior + GENERATION_STEP + 1L)) {
                    return;
                }
            }
        }

        void completeAdmission() {
            while (true) {
                long prior = admissionState.get();
                if (prior < 0L || (prior & PENDING_MASK) == 0L) {
                    throw new IllegalStateException("Local WAL admission completed without an owner");
                }
                if (admissionState.compareAndSet(prior, prior + GENERATION_STEP - 1L)) {
                    return;
                }
            }
        }

        long observedAdmissionRevision() {
            long snapshot = admissionState.get();
            return snapshot < 0L || (snapshot & PENDING_MASK) != 0L
                ? -1L : snapshot >>> 16;
        }

        long observedRevision() {
            long observed = sequence.get();
            return observed < 0L || (observed & 1L) != 0L ? -1L : observed >>> 1;
        }

        synchronized void destructiveChange(Runnable change) {
            sequence.incrementAndGet();
            try {
                change.run();
            } finally {
                sequence.incrementAndGet();
            }
        }
    }

    public int partitionCount() {
        return locations.size();
    }
}
