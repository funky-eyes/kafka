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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

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
                gate.destructiveChange(() -> {
                    // RecordBatch ranges are indivisible: an older batch
                    // overlapped by this replacement cannot be left readable.
                    if (floor != null && floor.getValue().lastOffset() >= record.firstOffset()) {
                        partitionLocations.remove(floor.getKey(), floor.getValue());
                    }
                    // Drop every overlapped successor before publishing the
                    // new record, not just an identical first-offset entry.
                    partitionLocations.subMap(record.firstOffset(), true, record.lastOffset(), true).clear();
                    partitionLocations.put(record.firstOffset(), location);
                });
            } else {
                // A pure tail append cannot invalidate a prior source LogStart observation.
                partitionLocations.put(record.firstOffset(), location);
            }
        }
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
