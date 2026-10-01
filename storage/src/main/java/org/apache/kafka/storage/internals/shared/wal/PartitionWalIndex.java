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
    private final ConcurrentHashMap<WalPartitionKey, AtomicLong> mutationRevisions = new ConcurrentHashMap<>();

    public void apply(WalRecord record, WalAppendResult appendResult) {
        WalPartitionKey key = WalPartitionKey.of(record);
        if (record.type() == WalRecordType.TRUNCATE) {
            truncate(key, record.truncateOffset());
            return;
        }
        WalLocation location = new WalLocation(
            appendResult.offset(),
            appendResult.length(),
            record.leaderEpoch(),
            record.firstOffset(),
            record.lastOffset()
        );
        ConcurrentNavigableMap<Long, WalLocation> partitionLocations =
            locations.computeIfAbsent(key, ignored -> new ConcurrentSkipListMap<>());
        Map.Entry<Long, WalLocation> floor = partitionLocations.floorEntry(record.firstOffset());
        Map.Entry<Long, WalLocation> ceiling = partitionLocations.ceilingEntry(record.firstOffset());
        boolean destructiveMutation =
            (floor != null && floor.getValue().lastOffset() >= record.firstOffset()) ||
                ceiling != null;
        WalLocation previous = partitionLocations.put(record.firstOffset(), location);
        if (destructiveMutation || (previous != null && !previous.equals(location))) {
            bumpMutationRevision(key);
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
        // A TRUNCATE is a generation fence even when it currently removes no indexed entry.
        bumpMutationRevision(key);
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
        for (Map.Entry<WalPartitionKey, ConcurrentNavigableMap<Long, WalLocation>> partition : locations.entrySet()) {
            ConcurrentNavigableMap<Long, WalLocation> partitionLocations = partition.getValue();
            boolean changed = false;
            for (Map.Entry<Long, WalLocation> entry : partitionLocations.entrySet()) {
                WalLocation location = entry.getValue();
                if (location.walOffset() < walOffsetExclusive &&
                    partitionLocations.remove(entry.getKey(), location)) {
                    changed = true;
                }
            }
            if (changed) {
                bumpMutationRevision(partition.getKey());
            }
            if (partitionLocations.isEmpty()) {
                locations.remove(partition.getKey(), partitionLocations);
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

    /** Clears every indexed WAL address before replaying the surviving logical recovery window. */
    public void clear() {
        locations.keySet().forEach(this::bumpMutationRevision);
        locations.clear();
    }

    /**
     * Returns a generation that changes only when existing logical WAL index state is destructively replaced or
     * removed. Ordinary tail appends deliberately do not advance this revision so active producers cannot starve
     * bounded upload selection.
     */
    public long mutationRevision(WalPartitionKey key) {
        AtomicLong revision = mutationRevisions.get(key);
        return revision == null ? 0L : revision.get();
    }

    private void bumpMutationRevision(WalPartitionKey key) {
        mutationRevisions.computeIfAbsent(key, ignored -> new AtomicLong()).incrementAndGet();
    }

    public int partitionCount() {
        return locations.size();
    }
}
