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

/**
 * In-memory logical index from partition offsets to physical broker-wide WAL locations.
 * TRUNCATE records invalidate old physical entries without truncating the shared WAL file itself.
 */
public final class PartitionWalIndex {
    private final ConcurrentHashMap<WalPartitionKey, ConcurrentNavigableMap<Long, WalLocation>> locations =
        new ConcurrentHashMap<>();

    public void apply(WalRecord record, WalAppendResult appendResult) {
        WalPartitionKey key = WalPartitionKey.of(record);
        if (record.type() == WalRecordType.TRUNCATE) {
            truncate(key, record.truncateOffset());
            return;
        }
        WalLocation location = new WalLocation(
            appendResult.segmentId(),
            appendResult.position(),
            appendResult.length(),
            record.leaderEpoch(),
            record.firstOffset(),
            record.lastOffset()
        );
        locations.computeIfAbsent(key, ignored -> new ConcurrentSkipListMap<>())
            .put(record.firstOffset(), location);
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
     * Removes locations whose immutable logical WAL segment has already been physically reclaimed.
     *
     * <p>The compare-and-remove form is important because appends may update the same logical offset concurrently
     * after a Kafka truncate/reappend. A newly installed location is never removed merely because an older snapshot
     * observed that offset on a reclaimed segment.</p>
     */
    public void removeSegmentsThrough(long segmentId) {
        if (segmentId < 0) {
            return;
        }
        for (Map.Entry<WalPartitionKey, ConcurrentNavigableMap<Long, WalLocation>> partition : locations.entrySet()) {
            ConcurrentNavigableMap<Long, WalLocation> partitionLocations = partition.getValue();
            for (Map.Entry<Long, WalLocation> entry : partitionLocations.entrySet()) {
                WalLocation location = entry.getValue();
                if (location.segmentId() <= segmentId) {
                    partitionLocations.remove(entry.getKey(), location);
                }
            }
            if (partitionLocations.isEmpty()) {
                locations.remove(partition.getKey(), partitionLocations);
            }
        }
    }

    /** Clears every physical WAL location before replaying the surviving post-reclaim segments. */
    public void clear() {
        locations.clear();
    }

    public int partitionCount() {
        return locations.size();
    }
}
