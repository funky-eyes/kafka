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
package org.apache.kafka.storage.internals.shared;

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.WalLocation;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Broker-local, post-start observations of successfully completed SharedWal
 * durability futures and their completed in-memory index publication.
 *
 * <p>The greatest WAL byte end is a maximum, NOT a contiguous recovered
 * prefix. This is not replay evidence after process failure and cannot
 * establish Kafka source/replica durability, current BrokerRegistration,
 * Controller KRaft authorization or safe COMMITTED physical deletion.</p>
 */
public final class LocalWalDurabilityObservation {
    private final ConcurrentHashMap<SharedPartitionId, AtomicReference<Snapshot>> partitions =
        new ConcurrentHashMap<>();

    /** No external caller can construct a durable-WAL observation. */
    LocalWalDurabilityObservation() {
    }

    public Snapshot snapshot(SharedPartitionId partition) {
        AtomicReference<Snapshot> ref = partitions.get(Objects.requireNonNull(partition, "partition"));
        return ref == null ? Snapshot.EMPTY : ref.get();
    }

    void dataCompleted(SharedPartitionId partition, WalLocation location) {
        update(partition, Objects.requireNonNull(location, "location"), 1, 0, false);
    }

    void dataGroupCompleted(SharedPartitionId partition, List<WalLocation> locations) {
        Objects.requireNonNull(locations, "locations");
        if (locations.isEmpty()) {
            throw new IllegalArgumentException("Successful WAL group cannot be empty");
        }
        long end = -1L;
        for (WalLocation location : locations) {
            end = Math.max(end, walEnd(Objects.requireNonNull(location, "WAL group location")));
        }
        update(partition, end, locations.size(), 1, false);
    }

    void truncateCompleted(SharedPartitionId partition, WalLocation location) {
        update(partition, Objects.requireNonNull(location, "location"), 0, 0, true);
    }

    private void update(SharedPartitionId partition, WalLocation location, int batches, int groups, boolean truncated) {
        update(partition, walEnd(location), batches, groups, truncated);
    }

    private static long walEnd(WalLocation location) {
        // Validated by WalLocation, including Long.MAX_VALUE address bounds.
        return location.walOffset() + location.length();
    }

    private void update(SharedPartitionId partition, long walEnd, int batches, int groups, boolean truncated) {
        AtomicReference<Snapshot> ref = partitions.computeIfAbsent(
            Objects.requireNonNull(partition, "partition"),
            ignored -> new AtomicReference<>(Snapshot.EMPTY)
        );
        while (true) {
            Snapshot old = ref.get();
            Snapshot next = new Snapshot(
                saturatingAdd(old.dataBatches(), batches),
                saturatingAdd(old.dataGroups(), groups),
                saturatingAdd(old.truncates(), truncated ? 1 : 0),
                Math.max(old.highestSuccessfulWalEndExclusive(), walEnd),
                truncated ? Math.max(old.highestTruncateWalEndExclusive(), walEnd)
                    : old.highestTruncateWalEndExclusive()
            );
            if (ref.compareAndSet(old, next)) {
                return;
            }
        }
    }

    private static long saturatingAdd(long old, long delta) {
        return old > Long.MAX_VALUE - delta ? Long.MAX_VALUE : old + delta;
    }

    /**
     * One immutable, atomically published diagnostic observation. Counts
     * include historical writes later truncated or reclaimed; they do not
     * imply current Kafka DATA coverage or a durable replay frontier.
     */
    public static final class Snapshot {
        public static final Snapshot EMPTY = new Snapshot(0L, 0L, 0L, -1L, -1L);

        private final long dataBatches;
        private final long dataGroups;
        private final long truncates;
        private final long highestSuccessfulWalEndExclusive;
        private final long highestTruncateWalEndExclusive;

        private Snapshot(long dataBatches, long dataGroups, long truncates,
                         long highestSuccessfulWalEndExclusive, long highestTruncateWalEndExclusive) {
            this.dataBatches = dataBatches;
            this.dataGroups = dataGroups;
            this.truncates = truncates;
            this.highestSuccessfulWalEndExclusive = highestSuccessfulWalEndExclusive;
            this.highestTruncateWalEndExclusive = highestTruncateWalEndExclusive;
        }

        public long dataBatches() {
            return dataBatches;
        }

        public long dataGroups() {
            return dataGroups;
        }

        public long truncates() {
            return truncates;
        }

        public long highestSuccessfulWalEndExclusive() {
            return highestSuccessfulWalEndExclusive;
        }

        public long highestTruncateWalEndExclusive() {
            return highestTruncateWalEndExclusive;
        }
    }
}
