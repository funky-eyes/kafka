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
import org.apache.kafka.storage.internals.shared.wal.WalAppendResult;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;
import org.apache.kafka.storage.internals.shared.wal.WalRecordType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable inventory of user records exposed by the WAL backend's startup
 * replay after File GROUP_COMMIT recovery or Ring durable checkpoint recovery.
 *
 * <p>It counts physical replay events, NOT live Kafka offset coverage. The
 * physical logical address may contain gaps, and a TRUNCATE can invalidate
 * earlier DATA. Reclaimed WAL prefixes may be absent. These values are
 * diagnostic broker-local recovery provenance only: no current fsync horizon,
 * BrokerEpoch/Incarnation certificate, cross-broker completeness, or
 * retirement / COMMITTED-object deletion authority is implied.</p>
 */
public final class StartupWalReplayEvidence {
    public static final StartupWalReplayEvidence EMPTY = new StartupWalReplayEvidence(Map.of(), 0L, -1L);

    private final Map<SharedPartitionId, PartitionSummary> partitions;
    private final long replayedRecords;
    private final long highestWalEndExclusive;

    // Outside the storage implementation, recovery provenance cannot be
    // fabricated with a public record constructor. This remains a
    // diagnostic, not a trusted cross-broker certificate.
    private StartupWalReplayEvidence(
        Map<SharedPartitionId, PartitionSummary> partitions,
        long replayedRecords,
        long highestWalEndExclusive
    ) {
        this.partitions = Map.copyOf(Objects.requireNonNull(partitions, "partitions"));
        if (replayedRecords < 0L || (replayedRecords == 0L) != (highestWalEndExclusive == -1L)
            || highestWalEndExclusive < -1L) {
            throw new IllegalArgumentException("Invalid startup WAL replay inventory");
        }
        this.replayedRecords = replayedRecords;
        this.highestWalEndExclusive = highestWalEndExclusive;
    }

    public Map<SharedPartitionId, PartitionSummary> partitions() {
        return partitions;
    }

    public long replayedRecords() {
        return replayedRecords;
    }

    public long highestWalEndExclusive() {
        return highestWalEndExclusive;
    }

    public Optional<PartitionSummary> partition(SharedPartitionId partition) {
        return Optional.ofNullable(partitions.get(Objects.requireNonNull(partition, "partition")));
    }

    public record PartitionSummary(
        long replayedRecords,
        long dataRecords,
        long truncateRecords,
        long highestWalEndExclusive
    ) {
        public PartitionSummary {
            if (replayedRecords <= 0L || dataRecords < 0L || truncateRecords < 0L
                || dataRecords > replayedRecords || truncateRecords != replayedRecords - dataRecords
                || highestWalEndExclusive <= 0L) {
                throw new IllegalArgumentException("Invalid replayed partition summary");
            }
        }
    }

    static final class Builder {
        private final Map<SharedPartitionId, MutablePartition> partitions = new HashMap<>();
        private long count;
        private long previousWalEndExclusive = -1L;

        void accept(WalRecord record, WalAppendResult location) {
            Objects.requireNonNull(record, "record");
            Objects.requireNonNull(location, "location");
            if (record.type() != WalRecordType.DATA && record.type() != WalRecordType.TRUNCATE) {
                throw new IllegalArgumentException("Startup recovery cannot expose WAL commit markers");
            }
            long end = Math.addExact(location.offset(), location.length());
            if (location.offset() < previousWalEndExclusive) {
                throw new IllegalStateException("Backend replay regressed or overlapped logical WAL addresses");
            }
            SharedPartitionId key = new SharedPartitionId(
                record.topicIdHigh(), record.topicIdLow(), record.partition()
            );
            MutablePartition partition = partitions.computeIfAbsent(key, ignored -> new MutablePartition());
            partition.records = Math.addExact(partition.records, 1L);
            if (record.type() == WalRecordType.DATA) {
                partition.data = Math.addExact(partition.data, 1L);
            } else {
                partition.truncates = Math.addExact(partition.truncates, 1L);
            }
            partition.lastWalEnd = end;
            count = Math.addExact(count, 1L);
            previousWalEndExclusive = end;
        }

        StartupWalReplayEvidence snapshot() {
            if (count == 0L) {
                return EMPTY;
            }
            Map<SharedPartitionId, PartitionSummary> immutable = new HashMap<>();
            partitions.forEach((key, value) -> immutable.put(key,
                new PartitionSummary(value.records, value.data, value.truncates, value.lastWalEnd)
            ));
            return new StartupWalReplayEvidence(immutable, count, previousWalEndExclusive);
        }
    }

    private static final class MutablePartition {
        private long records;
        private long data;
        private long truncates;
        private long lastWalEnd;
    }
}
