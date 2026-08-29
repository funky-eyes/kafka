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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Non-blocking bridge from Kafka's native log offsets and replica role into the shared-storage upload plane.
 *
 * <p>This class performs no I/O and no waiting because callbacks may execute while Kafka log or ReplicaManager locks
 * are held. Upload workers consume immutable snapshots asynchronously. Kafka remains the sole source of truth for the
 * log-start offset, exclusive high-watermark boundary and current local replica role.</p>
 */
public final class SharedCommitProgress {
    private final ConcurrentMap<SharedPartitionId, PartitionProgress> partitions = new ConcurrentHashMap<>();

    public void onLogLoaded(SharedPartitionId partition, long logStartOffset) {
        Objects.requireNonNull(partition, "partition");
        if (logStartOffset < 0) {
            throw new IllegalArgumentException("logStartOffset must be non-negative");
        }
        partitions.compute(partition, (ignored, current) -> new PartitionProgress(
            logStartOffset,
            current == null ? logStartOffset : current.highWatermark(),
            current == null ? ReplicaRole.UNKNOWN : current.role()
        ));
    }

    public void onHighWatermarkUpdated(SharedPartitionId partition, long highWatermark) {
        Objects.requireNonNull(partition, "partition");
        if (highWatermark < 0) {
            throw new IllegalArgumentException("highWatermark must be non-negative");
        }
        // Use assignment rather than max(): recovery or a leadership change may legitimately restore a lower HW.
        partitions.compute(partition, (ignored, current) -> new PartitionProgress(
            current == null ? 0L : current.logStartOffset(),
            highWatermark,
            current == null ? ReplicaRole.UNKNOWN : current.role()
        ));
    }

    public void onLeader(SharedPartitionId partition) {
        updateRole(partition, ReplicaRole.LEADER);
    }

    public void onFollower(SharedPartitionId partition) {
        updateRole(partition, ReplicaRole.FOLLOWER);
    }

    private void updateRole(SharedPartitionId partition, ReplicaRole role) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(role, "role");
        partitions.compute(partition, (ignored, current) -> new PartitionProgress(
            current == null ? 0L : current.logStartOffset(),
            current == null ? 0L : current.highWatermark(),
            role
        ));
    }

    public OptionalLong highWatermark(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        PartitionProgress progress = partitions.get(partition);
        return progress == null ? OptionalLong.empty() : OptionalLong.of(progress.highWatermark());
    }

    public Optional<PartitionProgress> partitionProgress(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        return Optional.ofNullable(partitions.get(partition));
    }

    public Map<SharedPartitionId, PartitionProgress> snapshot() {
        return Map.copyOf(partitions);
    }

    public void remove(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        partitions.remove(partition);
    }

    public enum ReplicaRole {
        UNKNOWN,
        LEADER,
        FOLLOWER
    }

    public record PartitionProgress(long logStartOffset, long highWatermark, ReplicaRole role) {
        public PartitionProgress {
            if (logStartOffset < 0) {
                throw new IllegalArgumentException("logStartOffset must be non-negative");
            }
            if (highWatermark < 0) {
                throw new IllegalArgumentException("highWatermark must be non-negative");
            }
            Objects.requireNonNull(role, "role");
        }

        public boolean isLeader() {
            return role == ReplicaRole.LEADER;
        }
    }
}
