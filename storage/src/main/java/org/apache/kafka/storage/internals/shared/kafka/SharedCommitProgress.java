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
 * Non-blocking bridge from Kafka's native log offsets into the shared-storage upload plane.
 *
 * <p>This class performs no I/O and no waiting because {@code LogOffsetsListener} callbacks may execute while Kafka
 * log locks are held. Upload workers consume immutable snapshots asynchronously. Kafka remains the sole source of truth
 * for both the log-start offset and the exclusive high-watermark boundary.</p>
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
            current == null ? logStartOffset : current.highWatermark()
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
            highWatermark
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

    public record PartitionProgress(long logStartOffset, long highWatermark) {
        public PartitionProgress {
            if (logStartOffset < 0) {
                throw new IllegalArgumentException("logStartOffset must be non-negative");
            }
            if (highWatermark < 0) {
                throw new IllegalArgumentException("highWatermark must be non-negative");
            }
        }
    }
}
