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

import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Non-blocking bridge from Kafka's native high-watermark updates into the shared-storage upload plane.
 *
 * <p>This class deliberately stores only the latest observed Kafka HW. It performs no I/O and no waiting because
 * {@code LogOffsetsListener} callbacks may execute while Kafka log locks are held. Upload workers consume the value
 * asynchronously and still validate every selected WAL batch against the current exclusive HW boundary.</p>
 */
public final class SharedCommitProgress {
    private final ConcurrentMap<SharedPartitionId, Long> highWatermarks = new ConcurrentHashMap<>();

    public void onHighWatermarkUpdated(SharedPartitionId partition, long highWatermark) {
        Objects.requireNonNull(partition, "partition");
        if (highWatermark < 0) {
            throw new IllegalArgumentException("highWatermark must be non-negative");
        }
        // Use assignment rather than max(): a newly initialized log may legitimately restore a lower HW after
        // leadership/recovery changes. Kafka remains the sole source of truth for the commit boundary.
        highWatermarks.put(partition, highWatermark);
    }

    public OptionalLong highWatermark(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        Long value = highWatermarks.get(partition);
        return value == null ? OptionalLong.empty() : OptionalLong.of(value);
    }

    public void remove(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        highWatermarks.remove(partition);
    }
}
