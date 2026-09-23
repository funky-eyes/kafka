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

/** Point-in-time cumulative diagnostics for WAL durability batching. */
public record WalDurabilityStats(
    long durabilityBatchCount,
    long durableAppendGroupCount,
    long durableBytes,
    long durabilityBarrierNanos,
    long durabilityDataForceNanos,
    long durabilityCheckpointForceNanos,
    long maxDurabilityBarrierNanos,
    long maxDurabilityDataForceNanos,
    long maxDurabilityCheckpointForceNanos,
    long maxGroupsPerDurabilityBatch,
    long singletonCoalesceWaitCount,
    long singletonCoalesceHitCount,
    long singletonCoalesceWaitNanos,
    long appendInterArrivalCount,
    long appendInterArrivalNanos,
    long appendInterArrivalLe100MicrosCount,
    long appendInterArrivalLe250MicrosCount,
    long appendInterArrivalLe500MicrosCount,
    long appendInterArrivalLe1000MicrosCount
) {
    public static final WalDurabilityStats EMPTY = new WalDurabilityStats(0L, 0L, 0L, 0L, 0L);

    public WalDurabilityStats(
        long durabilityBatchCount,
        long durableAppendGroupCount,
        long durableBytes,
        long durabilityBarrierNanos,
        long maxGroupsPerDurabilityBatch
    ) {
        this(
            durabilityBatchCount,
            durableAppendGroupCount,
            durableBytes,
            durabilityBarrierNanos,
            0L,
            0L,
            0L,
            0L,
            0L,
            maxGroupsPerDurabilityBatch,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L
        );
    }

    public WalDurabilityStats {
        requireNonNegative("durabilityBatchCount", durabilityBatchCount);
        requireNonNegative("durableAppendGroupCount", durableAppendGroupCount);
        requireNonNegative("durableBytes", durableBytes);
        requireNonNegative("durabilityBarrierNanos", durabilityBarrierNanos);
        requireNonNegative("durabilityDataForceNanos", durabilityDataForceNanos);
        requireNonNegative("durabilityCheckpointForceNanos", durabilityCheckpointForceNanos);
        requireNonNegative("maxDurabilityBarrierNanos", maxDurabilityBarrierNanos);
        requireNonNegative("maxDurabilityDataForceNanos", maxDurabilityDataForceNanos);
        requireNonNegative("maxDurabilityCheckpointForceNanos", maxDurabilityCheckpointForceNanos);
        requireNonNegative("maxGroupsPerDurabilityBatch", maxGroupsPerDurabilityBatch);
        requireAtMost(
            "maxDurabilityDataForceNanos",
            maxDurabilityDataForceNanos,
            "maxDurabilityBarrierNanos",
            maxDurabilityBarrierNanos
        );
        requireAtMost(
            "maxDurabilityCheckpointForceNanos",
            maxDurabilityCheckpointForceNanos,
            "maxDurabilityBarrierNanos",
            maxDurabilityBarrierNanos
        );
        requireNonNegative("singletonCoalesceWaitCount", singletonCoalesceWaitCount);
        requireNonNegative("singletonCoalesceHitCount", singletonCoalesceHitCount);
        requireNonNegative("singletonCoalesceWaitNanos", singletonCoalesceWaitNanos);
        requireNonNegative("appendInterArrivalCount", appendInterArrivalCount);
        requireNonNegative("appendInterArrivalNanos", appendInterArrivalNanos);
        requireNonNegative("appendInterArrivalLe100MicrosCount", appendInterArrivalLe100MicrosCount);
        requireNonNegative("appendInterArrivalLe250MicrosCount", appendInterArrivalLe250MicrosCount);
        requireNonNegative("appendInterArrivalLe500MicrosCount", appendInterArrivalLe500MicrosCount);
        requireNonNegative("appendInterArrivalLe1000MicrosCount", appendInterArrivalLe1000MicrosCount);
        requireAtMost(
            "singletonCoalesceHitCount",
            singletonCoalesceHitCount,
            "singletonCoalesceWaitCount",
            singletonCoalesceWaitCount
        );
        requireAtMost(
            "appendInterArrivalLe100MicrosCount",
            appendInterArrivalLe100MicrosCount,
            "appendInterArrivalLe250MicrosCount",
            appendInterArrivalLe250MicrosCount
        );
        requireAtMost(
            "appendInterArrivalLe250MicrosCount",
            appendInterArrivalLe250MicrosCount,
            "appendInterArrivalLe500MicrosCount",
            appendInterArrivalLe500MicrosCount
        );
        requireAtMost(
            "appendInterArrivalLe500MicrosCount",
            appendInterArrivalLe500MicrosCount,
            "appendInterArrivalLe1000MicrosCount",
            appendInterArrivalLe1000MicrosCount
        );
        requireAtMost(
            "appendInterArrivalLe1000MicrosCount",
            appendInterArrivalLe1000MicrosCount,
            "appendInterArrivalCount",
            appendInterArrivalCount
        );
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0L) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
    }

    private static void requireAtMost(String lowerName, long lower, String upperName, long upper) {
        if (lower > upper) {
            throw new IllegalArgumentException(lowerName + " cannot exceed " + upperName);
        }
    }
}
