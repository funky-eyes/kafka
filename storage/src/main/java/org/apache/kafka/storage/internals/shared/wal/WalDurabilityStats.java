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
        if (durabilityBatchCount < 0L ||
            durableAppendGroupCount < 0L ||
            durableBytes < 0L ||
            durabilityBarrierNanos < 0L ||
            maxGroupsPerDurabilityBatch < 0L ||
            singletonCoalesceWaitCount < 0L ||
            singletonCoalesceHitCount < 0L ||
            singletonCoalesceWaitNanos < 0L ||
            appendInterArrivalCount < 0L ||
            appendInterArrivalNanos < 0L ||
            appendInterArrivalLe100MicrosCount < 0L ||
            appendInterArrivalLe250MicrosCount < 0L ||
            appendInterArrivalLe500MicrosCount < 0L ||
            appendInterArrivalLe1000MicrosCount < 0L) {
            throw new IllegalArgumentException("WAL durability stats must be non-negative");
        }
        if (singletonCoalesceHitCount > singletonCoalesceWaitCount) {
            throw new IllegalArgumentException("WAL coalescing hits cannot exceed wait attempts");
        }
        if (appendInterArrivalLe100MicrosCount > appendInterArrivalLe250MicrosCount ||
            appendInterArrivalLe250MicrosCount > appendInterArrivalLe500MicrosCount ||
            appendInterArrivalLe500MicrosCount > appendInterArrivalLe1000MicrosCount ||
            appendInterArrivalLe1000MicrosCount > appendInterArrivalCount) {
            throw new IllegalArgumentException("WAL append inter-arrival buckets must be cumulative");
        }
    }
}
