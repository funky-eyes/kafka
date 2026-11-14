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

/**
 * Cumulative Ring WAL writer latency observations in nanoseconds.
 *
 * <p>Queue time is measured from append admission to the beginning of a drained
 * writer cycle. Physical write time includes all completed NIO/AIO batch writes,
 * but excludes data and checkpoint forces already reported by
 * {@link WalDurabilityStats}. These are counters and maxima, not p99 estimates. Request counts
 * describe staged fragments versus coalesced submitted requests, not the
 * actual storage device's completed IOPS.
 * End-to-end Kafka producer acknowledgement time is not measured here.</p>
 */
public record WalWriteLatencyStats(
    long queuedAppendGroups,
    long totalQueueNanos,
    long maxQueueNanos,
    long physicalWriteCalls,
    long totalPhysicalWriteNanos,
    long maxPhysicalWriteNanos,
    long stagedPhysicalRequests,
    long submittedPhysicalRequests
) {
    public static final WalWriteLatencyStats EMPTY =
        new WalWriteLatencyStats(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    /** Preserve existing diagnostics call sites that do not count physical fragments. */
    public WalWriteLatencyStats(
        long queuedAppendGroups,
        long totalQueueNanos,
        long maxQueueNanos,
        long physicalWriteCalls,
        long totalPhysicalWriteNanos,
        long maxPhysicalWriteNanos
    ) {
        this(queuedAppendGroups, totalQueueNanos, maxQueueNanos,
            physicalWriteCalls, totalPhysicalWriteNanos, maxPhysicalWriteNanos, 0L, 0L);
    }

    public WalWriteLatencyStats {
        if (queuedAppendGroups < 0L || totalQueueNanos < 0L || maxQueueNanos < 0L ||
            physicalWriteCalls < 0L || totalPhysicalWriteNanos < 0L || maxPhysicalWriteNanos < 0L) {
            throw new IllegalArgumentException("WAL write latency statistics must be non-negative");
        }
        if (stagedPhysicalRequests < 0L || submittedPhysicalRequests < 0L ||
            submittedPhysicalRequests > stagedPhysicalRequests) {
            throw new IllegalArgumentException("WAL coalesced request counts must be ordered and non-negative");
        }
    }
}
