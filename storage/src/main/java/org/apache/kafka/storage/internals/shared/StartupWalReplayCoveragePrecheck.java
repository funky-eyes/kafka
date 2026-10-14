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

/**
 * Fail-closed comparison of Kafka batch index ranges against DATA actually
 * replayed by the WAL at this broker's startup.
 *
 * <p>Even COMPLETE is strictly an advisory local coverage observation, NOT
 * proof of replicated WAL persistence, current Kafka source ownership,
 * a valid BrokerRegistration, KRaft authorization or safe remote deletion.
 * A missing/reclaimed batch, partial RecordBatch, or post-start write cannot
 * fill a startup recovery gap. Caller-supplied lists are not authenticated.</p>
 */
public final class StartupWalReplayCoveragePrecheck {
    private StartupWalReplayCoveragePrecheck() {
    }

    public enum Finding {
        RECOVERED_BATCH_WINDOW,
        EMPTY_SOURCE_WINDOW,
        NO_REPLAYED_DATA,
        KAFKA_BATCH_GAP_OR_OVERLAP,
        SOURCE_END_SPLITS_BATCH,
        POST_START_WAL_BATCH,
        SOURCE_WINDOW_NOT_RECOVERED
    }

    public static Finding assess(
        StartupWalReplayEvidence replay,
        SharedPartitionId partition,
        List<WalLocation> indexedBatches,
        long sourceLogStart,
        long sourceHighWatermark
    ) {
        Objects.requireNonNull(replay, "replay");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(indexedBatches, "indexedBatches");
        if (sourceLogStart < 0L || sourceHighWatermark < sourceLogStart) {
            throw new IllegalArgumentException("Invalid native source LogStart/High Watermark window");
        }
        if (sourceLogStart == sourceHighWatermark) {
            return Finding.EMPTY_SOURCE_WINDOW;
        }
        var recovered = replay.partition(partition);
        if (recovered.isEmpty() || recovered.get().dataRecords() == 0L) {
            return Finding.NO_REPLAYED_DATA;
        }

        long nextKafkaOffset = sourceLogStart;
        long highestReplayedWalEnd = recovered.get().highestWalEndExclusive();
        for (WalLocation location : indexedBatches) {
            Objects.requireNonNull(location, "indexed WAL batch");
            // Do not slice or silently omit any part of a RecordBatch.
            if (location.firstOffset() != nextKafkaOffset) {
                return Finding.KAFKA_BATCH_GAP_OR_OVERLAP;
            }
            // Indexed data can include bytes appended after this process
            // recovered; those bytes cannot be counted as recovered evidence.
            if (location.walOffset() + location.length() > highestReplayedWalEnd) {
                return Finding.POST_START_WAL_BATCH;
            }
            if (location.lastOffset() >= sourceHighWatermark) {
                return Finding.SOURCE_END_SPLITS_BATCH;
            }
            // lastOffset < sourceHighWatermark <= Long.MAX_VALUE guarantees
            // the increment is representable.
            nextKafkaOffset = location.lastOffset() + 1L;
        }
        // Validate every supplied entry; an extra batch beyond the native
        // source end must never be accepted by an early successful return.
        return nextKafkaOffset == sourceHighWatermark
            ? Finding.RECOVERED_BATCH_WINDOW : Finding.SOURCE_WINDOW_NOT_RECOVERED;
    }
}
