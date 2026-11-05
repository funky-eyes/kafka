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

import org.apache.kafka.storage.internals.shared.wal.WalLocation;

import java.util.List;
import java.util.Objects;

/**
 * Classifies exact Kafka batch coverage in the currently published local
 * durable-WAL index. Only the engine may provide the indexed batch list.
 *
 * <p>Index publication follows a successful backend WAL durability Future
 * or WAL replay. COMPLETE is NOT a recoverable/replicated durability
 * certificate, a source ownership proof, or Controller/GC authority.</p>
 */
public final class CurrentLocalWalCoveragePrecheck {
    private CurrentLocalWalCoveragePrecheck() {
    }

    public enum Finding {
        INDEXED_BATCH_WINDOW,
        EMPTY_SOURCE_WINDOW,
        NO_INDEXED_DATA,
        KAFKA_BATCH_GAP_OR_OVERLAP,
        SOURCE_END_SPLITS_BATCH,
        SOURCE_WINDOW_NOT_INDEXED
    }

    static Finding assess(List<WalLocation> batches, long logStart, long highWatermark) {
        Objects.requireNonNull(batches, "indexed WAL batches");
        if (logStart < 0L || highWatermark < logStart) {
            throw new IllegalArgumentException("Invalid native Kafka source window");
        }
        if (logStart == highWatermark) {
            return Finding.EMPTY_SOURCE_WINDOW;
        }
        if (batches.isEmpty()) {
            return Finding.NO_INDEXED_DATA;
        }
        long next = logStart;
        for (WalLocation location : batches) {
            Objects.requireNonNull(location, "indexed WAL location");
            // Neither a Kafka RecordBatch partial prefix nor overlapping
            // Kafka batches may fill a logical Kafka source interval.
            if (location.firstOffset() != next) {
                return Finding.KAFKA_BATCH_GAP_OR_OVERLAP;
            }
            if (location.lastOffset() >= highWatermark) {
                return Finding.SOURCE_END_SPLITS_BATCH;
            }
            next = location.lastOffset() + 1L;
        }
        return next == highWatermark
            ? Finding.INDEXED_BATCH_WINDOW : Finding.SOURCE_WINDOW_NOT_INDEXED;
    }
}
