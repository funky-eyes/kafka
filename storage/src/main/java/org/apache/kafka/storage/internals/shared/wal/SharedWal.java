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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public interface SharedWal extends AutoCloseable {
    /**
     * Atomically admits and durably appends a logical group of WAL records.
     *
     * <p>The returned future completes only after every record in the group crosses the same durability barrier.
     * Implementations must never report a successful prefix of the group.</p>
     */
    CompletableFuture<List<WalAppendResult>> appendBatch(List<WalRecord> records);

    default CompletableFuture<WalAppendResult> append(WalRecord record) {
        Objects.requireNonNull(record, "record");
        return appendBatch(List.of(record)).thenApply(results -> results.get(0));
    }

    WalRecord read(WalLocation location) throws IOException;

    default List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        List<WalRecord> records = new ArrayList<>(locations.size());
        for (WalLocation location : locations) {
            records.add(read(location));
        }
        return List.copyOf(records);
    }

    void replay(WalReplayConsumer consumer) throws IOException;

    /**
     * Reclaims a contiguous physical WAL prefix that is no longer required for recovery.
     *
     * <p>Implementations supporting rotation must preserve append-group atomicity and must never reclaim a record for
     * which {@code policy} returns false. The return value is the number of physical bytes released.</p>
     */
    default long reclaim(WalReclaimPolicy policy) throws IOException {
        Objects.requireNonNull(policy, "policy");
        throw new UnsupportedOperationException("WAL implementation does not support reclamation");
    }

    long usedBytes();

    long capacityBytes();

    @Override
    void close() throws IOException;
}
