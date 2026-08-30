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

/**
 * Performance decorator that retains recently durable DATA records in a bounded in-memory cache.
 *
 * <p>The delegate remains the sole durability authority. Records enter the cache only after the delegate append future
 * completes successfully, so producer acknowledgement semantics are unchanged. Reads prefer cached immutable payloads
 * and transparently fall back to the delegate WAL. Reclamation is always delegated first and then clears the cache,
 * preventing stale physical locations from surviving segment deletion or later reuse.</p>
 */
public final class CachingSharedWal implements SharedWal {
    private final SharedWal delegate;
    private final WalReadCache readCache;

    public CachingSharedWal(SharedWal delegate, long cacheCapacityBytes) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.readCache = new WalReadCache(cacheCapacityBytes);
    }

    @Override
    public CompletableFuture<List<WalAppendResult>> appendBatch(List<WalRecord> records) {
        Objects.requireNonNull(records, "records");
        List<WalRecord> immutableRecords = List.copyOf(records);
        return delegate.appendBatch(immutableRecords).thenApply(results -> {
            List<WalAppendResult> immutableResults = List.copyOf(results);
            readCache.putAll(immutableRecords, immutableResults);
            return immutableResults;
        });
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        Objects.requireNonNull(location, "location");
        WalRecord cached = readCache.get(location);
        return cached != null ? cached : delegate.read(location);
    }

    @Override
    public List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        Objects.requireNonNull(locations, "locations");
        if (locations.isEmpty()) {
            return List.of();
        }

        List<WalRecord> result = new ArrayList<>(locations.size());
        boolean allCached = true;
        for (WalLocation location : locations) {
            WalRecord cached = readCache.get(Objects.requireNonNull(location, "location"));
            if (cached == null) {
                allCached = false;
                break;
            }
            result.add(cached);
        }
        if (allCached) {
            return List.copyOf(result);
        }
        return delegate.readBatch(locations);
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        delegate.replay(consumer);
    }

    @Override
    public long reclaim(WalReclaimPolicy policy) throws IOException {
        long reclaimed = delegate.reclaim(policy);
        if (reclaimed > 0) {
            readCache.clear();
        }
        return reclaimed;
    }

    @Override
    public long reclaim(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        long reclaimed = delegate.reclaim(policy, desiredBytes);
        if (reclaimed > 0) {
            readCache.clear();
        }
        return reclaimed;
    }

    @Override
    public long usedBytes() {
        return delegate.usedBytes();
    }

    @Override
    public long capacityBytes() {
        return delegate.capacityBytes();
    }

    @Override
    public void close() throws IOException {
        readCache.clear();
        delegate.close();
    }

    int cachedEntryCount() {
        return readCache.entryCount();
    }

    long cachedBytes() {
        return readCache.usedBytes();
    }
}
