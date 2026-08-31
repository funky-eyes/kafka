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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bounded FIFO cache for DATA records that have already crossed the WAL durability barrier.
 *
 * <p>The cache is keyed by the stable logical WAL address, never by a physical file position or ring slot. Logical
 * Kafka metadata is still validated on lookup so a corrupt/stale index entry cannot expose the wrong RecordBatch.</p>
 */
final class WalReadCache {
    private final long capacityBytes;
    private final LinkedHashMap<Long, CacheEntry> entries = new LinkedHashMap<>();
    private long usedBytes;

    WalReadCache(long capacityBytes) {
        if (capacityBytes < 0) {
            throw new IllegalArgumentException("capacityBytes must not be negative");
        }
        this.capacityBytes = capacityBytes;
    }

    synchronized void putAll(List<WalRecord> records, List<WalAppendResult> results) {
        if (capacityBytes == 0 || records == null || results == null || records.size() != results.size()) {
            return;
        }
        for (int i = 0; i < records.size(); i++) {
            put(records.get(i), results.get(i));
        }
    }

    private void put(WalRecord record, WalAppendResult result) {
        if (record == null || result == null || record.type() != WalRecordType.DATA) {
            return;
        }
        int payloadBytes = record.payload().remaining();
        if (payloadBytes > capacityBytes) {
            return;
        }

        long key = result.offset();
        CacheEntry replaced = entries.remove(key);
        if (replaced != null) {
            usedBytes -= replaced.payloadBytes();
        }
        while (!entries.isEmpty() && usedBytes + payloadBytes > capacityBytes) {
            Iterator<Map.Entry<Long, CacheEntry>> iterator = entries.entrySet().iterator();
            CacheEntry eldest = iterator.next().getValue();
            usedBytes -= eldest.payloadBytes();
            iterator.remove();
        }
        entries.put(key, new CacheEntry(record, result.length(), payloadBytes));
        usedBytes += payloadBytes;
    }

    synchronized WalRecord get(WalLocation location) {
        CacheEntry entry = entries.get(location.walOffset());
        if (entry == null || !entry.matches(location)) {
            return null;
        }
        return entry.record();
    }

    synchronized boolean contains(WalLocation location) {
        return get(location) != null;
    }

    synchronized int entryCount() {
        return entries.size();
    }

    synchronized long usedBytes() {
        return usedBytes;
    }

    synchronized void clear() {
        entries.clear();
        usedBytes = 0L;
    }

    private record CacheEntry(WalRecord record, int encodedLength, int payloadBytes) {
        private boolean matches(WalLocation location) {
            return encodedLength == location.length() &&
                payloadBytes == location.payloadLength() &&
                record.leaderEpoch() == location.leaderEpoch() &&
                record.firstOffset() == location.firstOffset() &&
                record.lastOffset() == location.lastOffset();
        }
    }
}
