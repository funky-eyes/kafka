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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class WalReadCacheTest {
    @Test
    void shouldRetainOnlyDataRecordsWithinByteCapacity() {
        WalReadCache cache = new WalReadCache(100);
        WalRecord first = data(0, 60);
        WalRecord second = data(1, 60);
        WalRecord truncate = WalRecord.truncate(1, 2, 0, 3, 2);

        cache.putAll(
            List.of(first, truncate),
            List.of(new WalAppendResult(0, 0, 120), new WalAppendResult(0, 120, 60))
        );
        assertEquals(1, cache.entryCount());
        assertEquals(60, cache.usedBytes());
        assertSame(first, cache.get(location(0, 0, 120, 60, 0)));

        cache.putAll(List.of(second), List.of(new WalAppendResult(0, 180, 120)));
        assertEquals(1, cache.entryCount(), "FIFO eviction must keep the cache inside its byte budget");
        assertEquals(60, cache.usedBytes());
        assertNull(cache.get(location(0, 0, 120, 60, 0)));
        assertSame(second, cache.get(location(0, 180, 120, 60, 1)));
    }

    @Test
    void shouldRejectStalePhysicalLocationWithDifferentLogicalMetadata() {
        WalReadCache cache = new WalReadCache(1024);
        WalRecord record = data(10, 32);
        cache.putAll(List.of(record), List.of(new WalAppendResult(4, 128, 92)));

        assertSame(record, cache.get(location(4, 128, 92, 32, 10)));
        assertNull(cache.get(location(4, 128, 92, 32, 11)),
            "a reused physical WAL position must not expose stale logical data");
    }

    @Test
    void shouldSkipRecordLargerThanEntireCache() {
        WalReadCache cache = new WalReadCache(16);
        cache.putAll(List.of(data(0, 32)), List.of(new WalAppendResult(0, 0, 92)));

        assertEquals(0, cache.entryCount());
        assertEquals(0, cache.usedBytes());
    }

    private static WalRecord data(long offset, int payloadBytes) {
        return WalRecord.data(1, 2, 0, 3, offset, offset, new byte[payloadBytes]);
    }

    private static WalLocation location(
        long segmentId,
        long position,
        int encodedLength,
        int payloadLength,
        long offset
    ) {
        return new WalLocation(segmentId, position, encodedLength, payloadLength, 3, offset, offset);
    }
}
