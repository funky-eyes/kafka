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
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RotatingFileSharedWalCacheTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldCacheOnlySuccessfullyDurableDataAndEvictByPayloadBytes() throws Exception {
        Path directory = tempDir.resolve("durable-cache");
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(directory, 4096, 512, 100)) {
            WalRecord first = data(0, 60, 1);
            WalAppendResult firstResult = wal.append(first).get(10, TimeUnit.SECONDS);
            assertEquals(1, wal.cachedEntryCount());
            assertEquals(60, wal.cachedBytes());

            WalLocation firstLocation = location(first, firstResult);
            assertEquals(1, wal.read(firstLocation).payload().get(0));

            WalRecord second = data(1, 60, 2);
            WalAppendResult secondResult = wal.append(second).get(10, TimeUnit.SECONDS);
            assertEquals(1, wal.cachedEntryCount(), "the second payload must evict the first one");
            assertEquals(60, wal.cachedBytes());

            assertEquals(1, wal.read(firstLocation).payload().get(0),
                "an evicted cache entry must transparently fall back to the physical WAL");
            assertEquals(2, wal.read(location(second, secondResult)).payload().get(0));
        }
    }

    @Test
    void shouldClearDisposableCacheAcrossReclaimAndRestart() throws Exception {
        Path directory = tempDir.resolve("reclaim-cache");
        WalLocation survivingLocation;
        try (RotatingFileSharedWal wal = new RotatingFileSharedWal(directory, 2048, 256, 1024)) {
            WalRecord first = data(0, 100, 1);
            wal.append(first).get(10, TimeUnit.SECONDS);
            WalRecord second = data(1, 100, 2);
            WalAppendResult secondResult = wal.append(second).get(10, TimeUnit.SECONDS);
            survivingLocation = location(second, secondResult);
            assertEquals(2, wal.cachedEntryCount());

            long reclaimed = wal.reclaim((record, ignored) -> record.lastOffset() == 0, 1L);
            assertTrue(reclaimed > 0);
            assertEquals(0, wal.cachedEntryCount(), "reopen after physical reclaim must invalidate cached locations");
            assertEquals(2, wal.read(survivingLocation).payload().get(0),
                "surviving data must remain readable from the reopened WAL after cache invalidation");
        }

        try (RotatingFileSharedWal restarted = new RotatingFileSharedWal(directory, 2048, 256, 1024)) {
            assertEquals(0, restarted.cachedEntryCount(), "restart must never depend on process-local cache state");
            List<Long> replayed = new java.util.ArrayList<>();
            restarted.replay((record, ignored) -> replayed.add(record.firstOffset()));
            assertEquals(List.of(1L), replayed);
        }
    }

    private static WalRecord data(long offset, int payloadBytes, int marker) {
        ByteBuffer payload = ByteBuffer.allocate(payloadBytes);
        payload.put((byte) marker);
        while (payload.hasRemaining()) {
            payload.put((byte) 0);
        }
        payload.flip();
        return WalRecord.dataOwned(1, 2, 0, 3, offset, offset, payload);
    }

    private static WalLocation location(WalRecord record, WalAppendResult result) {
        return new WalLocation(
            result.segmentId(),
            result.position(),
            result.length(),
            record.payload().remaining(),
            record.leaderEpoch(),
            record.firstOffset(),
            record.lastOffset()
        );
    }
}
