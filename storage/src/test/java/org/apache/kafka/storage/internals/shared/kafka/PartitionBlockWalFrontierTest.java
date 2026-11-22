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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.wal.AsynchronousFileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.CoordinatedRingWalAdapter;
import org.apache.kafka.storage.internals.shared.wal.FileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.WalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Component experiment only: Broker fetch/ACK/leadership are not connected to this tracker. */
class PartitionBlockWalFrontierTest {
    @TempDir
    Path dir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void durableWalAndKafkaStateMustBothCompleteInOffsetOrder(boolean aio) throws Exception {
        Path path = dir.resolve("frontier-" + aio);
        long capacity = 16L * 4096;
        PartitionDurableOffsetTracker tracker = new PartitionDurableOffsetTracker(1L, 0L, Runnable::run, 8, 8192L);
        List<CompletableFuture<Void>> published = new ArrayList<>();
        List<CompletableFuture<?>> writes = new ArrayList<>();
        List<PartitionDurableOffsetTracker.Reservation> reservations = new ArrayList<>();
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.create(path, backend, capacity, 4096, 10_000_000L)) {
            for (int i = 0; i < 3; i++) {
                var reservation = tracker.reserve(1L, i, i + 1L, 64L);
                var publication = new CompletableFuture<Void>();
                var write = wal.append(WalRecord.data(1, 2, 0, 1, i, i, new byte[] {(byte) i}));
                reservation.bind(write, publication);
                reservations.add(reservation);
                published.add(publication);
                writes.add(write);
            }
            CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
            published.get(2).complete(null);
            published.get(1).complete(null);
            assertEquals(0L, tracker.confirmedOffset());
            assertFalse(reservations.get(0).confirmed().isDone());
            assertFalse(reservations.get(1).confirmed().isDone());
            assertFalse(reservations.get(2).confirmed().isDone());
            published.get(0).complete(null);
            for (var reservation : reservations) reservation.confirmed().get(10, TimeUnit.SECONDS);
            assertEquals(3L, tracker.confirmedOffset());
        }
        try (WalIoBackend backend = backend(aio);
             var wal = CoordinatedRingWalAdapter.reopen(path, backend, capacity, 4096, 0)) {
            List<Long> offsets = new ArrayList<>();
            wal.replay((record, location) -> offsets.add(record.firstOffset()));
            assertEquals(List.of(0L, 1L, 2L), offsets);
        }
    }

    private static WalIoBackend backend(boolean aio) {
        return aio ? new AsynchronousFileChannelWalIoBackend() : new FileChannelWalIoBackend();
    }
}
