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
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingSharedWalPaddingMarkerTest {
    private static final long DATA_CAPACITY = 512L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void replaysAcrossAuthenticatedLargeWrapPadding() throws Exception {
        Path path = tempDir.resolve("marked-padding.wal");
        WalRecord first = dataRecord(0L, 180);
        WalRecord beforeWrap = dataRecord(1L, 1);
        WalRecord afterWrap = dataRecord(2L, 40);

        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY)) {
            wal.appendBatch(List.of(first)).join();
            assertEquals(300L, wal.reclaim((record, ignored) -> true, Long.MAX_VALUE));
            assertEquals(300L, wal.reclaimedBeforeOffset());
            assertEquals(300L, wal.appendBatch(List.of(beforeWrap)).join().get(0).offset());
            assertEquals(512L, wal.appendBatch(List.of(afterWrap)).join().get(0).offset());
        }

        try (RingSharedWal reopened = new RingSharedWal(path, TOTAL_CAPACITY)) {
            List<WalRecord> replayed = new ArrayList<>();
            reopened.replay((record, ignored) -> replayed.add(record));
            assertEquals(2, replayed.size());
            assertRecordEquals(beforeWrap, replayed.get(0));
            assertRecordEquals(afterWrap, replayed.get(1));
        }
    }

    @Test
    void rejectsFullyZeroedCommittedGroupBeforeLargeWrapPadding() throws Exception {
        Path path = tempDir.resolve("zeroed-group-before-padding.wal");
        WalRecord first = dataRecord(0L, 180);
        WalRecord lostByCorruption = dataRecord(1L, 1);
        WalRecord afterWrap = dataRecord(2L, 40);

        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY)) {
            wal.appendBatch(List.of(first)).join();
            assertEquals(300L, wal.reclaim((record, ignored) -> true, Long.MAX_VALUE));
            assertEquals(300L, wal.appendBatch(List.of(lostByCorruption)).join().get(0).offset());
            assertEquals(512L, wal.appendBatch(List.of(afterWrap)).join().get(0).offset());
        }

        // Logical [300, 421) is a complete committed group. [421, 512) is legitimate wrap padding. Zeroing the
        // entire [300, 512) suffix used to make recovery silently classify the destroyed group as padding.
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer zeros = ByteBuffer.allocate(212);
            long physicalStart = RingWalLayout.DATA_START + 300L;
            while (zeros.hasRemaining()) {
                physicalStart += channel.write(zeros, physicalStart);
            }
            channel.force(false);
        }

        assertThrows(WalCorruptionException.class, () -> {
            try (RingSharedWal ignored = new RingSharedWal(path, TOTAL_CAPACITY)) {
                // The missing committed group must be detected instead of being reclassified as wrap padding.
            }
        });
    }

    private static WalRecord dataRecord(long offset, int payloadBytes) {
        byte[] payload = new byte[payloadBytes];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (offset + i + 1);
        }
        return WalRecord.data(1L, 2L, 0, 3, offset, offset, payload);
    }

    private static void assertRecordEquals(WalRecord expected, WalRecord actual) {
        assertEquals(expected.type(), actual.type());
        assertEquals(expected.topicIdHigh(), actual.topicIdHigh());
        assertEquals(expected.topicIdLow(), actual.topicIdLow());
        assertEquals(expected.partition(), actual.partition());
        assertEquals(expected.leaderEpoch(), actual.leaderEpoch());
        assertEquals(expected.firstOffset(), actual.firstOffset());
        assertEquals(expected.lastOffset(), actual.lastOffset());
        assertArrayEquals(bytes(expected.payload()), bytes(actual.payload()));
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }
}
