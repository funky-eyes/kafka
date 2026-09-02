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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingSharedWalCrashWindowTest {
    private static final long DATA_CAPACITY = 256L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void recoversPreviousWindowWhenCheckpointWriteFailsAfterDataForce() throws Exception {
        Path path = tempDir.resolve("checkpoint-failure.wal");
        FailingCheckpointBackend backend = new FailingCheckpointBackend();

        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY, backend)) {
            backend.failNextSuperblockWrite();
            CompletionException failure = assertThrows(
                CompletionException.class,
                () -> wal.appendBatch(List.of(dataRecord(0L, 24))).join()
            );
            assertInstanceOf(IOException.class, failure.getCause());
        }

        try (RingSharedWal reopened = new RingSharedWal(path, TOTAL_CAPACITY)) {
            assertEquals(0L, reopened.reclaimedBeforeOffset());
            assertEquals(0L, reopened.usedBytes());
            List<WalRecord> replayed = new ArrayList<>();
            reopened.replay((record, ignored) -> replayed.add(record));
            assertEquals(List.of(), replayed);
        }
    }

    @Test
    void recoversReclaimedWindowWhenReusedSlotsAreForcedButNextCheckpointFails() throws Exception {
        Path path = tempDir.resolve("reused-slot-checkpoint-failure.wal");
        FailingCheckpointBackend backend = new FailingCheckpointBackend();
        long reclaimedHead;

        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY, backend)) {
            WalAppendResult first = wal.appendBatch(List.of(dataRecord(0L, 60))).join().get(0);
            assertEquals(0L, first.offset());

            long reclaimed = wal.reclaim((record, ignored) -> true, Long.MAX_VALUE);
            assertEquals(180L, reclaimed,
                "first append group must advance the durable head far enough that the next group wraps and reuses it");
            reclaimedHead = wal.reclaimedBeforeOffset();
            assertEquals(reclaimed, reclaimedHead);
            assertEquals(0L, wal.usedBytes());

            backend.failNextSuperblockWrite();
            CompletionException failure = assertThrows(
                CompletionException.class,
                () -> wal.appendBatch(List.of(dataRecord(1L, 60))).join()
            );
            assertInstanceOf(IOException.class, failure.getCause());
        }

        try (RingSharedWal reopened = new RingSharedWal(path, TOTAL_CAPACITY)) {
            assertEquals(reclaimedHead, reopened.reclaimedBeforeOffset(),
                "recovery must select the reclaim checkpoint that no longer references the reused physical slots");
            assertEquals(0L, reopened.usedBytes(),
                "data forced into reused slots must stay invisible when its newer superblock never became durable");
            List<WalRecord> replayed = new ArrayList<>();
            reopened.replay((record, ignored) -> replayed.add(record));
            assertEquals(List.of(), replayed,
                "recovery must not fall back to the older superblock whose logical window was physically overwritten");
        }
    }

    @Test
    void rejectsCorruptedWrapPaddingOnRecovery() throws Exception {
        Path path = tempDir.resolve("corrupted-padding.wal");
        WalRecord first = dataRecord(0L, 10);
        WalRecord second = dataRecord(10L, 40);

        try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY)) {
            wal.appendBatch(List.of(first)).join();
            wal.reclaim((record, ignored) -> true, Long.MAX_VALUE);
            WalAppendResult secondResult = wal.appendBatch(List.of(second)).join().get(0);
            assertEquals(130L, secondResult.offset());
            assertEquals(186L, wal.usedBytes());
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer corruption = ByteBuffer.wrap(new byte[] {(byte) 0x7f});
            long firstPaddingByte = RingWalLayout.DATA_START + 230L;
            assertEquals(1, channel.write(corruption, firstPaddingByte));
            channel.force(false);
        }

        assertThrows(WalCorruptionException.class, () -> {
            try (RingSharedWal ignored = new RingSharedWal(path, TOTAL_CAPACITY)) {
                // Construction must reject the non-zero wrap suffix before the WAL becomes usable.
            }
        });
    }

    @Test
    void survivesRepeatedWrapReclaimAndReopenAcrossGenerations() throws Exception {
        Path path = tempDir.resolve("multi-generation.wal");
        long previousWalOffset = -1L;
        long previousReclaimedBeforeOffset = 0L;

        for (int generation = 0; generation < 12; generation++) {
            WalRecord record = dataRecord(generation, 20);
            try (RingSharedWal wal = new RingSharedWal(path, TOTAL_CAPACITY)) {
                WalAppendResult result = wal.appendBatch(List.of(record)).join().get(0);
                assertTrue(result.offset() > previousWalOffset,
                    "logical WAL offsets must keep increasing across physical ring generations");
                assertRecordEquals(record, wal.read(location(record, result)));
                previousWalOffset = result.offset();

                long reclaimed = wal.reclaim((candidate, ignored) -> true, Long.MAX_VALUE);
                assertTrue(reclaimed > 0L, "each fully safe append group must release local WAL bytes");
                assertTrue(wal.reclaimedBeforeOffset() > previousReclaimedBeforeOffset,
                    "durable reclaim watermark must move forward across ring generations");
                previousReclaimedBeforeOffset = wal.reclaimedBeforeOffset();
                assertEquals(0L, wal.usedBytes());
            }
        }

        try (RingSharedWal reopened = new RingSharedWal(path, TOTAL_CAPACITY)) {
            List<WalRecord> replayed = new ArrayList<>();
            reopened.replay((record, ignored) -> replayed.add(record));
            assertEquals(List.of(), replayed);
            assertEquals(0L, reopened.usedBytes());
            assertEquals(previousReclaimedBeforeOffset, reopened.reclaimedBeforeOffset());
            assertTrue(reopened.reclaimedBeforeOffset() > DATA_CAPACITY * 4,
                "test must cross several physical ring generations");
        }
    }

    private static WalRecord dataRecord(long offset, int payloadBytes) {
        byte[] payload = new byte[payloadBytes];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (offset + i + 1);
        }
        return WalRecord.data(1L, 2L, 0, 3, offset, offset, payload);
    }

    private static WalLocation location(WalRecord record, WalAppendResult result) {
        return new WalLocation(
            result.offset(),
            result.length(),
            record.payload().remaining(),
            record.leaderEpoch(),
            record.firstOffset(),
            record.lastOffset()
        );
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

    private static final class FailingCheckpointBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicBoolean failNextSuperblockWrite = new AtomicBoolean(false);

        void failNextSuperblockWrite() {
            failNextSuperblockWrite.set(true);
        }

        @Override
        public Handle openRead(Path path) throws IOException {
            return wrap(delegate.openRead(path));
        }

        @Override
        public Handle reopen(Path path) throws IOException {
            return wrap(delegate.reopen(path));
        }

        @Override
        public Handle create(Path path) throws IOException {
            return wrap(delegate.create(path));
        }

        @Override
        public long size(Path path) throws IOException {
            return delegate.size(path);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private Handle wrap(Handle handle) {
            return new Handle() {
                @Override
                public long size() throws IOException {
                    return handle.size();
                }

                @Override
                public int read(ByteBuffer destination, long position) throws IOException {
                    return handle.read(destination, position);
                }

                @Override
                public int write(ByteBuffer source, long position) throws IOException {
                    if (position < RingWalLayout.DATA_START && failNextSuperblockWrite.compareAndSet(true, false)) {
                        throw new IOException("injected superblock write failure after data force");
                    }
                    return handle.write(source, position);
                }

                @Override
                public void truncate(long size) throws IOException {
                    handle.truncate(size);
                }

                @Override
                public void force() throws IOException {
                    handle.force();
                }

                @Override
                public void seal() throws IOException {
                    handle.seal();
                }

                @Override
                public void close() throws IOException {
                    handle.close();
                }
            };
        }
    }
}
