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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSharedWalTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldPersistReplayAndRandomReadDataAndTruncateRecords() throws Exception {
        Path walDir = tempDir.resolve("wal");
        WalAppendResult dataLocation;
        try (FileSharedWal wal = new FileSharedWal(walDir, 1024 * 1024, 4096)) {
            dataLocation = wal.append(WalRecord.data(1, 2, 0, 3, 10, 19, new byte[]{1, 2, 3}))
                .get(10, TimeUnit.SECONDS);
            wal.append(WalRecord.truncate(1, 2, 0, 4, 15))
                .get(10, TimeUnit.SECONDS);

            WalRecord randomRead = wal.read(new WalLocation(
                dataLocation.segmentId(), dataLocation.position(), dataLocation.length(), 3, 10, 19));
            assertEquals(WalRecordType.DATA, randomRead.type());
            assertArrayEquals(new byte[]{1, 2, 3}, bytes(randomRead.payload()));
        }

        List<WalRecord> records = new ArrayList<>();
        try (FileSharedWal wal = new FileSharedWal(walDir, 1024 * 1024, 4096)) {
            wal.replay((record, ignored) -> records.add(record));
        }

        assertEquals(2, records.size());
        assertEquals(WalRecordType.DATA, records.get(0).type());
        assertArrayEquals(new byte[]{1, 2, 3}, bytes(records.get(0).payload()));
        assertEquals(WalRecordType.TRUNCATE, records.get(1).type());
        assertEquals(15, records.get(1).truncateOffset());
    }

    @Test
    void shouldDiscardPartialTailDuringRecovery() throws Exception {
        Path walDir = tempDir.resolve("wal-partial");
        try (FileSharedWal wal = new FileSharedWal(walDir, 1024 * 1024, 4096)) {
            wal.append(WalRecord.data(5, 6, 1, 1, 0, 0, new byte[]{9, 8, 7}))
                .get(10, TimeUnit.SECONDS);
        }

        Path segment = walDir.resolve("wal-00000000000000000000.log");
        long validSize;
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            validSize = channel.size();
            channel.write(ByteBuffer.wrap(new byte[]{0x4b, 0x53, 0x57}), validSize);
            channel.force(false);
        }

        try (FileSharedWal ignored = new FileSharedWal(walDir, 1024 * 1024, 4096)) {
            assertEquals(validSize, java.nio.file.Files.size(segment));
        }
    }

    @Test
    void shouldRejectAppendWhenCapacityWouldBeExceeded() throws Exception {
        Path walDir = tempDir.resolve("wal-capacity");
        try (FileSharedWal wal = new FileSharedWal(walDir, 256, 256)) {
            wal.append(WalRecord.data(1, 1, 0, 0, 0, 0, new byte[100]))
                .get(10, TimeUnit.SECONDS);

            ExecutionException error = assertThrows(ExecutionException.class, () ->
                wal.append(WalRecord.data(1, 1, 0, 0, 1, 1, new byte[100]))
                    .get(10, TimeUnit.SECONDS));
            assertTrue(error.getCause() instanceof WalCapacityExceededException);
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }
}
