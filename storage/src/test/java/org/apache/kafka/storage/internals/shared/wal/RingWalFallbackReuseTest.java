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

import static org.junit.jupiter.api.Assertions.assertEquals;

class RingWalFallbackReuseTest {
    private static final long DATA_CAPACITY = 128L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void tornLatestCheckpointCannotFallBackBehindReclaimedPhysicalReuse() throws Exception {
        Path path = tempDir.resolve("reuse-fallback.wal");
        RingWalSuperblock.State reclaimState;

        try (RingWalFile file = new RingWalFile(path, TOTAL_CAPACITY)) {
            RingWalLayout.Allocation first = file.layout().allocate(0L, 0L, 100);
            file.write(first, ByteBuffer.wrap(new byte[100]));
            file.forceAndCheckpoint(0L, first.nextTailOffset());

            reclaimState = file.forceAndCheckpoint(first.nextTailOffset(), first.nextTailOffset());

            RingWalLayout.Allocation reused = file.layout().allocate(
                reclaimState.headOffset(),
                reclaimState.tailOffset(),
                40
            );
            byte[] replacement = new byte[40];
            replacement[0] = 9;
            file.write(reused, ByteBuffer.wrap(replacement));
            RingWalSuperblock.State latest = file.forceAndCheckpoint(
                reclaimState.headOffset(),
                reused.nextTailOffset()
            );

            assertEquals(1L, reused.generation());
            assertEquals(3L, latest.sequence());
        }

        corruptSuperblock(path, 1);

        try (RingWalFile reopened = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(
                reclaimState,
                reopened.state(),
                "fallback must not retain a checkpoint that still references the reused generation-0 slot"
            );
        }
    }

    @Test
    void recoveryRepairsStaleFallbackBeforeLaterReuseIsAllowed() throws Exception {
        Path path = tempDir.resolve("recovery-repair.wal");
        RingWalSuperblock.State stale;
        RingWalSuperblock.State latest;

        try (RingWalFile file = new RingWalFile(path, TOTAL_CAPACITY)) {
            RingWalLayout.Allocation first = file.layout().allocate(0L, 0L, 100);
            file.write(first, ByteBuffer.wrap(new byte[100]));
            stale = file.forceAndCheckpoint(0L, first.nextTailOffset());
            latest = file.forceAndCheckpoint(first.nextTailOffset(), first.nextTailOffset());
        }

        writeSuperblock(path, 1, stale);
        try (RingWalFile recovered = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(latest, recovered.state());
        }

        corruptSuperblock(path, 0);
        try (RingWalFile recoveredAgain = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(
                latest,
                recoveredAgain.state(),
                "recovery must repair the stale copy before exposing the WAL for future physical reuse"
            );
        }
    }

    private static void corruptSuperblock(Path path, int copyIndex) throws Exception {
        long position = (long) copyIndex * RingWalSuperblock.SUPERBLOCK_BYTES + 8L;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, position);
            one.flip();
            byte changed = (byte) (one.get() ^ 0x01);
            channel.write(ByteBuffer.wrap(new byte[] {changed}), position);
            channel.force(false);
        }
    }

    private static void writeSuperblock(
        Path path,
        int copyIndex,
        RingWalSuperblock.State state
    ) throws Exception {
        long position = (long) copyIndex * RingWalSuperblock.SUPERBLOCK_BYTES;
        ByteBuffer bytes = RingWalSuperblock.encode(state);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            while (bytes.hasRemaining()) {
                int written = channel.write(bytes, position);
                if (written <= 0) {
                    throw new IllegalStateException("Unable to write test superblock");
                }
                position += written;
            }
            channel.force(false);
        }
    }
}
