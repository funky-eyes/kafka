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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingWalFileTest {
    private static final long DATA_CAPACITY = 512L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void persistsOnlyWindowThatCrossedDataAndSuperblockForceBarrier() throws Exception {
        Path path = tempDir.resolve("ring.wal");
        RingWalSuperblock.State durable;
        RingWalLayout.Allocation allocation;
        byte[] payload = new byte[] {1, 2, 3, 4, 5};

        try (RingWalFile file = new RingWalFile(path, TOTAL_CAPACITY)) {
            allocation = file.layout().allocate(0, 0, payload.length);
            file.write(allocation, ByteBuffer.wrap(payload));
            assertThrows(
                IllegalArgumentException.class,
                () -> file.read(allocation.walOffset(), allocation.encodedLength()),
                "staged data must not become readable through the durable logical window before checkpoint"
            );

            durable = file.forceAndCheckpoint(0, allocation.nextTailOffset());
            assertEquals(1, durable.sequence());
            assertArrayEquals(payload, bytes(file.read(allocation.walOffset(), payload.length)));
        }

        assertEquals(TOTAL_CAPACITY, Files.size(path));
        try (RingWalFile reopened = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(durable, reopened.state());
            assertArrayEquals(payload, bytes(reopened.read(allocation.walOffset(), payload.length)));
        }
    }

    @Test
    void usesBackendPreallocationForFreshRingWal() throws Exception {
        Path path = tempDir.resolve("preallocated-ring.wal");
        TrackingPreallocationBackend backend = new TrackingPreallocationBackend();
        try (RingWalFile ignored = new RingWalFile(path, TOTAL_CAPACITY, backend)) {
            assertEquals(TOTAL_CAPACITY, backend.preallocatedBytes.get());
        }
        assertEquals(TOTAL_CAPACITY, Files.size(path));
    }

    @Test
    void rejectsStaleLogicalAddressAfterPhysicalSlotIsReused() throws Exception {
        Path path = tempDir.resolve("reuse.wal");
        long totalCapacity = RingWalLayout.DATA_START + 128L;

        try (RingWalFile file = new RingWalFile(path, totalCapacity)) {
            RingWalLayout.Allocation first = file.layout().allocate(0, 0, 100);
            file.write(first, ByteBuffer.wrap(new byte[100]));
            file.forceAndCheckpoint(0, first.nextTailOffset());
            file.forceAndCheckpoint(first.nextTailOffset(), first.nextTailOffset());

            RingWalLayout.Allocation reused = file.layout().allocate(
                first.nextTailOffset(),
                first.nextTailOffset(),
                40
            );
            byte[] replacement = new byte[40];
            replacement[0] = 9;
            file.write(reused, ByteBuffer.wrap(replacement));
            file.forceAndCheckpoint(first.nextTailOffset(), reused.nextTailOffset());

            assertEquals(1, reused.generation());
            assertEquals(RingWalLayout.DATA_START, reused.physicalPosition());
            assertThrows(IllegalArgumentException.class, () -> file.read(first.walOffset(), 100));
            assertArrayEquals(replacement, bytes(file.read(reused.walOffset(), replacement.length)));
        }
    }

    @Test
    void recoversPreviousSuperblockWhenLatestCopyIsTorn() throws Exception {
        Path path = tempDir.resolve("torn-checkpoint.wal");
        RingWalSuperblock.State previous;
        try (RingWalFile file = new RingWalFile(path, TOTAL_CAPACITY)) {
            previous = file.forceAndCheckpoint(0, 0);
            RingWalSuperblock.State latest = file.forceAndCheckpoint(0, 0);
            assertEquals(1, previous.sequence());
            assertEquals(2, latest.sequence());
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            channel.read(one, 8L);
            one.flip();
            byte changed = (byte) (one.get() ^ 0x01);
            channel.write(ByteBuffer.wrap(new byte[] {changed}), 8L);
            channel.force(false);
        }

        try (RingWalFile reopened = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(previous, reopened.state());
        }
    }

    @Test
    void failedFreshInitializationNeverPublishesDurablePath() throws Exception {
        Path path = tempDir.resolve("failed-initialization.wal");
        Path stagingPath = RingWalFile.initializationPath(path);
        IOException failure = assertThrows(
            IOException.class,
            () -> new RingWalFile(path, TOTAL_CAPACITY, new FailFirstForceBackend())
        );
        assertEquals("injected initial ring WAL force failure", failure.getMessage());
        assertFalse(Files.exists(path), "failed initialization must never expose a partial durable WAL");
        assertFalse(Files.exists(stagingPath), "failed initialization should clean its staging artifact");

        try (RingWalFile retried = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(
                new RingWalSuperblock.State(0L, 0L, 0L, DATA_CAPACITY),
                retried.state()
            );
        }
    }

    @Test
    void discardsStagingArtifactLeftByProcessCrashBeforePublish() throws Exception {
        Path path = tempDir.resolve("crashed-initialization.wal");
        Path stagingPath = RingWalFile.initializationPath(path);
        Files.write(stagingPath, new byte[] {1, 2, 3, 4});
        assertFalse(Files.exists(path));

        try (RingWalFile recovered = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(
                new RingWalSuperblock.State(0L, 0L, 0L, DATA_CAPACITY),
                recovered.state()
            );
        }

        assertFalse(Files.exists(stagingPath), "crash residue must not survive successful publication");
        assertEquals(TOTAL_CAPACITY, Files.size(path));
    }

    @Test
    void rejectsExistingFileWithDifferentConfiguredCapacity() throws Exception {
        Path path = tempDir.resolve("wrong-size.wal");
        Files.write(path, new byte[32]);

        assertThrows(WalCorruptionException.class, () -> new RingWalFile(path, TOTAL_CAPACITY));
        assertEquals(32L, Files.size(path), "recovery failures must never delete a pre-existing WAL");
    }

    @Test
    void rejectsWriteWhoseBytesDoNotMatchAllocation() throws Exception {
        Path path = tempDir.resolve("length.wal");
        try (RingWalFile file = new RingWalFile(path, TOTAL_CAPACITY)) {
            RingWalLayout.Allocation allocation = file.layout().allocate(0, 0, 16);
            assertThrows(
                IllegalArgumentException.class,
                () -> file.write(allocation, ByteBuffer.wrap(new byte[15]))
            );
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }

    private static final class TrackingPreallocationBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicLong preallocatedBytes = new AtomicLong(-1L);

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
        public boolean supportsPreallocation() {
            return true;
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
                    return handle.write(source, position);
                }

                @Override
                public void truncate(long size) throws IOException {
                    handle.truncate(size);
                }

                @Override
                public void preallocate(long size) throws IOException {
                    preallocatedBytes.set(size);
                    handle.preallocate(size);
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

    private static final class FailFirstForceBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicBoolean failFirstForce = new AtomicBoolean(true);

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
                    return handle.write(source, position);
                }

                @Override
                public void truncate(long size) throws IOException {
                    handle.truncate(size);
                }

                @Override
                public void force() throws IOException {
                    if (failFirstForce.compareAndSet(true, false)) {
                        throw new IOException("injected initial ring WAL force failure");
                    }
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
