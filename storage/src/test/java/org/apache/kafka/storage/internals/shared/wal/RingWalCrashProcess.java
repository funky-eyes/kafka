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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Child JVM used by {@link RingWalProcessCrashTest} to establish a deterministic process-crash durability window.
 */
public final class RingWalCrashProcess {
    private static final long DATA_CAPACITY = 128L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    private RingWalCrashProcess() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected WAL path and crash marker path");
        }
        Path walPath = Path.of(args[0]);
        Path crashMarker = Path.of(args[1]);
        BlockingSuperblockBackend backend = new BlockingSuperblockBackend();

        try (RingWalFile file = new RingWalFile(walPath, TOTAL_CAPACITY, backend)) {
            RingWalLayout.Allocation first = file.layout().allocate(0L, 0L, 100);
            file.write(first, ByteBuffer.wrap(new byte[100]));
            file.forceAndCheckpoint(0L, first.nextTailOffset());

            RingWalSuperblock.State reclaimState = file.forceAndCheckpoint(
                first.nextTailOffset(),
                first.nextTailOffset()
            );
            RingWalLayout.Allocation reused = file.layout().allocate(
                reclaimState.headOffset(),
                reclaimState.tailOffset(),
                40
            );
            byte[] replacement = new byte[40];
            replacement[0] = 9;
            file.write(reused, ByteBuffer.wrap(replacement));

            backend.blockBeforeWriteAt(RingWalSuperblock.SUPERBLOCK_BYTES, crashMarker);
            file.forceAndCheckpoint(reclaimState.headOffset(), reused.nextTailOffset());
            throw new AssertionError("Checkpoint unexpectedly completed past the armed process-crash boundary");
        }
    }

    private static final class BlockingSuperblockBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicLong blockedWritePosition = new AtomicLong(-1L);
        private final CountDownLatch processKill = new CountDownLatch(1);
        private volatile Path crashMarker;

        void blockBeforeWriteAt(long position, Path marker) {
            crashMarker = marker;
            blockedWritePosition.set(position);
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
        public boolean supportsPreallocation() {
            return delegate.supportsPreallocation();
        }

        @Override
        public boolean supportsDirectIo() {
            return delegate.supportsDirectIo();
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
                    blockIfArmed(position);
                    return handle.write(source, position);
                }

                @Override
                public void truncate(long size) throws IOException {
                    handle.truncate(size);
                }

                @Override
                public void preallocate(long size) throws IOException {
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

        private void blockIfArmed(long position) throws IOException {
            if (position != blockedWritePosition.get() ||
                !blockedWritePosition.compareAndSet(position, -1L)) {
                return;
            }
            Path marker = crashMarker;
            if (marker == null) {
                throw new IOException("Ring WAL crash marker was not configured");
            }
            Files.writeString(
                marker,
                "replacement-forced-before-seq3-superblock",
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
            try {
                processKill.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for parent process kill", e);
            }
        }
    }
}
