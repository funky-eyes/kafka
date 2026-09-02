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
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingWalCheckpointFailureTest {
    private static final long DATA_CAPACITY = 128L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void fencesLiveWalWhenFallbackMirrorForceFails() throws Exception {
        Path path = tempDir.resolve("checkpoint-fence.wal");
        FailForceAfterWriteBackend backend = new FailForceAfterWriteBackend();
        long firstTail;

        try (RingWalFile file = new RingWalFile(path, TOTAL_CAPACITY, backend)) {
            RingWalLayout.Allocation first = file.layout().allocate(0L, 0L, 100);
            file.write(first, ByteBuffer.wrap(new byte[100]));
            file.forceAndCheckpoint(0L, first.nextTailOffset());
            firstTail = first.nextTailOffset();

            backend.failForceAfterWriteAt(RingWalSuperblock.SUPERBLOCK_BYTES);
            IOException failure = assertThrows(
                IOException.class,
                () -> file.forceAndCheckpoint(firstTail, firstTail)
            );
            assertEquals("injected fallback mirror force failure", failure.getMessage());

            IllegalStateException stateFailure = assertThrows(IllegalStateException.class, file::state);
            assertSame(failure, stateFailure.getCause());

            RingWalLayout.Allocation next = file.layout().allocate(0L, firstTail, 20);
            assertThrows(
                IllegalStateException.class,
                () -> file.write(next, ByteBuffer.wrap(new byte[20]))
            );
            assertThrows(
                IllegalStateException.class,
                () -> file.forceAndCheckpoint(0L, firstTail)
            );
        }

        try (RingWalFile reopened = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(
                new RingWalSuperblock.State(2L, firstTail, firstTail, DATA_CAPACITY),
                reopened.state(),
                "reopen must reconcile the durable canonical checkpoint before the WAL becomes usable again"
            );
        }
    }

    private static final class FailForceAfterWriteBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicLong failAfterWritePosition = new AtomicLong(-1L);
        private final AtomicBoolean failNextForce = new AtomicBoolean(false);

        void failForceAfterWriteAt(long position) {
            failAfterWritePosition.set(position);
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
                    int written = handle.write(source, position);
                    if (position == failAfterWritePosition.get() &&
                        failAfterWritePosition.compareAndSet(position, -1L)) {
                        failNextForce.set(true);
                    }
                    return written;
                }

                @Override
                public void truncate(long size) throws IOException {
                    handle.truncate(size);
                }

                @Override
                public void force() throws IOException {
                    if (failNextForce.compareAndSet(true, false)) {
                        throw new IOException("injected fallback mirror force failure");
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
