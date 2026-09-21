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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSharedWalLifecycleTest {

    @TempDir
    Path tempDir;

    @Test
    void closeOwnsBackendAndActiveHandleExactlyOnce() throws Exception {
        CountingBackend backend = new CountingBackend();
        FileSharedWal wal = new FileSharedWal(tempDir.resolve("wal"), 1024 * 1024, 64 * 1024, backend);
        wal.append(WalRecord.data(1L, 2L, 0, 3, 10L, 10L, new byte[]{1}))
            .get(10, TimeUnit.SECONDS);

        wal.close();
        assertEquals(1, backend.handleCloseCount.get(), "close must release the active physical WAL handle");
        assertEquals(1, backend.backendCloseCount.get(), "close must release backend-owned resources");

        wal.close();
        assertEquals(1, backend.handleCloseCount.get(), "repeated close must not close the handle twice");
        assertEquals(1, backend.backendCloseCount.get(), "repeated close must not close the backend twice");
    }

    @Test
    void closeTimeoutLeavesWriterIoOpenUntilWriterExits() throws Exception {
        BlockingForceBackend backend = new BlockingForceBackend();
        FileSharedWal wal = new FileSharedWal(
            tempDir.resolve("timeout-wal"),
            1024 * 1024,
            64 * 1024,
            backend,
            100L
        );
        backend.blockNextForce.set(true);
        CompletableFuture<List<WalAppendResult>> append = wal.appendBatch(List.of(
            WalRecord.data(1L, 2L, 0, 3, 10L, 10L, new byte[32])
        ));
        assertTrue(backend.blockedForceEntered.await(10, TimeUnit.SECONDS),
            "writer must reach the injected durability stall before close");

        long startedNanos = System.nanoTime();
        IOException closeFailure = assertThrows(IOException.class, wal::close);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

        assertTrue(closeFailure.getMessage().contains("Timed out waiting for WAL writer to stop after 100 ms"));
        assertTrue(elapsedMillis < 2_000L, "close must return its configured writer timeout");
        assertFalse(backend.backendClosed.await(100, TimeUnit.MILLISECONDS),
            "backend must stay open while the writer still owns the I/O path");

        backend.releaseBlockedForce.countDown();
        assertEquals(1, append.get(10, TimeUnit.SECONDS).size());
        assertTrue(backend.backendClosed.await(10, TimeUnit.SECONDS),
            "writer exit must eventually close backend-owned resources");
        assertEquals(1, backend.backendCloseCount.get());

        wal.close();
        assertEquals(1, backend.backendCloseCount.get(), "repeated close must not close resources twice");
    }

    private static final class BlockingForceBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicBoolean blockNextForce = new AtomicBoolean();
        private final AtomicBoolean blocked = new AtomicBoolean();
        private final AtomicInteger backendCloseCount = new AtomicInteger();
        private final CountDownLatch blockedForceEntered = new CountDownLatch(1);
        private final CountDownLatch releaseBlockedForce = new CountDownLatch(1);
        private final CountDownLatch backendClosed = new CountDownLatch(1);

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
            backendCloseCount.incrementAndGet();
            try {
                delegate.close();
            } finally {
                backendClosed.countDown();
            }
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
                    if (blockNextForce.get() && blocked.compareAndSet(false, true)) {
                        blockedForceEntered.countDown();
                        try {
                            releaseBlockedForce.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted while holding injected WAL force", e);
                        }
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

    private static final class CountingBackend implements WalIoBackend {
        private final FileChannelWalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicInteger handleCloseCount = new AtomicInteger();
        private final AtomicInteger backendCloseCount = new AtomicInteger();

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
            backendCloseCount.incrementAndGet();
            delegate.close();
        }

        private Handle wrap(Handle delegateHandle) {
            return new Handle() {
                @Override
                public long size() throws IOException {
                    return delegateHandle.size();
                }

                @Override
                public int read(ByteBuffer destination, long position) throws IOException {
                    return delegateHandle.read(destination, position);
                }

                @Override
                public int write(ByteBuffer source, long position) throws IOException {
                    return delegateHandle.write(source, position);
                }

                @Override
                public void truncate(long size) throws IOException {
                    delegateHandle.truncate(size);
                }

                @Override
                public void force() throws IOException {
                    delegateHandle.force();
                }

                @Override
                public void seal() throws IOException {
                    delegateHandle.seal();
                }

                @Override
                public void close() throws IOException {
                    handleCloseCount.incrementAndGet();
                    delegateHandle.close();
                }
            };
        }
    }
}
