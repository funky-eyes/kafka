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

class RingSharedWalLifecycleTest {
    private static final long DATA_CAPACITY = 1024L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void closeTimeoutDoesNotBlockOnWriterIoLockAndWriterEventuallyClosesResources() throws Exception {
        BlockingForceBackend backend = new BlockingForceBackend(2);
        RingSharedWal wal = new RingSharedWal(
            tempDir.resolve("ring-close-timeout.wal"),
            TOTAL_CAPACITY,
            backend,
            100L
        );

        CompletableFuture<List<WalAppendResult>> append = wal.appendBatch(List.of(
            WalRecord.data(1L, 2L, 0, 3, 0L, 0L, new byte[32])
        ));
        assertTrue(backend.blockedForceEntered.await(10, TimeUnit.SECONDS),
            "writer must reach the injected durability stall before close");

        long startedNanos = System.nanoTime();
        IOException closeFailure = assertThrows(IOException.class, wal::close);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);

        assertTrue(closeFailure.getMessage().contains("Timed out waiting for ring WAL writer to stop"));
        assertTrue(elapsedMillis < 2_000L,
            "close must return its configured timeout instead of blocking on the writer I/O monitor");
        assertFalse(backend.backendClosed.await(100, TimeUnit.MILLISECONDS),
            "resources must not be closed concurrently while the writer still owns the I/O path");

        backend.releaseBlockedForce.countDown();
        assertEquals(1, append.get(10, TimeUnit.SECONDS).size());
        assertTrue(backend.backendClosed.await(10, TimeUnit.SECONDS),
            "writer exit must eventually close backend-owned resources after a timed-out close");
        assertEquals(1, backend.backendCloseCount.get());

        wal.close();
        assertEquals(1, backend.backendCloseCount.get(), "repeated close must not close resources twice");
    }

    private static final class BlockingForceBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final int blockedForceCall;
        private final AtomicInteger forceCalls = new AtomicInteger();
        private final AtomicBoolean blocked = new AtomicBoolean(false);
        private final AtomicInteger backendCloseCount = new AtomicInteger();
        private final CountDownLatch blockedForceEntered = new CountDownLatch(1);
        private final CountDownLatch releaseBlockedForce = new CountDownLatch(1);
        private final CountDownLatch backendClosed = new CountDownLatch(1);

        private BlockingForceBackend(int blockedForceCall) {
            this.blockedForceCall = blockedForceCall;
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
                    int call = forceCalls.incrementAndGet();
                    if (call == blockedForceCall && blocked.compareAndSet(false, true)) {
                        blockedForceEntered.countDown();
                        try {
                            releaseBlockedForce.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted while holding injected ring WAL force", e);
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
}
