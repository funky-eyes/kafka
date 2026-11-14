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
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSharedWalIoBackendTest {

    @TempDir
    Path tempDir;

    @Test
    void appendFutureWaitsForBackendDurabilityBarrier() throws Exception {
        BlockingForceBackend backend = new BlockingForceBackend();
        try (FileSharedWal wal = new FileSharedWal(tempDir, 1024 * 1024, 64 * 1024, backend)) {
            WalRecord record = WalRecord.data(
                1L,
                2L,
                0,
                3,
                10L,
                10L,
                ByteBuffer.wrap("durable".getBytes(StandardCharsets.UTF_8))
            );
            CompletableFuture<List<WalAppendResult>> append = wal.appendBatch(List.of(record));

            assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
            assertFalse(append.isDone(), "append future must not complete before the backend force returns");

            backend.allowForce.countDown();
            WalAppendResult result = append.get(10, TimeUnit.SECONDS).get(0);
            WalLocation location = new WalLocation(
                result.segmentId(),
                result.position(),
                result.length(),
                record.leaderEpoch(),
                record.firstOffset(),
                record.lastOffset()
            );
            assertArrayEquals(
                "durable".getBytes(StandardCharsets.UTF_8),
                bytes(wal.read(location).payload())
            );
        }

        // If fsync/force fails, no Kafka durability ACK may be released.
        // The writer must fence subsequent appends rather than continuing
        // after a failed durability barrier. Replaying unacknowledged bytes
        // later is possible and is not by itself a durability violation.
        BlockingForceBackend failedBackend = new BlockingForceBackend(true);
        Path failedDir = tempDir.resolve("failed-durability");
        try (FileSharedWal wal = new FileSharedWal(failedDir, 1024 * 1024, 64 * 1024, failedBackend)) {
            WalRecord pending = WalRecord.data(1L, 2L, 0, 3, 11L, 11L,
                ByteBuffer.wrap("not-acked".getBytes(StandardCharsets.UTF_8)));
            CompletableFuture<List<WalAppendResult>> failedAppend = wal.appendBatch(List.of(pending));
            assertTrue(failedBackend.forceEntered.await(10, TimeUnit.SECONDS));
            assertFalse(failedAppend.isDone(), "no ACK before the failing force barrier");
            failedBackend.allowForce.countDown();
            ExecutionException firstFailure = assertThrows(ExecutionException.class,
                () -> failedAppend.get(10, TimeUnit.SECONDS));
            assertTrue(firstFailure.getCause() instanceof IOException,
                "force failure must propagate to caller");
            ExecutionException nextFailure = assertThrows(ExecutionException.class,
                () -> wal.append(pending).get(10, TimeUnit.SECONDS));
            assertTrue(nextFailure.getCause() instanceof IllegalStateException,
                "a failed WAL writer must reject subsequent appends");
        }
    }


    @Test
    void failedLaterDataForceKeepsPreviouslyAcknowledgedFileWalPrefixRecoverable() throws Exception {
        Path dir = tempDir.resolve("confirmed-prefix-survives-force-error");
        BlockingForceBackend backend = new BlockingForceBackend();
        WalRecord confirmed = WalRecord.data(1L, 2L, 0, 3, 0L, 0L,
            ByteBuffer.wrap("confirmed".getBytes(StandardCharsets.UTF_8)));
        WalRecord attempted = WalRecord.data(1L, 2L, 0, 3, 1L, 1L,
            ByteBuffer.wrap("no-ack".getBytes(StandardCharsets.UTF_8)));

        // Allow the first fsync to complete, then inject a failure only for
        // the next group. The older acknowledged prefix must remain intact.
        backend.allowForce.countDown();
        try (FileSharedWal wal = new FileSharedWal(dir, 1024 * 1024, 64 * 1024, backend)) {
            wal.appendBatch(List.of(confirmed)).get(10, TimeUnit.SECONDS);
            backend.failNextForce();
            ExecutionException failed = assertThrows(ExecutionException.class,
                () -> wal.appendBatch(List.of(attempted)).get(10, TimeUnit.SECONDS));
            assertTrue(failed.getCause() instanceof IOException,
                "an unsuccessful fsync must not release the append future");
            ExecutionException fenced = assertThrows(ExecutionException.class,
                () -> wal.appendBatch(List.of(attempted)).get(10, TimeUnit.SECONDS));
            assertTrue(fenced.getCause() instanceof IllegalStateException,
                "a failed WAL writer must reject later replica appends");
        }

        try (FileSharedWal reopened = new FileSharedWal(dir, 1024 * 1024, 64 * 1024)) {
            List<WalRecord> replayed = new ArrayList<>();
            reopened.replay((record, ignored) -> replayed.add(record));
            assertFalse(replayed.isEmpty(), "the previously acknowledged record must survive cold replay");
            assertArrayEquals(bytes(confirmed.payload()), bytes(replayed.get(0).payload()));
            // A failed fsync cannot prove whether its bytes reached stable media:
            // do not assume that a later unacknowledged record must disappear.
        }
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] result = new byte[copy.remaining()];
        copy.get(result);
        return result;
    }

    private static final class BlockingForceBackend implements WalIoBackend {
        private final FileChannelWalIoBackend delegate = new FileChannelWalIoBackend();
        private final CountDownLatch forceEntered = new CountDownLatch(1);
        private final CountDownLatch allowForce = new CountDownLatch(1);
        private final AtomicBoolean failForce;

        private BlockingForceBackend() {
            this(false);
        }

        private BlockingForceBackend(boolean failForce) {
            this.failForce = new AtomicBoolean(failForce);
        }

        void failNextForce() {
            failForce.set(true);
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
                    forceEntered.countDown();
                    try {
                        if (!allowForce.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out waiting to release test durability barrier");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted at test durability barrier", e);
                    }
                    if (failForce.compareAndSet(true, false)) {
                        throw new IOException("Injected WAL force failure");
                    }
                    delegateHandle.force();
                }

                @Override
                public void seal() throws IOException {
                    force();
                    delegateHandle.close();
                }

                @Override
                public void close() throws IOException {
                    delegateHandle.close();
                }
            };
        }
    }
}
