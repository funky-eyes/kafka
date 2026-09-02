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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RingWalPreallocationFailureTest {
    private static final long DATA_CAPACITY = 512L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;

    @TempDir
    Path tempDir;

    @Test
    void preallocationFailureNeverPublishesPartialRingWalAndRetrySucceeds() throws Exception {
        Path path = tempDir.resolve("preallocation-failure.wal");
        Path stagingPath = RingWalFile.initializationPath(path);
        FailingPreallocationBackend backend = new FailingPreallocationBackend();

        IOException failure = assertThrows(
            IOException.class,
            () -> new RingWalFile(path, TOTAL_CAPACITY, backend)
        );

        assertEquals("injected ring WAL preallocation failure", failure.getMessage());
        assertTrue(backend.preallocationAttempted.get());
        assertFalse(Files.exists(path),
            "capacity reservation failure must never publish a partial durable Ring WAL");
        assertFalse(Files.exists(stagingPath),
            "capacity reservation failure must clean the non-authoritative staging file");

        try (RingWalFile retried = new RingWalFile(path, TOTAL_CAPACITY)) {
            assertEquals(
                new RingWalSuperblock.State(0L, 0L, 0L, DATA_CAPACITY),
                retried.state()
            );
        }
        assertEquals(TOTAL_CAPACITY, Files.size(path));
    }

    private static final class FailingPreallocationBackend implements WalIoBackend {
        private final WalIoBackend delegate = new FileChannelWalIoBackend();
        private final AtomicBoolean preallocationAttempted = new AtomicBoolean(false);

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
                    preallocationAttempted.set(true);
                    ByteBuffer partial = ByteBuffer.allocate(128);
                    int written = handle.write(partial, 0L);
                    if (written <= 0) {
                        throw new IOException("unable to stage injected preallocation bytes");
                    }
                    throw new IOException("injected ring WAL preallocation failure");
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
