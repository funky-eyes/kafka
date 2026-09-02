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
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileChannelWalIoBackendTest {

    @TempDir
    Path tempDir;

    @Test
    void createForceReadReopenAndTruncatePreservePositionalContract() throws Exception {
        Path path = tempDir.resolve("wal-unit");
        try (WalIoBackend backend = new FileChannelWalIoBackend()) {
            try (WalIoBackend.Handle handle = backend.create(path)) {
                assertEquals(3, handle.write(ByteBuffer.wrap(new byte[] {1, 2, 3}), 0));
                assertEquals(2, handle.write(ByteBuffer.wrap(new byte[] {8, 9}), 5));
                handle.force();
                assertEquals(7, handle.size());
            }

            assertEquals(7, backend.size(path));
            try (WalIoBackend.Handle handle = backend.openRead(path)) {
                ByteBuffer bytes = ByteBuffer.allocate(7);
                assertEquals(7, handle.read(bytes, 0));
                assertArrayEquals(new byte[] {1, 2, 3, 0, 0, 8, 9}, bytes.array());
            }

            try (WalIoBackend.Handle handle = backend.reopen(path)) {
                handle.truncate(3);
                handle.force();
            }
            assertEquals(3, backend.size(path));
        }
    }

    @Test
    void preallocationMaterializesFreshFileBeforePublication() throws Exception {
        Path path = tempDir.resolve("preallocated");
        long capacity = 2L * 1024L * 1024L + 17L;
        try (WalIoBackend backend = new FileChannelWalIoBackend();
             WalIoBackend.Handle handle = backend.create(path)) {
            assertTrue(backend.supportsPreallocation());
            handle.preallocate(capacity);
            handle.force();
            assertEquals(capacity, handle.size());

            ByteBuffer first = ByteBuffer.allocate(1);
            ByteBuffer last = ByteBuffer.allocate(1);
            assertEquals(1, handle.read(first, 0L));
            assertEquals(1, handle.read(last, capacity - 1L));
            assertArrayEquals(new byte[] {0}, first.array());
            assertArrayEquals(new byte[] {0}, last.array());
        }
    }

    @Test
    void preallocationRejectsNonEmptyPhysicalUnit() throws Exception {
        Path path = tempDir.resolve("non-empty-preallocation");
        try (WalIoBackend backend = new FileChannelWalIoBackend();
             WalIoBackend.Handle handle = backend.create(path)) {
            assertEquals(1, handle.write(ByteBuffer.wrap(new byte[] {1}), 0L));
            IOException failure = assertThrows(IOException.class, () -> handle.preallocate(1024L));
            assertTrue(failure.getMessage().contains("newly-created empty file"));
        }
    }

    @Test
    void createIsExclusiveAndCapabilitiesMatchPortableFileChannelSemantics() throws Exception {
        Path path = tempDir.resolve("exclusive");
        try (WalIoBackend backend = new FileChannelWalIoBackend();
             WalIoBackend.Handle ignored = backend.create(path)) {
            assertThrows(FileAlreadyExistsException.class, () -> backend.create(path));
            assertTrue(backend.supportsPreallocation());
            assertFalse(backend.supportsDirectIo());
        }
    }

    @Test
    void sealForcesAndClosesThePhysicalUnit() throws Exception {
        Path path = tempDir.resolve("sealed");
        try (WalIoBackend backend = new FileChannelWalIoBackend()) {
            WalIoBackend.Handle handle = backend.create(path);
            assertEquals(1, handle.write(ByteBuffer.wrap(new byte[] {7}), 0));
            handle.seal();
            assertThrows(ClosedChannelException.class,
                () -> handle.write(ByteBuffer.wrap(new byte[] {8}), 1));

            try (WalIoBackend.Handle reopened = backend.openRead(path)) {
                ByteBuffer byteBuffer = ByteBuffer.allocate(1);
                assertEquals(1, reopened.read(byteBuffer, 0));
                assertArrayEquals(new byte[] {7}, byteBuffer.array());
            }
        }
    }
}
