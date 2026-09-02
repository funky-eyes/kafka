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

import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/** Portable reference {@link WalIoBackend} backed by {@link FileChannel}. */
public final class FileChannelWalIoBackend implements WalIoBackend {
    private static final int PREALLOCATION_CHUNK_BYTES = 1024 * 1024;
    private static final ByteBuffer ZERO_CHUNK = ByteBuffer.allocate(PREALLOCATION_CHUNK_BYTES).asReadOnlyBuffer();

    @Override
    public Handle openRead(Path path) throws IOException {
        return new FileChannelHandle(path, FileChannel.open(path, StandardOpenOption.READ), false);
    }

    @Override
    public Handle reopen(Path path) throws IOException {
        return new FileChannelHandle(
            path,
            FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE),
            false
        );
    }

    @Override
    public Handle create(Path path) throws IOException {
        return new FileChannelHandle(
            path,
            FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            ),
            true
        );
    }

    @Override
    public long size(Path path) throws IOException {
        return Files.size(path);
    }

    @Override
    public boolean supportsPreallocation() {
        return true;
    }

    private static final class FileChannelHandle implements Handle {
        private final Path path;
        private final FileChannel channel;
        private boolean directoryEntryDirty;

        private FileChannelHandle(Path path, FileChannel channel, boolean directoryEntryDirty) {
            this.path = path;
            this.channel = channel;
            this.directoryEntryDirty = directoryEntryDirty;
        }

        @Override
        public long size() throws IOException {
            return channel.size();
        }

        @Override
        public int read(ByteBuffer destination, long position) throws IOException {
            return channel.read(destination, position);
        }

        @Override
        public int write(ByteBuffer source, long position) throws IOException {
            return channel.write(source, position);
        }

        @Override
        public void truncate(long size) throws IOException {
            channel.truncate(size);
        }

        @Override
        public void preallocate(long size) throws IOException {
            if (size <= 0) {
                throw new IllegalArgumentException("preallocation size must be positive");
            }
            long currentSize = channel.size();
            if (currentSize != 0L) {
                throw new IOException(
                    "WAL preallocation requires a newly-created empty file: path=" + path + ", size=" + currentSize);
            }

            long position = 0L;
            while (position < size) {
                int chunkBytes = (int) Math.min(PREALLOCATION_CHUNK_BYTES, size - position);
                ByteBuffer zeroes = ZERO_CHUNK.duplicate();
                zeroes.limit(chunkBytes);
                while (zeroes.hasRemaining()) {
                    int written = channel.write(zeroes, position);
                    if (written <= 0) {
                        throw new IOException("Unable to make progress preallocating WAL at position " + position);
                    }
                    position = Math.addExact(position, written);
                }
            }
            if (channel.size() != size) {
                throw new IOException(
                    "WAL preallocation established unexpected size: expected=" + size + ", actual=" + channel.size());
            }
        }

        @Override
        public void force() throws IOException {
            channel.force(false);
            if (directoryEntryDirty) {
                Path parent = path.toAbsolutePath().normalize().getParent();
                if (parent != null) {
                    Utils.flushDir(parent);
                }
                directoryEntryDirty = false;
            }
        }

        @Override
        public void seal() throws IOException {
            IOException error = null;
            try {
                force();
            } catch (IOException e) {
                error = e;
            }
            try {
                close();
            } catch (IOException e) {
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
            if (error != null) {
                throw error;
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
