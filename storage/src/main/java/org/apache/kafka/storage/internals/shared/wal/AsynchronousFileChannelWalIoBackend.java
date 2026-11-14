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
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Java NIO.2 AIO positional I/O backend for the existing WAL durability contract.
 *
 * <p>Reads and writes are submitted through {@link AsynchronousFileChannel}.
 * The current WAL state machines expose synchronous positional operations, so
 * each operation waits for its AIO completion before accepting another write
 * or executing {@code force(false)}. This is deliberately a correctness-first
 * AIO backend, not yet a pipelined/non-blocking WAL writer. Neither
 * {@code AsynchronousFileChannel} nor its thread pool promises kernel-native
 * asynchronous I/O on every JVM or filesystem.</p>
 *
 * <p>The WAL state machine remains the owner of GROUP_COMMIT, force ordering,
 * crash recovery and Kafka ACK decisions. No java.io file-stream backend is
 * used for the physical WAL data path.</p>
 */
public final class AsynchronousFileChannelWalIoBackend implements WalIoBackend {
    private static final int PREALLOCATION_CHUNK_BYTES = 1024 * 1024;
    private static final ByteBuffer ZERO_CHUNK = ByteBuffer.allocate(PREALLOCATION_CHUNK_BYTES).asReadOnlyBuffer();

    private final ExecutorService ioExecutor = Executors.newFixedThreadPool(2, task -> {
        Thread worker = new Thread(task, "shared-wal-java-aio");
        worker.setDaemon(true);
        return worker;
    });

    @Override
    public Handle openRead(Path path) throws IOException {
        return new AioHandle(path,
            AsynchronousFileChannel.open(path, Set.of(StandardOpenOption.READ), ioExecutor), false);
    }

    @Override
    public Handle reopen(Path path) throws IOException {
        return new AioHandle(path,
            AsynchronousFileChannel.open(path,
                Set.of(StandardOpenOption.READ, StandardOpenOption.WRITE), ioExecutor), false);
    }

    @Override
    public Handle create(Path path) throws IOException {
        return new AioHandle(path,
            AsynchronousFileChannel.open(path,
                Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE),
                ioExecutor), true);
    }

    @Override
    public long size(Path path) throws IOException {
        return Files.size(path);
    }

    @Override
    public boolean supportsPreallocation() {
        return true;
    }

    @Override
    public void close() {
        ioExecutor.shutdown();
    }

    private static int await(Future<Integer> io) throws IOException {
        try {
            return io.get();
        } catch (InterruptedException e) {
            io.cancel(true);
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for asynchronous WAL I/O", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ioFailure) {
                throw ioFailure;
            }
            throw new IOException("Asynchronous WAL I/O failed", cause);
        } catch (CancellationException e) {
            throw new IOException("Asynchronous WAL I/O was cancelled", e);
        }
    }

    private static final class AioHandle implements Handle {
        private final Path path;
        private final AsynchronousFileChannel channel;
        private boolean directoryEntryDirty;

        private AioHandle(Path path, AsynchronousFileChannel channel, boolean newlyCreated) {
            this.path = path;
            this.channel = channel;
            this.directoryEntryDirty = newlyCreated;
        }

        @Override
        public long size() throws IOException {
            return channel.size();
        }

        @Override
        public int read(ByteBuffer destination, long position) throws IOException {
            // The WAL state machine needs completed read bytes before parsing a
            // record header. Submit through Java AIO, then await that result.
            return await(channel.read(destination, position));
        }

        @Override
        public int write(ByteBuffer source, long position) throws IOException {
            // No subsequent force or GROUP_COMMIT publication may overtake an
            // unfinished AIO write. Full writes are retried by the caller.
            return await(channel.write(source, position));
        }

        @Override
        public void truncate(long size) throws IOException {
            channel.truncate(size);
        }

        @Override
        public void preallocate(long size) throws IOException {
            if (size <= 0L) {
                throw new IllegalArgumentException("preallocation size must be positive");
            }
            long current = channel.size();
            if (current != 0L) {
                throw new IOException(
                    "WAL preallocation requires a newly-created empty file: path=" + path + ", size=" + current);
            }

            long position = 0L;
            while (position < size) {
                int chunkBytes = (int) Math.min(PREALLOCATION_CHUNK_BYTES, size - position);
                ByteBuffer zeros = ZERO_CHUNK.duplicate();
                zeros.limit(chunkBytes);
                while (zeros.hasRemaining()) {
                    int written = write(zeros, position);
                    if (written <= 0) {
                        throw new IOException("Unable to make progress preallocating WAL at position " + position);
                    }
                    position = Math.addExact(position, written);
                }
            }
            if (channel.size() != size) {
                throw new IOException(
                    "WAL preallocation established unexpected size: expected=" + size +
                        ", actual=" + channel.size());
            }
        }

        @Override
        public void force() throws IOException {
            // Java AsynchronousFileChannel.force is synchronous. There is no
            // safe asynchronous durability completion API in Java NIO.2.
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
