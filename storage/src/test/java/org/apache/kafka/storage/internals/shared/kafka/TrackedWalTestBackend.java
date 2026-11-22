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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.wal.AsynchronousFileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.FileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.WalIoBackend;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class TrackedWalTestBackend implements WalIoBackend {
    private final WalIoBackend delegate;
    private final int failAt;
    private final AtomicInteger forces = new AtomicInteger();
    final CountDownLatch forceEntered = new CountDownLatch(1);
    final CountDownLatch forceRelease = new CountDownLatch(1);
    final CountDownLatch checkpointForced = new CountDownLatch(1);
    private volatile boolean armed;

    TrackedWalTestBackend(boolean aio, int failAt) {
        delegate = aio ? new AsynchronousFileChannelWalIoBackend() : new FileChannelWalIoBackend();
        this.failAt = failAt;
    }

    void arm() {
        armed = true;
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
            public void writeBatch(List<WriteRequest> writes) throws IOException {
                handle.writeBatch(writes);
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
                int force = armed ? forces.incrementAndGet() : 0;
                if (force == 1) {
                    forceEntered.countDown();
                    try {
                        if (!forceRelease.await(10, TimeUnit.SECONDS)) {
                            throw new IOException("timed out waiting for data force");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted while waiting for data force", e);
                    }
                }
                if (force == failAt && failAt != 0) {
                    throw new IOException("injected force failure " + force);
                }
                handle.force();
                if (force == 2) {
                    checkpointForced.countDown();
                }
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
