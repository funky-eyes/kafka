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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/** Advisory single-writer ownership for cooperating experimental ring WALs. */
final class RingWalOwnership implements AutoCloseable {
    private static final Set<Path> LOCAL_OWNERS = new HashSet<>();
    private final Path path;
    private final FileChannel channel;
    private final FileLock lock;
    private final AtomicBoolean closed = new AtomicBoolean();

    private RingWalOwnership(Path path, FileChannel channel, FileLock lock) {
        this.path = path;
        this.channel = channel;
        this.lock = lock;
    }

    static RingWalOwnership acquire(Path path) throws IOException {
        Path normalized = path.toAbsolutePath().normalize();
        Path parent = normalized.getParent();
        Path filename = normalized.getFileName();
        if (parent == null || filename == null) throw new IOException("Invalid WAL file path: " + normalized);
        Path absolute = parent.toRealPath().resolve(filename);
        if (Files.isSymbolicLink(absolute)) throw new IOException("Refusing symbolic-link WAL path: " + absolute);
        Path lockPath = absolute.resolveSibling(absolute.getFileName() + ".lock");
        synchronized (LOCAL_OWNERS) {
            if (!LOCAL_OWNERS.add(lockPath)) throw new IOException("WAL is already owned: " + absolute);
        }
        FileChannel channel = null;
        try {
            channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS);
            FileLock lock;
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                throw new IOException("WAL is already owned: " + absolute, e);
            }
            if (lock == null) throw new IOException("WAL is already owned: " + absolute);
            return new RingWalOwnership(lockPath, channel, lock);
        } catch (Throwable t) {
            try {
                if (channel != null) channel.close();
            } catch (Throwable close) {
                t.addSuppressed(close);
            }
            synchronized (LOCAL_OWNERS) {
                LOCAL_OWNERS.remove(lockPath);
            }
            throw t;
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;
        try {
            lock.release();
        } finally {
            try {
                channel.close();
            } finally {
                synchronized (LOCAL_OWNERS) {
                    LOCAL_OWNERS.remove(path);
                }
            }
        }
    }
}
