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
import java.nio.file.Path;

/**
 * Holds cooperative exclusive ownership while inspecting an experimental windowed WAL.
 * The snapshot is untrusted evidence, not authorization to publish or reclaim data.
 * No writer may start until recovery is reconciled and ownership is transferred safely.
 * The caller retains ownership of the backend itself.
 */
final class WindowRecoveryLease implements AutoCloseable {
    private final RingWalOwnership ownership;
    private final WalIoBackend.Handle reader;
    private final BlockRingRecovery.WindowSnapshot snapshot;
    private boolean closed;

    private WindowRecoveryLease(RingWalOwnership ownership, WalIoBackend.Handle reader,
                                BlockRingRecovery.WindowSnapshot snapshot) {
        this.ownership = ownership;
        this.reader = reader;
        this.snapshot = snapshot;
    }

    static WindowRecoveryLease open(Path path, WalIoBackend backend, int alignment, long fileSize,
                                    int maxBlock, int maxItems, int maxRecords) throws IOException {
        RingWalOwnership ownership = RingWalOwnership.acquire(path);
        WalIoBackend.Handle reader = null;
        try {
            reader = backend.openRead(path);
            var snapshot = BlockRingCheckpoint.inspectWindowSnapshotByFileSize(reader, alignment, fileSize,
                maxBlock, maxItems, maxRecords);
            return new WindowRecoveryLease(ownership, reader, snapshot);
        } catch (Throwable failure) {
            try {
                if (reader != null) reader.close();
            } catch (Throwable close) {
                failure.addSuppressed(close);
            }
            try {
                ownership.close();
            } catch (Throwable close) {
                failure.addSuppressed(close);
            }
            throw failure;
        }
    }

    /** The snapshot may only be used while this lease remains open. */
    synchronized BlockRingRecovery.WindowSnapshot snapshot() throws IOException {
        if (closed) throw new IOException("WAL recovery lease is closed");
        return snapshot;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        try (RingWalOwnership ignoredOwnership = ownership; WalIoBackend.Handle ignoredReader = reader) {
            // Close the reader before releasing exclusive ownership.
        }
    }
}
