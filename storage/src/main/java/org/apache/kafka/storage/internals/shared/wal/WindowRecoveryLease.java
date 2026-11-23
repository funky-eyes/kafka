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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
    private final Map<Long, BlockRingRecovery.WindowCandidate> candidates;
    private final int alignment;
    private final int maxBlock;
    private final int maxRecords;
    private boolean closed;

    private WindowRecoveryLease(RingWalOwnership ownership, WalIoBackend.Handle reader,
                                BlockRingRecovery.WindowSnapshot snapshot, int alignment, int maxBlock, int maxRecords) {
        this.ownership = ownership;
        this.reader = reader;
        this.snapshot = snapshot;
        this.alignment = alignment;
        this.maxBlock = maxBlock;
        this.maxRecords = maxRecords;
        Map<Long, BlockRingRecovery.WindowCandidate> found = new HashMap<>();
        for (var item : snapshot.items()) {
            if (item instanceof BlockRingRecovery.WindowCandidate candidate) found.put(candidate.start(), candidate);
        }
        this.candidates = Map.copyOf(found);
    }

    static WindowRecoveryLease open(Path path, WalIoBackend backend, int alignment, long fileSize,
                                    int maxBlock, int maxItems, int maxRecords) throws IOException {
        RingWalOwnership ownership = RingWalOwnership.acquire(path);
        WalIoBackend.Handle reader = null;
        try {
            reader = backend.openRead(path);
            var snapshot = BlockRingCheckpoint.inspectWindowSnapshotByFileSize(reader, alignment, fileSize,
                maxBlock, maxItems, maxRecords);
            return new WindowRecoveryLease(ownership, reader, snapshot, alignment, maxBlock, maxRecords);
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

    /** Classifies candidates while this lease still owns the WAL; it does not publish or reclaim them. */
    synchronized WindowPartitionReconciler.Analysis analyze(
        Map<WalPartitionKey, WindowPartitionReconciler.Boundary> boundaries) throws IOException {
        if (closed) throw new IOException("WAL recovery lease is closed");
        return WindowPartitionReconciler.analyze(snapshot, boundaries);
    }

    /**
     * Reread a scanned block while retaining exclusive ownership. The result is
     * untrusted recovery data, not proof of force, partition authority or visibility.
     */
    synchronized AlignedWalBlockCodec.DecodedBlock readCandidate(long blockStart) throws IOException {
        if (closed) throw new IOException("WAL recovery lease is closed");
        var candidate = candidates.get(blockStart);
        if (candidate == null) throw new IOException("Block is not a recovery candidate");
        BlockRingCheckpoint.validateWindowSnapshot(reader, alignment, snapshot);
        var result = BlockRingRecovery.rereadCandidate(reader, alignment, maxBlock, snapshot, candidate, maxRecords);
        BlockRingCheckpoint.validateWindowSnapshot(reader, alignment, snapshot);
        return result;
    }

    /**
     * Read the complete classified prefix into a bounded staging result. Neither
     * caller-provided offsets nor checksum-valid blocks establish authority or
     * durability; no result authorizes publication, replication or reclamation.
     */
    synchronized List<List<WalRecord>> readContiguous(WalPartitionKey partition,
        WindowPartitionReconciler.Boundary boundary, long maxPayloadBytes) throws IOException {
        if (closed) throw new IOException("WAL recovery lease is closed");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(boundary, "boundary");
        if (maxPayloadBytes < 0) throw new IllegalArgumentException("Negative recovery payload limit");
        var result = WindowPartitionReconciler.analyze(snapshot, Map.of(partition, boundary)).partitions().get(partition);
        if (!result.problems().isEmpty()) throw new IOException("Cannot stage discontinuous partition recovery");
        validatePayloadBudget(result.contiguous(), maxPayloadBytes);
        List<List<WalRecord>> staged = new ArrayList<>(result.contiguous().size());
        long loadedStart = -1;
        AlignedWalBlockCodec.DecodedBlock loaded = null;
        for (var candidate : result.contiguous()) {
            if (candidate.blockStart() != loadedStart) {
                loaded = readCandidate(candidate.blockStart());
                loadedStart = candidate.blockStart();
            }
            staged.add(recordsForGroup(loaded, candidate.group().id()));
        }
        BlockRingCheckpoint.validateWindowSnapshot(reader, alignment, snapshot);
        return List.copyOf(staged);
    }

    private static void validatePayloadBudget(List<WindowPartitionReconciler.CandidateGroup> groups,
                                              long maxPayloadBytes) throws IOException {
        long payloadBytes = 0;
        for (var candidate : groups) {
            for (var entry : candidate.group().entries()) {
                if (entry.payloadLength() > maxPayloadBytes - payloadBytes)
                    throw new IOException("Recovery payload limit exceeded");
                payloadBytes += entry.payloadLength();
            }
        }
    }

    private static List<WalRecord> recordsForGroup(AlignedWalBlockCodec.DecodedBlock block, long groupId)
        throws IOException {
        for (var group : block.groups()) {
            if (group.id() == groupId) {
                List<WalRecord> records = new ArrayList<>(group.records().size());
                for (var entry : group.records()) records.add(entry.record());
                return List.copyOf(records);
            }
        }
        throw new IOException("Recovery group disappeared");
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
