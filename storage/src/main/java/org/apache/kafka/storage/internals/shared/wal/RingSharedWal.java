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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed-capacity circular implementation of {@link SharedWal}.
 *
 * <p>Logical WAL offsets only move forward. Physical slots are reused after the durable head advances, while the two
 * alternating superblocks in {@link RingWalFile} make the durable head/tail window crash-safe. Append groups use the
 * same DATA/TRUNCATE + GROUP_COMMIT format as the file WAL and one drained writer batch crosses one physical force +
 * superblock checkpoint barrier.</p>
 *
 * <p>Wrap padding is explicitly zeroed before the next generation is written. Recovery accepts a zero suffix only when
 * a later durable generation exists beyond the physical boundary; non-zero bytes where padding is expected are treated
 * as corruption. This prevents stale bytes from an older generation from being replayed as current WAL records.</p>
 */
public final class RingSharedWal implements SharedWal {
    private static final int MAX_DRAINED_APPENDS = 1024;
    private static final int ZERO_CHUNK_BYTES = 64 * 1024;
    private static final ByteBuffer ZERO_CHUNK = ByteBuffer.allocate(ZERO_CHUNK_BYTES).asReadOnlyBuffer();

    private final RingWalFile file;
    private final RingWalLayout layout;
    private final LinkedBlockingQueue<PendingAppend> pendingAppends = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong nextGroupId;
    private final Object lifecycleLock = new Object();
    private final Object ioLock = new Object();
    private final Thread writerThread;

    private volatile boolean accepting = true;
    private volatile Throwable failure;

    public RingSharedWal(Path path, long totalCapacityBytes) throws IOException {
        this(path, totalCapacityBytes, new FileChannelWalIoBackend());
    }

    RingSharedWal(Path path, long totalCapacityBytes, WalIoBackend ioBackend) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(ioBackend, "ioBackend");
        this.file = new RingWalFile(path, totalCapacityBytes, ioBackend);
        this.layout = file.layout();

        long recoveredNextGroupId;
        try {
            recoveredNextGroupId = Math.addExact(recoverMaxGroupId(), 1L);
        } catch (Throwable recoveryFailure) {
            closeAfterRecoveryFailure(recoveryFailure);
            if (recoveryFailure instanceof IOException ioException) {
                throw ioException;
            }
            if (recoveryFailure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (recoveryFailure instanceof Error error) {
                throw error;
            }
            throw new IOException("Unexpected ring WAL recovery failure", recoveryFailure);
        }
        this.nextGroupId = new AtomicLong(recoveredNextGroupId);

        this.writerThread = new Thread(this::writerLoop, "shared-ring-wal-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    @Override
    public CompletableFuture<List<WalAppendResult>> appendBatch(List<WalRecord> records) {
        Objects.requireNonNull(records, "records");
        CompletableFuture<List<WalAppendResult>> future = new CompletableFuture<>();
        if (records.isEmpty()) {
            future.completeExceptionally(new IllegalArgumentException("records must not be empty"));
            return future;
        }

        long groupId = nextGroupId.getAndIncrement();
        List<WalRecordCodec.EncodedRecord> encoded = new ArrayList<>(records.size() + 1);
        try {
            for (WalRecord record : records) {
                Objects.requireNonNull(record, "record");
                if (record.type() == WalRecordType.GROUP_COMMIT) {
                    throw new IllegalArgumentException("GROUP_COMMIT is internal and cannot be appended directly");
                }
                WalRecordCodec.EncodedRecord encodedRecord = WalRecordCodec.encode(record);
                validateRecordFitsRing(encodedRecord);
                encoded.add(encodedRecord);
            }
            WalRecordCodec.EncodedRecord commit = WalRecordCodec.encode(WalRecord.groupCommit(groupId, records.size()));
            validateRecordFitsRing(commit);
            encoded.add(commit);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }

        synchronized (lifecycleLock) {
            Throwable currentFailure = failure;
            if (currentFailure != null) {
                future.completeExceptionally(new IllegalStateException("Ring WAL writer has failed", currentFailure));
                return future;
            }
            if (!accepting) {
                future.completeExceptionally(new IllegalStateException("Ring WAL is closed"));
                return future;
            }
            pendingAppends.add(new PendingAppend(List.copyOf(encoded), records.size(), future, false));
        }
        return future;
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        Objects.requireNonNull(location, "location");
        synchronized (ioLock) {
            ensureOpen();
            RingWalSuperblock.State state = file.state();
            long endOffset = Math.addExact(location.walOffset(), location.length());
            if (location.walOffset() < state.headOffset() || endOffset > state.tailOffset()) {
                throw new IllegalArgumentException(
                    "WAL location is outside durable ring window: location=" + location +
                        ", durable=[" + state.headOffset() + ", " + state.tailOffset() + ")");
            }
            WalRecordCodec.ReadResult result = WalRecordCodec.read(new LogicalReadHandle(state), location.walOffset());
            if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) {
                throw new WalCorruptionException("WAL location does not point to a complete record: " + location);
            }
            if (result.length() != location.length()) {
                throw new WalCorruptionException(
                    "WAL location length mismatch: expected=" + location.length() + ", actual=" + result.length());
            }
            if (result.record().type() == WalRecordType.GROUP_COMMIT) {
                throw new WalCorruptionException("WAL data location unexpectedly points to GROUP_COMMIT: " + location);
            }
            return result.record();
        }
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        Objects.requireNonNull(consumer, "consumer");
        synchronized (ioLock) {
            ensureOpen();
            RingWalSuperblock.State state = file.state();
            LogicalReadHandle handle = new LogicalReadHandle(state);
            List<ReplayEntry> pendingGroup = new ArrayList<>();
            long cursor = state.headOffset();
            while (cursor < state.tailOffset()) {
                ScanItem item = scanNext(state, handle, cursor);
                cursor = item.nextOffset();
                if (item.record() == null) {
                    continue;
                }
                WalRecord record = item.record();
                if (record.type() == WalRecordType.GROUP_COMMIT) {
                    validateGroupCommit(record, pendingGroup.size(), item.offset());
                    for (ReplayEntry entry : pendingGroup) {
                        consumer.accept(entry.record(), entry.appendResult());
                    }
                    pendingGroup.clear();
                } else {
                    pendingGroup.add(new ReplayEntry(record, new WalAppendResult(item.offset(), item.length())));
                }
            }
            requireCompleteReplayGroup(pendingGroup, state.tailOffset());
        }
    }

    @Override
    public long reclaim(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        Objects.requireNonNull(policy, "policy");
        if (desiredBytes <= 0) {
            throw new IllegalArgumentException("desiredBytes must be positive");
        }

        synchronized (ioLock) {
            ensureOpen();
            RingWalSuperblock.State state = file.state();
            if (state.headOffset() == state.tailOffset()) {
                return 0L;
            }

            LogicalReadHandle handle = new LogicalReadHandle(state);
            List<ReplayEntry> pendingGroup = new ArrayList<>();
            long cursor = state.headOffset();
            long safeHead = state.headOffset();
            while (cursor < state.tailOffset()) {
                ScanItem item = scanNext(state, handle, cursor);
                cursor = item.nextOffset();
                if (item.record() == null) {
                    if (pendingGroup.isEmpty()) {
                        safeHead = cursor;
                        if (safeHead - state.headOffset() >= desiredBytes) {
                            break;
                        }
                    }
                    continue;
                }

                WalRecord record = item.record();
                if (record.type() != WalRecordType.GROUP_COMMIT) {
                    pendingGroup.add(new ReplayEntry(record, new WalAppendResult(item.offset(), item.length())));
                    continue;
                }

                validateGroupCommit(record, pendingGroup.size(), item.offset());
                if (!groupReclaimable(policy, pendingGroup)) {
                    break;
                }
                pendingGroup.clear();
                safeHead = cursor;
                if (safeHead - state.headOffset() >= desiredBytes) {
                    break;
                }
            }

            long reclaimed = safeHead - state.headOffset();
            if (reclaimed <= 0) {
                return 0L;
            }
            file.forceAndCheckpoint(safeHead, state.tailOffset());
            return reclaimed;
        }
    }

    @Override
    public long reclaimedBeforeOffset() {
        synchronized (ioLock) {
            ensureOpen();
            return file.state().headOffset();
        }
    }

    @Override
    public long usedBytes() {
        synchronized (ioLock) {
            ensureOpen();
            RingWalSuperblock.State state = file.state();
            return state.tailOffset() - state.headOffset();
        }
    }

    @Override
    public long capacityBytes() {
        return layout.dataCapacityBytes();
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (lifecycleLock) {
            accepting = false;
            if (running.getAndSet(false)) {
                pendingAppends.offer(PendingAppend.poisonPill());
            }
        }

        IOException closeError = null;
        try {
            writerThread.join(TimeUnit.SECONDS.toMillis(30));
            if (writerThread.isAlive()) {
                closeError = new IOException("Timed out waiting for ring WAL writer to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeError = new IOException("Interrupted while closing ring WAL", e);
        }

        synchronized (ioLock) {
            try {
                file.close();
            } catch (IOException closeFailure) {
                if (closeError == null) {
                    closeError = closeFailure;
                } else {
                    closeError.addSuppressed(closeFailure);
                }
            }
        }
        if (closeError != null) {
            throw closeError;
        }
    }

    private void validateRecordFitsRing(WalRecordCodec.EncodedRecord encoded) {
        if (encoded.totalLength() > layout.dataCapacityBytes()) {
            throw new IllegalArgumentException(
                "Encoded WAL record size " + encoded.totalLength() +
                    " exceeds ring data capacity " + layout.dataCapacityBytes());
        }
    }

    private void writerLoop() {
        List<PendingAppend> drained = new ArrayList<>(MAX_DRAINED_APPENDS);
        while (running.get() || !pendingAppends.isEmpty()) {
            try {
                drained.clear();
                PendingAppend first = pendingAppends.take();
                if (first.poison()) {
                    if (!running.get() && pendingAppends.isEmpty()) {
                        break;
                    }
                    continue;
                }
                drained.add(first);
                while (drained.size() < MAX_DRAINED_APPENDS) {
                    PendingAppend next = pendingAppends.poll();
                    if (next == null) {
                        break;
                    }
                    if (next.poison()) {
                        pendingAppends.offer(next);
                        break;
                    }
                    drained.add(next);
                }
                writeDrainedGroups(drained);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failWriter(new IOException("Ring WAL writer interrupted", e), drained);
                return;
            } catch (Throwable t) {
                failWriter(t, drained);
                return;
            }
        }
    }

    private void writeDrainedGroups(List<PendingAppend> groups) throws IOException {
        synchronized (ioLock) {
            RingWalSuperblock.State durable = file.state();
            long plannedTail = durable.tailOffset();
            List<PlannedGroup> admitted = new ArrayList<>(groups.size());
            int firstRejected = -1;
            WalCapacityExceededException capacityFailure = null;

            for (int i = 0; i < groups.size(); i++) {
                PendingAppend group = groups.get(i);
                try {
                    PlannedGroup plan = planGroup(group, durable.headOffset(), plannedTail);
                    admitted.add(plan);
                    plannedTail = plan.nextTailOffset();
                } catch (WalCapacityExceededException e) {
                    firstRejected = i;
                    capacityFailure = e;
                    break;
                }
            }

            if (firstRejected >= 0) {
                for (int i = firstRejected; i < groups.size(); i++) {
                    groups.get(i).future().completeExceptionally(capacityFailure);
                }
            }
            if (admitted.isEmpty()) {
                return;
            }

            for (PlannedGroup group : admitted) {
                for (PlannedRecord record : group.records()) {
                    writePadding(record.allocation());
                    writeEncoded(record.allocation().walOffset(), record.encoded());
                }
            }
            file.forceAndCheckpoint(durable.headOffset(), plannedTail);
            for (PlannedGroup group : admitted) {
                group.pending().future().complete(group.userResults());
            }
        }
    }

    private PlannedGroup planGroup(PendingAppend pending, long headOffset, long tailOffset) {
        List<PlannedRecord> records = new ArrayList<>(pending.encodedRecords().size());
        List<WalAppendResult> userResults = new ArrayList<>(pending.userRecordCount());
        long cursor = tailOffset;
        for (int i = 0; i < pending.encodedRecords().size(); i++) {
            WalRecordCodec.EncodedRecord encoded = pending.encodedRecords().get(i);
            RingWalLayout.Allocation allocation = layout.allocate(headOffset, cursor, encoded.totalLength());
            records.add(new PlannedRecord(encoded, allocation));
            if (i < pending.userRecordCount()) {
                userResults.add(new WalAppendResult(allocation.walOffset(), allocation.encodedLength()));
            }
            cursor = allocation.nextTailOffset();
        }
        return new PlannedGroup(pending, List.copyOf(records), List.copyOf(userResults), cursor);
    }

    private void writePadding(RingWalLayout.Allocation allocation) throws IOException {
        long remaining = allocation.paddingBytes();
        long cursor = allocation.walOffset() - remaining;
        while (remaining > 0) {
            int chunkBytes = (int) Math.min(ZERO_CHUNK_BYTES, remaining);
            ByteBuffer zeroes = ZERO_CHUNK.duplicate();
            zeroes.limit(chunkBytes);
            writeRaw(cursor, zeroes);
            cursor += chunkBytes;
            remaining -= chunkBytes;
        }
    }

    private void writeEncoded(long walOffset, WalRecordCodec.EncodedRecord encoded) throws IOException {
        writeRaw(walOffset, encoded.header());
        ByteBuffer payload = encoded.payload();
        if (payload.hasRemaining()) {
            writeRaw(Math.addExact(walOffset, WalRecordCodec.HEADER_BYTES), payload);
        }
    }

    private void writeRaw(long walOffset, ByteBuffer source) throws IOException {
        ByteBuffer bytes = source.duplicate();
        if (!bytes.hasRemaining()) {
            return;
        }
        int length = bytes.remaining();
        RingWalLayout.PhysicalAddress address = layout.address(walOffset);
        RingWalLayout.Allocation fragment = new RingWalLayout.Allocation(
            walOffset,
            address.generation(),
            address.position(),
            length,
            0L,
            Math.addExact(walOffset, length)
        );
        file.write(fragment, bytes);
    }

    private long recoverMaxGroupId() throws IOException {
        RingWalSuperblock.State state = file.state();
        LogicalReadHandle handle = new LogicalReadHandle(state);
        long maxGroupId = -1L;
        int pendingRecordCount = 0;
        long cursor = state.headOffset();
        while (cursor < state.tailOffset()) {
            ScanItem item = scanNext(state, handle, cursor);
            cursor = item.nextOffset();
            if (item.record() == null) {
                continue;
            }
            WalRecord record = item.record();
            if (record.type() == WalRecordType.GROUP_COMMIT) {
                validateGroupCommit(record, pendingRecordCount, item.offset());
                maxGroupId = Math.max(maxGroupId, record.groupId());
                pendingRecordCount = 0;
            } else {
                pendingRecordCount++;
            }
        }
        if (pendingRecordCount != 0) {
            throw new WalCorruptionException(
                "Incomplete ring WAL append group remained at durable tail " + state.tailOffset());
        }
        return maxGroupId;
    }

    private ScanItem scanNext(
        RingWalSuperblock.State state,
        LogicalReadHandle handle,
        long walOffset
    ) throws IOException {
        if (walOffset < state.headOffset() || walOffset >= state.tailOffset()) {
            throw new IllegalArgumentException("scan offset is outside durable ring window: " + walOffset);
        }

        long positionInRing = walOffset % layout.dataCapacityBytes();
        long bytesToBoundary = layout.dataCapacityBytes() - positionInRing;
        long boundaryOffset = Math.addExact(walOffset, bytesToBoundary);
        if (bytesToBoundary < WalRecordCodec.MIN_RECORD_BYTES) {
            return scanPadding(state, walOffset, boundaryOffset);
        }

        if (state.tailOffset() - walOffset < Integer.BYTES) {
            throw new WalCorruptionException("Durable ring WAL tail truncates a record prefix at offset " + walOffset);
        }
        ByteBuffer magicBytes = file.read(walOffset, Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        if (magicBytes.getInt() != WalRecordCodec.MAGIC) {
            return scanPadding(state, walOffset, boundaryOffset);
        }

        final WalRecordCodec.ReadResult result;
        try {
            result = WalRecordCodec.read(handle, walOffset);
        } catch (IllegalArgumentException e) {
            throw new WalCorruptionException(
                "Ring WAL record crosses a physical wrap boundary at logical offset " + walOffset);
        }
        if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) {
            throw new WalCorruptionException("Durable ring WAL contains a partial record at offset " + walOffset);
        }
        long nextOffset = Math.addExact(walOffset, result.length());
        if (nextOffset > boundaryOffset) {
            throw new WalCorruptionException(
                "Ring WAL record crosses a physical wrap boundary at logical offset " + walOffset);
        }
        return new ScanItem(walOffset, nextOffset, result.length(), result.record());
    }

    private ScanItem scanPadding(
        RingWalSuperblock.State state,
        long paddingOffset,
        long boundaryOffset
    ) throws IOException {
        if (boundaryOffset >= state.tailOffset()) {
            throw new WalCorruptionException(
                "Unexpected padding at durable ring WAL tail: offset=" + paddingOffset +
                    ", tail=" + state.tailOffset());
        }
        verifyZeroPadding(paddingOffset, boundaryOffset);
        return new ScanItem(paddingOffset, boundaryOffset, 0, null);
    }

    private void verifyZeroPadding(long startOffset, long endOffset) throws IOException {
        long cursor = startOffset;
        while (cursor < endOffset) {
            int length = (int) Math.min(ZERO_CHUNK_BYTES, endOffset - cursor);
            ByteBuffer bytes = file.read(cursor, length);
            long byteOffset = cursor;
            while (bytes.hasRemaining()) {
                if (bytes.get() != 0) {
                    throw new WalCorruptionException(
                        "Non-zero ring WAL wrap padding at logical offset " + byteOffset);
                }
                byteOffset++;
            }
            cursor += length;
        }
    }

    private static void validateGroupCommit(WalRecord commit, int pendingCount, long offset)
        throws WalCorruptionException {
        if (pendingCount <= 0 || pendingCount != commit.groupRecordCount()) {
            throw new WalCorruptionException(
                "Invalid ring WAL group commit at offset " + offset + ": expectedRecords=" +
                    commit.groupRecordCount() + ", pendingRecords=" + pendingCount);
        }
    }

    private static void requireCompleteReplayGroup(List<ReplayEntry> pendingGroup, long tailOffset)
        throws WalCorruptionException {
        if (!pendingGroup.isEmpty()) {
            throw new WalCorruptionException(
                "Incomplete ring WAL append group remained at durable tail " + tailOffset);
        }
    }

    private static boolean groupReclaimable(WalReclaimPolicy policy, List<ReplayEntry> group) {
        for (ReplayEntry entry : group) {
            if (!policy.canReclaim(entry.record(), entry.appendResult())) {
                return false;
            }
        }
        return true;
    }

    private void failWriter(Throwable t, List<PendingAppend> currentGroups) {
        synchronized (lifecycleLock) {
            failure = t;
            accepting = false;
            running.set(false);
        }
        for (PendingAppend group : currentGroups) {
            if (!group.poison()) {
                group.future().completeExceptionally(t);
            }
        }
        PendingAppend pending;
        while ((pending = pendingAppends.poll()) != null) {
            if (!pending.poison()) {
                pending.future().completeExceptionally(t);
            }
        }
    }

    private void closeAfterRecoveryFailure(Throwable recoveryFailure) {
        try {
            file.close();
        } catch (Throwable closeFailure) {
            recoveryFailure.addSuppressed(closeFailure);
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Ring WAL is closed");
        }
    }

    private final class LogicalReadHandle implements WalIoBackend.Handle {
        private final RingWalSuperblock.State state;

        private LogicalReadHandle(RingWalSuperblock.State state) {
            this.state = state;
        }

        @Override
        public long size() {
            return state.tailOffset();
        }

        @Override
        public int read(ByteBuffer destination, long position) throws IOException {
            if (!destination.hasRemaining()) {
                return 0;
            }
            if (position < state.headOffset() || position >= state.tailOffset()) {
                return -1;
            }
            int length = (int) Math.min((long) destination.remaining(), state.tailOffset() - position);
            ByteBuffer source = file.read(position, length);
            destination.put(source);
            return length;
        }

        @Override
        public int write(ByteBuffer source, long position) {
            throw new UnsupportedOperationException("logical ring read handle is read-only");
        }

        @Override
        public void truncate(long size) {
            throw new UnsupportedOperationException("logical ring read handle is read-only");
        }

        @Override
        public void force() {
            throw new UnsupportedOperationException("logical ring read handle is read-only");
        }

        @Override
        public void seal() {
            throw new UnsupportedOperationException("logical ring read handle is read-only");
        }

        @Override
        public void close() {
        }
    }

    private record PendingAppend(
        List<WalRecordCodec.EncodedRecord> encodedRecords,
        int userRecordCount,
        CompletableFuture<List<WalAppendResult>> future,
        boolean poison
    ) {
        private static PendingAppend poisonPill() {
            return new PendingAppend(List.of(), 0, new CompletableFuture<>(), true);
        }
    }

    private record PlannedRecord(WalRecordCodec.EncodedRecord encoded, RingWalLayout.Allocation allocation) {
    }

    private record PlannedGroup(
        PendingAppend pending,
        List<PlannedRecord> records,
        List<WalAppendResult> userResults,
        long nextTailOffset
    ) {
    }

    private record ReplayEntry(WalRecord record, WalAppendResult appendResult) {
    }

    private record ScanItem(long offset, long nextOffset, int length, WalRecord record) {
    }
}
