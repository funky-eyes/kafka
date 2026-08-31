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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Broker-wide append-only WAL with a single writer and natural group commit.
 *
 * <p>Each logical append group is encoded as one or more data/control records followed by a GROUP_COMMIT marker.
 * Replay exposes the group only when its commit marker is present and valid. This makes a multi-RecordBatch Kafka
 * append crash-atomic even when the group crosses physical WAL segments.</p>
 *
 * <p>Append futures complete only after the full drained writer batch crosses the physical durability barrier exposed
 * by {@link WalIoBackend.Handle#force()}. Physical I/O is delegated to {@link WalIoBackend}; Kafka offset, append-group,
 * capacity, recovery and reclaim semantics remain in this state machine.</p>
 */
public final class FileSharedWal implements SharedWal {
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("wal-(\\d{20})\\.log");
    private static final int MAX_DRAINED_APPENDS = 1024;

    private final Path directory;
    private final long capacityBytes;
    private final long segmentBytes;
    private final WalIoBackend ioBackend;
    private final LinkedBlockingQueue<PendingAppend> pendingAppends = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong nextGroupId;
    private final AtomicLong usedBytes;
    private final Object lifecycleLock = new Object();
    private final Object writerIoLock = new Object();
    private final Thread writerThread;

    private volatile boolean accepting = true;
    private volatile Throwable failure;
    private volatile SegmentWriter activeSegment;
    private long nextSegmentId;

    public FileSharedWal(Path directory, long capacityBytes, long segmentBytes) throws IOException {
        this(directory, capacityBytes, segmentBytes, new FileChannelWalIoBackend());
    }

    FileSharedWal(Path directory, long capacityBytes, long segmentBytes, WalIoBackend ioBackend) throws IOException {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.ioBackend = Objects.requireNonNull(ioBackend, "ioBackend");
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive");
        }
        if (segmentBytes < WalRecordCodec.MIN_RECORD_BYTES) {
            throw new IllegalArgumentException("segmentBytes is too small: " + segmentBytes);
        }
        if (segmentBytes > capacityBytes) {
            throw new IllegalArgumentException("segmentBytes must not exceed capacityBytes");
        }
        this.capacityBytes = capacityBytes;
        this.segmentBytes = segmentBytes;

        Files.createDirectories(directory);
        RecoveryState recovery = recoverSegments();
        this.usedBytes = new AtomicLong(recovery.usedBytes);
        this.nextSegmentId = recovery.nextSegmentId;
        this.nextGroupId = new AtomicLong(recovery.nextGroupId);
        this.activeSegment = openActiveSegment(recovery.lastSegmentId);

        this.writerThread = new Thread(this::writerLoop, "shared-wal-writer");
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
                validateRecordFitsSegment(encodedRecord);
                encoded.add(encodedRecord);
            }
            WalRecordCodec.EncodedRecord commit = WalRecordCodec.encode(WalRecord.groupCommit(groupId, records.size()));
            validateRecordFitsSegment(commit);
            encoded.add(commit);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }

        synchronized (lifecycleLock) {
            Throwable currentFailure = failure;
            if (currentFailure != null) {
                future.completeExceptionally(new IllegalStateException("WAL writer has failed", currentFailure));
                return future;
            }
            if (!accepting) {
                future.completeExceptionally(new IllegalStateException("WAL is closed"));
                return future;
            }
            pendingAppends.add(new PendingAppend(encoded, records.size(), future));
        }
        return future;
    }

    private void validateRecordFitsSegment(WalRecordCodec.EncodedRecord encoded) {
        if (encoded.totalLength() > segmentBytes) {
            throw new IllegalArgumentException(
                "Encoded WAL record size " + encoded.totalLength() + " exceeds segmentBytes " + segmentBytes);
        }
    }

    @Override
    public long usedBytes() {
        return usedBytes.get();
    }

    @Override
    public long capacityBytes() {
        return capacityBytes;
    }

    long activeSegmentId() {
        SegmentWriter current = activeSegment;
        return current == null ? -1L : current.id;
    }

    long sealActiveSegment() throws IOException {
        synchronized (writerIoLock) {
            SegmentWriter current = activeSegment;
            if (current == null) {
                return -1L;
            }
            current.seal();
            activeSegment = null;
            return current.id;
        }
    }

    void releaseReclaimedBytes(long reclaimedBytes) {
        if (reclaimedBytes < 0) {
            throw new IllegalArgumentException("reclaimedBytes must not be negative");
        }
        if (reclaimedBytes == 0) {
            return;
        }
        long remaining = usedBytes.addAndGet(-reclaimedBytes);
        if (remaining < 0) {
            usedBytes.addAndGet(reclaimedBytes);
            throw new IllegalStateException(
                "Reclaimed WAL bytes exceed accounted usage: reclaimed=" + reclaimedBytes +
                    ", usedBefore=" + (remaining + reclaimedBytes));
        }
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        Objects.requireNonNull(location, "location");
        try (WalIoBackend.Handle handle = ioBackend.openRead(segmentPath(location.segmentId()))) {
            return readAt(handle, location);
        }
    }

    @Override
    public List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        Objects.requireNonNull(locations, "locations");
        if (locations.isEmpty()) {
            return List.of();
        }

        Map<Long, List<IndexedLocation>> bySegment = new LinkedHashMap<>();
        for (int i = 0; i < locations.size(); i++) {
            WalLocation location = Objects.requireNonNull(locations.get(i), "location");
            bySegment.computeIfAbsent(location.segmentId(), ignored -> new ArrayList<>())
                .add(new IndexedLocation(i, location));
        }

        WalRecord[] result = new WalRecord[locations.size()];
        for (Map.Entry<Long, List<IndexedLocation>> entry : bySegment.entrySet()) {
            try (WalIoBackend.Handle handle = ioBackend.openRead(segmentPath(entry.getKey()))) {
                for (IndexedLocation indexed : entry.getValue()) {
                    result[indexed.index] = readAt(handle, indexed.location);
                }
            }
        }
        return List.copyOf(Arrays.asList(result));
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        Objects.requireNonNull(consumer, "consumer");
        List<ReplayEntry> pendingGroup = new ArrayList<>();
        for (Path segment : segmentFiles()) {
            long segmentId = parseSegmentId(segment);
            try (WalIoBackend.Handle handle = ioBackend.openRead(segment)) {
                long position = 0;
                while (true) {
                    WalRecordCodec.ReadResult result = WalRecordCodec.read(handle, position);
                    if (result.status() == WalRecordCodec.ReadStatus.EOF ||
                        result.status() == WalRecordCodec.ReadStatus.PARTIAL) {
                        break;
                    }
                    WalRecord record = result.record();
                    if (record.type() == WalRecordType.GROUP_COMMIT) {
                        if (pendingGroup.size() != record.groupRecordCount()) {
                            throw new WalCorruptionException(
                                "WAL group commit count mismatch for group " + record.groupId() +
                                    ": expected=" + record.groupRecordCount() + ", actual=" + pendingGroup.size());
                        }
                        for (ReplayEntry entry : pendingGroup) {
                            consumer.accept(entry.record, entry.appendResult);
                        }
                        pendingGroup.clear();
                    } else {
                        pendingGroup.add(new ReplayEntry(
                            record,
                            new WalAppendResult(segmentId, position, result.length())
                        ));
                    }
                    position += result.length();
                }
            }
        }
        if (!pendingGroup.isEmpty()) {
            throw new WalCorruptionException("Incomplete WAL append group remained after recovery");
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        stopWriter();
        IOException closeError = awaitWriterStop();
        closeError = closeIoResources(closeError);
        if (closeError != null) {
            throw closeError;
        }
    }

    private void stopWriter() {
        synchronized (lifecycleLock) {
            accepting = false;
            if (running.getAndSet(false)) {
                pendingAppends.offer(PendingAppend.poison());
            }
        }
    }

    private IOException awaitWriterStop() {
        try {
            writerThread.join(TimeUnit.SECONDS.toMillis(30));
            if (writerThread.isAlive()) {
                return new IOException("Timed out waiting for WAL writer to stop");
            }
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new IOException("Interrupted while closing WAL", e);
        }
    }

    private IOException closeIoResources(IOException closeError) {
        synchronized (writerIoLock) {
            closeError = closeActiveSegment(closeError);
        }
        return closeBackend(closeError);
    }

    private IOException closeActiveSegment(IOException closeError) {
        SegmentWriter segment = activeSegment;
        activeSegment = null;
        if (segment == null) {
            return closeError;
        }
        try {
            segment.close();
            return closeError;
        } catch (IOException e) {
            return mergeCloseError(closeError, e);
        }
    }

    private IOException closeBackend(IOException closeError) {
        try {
            ioBackend.close();
            return closeError;
        } catch (IOException e) {
            return mergeCloseError(closeError, e);
        }
    }

    private static IOException mergeCloseError(IOException closeError, IOException additional) {
        if (closeError == null) {
            return additional;
        }
        closeError.addSuppressed(additional);
        return closeError;
    }

    private void writerLoop() {
        List<PendingAppend> drained = new ArrayList<>(MAX_DRAINED_APPENDS);
        while (running.get() || !pendingAppends.isEmpty()) {
            try {
                drained.clear();
                PendingAppend first = pendingAppends.take();
                if (first.poison) {
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
                    if (next.poison) {
                        pendingAppends.offer(next);
                        break;
                    }
                    drained.add(next);
                }
                writeDrainedGroups(drained);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failWriter(new IOException("WAL writer interrupted", e), drained);
                return;
            } catch (Throwable t) {
                failWriter(t, drained);
                return;
            }
        }
    }

    private void failWriter(Throwable t, List<PendingAppend> currentGroups) {
        synchronized (lifecycleLock) {
            failure = t;
            accepting = false;
            running.set(false);
        }
        failPending(currentGroups, t);
        PendingAppend append;
        while ((append = pendingAppends.poll()) != null) {
            if (!append.poison) {
                append.future.completeExceptionally(t);
            }
        }
    }

    private void writeDrainedGroups(List<PendingAppend> groups) throws IOException {
        synchronized (writerIoLock) {
            List<PendingAppend> admitted = new ArrayList<>(groups.size());
            long plannedBytes = 0;
            int firstRejected = -1;
            long currentUsedBytes = usedBytes.get();
            for (int i = 0; i < groups.size(); i++) {
                PendingAppend group = groups.get(i);
                long groupBytes = group.totalBytes();
                if (currentUsedBytes + plannedBytes + groupBytes > capacityBytes) {
                    firstRejected = i;
                    break;
                }
                admitted.add(group);
                plannedBytes = Math.addExact(plannedBytes, groupBytes);
            }

            if (firstRejected >= 0) {
                WalCapacityExceededException error = new WalCapacityExceededException(
                    "WAL capacity exceeded: used=" + currentUsedBytes + ", admitted=" + plannedBytes +
                        ", capacity=" + capacityBytes);
                for (int i = firstRejected; i < groups.size(); i++) {
                    groups.get(i).future.completeExceptionally(error);
                }
            }
            if (admitted.isEmpty()) {
                return;
            }

            List<GroupWriteResult> completedGroups = new ArrayList<>(admitted.size());
            for (PendingAppend group : admitted) {
                List<WalAppendResult> userResults = new ArrayList<>(group.userRecordCount);
                for (int i = 0; i < group.encodedRecords.size(); i++) {
                    WalRecordCodec.EncodedRecord encoded = group.encodedRecords.get(i);
                    int length = encoded.totalLength();
                    ensureWritableSegment(length);
                    long position = activeSegment.position;
                    writeEncoded(activeSegment.handle, encoded, position);
                    activeSegment.position += length;
                    usedBytes.addAndGet(length);
                    if (i < group.userRecordCount) {
                        userResults.add(new WalAppendResult(activeSegment.id, position, length));
                    }
                }
                completedGroups.add(new GroupWriteResult(group.future, List.copyOf(userResults)));
            }

            forceSegment(activeSegment);
            for (GroupWriteResult completed : completedGroups) {
                completed.future.complete(completed.results);
            }
        }
    }

    private void ensureWritableSegment(int recordLength) throws IOException {
        if (activeSegment == null) {
            activeSegment = createSegment(nextSegmentId++);
            return;
        }
        if (activeSegment.position > 0 && activeSegment.position + recordLength > segmentBytes) {
            activeSegment.seal();
            activeSegment = createSegment(nextSegmentId++);
        }
    }

    private void forceSegment(SegmentWriter segment) throws IOException {
        segment.handle.force();
    }

    private RecoveryState recoverSegments() throws IOException {
        List<Path> segments = segmentFiles();
        RecoveryScan scan = scanSegments(segments);
        if (scan.truncateFrom() != null) {
            truncateIncompleteTail(segments, scan.truncateFrom());
            segments = segmentFiles();
        }

        SegmentSummary summary = summarizeSegments(segments);
        ensureRecoveredCapacity(summary.totalBytes());
        return new RecoveryState(
            summary.totalBytes(),
            summary.lastSegmentId(),
            summary.lastSegmentId() + 1,
            scan.maxGroupId() + 1
        );
    }

    private RecoveryScan scanSegments(List<Path> segments) throws IOException {
        RecoveryAccumulator accumulator = new RecoveryAccumulator();
        for (int segmentIndex = 0; segmentIndex < segments.size(); segmentIndex++) {
            if (scanSegment(segments.get(segmentIndex), segmentIndex, accumulator)) {
                break;
            }
        }
        return accumulator.finish();
    }

    private boolean scanSegment(Path segment, int segmentIndex, RecoveryAccumulator accumulator) throws IOException {
        try (WalIoBackend.Handle handle = ioBackend.openRead(segment)) {
            long position = 0;
            while (true) {
                WalRecordCodec.ReadResult result = WalRecordCodec.read(handle, position);
                if (result.status() == WalRecordCodec.ReadStatus.EOF) {
                    return false;
                }
                if (result.status() == WalRecordCodec.ReadStatus.PARTIAL) {
                    accumulator.markPartial(segmentIndex, position);
                    return true;
                }
                accumulator.accept(result.record(), segmentIndex, position);
                position += result.length();
            }
        }
    }

    private SegmentSummary summarizeSegments(List<Path> segments) throws IOException {
        long totalBytes = 0;
        long lastSegmentId = -1;
        for (Path segment : segments) {
            totalBytes = Math.addExact(totalBytes, ioBackend.size(segment));
            lastSegmentId = Math.max(lastSegmentId, parseSegmentId(segment));
        }
        return new SegmentSummary(totalBytes, lastSegmentId);
    }

    private void ensureRecoveredCapacity(long recoveredBytes) throws WalCapacityExceededException {
        if (recoveredBytes > capacityBytes) {
            throw new WalCapacityExceededException(
                "Recovered WAL exceeds configured capacity: used=" + recoveredBytes + ", capacity=" + capacityBytes);
        }
    }

    private void truncateIncompleteTail(List<Path> segments, GroupStart start) throws IOException {
        Path first = segments.get(start.segmentIndex);
        try (WalIoBackend.Handle handle = ioBackend.reopen(first)) {
            handle.truncate(start.position);
            handle.force();
        }
        boolean deleted = false;
        for (int i = start.segmentIndex + 1; i < segments.size(); i++) {
            deleted |= Files.deleteIfExists(segments.get(i));
        }
        if (deleted) {
            Utils.flushDir(directory.toAbsolutePath().normalize());
        }
    }

    private SegmentWriter openActiveSegment(long lastSegmentId) throws IOException {
        if (lastSegmentId < 0) {
            return null;
        }
        Path path = segmentPath(lastSegmentId);
        long size = ioBackend.size(path);
        if (size >= segmentBytes) {
            return null;
        }
        return new SegmentWriter(lastSegmentId, ioBackend.reopen(path), size);
    }

    private SegmentWriter createSegment(long segmentId) throws IOException {
        return new SegmentWriter(segmentId, ioBackend.create(segmentPath(segmentId)), 0);
    }

    private WalRecord readAt(WalIoBackend.Handle handle, WalLocation location) throws IOException {
        WalRecordCodec.ReadResult result = WalRecordCodec.read(handle, location.position());
        if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) {
            throw new WalCorruptionException("WAL location does not point to a complete record: " + location);
        }
        if (result.length() != location.length()) {
            throw new WalCorruptionException("WAL location length mismatch: expected=" + location.length() +
                ", actual=" + result.length());
        }
        if (result.record().type() == WalRecordType.GROUP_COMMIT) {
            throw new WalCorruptionException("WAL data location unexpectedly points to GROUP_COMMIT: " + location);
        }
        return result.record();
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(FileSharedWal::isSegmentFile)
                .sorted(Comparator.comparingLong(FileSharedWal::parseSegmentId))
                .toList();
        }
    }

    private Path segmentPath(long segmentId) {
        return directory.resolve(String.format("wal-%020d.log", segmentId));
    }

    private static boolean isSegmentFile(Path path) {
        Path fileName = path.getFileName();
        return fileName != null && SEGMENT_FILE_PATTERN.matcher(fileName.toString()).matches();
    }

    private static long parseSegmentId(Path path) {
        Path fileName = path.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("WAL segment path has no file name: " + path);
        }
        Matcher matcher = SEGMENT_FILE_PATTERN.matcher(fileName.toString());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a WAL segment: " + path);
        }
        return Long.parseLong(matcher.group(1));
    }

    private static void writeEncoded(
        WalIoBackend.Handle handle,
        WalRecordCodec.EncodedRecord encoded,
        long position
    ) throws IOException {
        long currentPosition = position;
        currentPosition += writeFully(handle, encoded.header(), currentPosition);
        ByteBuffer payload = encoded.payload();
        if (payload.hasRemaining()) {
            currentPosition += writeFully(handle, payload, currentPosition);
        }
        if (currentPosition - position != encoded.totalLength()) {
            throw new IOException("WAL write length mismatch: expected=" + encoded.totalLength() +
                ", actual=" + (currentPosition - position));
        }
    }

    private static int writeFully(WalIoBackend.Handle handle, ByteBuffer buffer, long position) throws IOException {
        long currentPosition = position;
        int totalWritten = 0;
        while (buffer.hasRemaining()) {
            int written = handle.write(buffer, currentPosition);
            if (written <= 0) {
                throw new IOException("Unable to make progress writing WAL at position " + currentPosition);
            }
            currentPosition += written;
            totalWritten += written;
        }
        return totalWritten;
    }

    private static void failPending(List<PendingAppend> groups, Throwable t) {
        for (PendingAppend group : groups) {
            if (!group.poison) {
                group.future.completeExceptionally(t);
            }
        }
    }

    private static final class RecoveryAccumulator {
        private GroupStart pendingGroupStart;
        private int pendingRecordCount;
        private long maxGroupId = -1L;
        private GroupStart truncateFrom;

        private void accept(WalRecord record, int segmentIndex, long position) throws WalCorruptionException {
            if (record.type() == WalRecordType.GROUP_COMMIT) {
                acceptCommit(record);
                return;
            }
            if (pendingGroupStart == null) {
                pendingGroupStart = new GroupStart(segmentIndex, position);
            }
            pendingRecordCount++;
        }

        private void acceptCommit(WalRecord record) throws WalCorruptionException {
            if (pendingGroupStart == null || pendingRecordCount != record.groupRecordCount()) {
                throw new WalCorruptionException(
                    "Invalid WAL group commit marker id=" + record.groupId() +
                        ", expectedRecords=" + record.groupRecordCount() +
                        ", pendingRecords=" + pendingRecordCount);
            }
            maxGroupId = Math.max(maxGroupId, record.groupId());
            pendingGroupStart = null;
            pendingRecordCount = 0;
        }

        private void markPartial(int segmentIndex, long position) {
            truncateFrom = pendingGroupStart != null
                ? pendingGroupStart
                : new GroupStart(segmentIndex, position);
        }

        private RecoveryScan finish() {
            GroupStart effectiveTruncate = truncateFrom != null ? truncateFrom : pendingGroupStart;
            return new RecoveryScan(effectiveTruncate, maxGroupId);
        }
    }

    private static final class PendingAppend {
        private final List<WalRecordCodec.EncodedRecord> encodedRecords;
        private final int userRecordCount;
        private final CompletableFuture<List<WalAppendResult>> future;
        private final boolean poison;

        private PendingAppend(
            List<WalRecordCodec.EncodedRecord> encodedRecords,
            int userRecordCount,
            CompletableFuture<List<WalAppendResult>> future
        ) {
            this(encodedRecords, userRecordCount, future, false);
        }

        private PendingAppend(
            List<WalRecordCodec.EncodedRecord> encodedRecords,
            int userRecordCount,
            CompletableFuture<List<WalAppendResult>> future,
            boolean poison
        ) {
            this.encodedRecords = encodedRecords;
            this.userRecordCount = userRecordCount;
            this.future = future;
            this.poison = poison;
        }

        private long totalBytes() {
            long total = 0;
            for (WalRecordCodec.EncodedRecord encoded : encodedRecords) {
                total = Math.addExact(total, encoded.totalLength());
            }
            return total;
        }

        private static PendingAppend poison() {
            return new PendingAppend(List.of(), 0, new CompletableFuture<>(), true);
        }
    }

    private record GroupWriteResult(
        CompletableFuture<List<WalAppendResult>> future,
        List<WalAppendResult> results
    ) {
    }

    private record ReplayEntry(WalRecord record, WalAppendResult appendResult) {
    }

    private record IndexedLocation(int index, WalLocation location) {
    }

    private record GroupStart(int segmentIndex, long position) {
    }

    private record RecoveryScan(GroupStart truncateFrom, long maxGroupId) {
    }

    private record SegmentSummary(long totalBytes, long lastSegmentId) {
    }

    private record RecoveryState(long usedBytes, long lastSegmentId, long nextSegmentId, long nextGroupId) {
    }

    private static final class SegmentWriter implements AutoCloseable {
        private final long id;
        private final WalIoBackend.Handle handle;
        private long position;

        private SegmentWriter(long id, WalIoBackend.Handle handle, long position) {
            this.id = id;
            this.handle = handle;
            this.position = position;
        }

        private void seal() throws IOException {
            handle.seal();
        }

        @Override
        public void close() throws IOException {
            handle.close();
        }
    }
}
