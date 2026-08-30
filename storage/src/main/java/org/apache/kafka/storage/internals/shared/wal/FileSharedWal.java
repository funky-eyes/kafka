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

public final class FileSharedWal implements SharedWal {
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("wal-(\\d{20})\\.log");
    private static final int MAX_DRAINED_APPENDS = 1024;

    private final Path directory;
    private final long capacityBytes;
    private final long segmentBytes;
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
        this.directory = Objects.requireNonNull(directory, "directory");
        if (capacityBytes <= 0) throw new IllegalArgumentException("capacityBytes must be positive");
        if (segmentBytes < WalRecordCodec.MIN_RECORD_BYTES) throw new IllegalArgumentException("segmentBytes is too small: " + segmentBytes);
        if (segmentBytes > capacityBytes) throw new IllegalArgumentException("segmentBytes must not exceed capacityBytes");
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
                if (record.type() == WalRecordType.GROUP_COMMIT) throw new IllegalArgumentException("GROUP_COMMIT is internal and cannot be appended directly");
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
        if (encoded.totalLength() > segmentBytes) throw new IllegalArgumentException("Encoded WAL record size " + encoded.totalLength() + " exceeds segmentBytes " + segmentBytes);
    }

    @Override public long usedBytes() { return usedBytes.get(); }
    @Override public long capacityBytes() { return capacityBytes; }

    long activeSegmentId() {
        SegmentWriter current = activeSegment;
        return current == null ? -1L : current.id;
    }

    long sealActiveSegment() throws IOException {
        synchronized (writerIoLock) {
            SegmentWriter current = activeSegment;
            if (current == null) return -1L;
            forceSegment(current);
            current.close();
            activeSegment = null;
            return current.id;
        }
    }

    void releaseReclaimedBytes(long reclaimedBytes) {
        if (reclaimedBytes < 0) throw new IllegalArgumentException("reclaimedBytes must not be negative");
        if (reclaimedBytes == 0) return;
        long remaining = usedBytes.addAndGet(-reclaimedBytes);
        if (remaining < 0) {
            usedBytes.addAndGet(reclaimedBytes);
            throw new IllegalStateException("Reclaimed WAL bytes exceed accounted usage: reclaimed=" + reclaimedBytes + ", usedBefore=" + (remaining + reclaimedBytes));
        }
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        Objects.requireNonNull(location, "location");
        try (FileChannel channel = FileChannel.open(segmentPath(location.segmentId()), StandardOpenOption.READ)) {
            return readAt(channel, location);
        }
    }

    @Override
    public List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        Objects.requireNonNull(locations, "locations");
        if (locations.isEmpty()) return List.of();
        Map<Long, List<IndexedLocation>> bySegment = new LinkedHashMap<>();
        for (int i = 0; i < locations.size(); i++) {
            WalLocation location = Objects.requireNonNull(locations.get(i), "location");
            bySegment.computeIfAbsent(location.segmentId(), ignored -> new ArrayList<>()).add(new IndexedLocation(i, location));
        }
        WalRecord[] result = new WalRecord[locations.size()];
        for (Map.Entry<Long, List<IndexedLocation>> entry : bySegment.entrySet()) {
            try (FileChannel channel = FileChannel.open(segmentPath(entry.getKey()), StandardOpenOption.READ)) {
                for (IndexedLocation indexed : entry.getValue()) result[indexed.index] = readAt(channel, indexed.location);
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
            try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
                long position = 0;
                while (true) {
                    WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, position);
                    if (result.status() == WalRecordCodec.ReadStatus.EOF || result.status() == WalRecordCodec.ReadStatus.PARTIAL) break;
                    WalRecord record = result.record();
                    if (record.type() == WalRecordType.GROUP_COMMIT) {
                        if (pendingGroup.size() != record.groupRecordCount()) throw new WalCorruptionException("WAL group commit count mismatch for group " + record.groupId() + ": expected=" + record.groupRecordCount() + ", actual=" + pendingGroup.size());
                        for (ReplayEntry entry : pendingGroup) consumer.accept(entry.record, entry.appendResult);
                        pendingGroup.clear();
                    } else {
                        pendingGroup.add(new ReplayEntry(record, new WalAppendResult(segmentId, position, result.length())));
                    }
                    position += result.length();
                }
            }
        }
        if (!pendingGroup.isEmpty()) throw new WalCorruptionException("Incomplete WAL append group remained after recovery");
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) return;
        synchronized (lifecycleLock) {
            accepting = false;
            if (running.getAndSet(false)) pendingAppends.offer(PendingAppend.poison());
        }
        IOException closeError = null;
        try {
            writerThread.join(TimeUnit.SECONDS.toMillis(30));
            if (writerThread.isAlive()) closeError = new IOException("Timed out waiting for WAL writer to stop");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeError = new IOException("Interrupted while closing WAL", e);
        } finally {
            synchronized (writerIoLock) {
                if (activeSegment != null) {
                    try { activeSegment.close(); } catch (IOException e) { if (closeError == null) closeError = e; else closeError.addSuppressed(e); } finally { activeSegment = null; }
                }
            }
        }
        if (closeError != null) throw closeError;
    }

    private void writerLoop() {
        List<PendingAppend> drained = new ArrayList<>(MAX_DRAINED_APPENDS);
        while (running.get() || !pendingAppends.isEmpty()) {
            try {
                drained.clear();
                PendingAppend first = pendingAppends.take();
                if (first.poison) {
                    if (!running.get() && pendingAppends.isEmpty()) break;
                    continue;
                }
                drained.add(first);
                while (drained.size() < MAX_DRAINED_APPENDS) {
                    PendingAppend next = pendingAppends.poll();
                    if (next == null) break;
                    if (next.poison) { pendingAppends.offer(next); break; }
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
        synchronized (lifecycleLock) { failure = t; accepting = false; running.set(false); }
        failPending(currentGroups, t);
        PendingAppend append;
        while ((append = pendingAppends.poll()) != null) if (!append.poison) append.future.completeExceptionally(t);
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
                if (currentUsedBytes + plannedBytes + groupBytes > capacityBytes) { firstRejected = i; break; }
                admitted.add(group);
                plannedBytes = Math.addExact(plannedBytes, groupBytes);
            }
            if (firstRejected >= 0) {
                WalCapacityExceededException error = new WalCapacityExceededException("WAL capacity exceeded: used=" + currentUsedBytes + ", admitted=" + plannedBytes + ", capacity=" + capacityBytes);
                for (int i = firstRejected; i < groups.size(); i++) groups.get(i).future.completeExceptionally(error);
            }
            if (admitted.isEmpty()) return;
            List<GroupWriteResult> completedGroups = new ArrayList<>(admitted.size());
            for (PendingAppend group : admitted) {
                List<WalAppendResult> userResults = new ArrayList<>(group.userRecordCount);
                for (int i = 0; i < group.encodedRecords.size(); i++) {
                    WalRecordCodec.EncodedRecord encoded = group.encodedRecords.get(i);
                    int length = encoded.totalLength();
                    ensureWritableSegment(length);
                    long position = activeSegment.position;
                    writeEncoded(activeSegment.channel, encoded, position);
                    activeSegment.position += length;
                    usedBytes.addAndGet(length);
                    if (i < group.userRecordCount) userResults.add(new WalAppendResult(activeSegment.id, position, length));
                }
                completedGroups.add(new GroupWriteResult(group.future, List.copyOf(userResults)));
            }
            forceSegment(activeSegment);
            for (GroupWriteResult completed : completedGroups) completed.future.complete(completed.results);
        }
    }

    private void ensureWritableSegment(int recordLength) throws IOException {
        if (activeSegment == null) { activeSegment = createSegment(nextSegmentId++); return; }
        if (activeSegment.position > 0 && activeSegment.position + recordLength > segmentBytes) {
            forceSegment(activeSegment); activeSegment.close(); activeSegment = createSegment(nextSegmentId++);
        }
    }

    private void forceSegment(SegmentWriter segment) throws IOException {
        segment.channel.force(false);
        if (segment.directoryEntryDirty) { Utils.flushDir(directory.toAbsolutePath().normalize()); segment.directoryEntryDirty = false; }
    }

    private RecoveryState recoverSegments() throws IOException {
        List<Path> segments = segmentFiles();
        RecoveryScan scan = scanSegments(segments);
        if (scan.truncateFrom() != null) { truncateIncompleteTail(segments, scan.truncateFrom()); segments = segmentFiles(); }
        SegmentSummary summary = summarizeSegments(segments);
        ensureRecoveredCapacity(summary.totalBytes());
        return new RecoveryState(summary.totalBytes(), summary.lastSegmentId(), summary.lastSegmentId() + 1, scan.maxGroupId() + 1);
    }

    private RecoveryScan scanSegments(List<Path> segments) throws IOException {
        RecoveryAccumulator accumulator = new RecoveryAccumulator();
        for (int i = 0; i < segments.size(); i++) if (scanSegment(segments.get(i), i, accumulator)) break;
        return accumulator.finish();
    }

    private boolean scanSegment(Path segment, int segmentIndex, RecoveryAccumulator accumulator) throws IOException {
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
            long position = 0;
            while (true) {
                WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, position);
                if (result.status() == WalRecordCodec.ReadStatus.EOF) return false;
                if (result.status() == WalRecordCodec.ReadStatus.PARTIAL) { accumulator.markPartial(segmentIndex, position); return true; }
                accumulator.accept(result.record(), segmentIndex, position); position += result.length();
            }
        }
    }

    private SegmentSummary summarizeSegments(List<Path> segments) throws IOException {
        long total = 0, last = -1;
        for (Path segment : segments) { total = Math.addExact(total, Files.size(segment)); last = Math.max(last, parseSegmentId(segment)); }
        return new SegmentSummary(total, last);
    }

    private void ensureRecoveredCapacity(long recoveredBytes) throws WalCapacityExceededException {
        if (recoveredBytes > capacityBytes) throw new WalCapacityExceededException("Recovered WAL exceeds configured capacity: used=" + recoveredBytes + ", capacity=" + capacityBytes);
    }

    private void truncateIncompleteTail(List<Path> segments, GroupStart start) throws IOException {
        Path first = segments.get(start.segmentIndex);
        try (FileChannel channel = FileChannel.open(first, StandardOpenOption.READ, StandardOpenOption.WRITE)) { channel.truncate(start.position); channel.force(false); }
        boolean deleted = false;
        for (int i = start.segmentIndex + 1; i < segments.size(); i++) deleted |= Files.deleteIfExists(segments.get(i));
        if (deleted) Utils.flushDir(directory.toAbsolutePath().normalize());
    }

    private SegmentWriter openActiveSegment(long lastSegmentId) throws IOException {
        if (lastSegmentId < 0) return null;
        Path path = segmentPath(lastSegmentId); long size = Files.size(path);
        if (size >= segmentBytes) return null;
        return new SegmentWriter(lastSegmentId, FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE), size, false);
    }

    private SegmentWriter createSegment(long segmentId) throws IOException {
        return new SegmentWriter(segmentId, FileChannel.open(segmentPath(segmentId), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE), 0, true);
    }

    private WalRecord readAt(FileChannel channel, WalLocation location) throws IOException {
        WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, location.position());
        if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) throw new WalCorruptionException("WAL location does not point to a complete record: " + location);
        if (result.length() != location.length()) throw new WalCorruptionException("WAL location length mismatch: expected=" + location.length() + ", actual=" + result.length());
        if (result.record().type() == WalRecordType.GROUP_COMMIT) throw new WalCorruptionException("WAL data location unexpectedly points to GROUP_COMMIT: " + location);
        return result.record();
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.filter(Files::isRegularFile).filter(FileSharedWal::isSegmentFile).sorted(Comparator.comparingLong(FileSharedWal::parseSegmentId)).toList();
        }
    }

    private Path segmentPath(long segmentId) { return directory.resolve(String.format("wal-%020d.log", segmentId)); }
    private static boolean isSegmentFile(Path path) { Path f = path.getFileName(); return f != null && SEGMENT_FILE_PATTERN.matcher(f.toString()).matches(); }
    private static long parseSegmentId(Path path) { Matcher m = SEGMENT_FILE_PATTERN.matcher(path.getFileName().toString()); if (!m.matches()) throw new IllegalArgumentException("Not a WAL segment: " + path); return Long.parseLong(m.group(1)); }

    private static void writeEncoded(FileChannel channel, WalRecordCodec.EncodedRecord encoded, long position) throws IOException {
        long p = position; p += writeFully(channel, encoded.header(), p); ByteBuffer payload = encoded.payload(); if (payload.hasRemaining()) p += writeFully(channel, payload, p);
        if (p - position != encoded.totalLength()) throw new IOException("WAL write length mismatch: expected=" + encoded.totalLength() + ", actual=" + (p - position));
    }

    private static int writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long p = position; int total = 0; while (buffer.hasRemaining()) { int written = channel.write(buffer, p); if (written <= 0) throw new IOException("Unable to make progress writing WAL at position " + p); p += written; total += written; } return total;
    }

    private static void failPending(List<PendingAppend> groups, Throwable t) { for (PendingAppend group : groups) if (!group.poison) group.future.completeExceptionally(t); }

    private static final class RecoveryAccumulator {
        private GroupStart pendingGroupStart; private int pendingRecordCount; private long maxGroupId = -1L; private GroupStart truncateFrom;
        private void accept(WalRecord record, int segmentIndex, long position) throws WalCorruptionException { if (record.type() == WalRecordType.GROUP_COMMIT) { acceptCommit(record); return; } if (pendingGroupStart == null) pendingGroupStart = new GroupStart(segmentIndex, position); pendingRecordCount++; }
        private void acceptCommit(WalRecord record) throws WalCorruptionException { if (pendingGroupStart == null || pendingRecordCount != record.groupRecordCount()) throw new WalCorruptionException("Invalid WAL group commit marker id=" + record.groupId() + ", expectedRecords=" + record.groupRecordCount() + ", pendingRecords=" + pendingRecordCount); maxGroupId = Math.max(maxGroupId, record.groupId()); pendingGroupStart = null; pendingRecordCount = 0; }
        private void markPartial(int segmentIndex, long position) { truncateFrom = pendingGroupStart != null ? pendingGroupStart : new GroupStart(segmentIndex, position); }
        private RecoveryScan finish() { return new RecoveryScan(truncateFrom != null ? truncateFrom : pendingGroupStart, maxGroupId); }
    }

    private static final class PendingAppend {
        private final List<WalRecordCodec.EncodedRecord> encodedRecords; private final int userRecordCount; private final CompletableFuture<List<WalAppendResult>> future; private final boolean poison;
        private PendingAppend(List<WalRecordCodec.EncodedRecord> e, int c, CompletableFuture<List<WalAppendResult>> f) { this(e, c, f, false); }
        private PendingAppend(List<WalRecordCodec.EncodedRecord> e, int c, CompletableFuture<List<WalAppendResult>> f, boolean p) { encodedRecords = e; userRecordCount = c; future = f; poison = p; }
        private long totalBytes() { long total = 0; for (WalRecordCodec.EncodedRecord e : encodedRecords) total = Math.addExact(total, e.totalLength()); return total; }
        private static PendingAppend poison() { return new PendingAppend(List.of(), 0, new CompletableFuture<>(), true); }
    }

    private record GroupWriteResult(CompletableFuture<List<WalAppendResult>> future, List<WalAppendResult> results) {}
    private record ReplayEntry(WalRecord record, WalAppendResult appendResult) {}
    private record IndexedLocation(int index, WalLocation location) {}
    private record GroupStart(int segmentIndex, long position) {}
    private record RecoveryScan(GroupStart truncateFrom, long maxGroupId) {}
    private record SegmentSummary(long totalBytes, long lastSegmentId) {}
    private record RecoveryState(long usedBytes, long lastSegmentId, long nextSegmentId, long nextGroupId) {}
    private static final class SegmentWriter implements AutoCloseable {
        private final long id; private final FileChannel channel; private long position; private boolean directoryEntryDirty;
        private SegmentWriter(long id, FileChannel channel, long position, boolean dirty) { this.id = id; this.channel = channel; this.position = position; this.directoryEntryDirty = dirty; }
        @Override public void close() throws IOException { channel.close(); }
    }
}
