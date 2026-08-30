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
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Rotating facade over {@link FileSharedWal}.
 *
 * <p>The underlying WAL remains the single-writer, group-commit implementation. Reclamation temporarily blocks new
 * operations, waits for already admitted async appends to finish, closes the writer, deletes only a contiguous prefix
 * of segment files whose complete append groups are approved by the supplied {@link WalReclaimPolicy}, fsyncs the WAL
 * directory, and reopens the writer. This deliberately favors safety over reclamation aggressiveness.</p>
 *
 * <p>Remote commit makes bytes reclaimable but does not immediately discard their local recovery copy. Normal
 * maintenance retains the most recent WAL until physical usage reaches the high watermark, then frees only enough of
 * the oldest safe prefix to return near the low watermark. This preserves a useful local recovery window through a
 * short object-store outage while still making the configured WAL capacity reusable indefinitely in steady state.</p>
 *
 * <p>Deletion is permitted only through a physical segment boundary that is also an append-group boundary. A group
 * spanning two files therefore pins both files until its commit marker is safely covered. Bounded reclamation stops at
 * the first such boundary providing the requested headroom.</p>
 */
public final class RotatingFileSharedWal implements SharedWal {
    static final int DEFAULT_RECLAIM_HIGH_WATERMARK_PERCENT = 85;
    static final int DEFAULT_RECLAIM_LOW_WATERMARK_PERCENT = 70;
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("wal-(\\d{20})\\.log");

    private final Object lifecycleLock = new Object();
    private final Path directory;
    private final long capacityBytes;
    private final long segmentBytes;

    private FileSharedWal delegate;
    private int inFlightOperations;
    private boolean reclaiming;
    private boolean closed;

    public RotatingFileSharedWal(Path directory, long capacityBytes, long segmentBytes) throws IOException {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.capacityBytes = capacityBytes;
        this.segmentBytes = segmentBytes;
        this.delegate = new FileSharedWal(directory, capacityBytes, segmentBytes);
    }

    @Override
    public CompletableFuture<List<WalAppendResult>> appendBatch(List<WalRecord> records) {
        Objects.requireNonNull(records, "records");
        final FileSharedWal current;
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            current = delegate;
            inFlightOperations++;
        }

        final CompletableFuture<List<WalAppendResult>> append;
        try {
            append = current.appendBatch(records);
        } catch (Throwable t) {
            endOperation();
            throw t;
        }
        return append.whenComplete((ignored, error) -> endOperation());
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        FileSharedWal current = beginOperation();
        try {
            return current.read(location);
        } finally {
            endOperation();
        }
    }

    @Override
    public List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        FileSharedWal current = beginOperation();
        try {
            return current.readBatch(locations);
        } finally {
            endOperation();
        }
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        FileSharedWal current = beginOperation();
        try {
            current.replay(consumer);
        } finally {
            endOperation();
        }
    }

    /**
     * Production maintenance entry point. Below the high watermark this intentionally keeps even remotely committed
     * bytes local. Once pressure reaches the high watermark it frees only enough safe prefix to return near the low
     * watermark; segment-boundary granularity may release slightly more.
     */
    @Override
    public long reclaim(WalReclaimPolicy policy) throws IOException {
        Objects.requireNonNull(policy, "policy");
        long used;
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            used = delegate.usedBytes();
        }
        long highWatermarkBytes = watermarkBytes(capacityBytes, DEFAULT_RECLAIM_HIGH_WATERMARK_PERCENT);
        if (used < highWatermarkBytes) {
            return 0L;
        }
        long lowWatermarkBytes = watermarkBytes(capacityBytes, DEFAULT_RECLAIM_LOW_WATERMARK_PERCENT);
        long desiredBytes = Math.max(1L, used - lowWatermarkBytes);
        return reclaim(policy, desiredBytes);
    }

    @Override
    public long reclaim(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        Objects.requireNonNull(policy, "policy");
        if (desiredBytes <= 0) {
            throw new IllegalArgumentException("desiredBytes must be positive");
        }
        FileSharedWal oldDelegate;
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            reclaiming = true;
            while (inFlightOperations > 0) {
                waitUninterruptibly();
            }
            oldDelegate = delegate;
        }

        IOException failure = null;
        long reclaimedBytes = 0L;
        try {
            oldDelegate.close();
            ReclaimPlan plan = buildReclaimPlan(policy, desiredBytes);
            reclaimedBytes = deleteReclaimableSegments(plan);
        } catch (IOException e) {
            failure = e;
        } finally {
            try {
                FileSharedWal reopened = new FileSharedWal(directory, capacityBytes, segmentBytes);
                synchronized (lifecycleLock) {
                    delegate = reopened;
                }
            } catch (IOException reopenError) {
                if (failure == null) {
                    failure = reopenError;
                } else {
                    failure.addSuppressed(reopenError);
                }
            }
            synchronized (lifecycleLock) {
                reclaiming = false;
                lifecycleLock.notifyAll();
            }
        }
        if (failure != null) {
            throw failure;
        }
        return reclaimedBytes;
    }

    @Override
    public long usedBytes() {
        synchronized (lifecycleLock) {
            ensureOpen();
            return delegate.usedBytes();
        }
    }

    @Override
    public long capacityBytes() {
        return capacityBytes;
    }

    @Override
    public void close() throws IOException {
        FileSharedWal current;
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            awaitReclaim();
            closed = true;
            while (inFlightOperations > 0) {
                waitUninterruptibly();
            }
            current = delegate;
        }
        current.close();
    }

    private FileSharedWal beginOperation() {
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            inFlightOperations++;
            return delegate;
        }
    }

    private void endOperation() {
        synchronized (lifecycleLock) {
            inFlightOperations--;
            if (inFlightOperations < 0) {
                throw new IllegalStateException("Rotating WAL operation count underflow");
            }
            if (inFlightOperations == 0) {
                lifecycleLock.notifyAll();
            }
        }
    }

    private void awaitReclaim() {
        while (reclaiming) {
            waitUninterruptibly();
        }
    }

    private void waitUninterruptibly() {
        boolean interrupted = false;
        while (true) {
            try {
                lifecycleLock.wait();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Rotating WAL is closed");
        }
    }

    private ReclaimPlan buildReclaimPlan(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        List<Path> segments = segmentFiles();
        List<GroupEntry> pendingGroup = new ArrayList<>();
        long safeBoundarySegmentId = -1L;
        long safeBoundaryBytes = 0L;
        long scannedSegmentBytes = 0L;
        boolean prefixReclaimable = true;

        for (Path segment : segments) {
            long segmentId = parseSegmentId(segment);
            boolean scannedWholeSegment = true;
            try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
                long position = 0L;
                while (prefixReclaimable) {
                    WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, position);
                    if (result.status() == WalRecordCodec.ReadStatus.EOF) {
                        break;
                    }
                    if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) {
                        throw new WalCorruptionException(
                            "Cannot reclaim WAL containing a partial record at segment=" + segmentId +
                                ", position=" + position);
                    }
                    WalRecord record = result.record();
                    if (record.type() == WalRecordType.GROUP_COMMIT) {
                        if (pendingGroup.size() != record.groupRecordCount()) {
                            throw new WalCorruptionException(
                                "WAL group commit count mismatch during reclaim for group " + record.groupId() +
                                    ": expected=" + record.groupRecordCount() +
                                    ", actual=" + pendingGroup.size());
                        }
                        boolean reclaimGroup = true;
                        for (GroupEntry entry : pendingGroup) {
                            if (!policy.canReclaim(entry.record(), entry.appendResult())) {
                                reclaimGroup = false;
                                break;
                            }
                        }
                        pendingGroup.clear();
                        if (!reclaimGroup) {
                            prefixReclaimable = false;
                            scannedWholeSegment = false;
                            break;
                        }
                    } else {
                        pendingGroup.add(new GroupEntry(
                            record,
                            new WalAppendResult(segmentId, position, result.length())
                        ));
                    }
                    position += result.length();
                }
            }
            if (!scannedWholeSegment || !prefixReclaimable) {
                break;
            }
            scannedSegmentBytes = Math.addExact(scannedSegmentBytes, Files.size(segment));
            if (pendingGroup.isEmpty()) {
                safeBoundarySegmentId = segmentId;
                safeBoundaryBytes = scannedSegmentBytes;
                if (safeBoundaryBytes >= desiredBytes) {
                    break;
                }
            }
        }
        return new ReclaimPlan(safeBoundarySegmentId, safeBoundaryBytes);
    }

    private long deleteReclaimableSegments(ReclaimPlan plan) throws IOException {
        if (plan.safeBoundarySegmentId() < 0) {
            return 0L;
        }
        long reclaimedBytes = 0L;
        boolean deleted = false;
        for (Path segment : segmentFiles()) {
            long segmentId = parseSegmentId(segment);
            if (segmentId > plan.safeBoundarySegmentId()) {
                break;
            }
            reclaimedBytes = Math.addExact(reclaimedBytes, Files.size(segment));
            deleted |= Files.deleteIfExists(segment);
        }
        if (reclaimedBytes != plan.safeBoundaryBytes()) {
            throw new IOException(
                "WAL reclaim plan changed while writer was stopped: planned=" + plan.safeBoundaryBytes() +
                    ", actual=" + reclaimedBytes);
        }
        if (deleted) {
            Utils.flushDir(directory.toAbsolutePath().normalize());
        }
        return reclaimedBytes;
    }

    private static long watermarkBytes(long capacityBytes, int percent) {
        long quotient = capacityBytes / 100L;
        long remainder = capacityBytes % 100L;
        return Math.addExact(
            Math.multiplyExact(quotient, percent),
            Math.multiplyExact(remainder, percent) / 100L
        );
    }

    private List<Path> segmentFiles() throws IOException {
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(RotatingFileSharedWal::isSegmentFile)
                .sorted(Comparator.comparingLong(RotatingFileSharedWal::parseSegmentId))
                .toList();
        }
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

    private record GroupEntry(WalRecord record, WalAppendResult appendResult) {
    }

    private record ReclaimPlan(long safeBoundarySegmentId, long safeBoundaryBytes) {
    }
}
