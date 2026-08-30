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
 * <p>The underlying WAL remains the single-writer, group-commit implementation. Reclamation is online with respect to
 * appends: it snapshots the current active logical segment, briefly drains readers that may still have old segment
 * files open, and deletes only a contiguous safe prefix strictly before that active segment. The WAL writer remains
 * open and can continue group-committing into the active or newer segments while old immutable segments are reclaimed.
 * This preserves the existing crash format while removing the former close/delete/reopen stop-the-world cycle.</p>
 *
 * <p>Recently durable DATA records are retained in a bounded in-memory read cache. Producer acknowledgement still
 * depends exclusively on the underlying WAL durability barrier; the cache is populated only after that barrier and is
 * therefore a disposable performance layer. Upload packing and hot reads can reuse the owned Kafka RecordBatch bytes
 * without re-reading the physical WAL. Cache misses transparently fall back to the WAL.</p>
 *
 * <p>Remote commit makes bytes reclaimable but does not immediately discard their local recovery copy. Normal
 * maintenance retains the most recent WAL until physical usage reaches the high watermark, then frees only enough of
 * the oldest safe prefix to return near the low watermark. This preserves a useful local recovery window through a
 * short object-store outage while still making the configured WAL capacity reusable indefinitely in steady state.</p>
 *
 * <p>Deletion is permitted only through a physical segment boundary that is also an append-group boundary. A group
 * spanning an immutable segment and the current active segment therefore pins the old segment until the active segment
 * rolls and the complete group can be proven safe on a later maintenance pass.</p>
 */
public final class RotatingFileSharedWal implements SharedWal {
    static final int DEFAULT_RECLAIM_HIGH_WATERMARK_PERCENT = 85;
    static final int DEFAULT_RECLAIM_LOW_WATERMARK_PERCENT = 70;
    static final long DEFAULT_READ_CACHE_BYTES = 256L * 1024 * 1024;
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("wal-(\\d{20})\\.log");

    private final Object lifecycleLock = new Object();
    private final Path directory;
    private final long capacityBytes;
    private final WalReadCache readCache;
    private final FileSharedWal delegate;

    private int inFlightOperations;
    private int inFlightReaders;
    private boolean reclaiming;
    private boolean closed;
    private volatile long reclaimedThroughSegmentId = -1L;

    public RotatingFileSharedWal(Path directory, long capacityBytes, long segmentBytes) throws IOException {
        this(directory, capacityBytes, segmentBytes, DEFAULT_READ_CACHE_BYTES);
    }

    RotatingFileSharedWal(Path directory, long capacityBytes, long segmentBytes, long readCacheBytes) throws IOException {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.capacityBytes = capacityBytes;
        this.readCache = new WalReadCache(readCacheBytes);
        this.delegate = new FileSharedWal(directory, capacityBytes, segmentBytes);
    }

    @Override
    public CompletableFuture<List<WalAppendResult>> appendBatch(List<WalRecord> records) {
        Objects.requireNonNull(records, "records");
        List<WalRecord> immutableRecords = List.copyOf(records);
        synchronized (lifecycleLock) {
            ensureOpen();
            inFlightOperations++;
        }

        final CompletableFuture<List<WalAppendResult>> append;
        try {
            append = delegate.appendBatch(immutableRecords);
        } catch (Throwable t) {
            endOperation(false);
            throw t;
        }
        return append.thenApply(results -> {
            List<WalAppendResult> immutableResults = List.copyOf(results);
            readCache.putAll(immutableRecords, immutableResults);
            return immutableResults;
        }).whenComplete((ignored, error) -> endOperation(false));
    }

    @Override
    public WalRecord read(WalLocation location) throws IOException {
        Objects.requireNonNull(location, "location");
        beginReadOperation();
        try {
            WalRecord cached = readCache.get(location);
            return cached != null ? cached : delegate.read(location);
        } finally {
            endOperation(true);
        }
    }

    @Override
    public List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        Objects.requireNonNull(locations, "locations");
        if (locations.isEmpty()) {
            return List.of();
        }
        beginReadOperation();
        try {
            WalRecord[] records = new WalRecord[locations.size()];
            List<WalLocation> misses = new ArrayList<>();
            List<Integer> missIndexes = new ArrayList<>();
            for (int i = 0; i < locations.size(); i++) {
                WalLocation location = Objects.requireNonNull(locations.get(i), "location");
                WalRecord cached = readCache.get(location);
                if (cached == null) {
                    misses.add(location);
                    missIndexes.add(i);
                } else {
                    records[i] = cached;
                }
            }
            if (!misses.isEmpty()) {
                List<WalRecord> loaded = delegate.readBatch(misses);
                for (int i = 0; i < loaded.size(); i++) {
                    records[missIndexes.get(i)] = loaded.get(i);
                }
            }
            return List.of(records);
        } finally {
            endOperation(true);
        }
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        beginReadOperation();
        try {
            delegate.replay(consumer);
        } finally {
            endOperation(true);
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

        long activeSegmentId;
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            reclaiming = true;
            while (inFlightReaders > 0) {
                waitUninterruptibly();
            }
            activeSegmentId = delegate.activeSegmentId();
        }

        try {
            ReclaimPlan plan = buildReclaimPlan(policy, desiredBytes, activeSegmentId);
            try {
                ReclaimResult result = deleteReclaimableSegments(plan);
                applyReclaimResult(result);
                return result.reclaimedBytes();
            } catch (PartialReclaimException e) {
                applyReclaimResult(e.result());
                throw e;
            }
        } finally {
            synchronized (lifecycleLock) {
                reclaiming = false;
                lifecycleLock.notifyAll();
            }
        }
    }

    private void applyReclaimResult(ReclaimResult result) {
        if (result.reclaimedBytes() <= 0) {
            return;
        }
        delegate.releaseReclaimedBytes(result.reclaimedBytes());
        reclaimedThroughSegmentId = Math.max(reclaimedThroughSegmentId, result.reclaimedThroughSegmentId());
        readCache.clear();
    }

    @Override
    public long reclaimedThroughSegmentId() {
        return reclaimedThroughSegmentId;
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
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            awaitReclaim();
            closed = true;
            while (inFlightOperations > 0) {
                waitUninterruptibly();
            }
        }
        readCache.clear();
        delegate.close();
    }

    int cachedEntryCount() {
        return readCache.entryCount();
    }

    long cachedBytes() {
        return readCache.usedBytes();
    }

    private void beginReadOperation() {
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            inFlightOperations++;
            inFlightReaders++;
        }
    }

    private void endOperation(boolean reader) {
        synchronized (lifecycleLock) {
            inFlightOperations--;
            if (reader) {
                inFlightReaders--;
            }
            if (inFlightOperations < 0 || inFlightReaders < 0 || inFlightReaders > inFlightOperations) {
                throw new IllegalStateException(
                    "Rotating WAL operation accounting underflow: operations=" + inFlightOperations +
                        ", readers=" + inFlightReaders);
            }
            if (inFlightOperations == 0 || inFlightReaders == 0) {
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

    private ReclaimPlan buildReclaimPlan(
        WalReclaimPolicy policy,
        long desiredBytes,
        long activeSegmentId
    ) throws IOException {
        List<Path> segments = segmentFiles();
        List<GroupEntry> pendingGroup = new ArrayList<>();
        long safeBoundarySegmentId = -1L;
        long safeBoundaryBytes = 0L;
        long scannedSegmentBytes = 0L;
        boolean prefixReclaimable = true;

        for (Path segment : segments) {
            long segmentId = parseSegmentId(segment);
            if (activeSegmentId >= 0 && segmentId >= activeSegmentId) {
                break;
            }
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

    private ReclaimResult deleteReclaimableSegments(ReclaimPlan plan) throws IOException {
        if (plan.safeBoundarySegmentId() < 0) {
            return ReclaimResult.NONE;
        }
        long reclaimedBytes = 0L;
        long reclaimedThroughSegmentId = -1L;
        boolean deleted = false;
        IOException failure = null;
        for (Path segment : segmentFiles()) {
            long segmentId = parseSegmentId(segment);
            if (segmentId > plan.safeBoundarySegmentId()) {
                break;
            }
            long segmentBytes = Files.size(segment);
            try {
                if (Files.deleteIfExists(segment)) {
                    reclaimedBytes = Math.addExact(reclaimedBytes, segmentBytes);
                    reclaimedThroughSegmentId = segmentId;
                    deleted = true;
                }
            } catch (IOException e) {
                failure = e;
                break;
            }
        }
        if (deleted) {
            try {
                Utils.flushDir(directory.toAbsolutePath().normalize());
            } catch (IOException flushError) {
                if (failure == null) {
                    failure = flushError;
                } else {
                    failure.addSuppressed(flushError);
                }
            }
        }
        ReclaimResult result = new ReclaimResult(reclaimedBytes, reclaimedThroughSegmentId);
        if (failure != null) {
            throw new PartialReclaimException(failure, result);
        }
        if (reclaimedBytes != plan.safeBoundaryBytes()) {
            throw new PartialReclaimException(
                new IOException(
                    "WAL reclaim plan changed while deleting immutable segments: planned=" + plan.safeBoundaryBytes() +
                        ", actual=" + reclaimedBytes),
                result
            );
        }
        return result;
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

    private record ReclaimResult(long reclaimedBytes, long reclaimedThroughSegmentId) {
        private static final ReclaimResult NONE = new ReclaimResult(0L, -1L);
    }

    private static final class PartialReclaimException extends IOException {
        private final ReclaimResult result;

        private PartialReclaimException(IOException cause, ReclaimResult result) {
            super(
                "WAL reclaim partially deleted immutable segments: reclaimedBytes=" + result.reclaimedBytes() +
                    ", reclaimedThroughSegmentId=" + result.reclaimedThroughSegmentId(),
                cause
            );
            this.result = result;
        }

        private ReclaimResult result() {
            return result;
        }
    }
}
