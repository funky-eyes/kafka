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
 * appends: it briefly drains readers that may still have old segment files open, reclaims an immutable safe prefix, and
 * only seals the current active segment when additional safe headroom is required. The writer thread remains alive and
 * new appends continue after the short segment-seal I/O critical section.</p>
 *
 * <p>Recently durable DATA records are retained in a bounded in-memory read cache. Producer acknowledgement still
 * depends exclusively on the underlying WAL durability barrier; the cache is populated only after that barrier and is
 * therefore a disposable performance layer.</p>
 *
 * <p>Remote commit makes bytes reclaimable but does not immediately discard their local recovery copy. Normal
 * maintenance retains the most recent WAL until physical usage reaches the high watermark, then frees only enough of
 * the oldest safe prefix to return near the low watermark.</p>
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
        return reclaim(policy, Math.max(1L, used - lowWatermarkBytes));
    }

    @Override
    public long reclaim(WalReclaimPolicy policy, long desiredBytes) throws IOException {
        Objects.requireNonNull(policy, "policy");
        if (desiredBytes <= 0) {
            throw new IllegalArgumentException("desiredBytes must be positive");
        }

        long activeSegmentId = beginReclaim();
        try {
            ReclaimPlan plan = planWithOptionalActiveSeal(policy, desiredBytes, activeSegmentId);
            return executeReclaim(plan);
        } finally {
            endReclaim();
        }
    }

    private long beginReclaim() {
        synchronized (lifecycleLock) {
            awaitReclaim();
            ensureOpen();
            reclaiming = true;
            while (inFlightReaders > 0) {
                waitUninterruptibly();
            }
            return delegate.activeSegmentId();
        }
    }

    private void endReclaim() {
        synchronized (lifecycleLock) {
            reclaiming = false;
            lifecycleLock.notifyAll();
        }
    }

    private ReclaimPlan planWithOptionalActiveSeal(
        WalReclaimPolicy policy,
        long desiredBytes,
        long activeSegmentId
    ) throws IOException {
        long exclusiveSegmentId = activeSegmentId >= 0 ? activeSegmentId : Long.MAX_VALUE;
        ReclaimPlan plan = buildReclaimPlan(policy, desiredBytes, exclusiveSegmentId);
        if (!shouldSealActiveSegment(plan, desiredBytes, activeSegmentId)) {
            return plan;
        }
        long sealedSegmentId = delegate.sealActiveSegment();
        if (sealedSegmentId < 0) {
            return plan;
        }
        return buildReclaimPlan(policy, desiredBytes, Math.addExact(sealedSegmentId, 1L));
    }

    private static boolean shouldSealActiveSegment(
        ReclaimPlan plan,
        long desiredBytes,
        long activeSegmentId
    ) {
        return activeSegmentId >= 0 &&
            plan.safeBoundaryBytes() < desiredBytes &&
            !plan.blockedByUnsafeGroup();
    }

    private long executeReclaim(ReclaimPlan plan) throws IOException {
        try {
            ReclaimResult result = deleteReclaimableSegments(plan);
            applyReclaimResult(result);
            return result.reclaimedBytes();
        } catch (PartialReclaimException e) {
            applyReclaimResult(e.result());
            throw e;
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
        long exclusiveSegmentId
    ) throws IOException {
        List<GroupEntry> pendingGroup = new ArrayList<>();
        long safeBoundarySegmentId = -1L;
        long safeBoundaryBytes = 0L;
        long scannedSegmentBytes = 0L;
        boolean blockedByUnsafeGroup = false;

        for (Path segment : segmentFiles()) {
            long segmentId = parseSegmentId(segment);
            if (segmentId >= exclusiveSegmentId) {
                break;
            }
            if (!scanReclaimableSegment(segment, segmentId, policy, pendingGroup)) {
                blockedByUnsafeGroup = true;
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
        return new ReclaimPlan(safeBoundarySegmentId, safeBoundaryBytes, blockedByUnsafeGroup);
    }

    private boolean scanReclaimableSegment(
        Path segment,
        long segmentId,
        WalReclaimPolicy policy,
        List<GroupEntry> pendingGroup
    ) throws IOException {
        try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
            long position = 0L;
            while (true) {
                WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, position);
                if (result.status() == WalRecordCodec.ReadStatus.EOF) {
                    return true;
                }
                requireCompleteReclaimRecord(result, segmentId, position);
                if (!acceptReclaimRecord(result.record(), result.length(), segmentId, position, policy, pendingGroup)) {
                    return false;
                }
                position += result.length();
            }
        }
    }

    private static void requireCompleteReclaimRecord(
        WalRecordCodec.ReadResult result,
        long segmentId,
        long position
    ) throws WalCorruptionException {
        if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) {
            throw new WalCorruptionException(
                "Cannot reclaim WAL containing a partial record at segment=" + segmentId + ", position=" + position);
        }
    }

    private static boolean acceptReclaimRecord(
        WalRecord record,
        int recordLength,
        long segmentId,
        long position,
        WalReclaimPolicy policy,
        List<GroupEntry> pendingGroup
    ) throws WalCorruptionException {
        if (record.type() != WalRecordType.GROUP_COMMIT) {
            pendingGroup.add(new GroupEntry(
                record,
                new WalAppendResult(segmentId, position, recordLength)
            ));
            return true;
        }
        validateGroupCommit(record, pendingGroup);
        boolean reclaimable = groupReclaimable(policy, pendingGroup);
        pendingGroup.clear();
        return reclaimable;
    }

    private static void validateGroupCommit(WalRecord commit, List<GroupEntry> pendingGroup)
        throws WalCorruptionException {
        if (pendingGroup.size() != commit.groupRecordCount()) {
            throw new WalCorruptionException(
                "WAL group commit count mismatch during reclaim for group " + commit.groupId() +
                    ": expected=" + commit.groupRecordCount() + ", actual=" + pendingGroup.size());
        }
    }

    private static boolean groupReclaimable(WalReclaimPolicy policy, List<GroupEntry> pendingGroup) {
        for (GroupEntry entry : pendingGroup) {
            if (!policy.canReclaim(entry.record(), entry.appendResult())) {
                return false;
            }
        }
        return true;
    }

    private ReclaimResult deleteReclaimableSegments(ReclaimPlan plan) throws IOException {
        if (plan.safeBoundarySegmentId() < 0) {
            return ReclaimResult.NONE;
        }
        DeletionState state = new DeletionState();
        IOException failure = deletePlannedSegments(plan.safeBoundarySegmentId(), state);
        failure = flushDeletedDirectory(state, failure);
        ReclaimResult result = state.result();
        validateDeletionResult(plan, result, failure);
        return result;
    }

    private IOException deletePlannedSegments(long safeBoundarySegmentId, DeletionState state) throws IOException {
        for (Path segment : segmentFiles()) {
            long segmentId = parseSegmentId(segment);
            if (segmentId > safeBoundarySegmentId) {
                break;
            }
            IOException failure = deleteSegment(segment, segmentId, state);
            if (failure != null) {
                return failure;
            }
        }
        return null;
    }

    private static IOException deleteSegment(Path segment, long segmentId, DeletionState state) {
        try {
            long segmentBytes = Files.size(segment);
            if (Files.deleteIfExists(segment)) {
                state.record(segmentId, segmentBytes);
            }
            return null;
        } catch (IOException e) {
            return e;
        }
    }

    private IOException flushDeletedDirectory(DeletionState state, IOException failure) {
        if (!state.deleted()) {
            return failure;
        }
        try {
            Utils.flushDir(directory.toAbsolutePath().normalize());
            return failure;
        } catch (IOException flushError) {
            return mergeFailures(failure, flushError);
        }
    }

    private static IOException mergeFailures(IOException primary, IOException additional) {
        if (primary == null) {
            return additional;
        }
        primary.addSuppressed(additional);
        return primary;
    }

    private static void validateDeletionResult(
        ReclaimPlan plan,
        ReclaimResult result,
        IOException failure
    ) throws PartialReclaimException {
        if (failure != null) {
            throw new PartialReclaimException(failure, result);
        }
        if (result.reclaimedBytes() != plan.safeBoundaryBytes()) {
            throw new PartialReclaimException(
                new IOException(
                    "WAL reclaim plan changed while deleting immutable segments: planned=" + plan.safeBoundaryBytes() +
                        ", actual=" + result.reclaimedBytes()),
                result
            );
        }
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

    private record ReclaimPlan(long safeBoundarySegmentId, long safeBoundaryBytes, boolean blockedByUnsafeGroup) {
    }

    private record ReclaimResult(long reclaimedBytes, long reclaimedThroughSegmentId) {
        private static final ReclaimResult NONE = new ReclaimResult(0L, -1L);
    }

    private static final class DeletionState {
        private long reclaimedBytes;
        private long reclaimedThroughSegmentId = -1L;
        private boolean deleted;

        private void record(long segmentId, long segmentBytes) {
            reclaimedBytes = Math.addExact(reclaimedBytes, segmentBytes);
            reclaimedThroughSegmentId = segmentId;
            deleted = true;
        }

        private boolean deleted() {
            return deleted;
        }

        private ReclaimResult result() {
            return new ReclaimResult(reclaimedBytes, reclaimedThroughSegmentId);
        }
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
