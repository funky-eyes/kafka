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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Broker-wide append-only WAL with a single writer and natural group commit.
 *
 * <p>Append futures are completed only after every record in the drained writer batch is persisted with
 * {@link FileChannel#force(boolean)}. When a new segment is created, its parent directory is also flushed before
 * the append future completes. This is the durability boundary consumed by Kafka's leader and follower append paths.</p>
 */
public final class FileSharedWal implements SharedWal {
    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile("wal-(\\d{20})\\.log");
    private static final int MAX_DRAINED_APPENDS = 1024;

    private final Path directory;
    private final long capacityBytes;
    private final long segmentBytes;
    private final LinkedBlockingQueue<PendingAppend> pendingAppends = new LinkedBlockingQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object lifecycleLock = new Object();
    private final Thread writerThread;

    private volatile boolean accepting = true;
    private volatile long usedBytes;
    private volatile Throwable failure;
    private long nextSegmentId;
    private SegmentWriter activeSegment;

    public FileSharedWal(Path directory, long capacityBytes, long segmentBytes) throws IOException {
        this.directory = Objects.requireNonNull(directory, "directory");
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
        this.usedBytes = recovery.usedBytes;
        this.nextSegmentId = recovery.nextSegmentId;
        this.activeSegment = openActiveSegment(recovery.lastSegmentId);

        this.writerThread = new Thread(this::writerLoop, "shared-wal-writer");
        this.writerThread.setDaemon(true);
        this.writerThread.start();
    }

    @Override
    public CompletableFuture<WalAppendResult> append(WalRecord record) {
        Objects.requireNonNull(record, "record");
        CompletableFuture<WalAppendResult> future = new CompletableFuture<>();
        WalRecordCodec.EncodedRecord encoded;
        try {
            encoded = WalRecordCodec.encode(record);
        } catch (Throwable t) {
            future.completeExceptionally(t);
            return future;
        }
        if (encoded.totalLength() > segmentBytes) {
            future.completeExceptionally(new IllegalArgumentException(
                "Encoded WAL record size " + encoded.totalLength() + " exceeds segmentBytes " + segmentBytes));
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
            pendingAppends.add(new PendingAppend(encoded, future));
        }
        return future;
    }

    @Override
    public long usedBytes() {
        return usedBytes;
    }

    @Override
    public long capacityBytes() {
        return capacityBytes;
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
            try (FileChannel channel = FileChannel.open(segmentPath(entry.getKey()), StandardOpenOption.READ)) {
                for (IndexedLocation indexed : entry.getValue()) {
                    result[indexed.index] = readAt(channel, indexed.location);
                }
            }
        }
        return List.copyOf(Arrays.asList(result));
    }

    @Override
    public void replay(WalReplayConsumer consumer) throws IOException {
        Objects.requireNonNull(consumer, "consumer");
        List<Path> segments = segmentFiles();
        for (int i = 0; i < segments.size(); i++) {
            Path segment = segments.get(i);
            long segmentId = parseSegmentId(segment);
            boolean lastSegment = i == segments.size() - 1;
            try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ)) {
                long position = 0;
                while (true) {
                    WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, position);
                    if (result.status() == WalRecordCodec.ReadStatus.EOF) {
                        break;
                    }
                    if (result.status() == WalRecordCodec.ReadStatus.PARTIAL) {
                        if (!lastSegment) {
                            throw new WalCorruptionException("Partial WAL record found in sealed segment " + segment);
                        }
                        break;
                    }
                    consumer.accept(result.record(), new WalAppendResult(segmentId, position, result.length()));
                    position += result.length();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        synchronized (lifecycleLock) {
            accepting = false;
            if (running.getAndSet(false)) {
                pendingAppends.offer(PendingAppend.poison());
            }
        }

        IOException closeError = null;
        try {
            writerThread.join(TimeUnit.SECONDS.toMillis(30));
            if (writerThread.isAlive()) {
                closeError = new IOException("Timed out waiting for WAL writer to stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closeError = new IOException("Interrupted while closing WAL", e);
        } finally {
            if (activeSegment != null) {
                try {
                    activeSegment.close();
                } catch (IOException e) {
                    if (closeError == null) {
                        closeError = e;
                    } else {
                        closeError.addSuppressed(e);
                    }
                } finally {
                    activeSegment = null;
                }
            }
        }
        if (closeError != null) {
            throw closeError;
        }
    }

    private void writerLoop() {
        List<PendingAppend> batch = new ArrayList<>(MAX_DRAINED_APPENDS);
        while (running.get() || !pendingAppends.isEmpty()) {
            try {
                batch.clear();
                PendingAppend first = pendingAppends.take();
                if (first.poison) {
                    if (!running.get() && pendingAppends.isEmpty()) {
                        break;
                    }
                    continue;
                }
                batch.add(first);
                while (batch.size() < MAX_DRAINED_APPENDS) {
                    PendingAppend next = pendingAppends.poll();
                    if (next == null) {
                        break;
                    }
                    if (next.poison) {
                        pendingAppends.offer(next);
                        break;
                    }
                    batch.add(next);
                }
                writeBatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failWriter(new IOException("WAL writer interrupted", e), batch);
                return;
            } catch (Throwable t) {
                failWriter(t, batch);
                return;
            }
        }
    }

    private void failWriter(Throwable t, List<PendingAppend> currentBatch) {
        synchronized (lifecycleLock) {
            failure = t;
            accepting = false;
            running.set(false);
        }
        failPending(currentBatch, t);
        PendingAppend append;
        while ((append = pendingAppends.poll()) != null) {
            if (!append.poison) {
                append.future.completeExceptionally(t);
            }
        }
    }

    private void writeBatch(List<PendingAppend> batch) throws IOException {
        long batchBytes = 0;
        for (PendingAppend append : batch) {
            batchBytes = Math.addExact(batchBytes, append.encoded.totalLength());
        }
        if (usedBytes + batchBytes > capacityBytes) {
            WalCapacityExceededException error = new WalCapacityExceededException(
                "WAL capacity exceeded: used=" + usedBytes + ", batch=" + batchBytes + ", capacity=" + capacityBytes);
            failPending(batch, error);
            return;
        }

        List<PendingResult> results = new ArrayList<>(batch.size());
        for (PendingAppend append : batch) {
            int length = append.encoded.totalLength();
            ensureWritableSegment(length);
            long position = activeSegment.position;
            writeEncoded(activeSegment.channel, append.encoded, position);
            activeSegment.position += length;
            usedBytes += length;
            results.add(new PendingResult(append.future, new WalAppendResult(activeSegment.id, position, length)));
        }

        // One durability barrier for the entire drained batch. Segment creation metadata is flushed as part of the same barrier.
        forceSegment(activeSegment);
        for (PendingResult result : results) {
            result.future.complete(result.result);
        }
    }

    private void ensureWritableSegment(int recordLength) throws IOException {
        if (activeSegment == null) {
            activeSegment = createSegment(nextSegmentId++);
            return;
        }
        if (activeSegment.position > 0 && activeSegment.position + recordLength > segmentBytes) {
            forceSegment(activeSegment);
            activeSegment.close();
            activeSegment = createSegment(nextSegmentId++);
        }
    }

    private void forceSegment(SegmentWriter segment) throws IOException {
        segment.channel.force(false);
        if (segment.directoryEntryDirty) {
            Utils.flushDir(directory.toAbsolutePath().normalize());
            segment.directoryEntryDirty = false;
        }
    }

    private RecoveryState recoverSegments() throws IOException {
        List<Path> segments = segmentFiles();
        long totalBytes = 0;
        long lastSegmentId = -1;
        for (int i = 0; i < segments.size(); i++) {
            Path segment = segments.get(i);
            long segmentId = parseSegmentId(segment);
            lastSegmentId = Math.max(lastSegmentId, segmentId);
            boolean lastSegment = i == segments.size() - 1;
            try (FileChannel channel = FileChannel.open(segment, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                long position = 0;
                while (true) {
                    WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, position);
                    if (result.status() == WalRecordCodec.ReadStatus.EOF) {
                        break;
                    }
                    if (result.status() == WalRecordCodec.ReadStatus.PARTIAL) {
                        if (!lastSegment) {
                            throw new WalCorruptionException("Partial WAL record found in sealed segment " + segment);
                        }
                        channel.truncate(position);
                        channel.force(false);
                        break;
                    }
                    position += result.length();
                }
                totalBytes += channel.size();
            }
        }
        if (totalBytes > capacityBytes) {
            throw new WalCapacityExceededException(
                "Recovered WAL exceeds configured capacity: used=" + totalBytes + ", capacity=" + capacityBytes);
        }
        return new RecoveryState(totalBytes, lastSegmentId, lastSegmentId + 1);
    }

    private SegmentWriter openActiveSegment(long lastSegmentId) throws IOException {
        if (lastSegmentId < 0) {
            return null;
        }
        Path path = segmentPath(lastSegmentId);
        long size = Files.size(path);
        if (size >= segmentBytes) {
            return null;
        }
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        return new SegmentWriter(lastSegmentId, channel, size, false);
    }

    private SegmentWriter createSegment(long segmentId) throws IOException {
        Path path = segmentPath(segmentId);
        FileChannel channel = FileChannel.open(
            path,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
        );
        return new SegmentWriter(segmentId, channel, 0, true);
    }

    private WalRecord readAt(FileChannel channel, WalLocation location) throws IOException {
        WalRecordCodec.ReadResult result = WalRecordCodec.read(channel, location.position());
        if (result.status() != WalRecordCodec.ReadStatus.COMPLETE) {
            throw new WalCorruptionException("WAL location does not point to a complete record: " + location);
        }
        if (result.length() != location.length()) {
            throw new WalCorruptionException("WAL location length mismatch: expected=" + location.length() +
                ", actual=" + result.length());
        }
        return result.record();
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream
                .filter(Files::isRegularFile)
                .filter(path -> SEGMENT_FILE_PATTERN.matcher(path.getFileName().toString()).matches())
                .sorted(Comparator.comparingLong(FileSharedWal::parseSegmentId))
                .toList();
        }
    }

    private Path segmentPath(long segmentId) {
        return directory.resolve(String.format("wal-%020d.log", segmentId));
    }

    private static long parseSegmentId(Path path) {
        Matcher matcher = SEGMENT_FILE_PATTERN.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a WAL segment: " + path);
        }
        return Long.parseLong(matcher.group(1));
    }

    private static void writeEncoded(FileChannel channel, WalRecordCodec.EncodedRecord encoded, long position) throws IOException {
        long currentPosition = position;
        ByteBuffer header = encoded.header();
        currentPosition += writeFully(channel, header, currentPosition);
        ByteBuffer payload = encoded.payload();
        if (payload.hasRemaining()) {
            currentPosition += writeFully(channel, payload, currentPosition);
        }
        if (currentPosition - position != encoded.totalLength()) {
            throw new IOException("WAL write length mismatch: expected=" + encoded.totalLength() +
                ", actual=" + (currentPosition - position));
        }
    }

    private static int writeFully(FileChannel channel, ByteBuffer buffer, long position) throws IOException {
        long currentPosition = position;
        int totalWritten = 0;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, currentPosition);
            if (written <= 0) {
                throw new IOException("Unable to make progress writing WAL at position " + currentPosition);
            }
            currentPosition += written;
            totalWritten += written;
        }
        return totalWritten;
    }

    private static void failPending(List<PendingAppend> batch, Throwable t) {
        for (PendingAppend append : batch) {
            append.future.completeExceptionally(t);
        }
    }

    private static final class PendingAppend {
        private final WalRecordCodec.EncodedRecord encoded;
        private final CompletableFuture<WalAppendResult> future;
        private final boolean poison;

        private PendingAppend(WalRecordCodec.EncodedRecord encoded, CompletableFuture<WalAppendResult> future) {
            this(encoded, future, false);
        }

        private PendingAppend(WalRecordCodec.EncodedRecord encoded, CompletableFuture<WalAppendResult> future, boolean poison) {
            this.encoded = encoded;
            this.future = future;
            this.poison = poison;
        }

        private static PendingAppend poison() {
            return new PendingAppend(null, new CompletableFuture<>(), true);
        }
    }

    private record PendingResult(CompletableFuture<WalAppendResult> future, WalAppendResult result) {
    }

    private record IndexedLocation(int index, WalLocation location) {
    }

    private record RecoveryState(long usedBytes, long lastSegmentId, long nextSegmentId) {
    }

    private static final class SegmentWriter implements AutoCloseable {
        private final long id;
        private final FileChannel channel;
        private long position;
        private boolean directoryEntryDirty;

        private SegmentWriter(long id, FileChannel channel, long position, boolean directoryEntryDirty) {
            this.id = id;
            this.channel = channel;
            this.position = position;
            this.directoryEntryDirty = directoryEntryDirty;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
