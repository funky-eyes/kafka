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
package org.apache.kafka.storage.internals.shared.metadata;

import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.zip.CRC32C;

/**
 * Crash-safe local snapshot of authoritative remote Kafka batch references.
 *
 * <p>The compacted Kafka metadata topic remains the cluster source of truth. This checkpoint is a broker-local recovery
 * accelerator and, more importantly, the durable prerequisite for deleting a WAL payload: after an unclean restart the
 * broker can reconstruct the logical cold ranges before the metadata client and S3 client are brought online.</p>
 *
 * <p>Visibility follows durability. A newly committed reference is published to readers only after a temporary file is
 * fsynced, atomically renamed over the previous checkpoint, and the parent directory is fsynced. Corrupt checkpoints
 * fail closed because the corresponding WAL bytes may already have been reclaimed.</p>
 */
public final class LocalRemoteObjectCheckpoint {
    static final int MAGIC = 0x4b524331; // KRC1
    static final short VERSION = 1;
    static final String FILE_NAME = "remote-object-ranges.checkpoint";
    private static final int HEADER_BYTES = Integer.BYTES + Short.BYTES + Short.BYTES + Integer.BYTES;
    private static final int ENTRY_BYTES =
        Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES +
            Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES;
    private static final int CHECKSUM_BYTES = Integer.BYTES;

    private final Path directory;
    private final Path checkpoint;
    private volatile Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> byPartition;

    public LocalRemoteObjectCheckpoint(Path directory) throws IOException {
        this.directory = Objects.requireNonNull(directory, "directory");
        Files.createDirectories(directory);
        this.checkpoint = directory.resolve(FILE_NAME);
        this.byPartition = immutable(load());
    }

    public Optional<RemoteObjectIndex.RangeReference> find(SharedPartitionId partition, long offset) {
        Objects.requireNonNull(partition, "partition");
        NavigableMap<Long, RemoteObjectIndex.RangeReference> ranges = byPartition.get(partition);
        if (ranges == null) {
            return Optional.empty();
        }
        Map.Entry<Long, RemoteObjectIndex.RangeReference> floor = ranges.floorEntry(offset);
        if (floor == null) {
            return Optional.empty();
        }
        OffsetRange offsets = floor.getValue().range().offsets();
        return offsets.startOffset() <= offset && offset < offsets.endOffset()
            ? Optional.of(floor.getValue())
            : Optional.empty();
    }

    public List<RemoteObjectIndex.RangeReference> ranges(
        SharedPartitionId partition,
        long startOffset,
        long endOffsetExclusive
    ) {
        Objects.requireNonNull(partition, "partition");
        if (startOffset < 0 || endOffsetExclusive < startOffset) {
            throw new IllegalArgumentException("invalid offset range");
        }
        NavigableMap<Long, RemoteObjectIndex.RangeReference> ranges = byPartition.get(partition);
        if (ranges == null || startOffset == endOffsetExclusive) {
            return List.of();
        }
        List<RemoteObjectIndex.RangeReference> result = new ArrayList<>();
        Map.Entry<Long, RemoteObjectIndex.RangeReference> floor = ranges.floorEntry(startOffset);
        if (floor != null && floor.getValue().range().offsets().endOffset() > startOffset) {
            result.add(floor.getValue());
        }
        for (RemoteObjectIndex.RangeReference reference : ranges.tailMap(startOffset, true).values()) {
            if (!result.isEmpty() && result.get(result.size() - 1) == reference) {
                continue;
            }
            if (reference.range().offsets().startOffset() >= endOffsetExclusive) {
                break;
            }
            result.add(reference);
        }
        return List.copyOf(result);
    }

    public List<RemoteObjectIndex.RangeReference> references() {
        return flatten(byPartition);
    }

    /** Atomically merges one authoritative committed object into the local durable snapshot. */
    public void add(SharedObjectMetadata object) throws IOException {
        addAll(List.of(Objects.requireNonNull(object, "object")));
    }

    /**
     * Atomically merges many authoritative committed objects and crosses one checkpoint durability barrier.
     *
     * <p>This is used after metadata replay so thousands of COMMIT records do not translate into thousands of complete
     * checkpoint rewrites. Either the whole staged snapshot becomes durable and visible or the previous snapshot stays
     * authoritative for local WAL-reclaim purposes.</p>
     */
    public synchronized void addAll(List<SharedObjectMetadata> objects) throws IOException {
        Objects.requireNonNull(objects, "objects");
        if (objects.isEmpty()) {
            return;
        }
        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> staged = mutableCopy(byPartition);
        for (SharedObjectMetadata object : objects) {
            mergeObject(staged, Objects.requireNonNull(object, "object"));
        }
        persist(flatten(staged));
        byPartition = immutable(staged);
    }

    private static void mergeObject(
        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> staged,
        SharedObjectMetadata object
    ) throws IOException {
        for (SharedObjectRange range : object.ranges()) {
            RemoteObjectIndex.RangeReference incoming = new RemoteObjectIndex.RangeReference(object.objectId(), range);
            NavigableMap<Long, RemoteObjectIndex.RangeReference> ranges = staged.computeIfAbsent(
                range.partition(), ignored -> new TreeMap<>());
            RemoteObjectIndex.RangeReference overlap = overlappingReference(ranges, range.offsets());
            if (overlap != null) {
                if (sameLogicalRange(overlap.range(), range)) {
                    continue;
                }
                throw new IOException(
                    "Conflicting local remote checkpoint range: existing=" + overlap + ", incoming=" + incoming);
            }
            ranges.put(range.offsets().startOffset(), incoming);
        }
    }

    private Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> load() throws IOException {
        if (!Files.exists(checkpoint)) {
            return new HashMap<>();
        }
        long fileSize = Files.size(checkpoint);
        if (fileSize < HEADER_BYTES + CHECKSUM_BYTES || fileSize > Integer.MAX_VALUE) {
            throw new IOException("Invalid local remote checkpoint size " + fileSize);
        }
        ByteBuffer bytes = ByteBuffer.allocate((int) fileSize).order(ByteOrder.BIG_ENDIAN);
        try (FileChannel channel = FileChannel.open(checkpoint, StandardOpenOption.READ)) {
            readFully(channel, bytes);
        }
        bytes.flip();
        int storedChecksum = bytes.getInt(bytes.limit() - CHECKSUM_BYTES);
        ByteBuffer checksummed = bytes.duplicate();
        checksummed.limit(bytes.limit() - CHECKSUM_BYTES);
        if ((int) crc32c(checksummed) != storedChecksum) {
            throw new IOException("Local remote checkpoint checksum mismatch");
        }

        int magic = bytes.getInt();
        short version = bytes.getShort();
        short reserved = bytes.getShort();
        int count = bytes.getInt();
        if (magic != MAGIC || version != VERSION || reserved != 0 || count < 0) {
            throw new IOException("Invalid local remote checkpoint header");
        }
        long expectedSize = HEADER_BYTES + Math.multiplyExact((long) count, ENTRY_BYTES) + CHECKSUM_BYTES;
        if (expectedSize != fileSize) {
            throw new IOException(
                "Local remote checkpoint length mismatch: expected=" + expectedSize + ", actual=" + fileSize);
        }

        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> loaded = new HashMap<>();
        for (int i = 0; i < count; i++) {
            RemoteObjectIndex.RangeReference reference = decode(bytes);
            NavigableMap<Long, RemoteObjectIndex.RangeReference> ranges = loaded.computeIfAbsent(
                reference.range().partition(), ignored -> new TreeMap<>());
            if (overlappingReference(ranges, reference.range().offsets()) != null) {
                throw new IOException("Overlapping ranges in local remote checkpoint");
            }
            ranges.put(reference.range().offsets().startOffset(), reference);
        }
        bytes.position(bytes.position() + CHECKSUM_BYTES);
        if (bytes.hasRemaining()) {
            throw new IOException("Trailing bytes in local remote checkpoint");
        }
        return loaded;
    }

    private void persist(List<RemoteObjectIndex.RangeReference> references) throws IOException {
        long totalBytes = HEADER_BYTES + Math.multiplyExact((long) references.size(), ENTRY_BYTES) + CHECKSUM_BYTES;
        if (totalBytes > Integer.MAX_VALUE) {
            throw new IOException("Local remote checkpoint exceeds Java buffer limit: " + totalBytes);
        }
        ByteBuffer bytes = ByteBuffer.allocate((int) totalBytes).order(ByteOrder.BIG_ENDIAN);
        bytes.putInt(MAGIC).putShort(VERSION).putShort((short) 0).putInt(references.size());
        for (RemoteObjectIndex.RangeReference reference : references) {
            encode(bytes, reference);
        }
        ByteBuffer checksummed = bytes.duplicate();
        checksummed.flip();
        bytes.putInt((int) crc32c(checksummed));
        bytes.flip();

        Path temporary = directory.resolve(FILE_NAME + ".tmp");
        try (FileChannel channel = FileChannel.open(
            temporary,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )) {
            writeFully(channel, bytes);
            channel.force(true);
        }
        try {
            Files.move(
                temporary,
                checkpoint,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
            );
        } catch (AtomicMoveNotSupportedException e) {
            Files.deleteIfExists(temporary);
            throw new IOException("WAL filesystem must support atomic remote-checkpoint replacement", e);
        }
        Utils.flushDir(directory.toAbsolutePath().normalize());
    }

    private static void encode(ByteBuffer bytes, RemoteObjectIndex.RangeReference reference) {
        SharedObjectRange range = reference.range();
        bytes.putLong(reference.objectId());
        bytes.putLong(range.partition().topicIdHigh());
        bytes.putLong(range.partition().topicIdLow());
        bytes.putInt(range.partition().partition());
        bytes.putInt(range.leaderEpoch());
        bytes.putLong(range.offsets().startOffset());
        bytes.putLong(range.offsets().endOffset());
        bytes.putLong(range.objectPosition());
        bytes.putInt(range.objectLength());
        bytes.putLong(range.checksum());
    }

    private static RemoteObjectIndex.RangeReference decode(ByteBuffer bytes) throws IOException {
        try {
            long objectId = bytes.getLong();
            SharedPartitionId partition = new SharedPartitionId(bytes.getLong(), bytes.getLong(), bytes.getInt());
            int leaderEpoch = bytes.getInt();
            OffsetRange offsets = new OffsetRange(bytes.getLong(), bytes.getLong());
            long objectPosition = bytes.getLong();
            int objectLength = bytes.getInt();
            long checksum = bytes.getLong();
            return new RemoteObjectIndex.RangeReference(
                objectId,
                new SharedObjectRange(
                    partition,
                    offsets,
                    leaderEpoch,
                    objectPosition,
                    objectLength,
                    checksum
                )
            );
        } catch (IllegalArgumentException | java.nio.BufferUnderflowException e) {
            throw new IOException("Invalid local remote checkpoint entry", e);
        }
    }

    private static Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> mutableCopy(
        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> source
    ) {
        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> copy = new HashMap<>();
        source.forEach((partition, ranges) -> copy.put(partition, new TreeMap<>(ranges)));
        return copy;
    }

    private static Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> immutable(
        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> source
    ) {
        Map<SharedPartitionId, NavigableMap<Long, RemoteObjectIndex.RangeReference>> copy = new HashMap<>();
        source.forEach((partition, ranges) -> copy.put(
            partition,
            java.util.Collections.unmodifiableNavigableMap(new TreeMap<>(ranges))
        ));
        return Map.copyOf(copy);
    }

    private static List<RemoteObjectIndex.RangeReference> flatten(
        Map<SharedPartitionId, ? extends NavigableMap<Long, RemoteObjectIndex.RangeReference>> source
    ) {
        List<RemoteObjectIndex.RangeReference> result = new ArrayList<>();
        source.values().forEach(ranges -> result.addAll(ranges.values()));
        result.sort(REFERENCE_ORDER);
        return List.copyOf(result);
    }

    private static RemoteObjectIndex.RangeReference overlappingReference(
        NavigableMap<Long, RemoteObjectIndex.RangeReference> ranges,
        OffsetRange incoming
    ) {
        Map.Entry<Long, RemoteObjectIndex.RangeReference> floor = ranges.floorEntry(incoming.startOffset());
        if (floor != null && overlaps(floor.getValue().range().offsets(), incoming)) {
            return floor.getValue();
        }
        Map.Entry<Long, RemoteObjectIndex.RangeReference> next = ranges.ceilingEntry(incoming.startOffset());
        if (next != null && overlaps(next.getValue().range().offsets(), incoming)) {
            return next.getValue();
        }
        return null;
    }

    private static boolean overlaps(OffsetRange left, OffsetRange right) {
        return left.startOffset() < right.endOffset() && right.startOffset() < left.endOffset();
    }

    private static boolean sameLogicalRange(SharedObjectRange left, SharedObjectRange right) {
        return left.offsets().equals(right.offsets()) &&
            left.checksum() == right.checksum() &&
            left.leaderEpoch() == right.leaderEpoch() &&
            left.objectLength() == right.objectLength();
    }

    private static void readFully(FileChannel channel, ByteBuffer target) throws IOException {
        while (target.hasRemaining()) {
            int read = channel.read(target);
            if (read < 0) {
                throw new IOException("Unexpected EOF reading local remote checkpoint");
            }
            if (read == 0) {
                Thread.yield();
            }
        }
    }

    private static void writeFully(FileChannel channel, ByteBuffer source) throws IOException {
        while (source.hasRemaining()) {
            int written = channel.write(source);
            if (written <= 0) {
                throw new IOException("Unable to make progress writing local remote checkpoint");
            }
        }
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }

    private static final Comparator<RemoteObjectIndex.RangeReference> REFERENCE_ORDER = Comparator
        .comparingLong((RemoteObjectIndex.RangeReference reference) -> reference.range().partition().topicIdHigh())
        .thenComparingLong(reference -> reference.range().partition().topicIdLow())
        .thenComparingInt(reference -> reference.range().partition().partition())
        .thenComparingLong(reference -> reference.range().offsets().startOffset());
}