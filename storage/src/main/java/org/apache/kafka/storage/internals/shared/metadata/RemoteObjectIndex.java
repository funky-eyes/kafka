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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Logical remote index. Physical objects may be duplicated after a leader race, but a logical Kafka offset range
 * has one content identity. Overlapping ranges are accepted only when the offset range, leader epoch, RecordBatch
 * byte length and checksum are identical. Different physical pack layouts may use different object positions,
 * whole-object sizes and checksums.
 *
 * <p>Uploaders are expected to emit metadata at Kafka RecordBatch boundaries. This makes conflict detection
 * deterministic even when multiple partitions are packed into different physical S3 objects.
 * Each logical batch retains at most one alternative physical reference for
 * verified read fallback; neither reference is authority for COMMITTED GC.</p>
 */
public final class RemoteObjectIndex {
    private final ConcurrentHashMap<SharedPartitionId, ConcurrentNavigableMap<Long, RangeReference>> byPartition =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SharedPartitionId, ConcurrentNavigableMap<Long, RangeReference>> alternates =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SharedPartitionId, PartitionRemoteCoverage> coverage = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SharedPartitionId, AtomicLong> revisions = new ConcurrentHashMap<>();

    /**
     * Validates every range in an object before publishing any of them.
     *
     * <p>A single S3 object can contain ranges from many partitions. A conflict in one partition must not leave ranges
     * from earlier partitions visible, otherwise a corruption event could expose a logically half-committed object.
     * Remote metadata commits are infrequent compared with reads, so serializing writers here is an acceptable cost;
     * readers remain lock-free over the concurrent range maps.</p>
     */
    public synchronized PublicationResult add(SharedObjectMetadata object) {
        Objects.requireNonNull(object, "object");
        List<RangeReference> references = object.ranges().stream()
            .map(range -> new RangeReference(
                object.objectId(),
                object.objectSize(),
                object.objectChecksum(),
                range
            ))
            .toList();
        boolean readViewChanged = addReferences(references);
        boolean objectReferenced = references.stream().anyMatch(this::isCurrentReference);
        return new PublicationResult(readViewChanged, objectReferenced);
    }

    /**
     * Restores ranges that were durably checkpointed locally after an authoritative metadata COMMIT.
     *
     * <p>This is intentionally the same conflict-checked publication path as live metadata. A stale or corrupt local
     * checkpoint therefore cannot silently override a different logical Kafka range when the authoritative metadata
     * image is replayed later during broker startup.</p>
     */
    public synchronized void restore(List<RangeReference> references) {
        Objects.requireNonNull(references, "references");
        addReferences(List.copyOf(references));
    }

    private boolean addReferences(List<RangeReference> references) {
        Map<SharedPartitionId, NavigableMap<Long, RangeReference>> stagedByPartition = new HashMap<>();
        Map<SharedPartitionId, NavigableMap<Long, RangeReference>> stagedAlternates = new HashMap<>();
        List<RangeReference> updates = new ArrayList<>();
        List<RangeReference> alternateUpdates = new ArrayList<>();

        for (RangeReference incoming : references) {
            stageReference(incoming, stagedByPartition, stagedAlternates, updates, alternateUpdates);
        }

        // Both primary and alternate maps are published only after every
        // packed range has passed conflict and immutable-descriptor validation.
        for (RangeReference alternate : alternateUpdates) {
            alternates.computeIfAbsent(
                alternate.range().partition(), ignored -> new ConcurrentSkipListMap<>()
            ).put(alternate.range().offsets().startOffset(), alternate);
        }
        Set<SharedPartitionId> changedPartitions = new HashSet<>();
        for (RangeReference reference : updates) {
            SharedObjectRange range = reference.range();
            byPartition
                .computeIfAbsent(range.partition(), ignored -> new ConcurrentSkipListMap<>())
                .put(range.offsets().startOffset(), reference);
            changedPartitions.add(range.partition());
        }
        for (RangeReference reference : references) {
            SharedObjectRange range = reference.range();
            coverage(range.partition()).add(range.offsets());
        }
        for (SharedPartitionId partition : changedPartitions) {
            revisions.computeIfAbsent(partition, ignored -> new AtomicLong()).incrementAndGet();
        }
        return !changedPartitions.isEmpty();
    }

    private void stageReference(
        RangeReference incoming,
        Map<SharedPartitionId, NavigableMap<Long, RangeReference>> stagedByPartition,
        Map<SharedPartitionId, NavigableMap<Long, RangeReference>> stagedAlternates,
        List<RangeReference> updates,
        List<RangeReference> alternateUpdates
    ) {
        Objects.requireNonNull(incoming, "range reference");
        SharedObjectRange range = incoming.range();
        SharedPartitionId partition = range.partition();
        NavigableMap<Long, RangeReference> staged =
            stagedByPartition.computeIfAbsent(partition, this::copyExistingRanges);
        RangeReference overlapping = overlappingReference(staged, range.offsets());
        if (overlapping == null) {
            staged.put(range.offsets().startOffset(), incoming);
            updates.add(incoming);
            return;
        }
        if (!sameLogicalRange(overlapping.range(), range)) {
            throw conflict(overlapping, incoming);
        }
        RangeReference merged = mergeEquivalentReference(overlapping, incoming);
        stageAlternate(overlapping, incoming, merged, stagedAlternates, alternateUpdates);
        if (merged != overlapping) {
            staged.put(range.offsets().startOffset(), merged);
            updates.add(merged);
        }
    }

    private void stageAlternate(
        RangeReference overlapping,
        RangeReference incoming,
        RangeReference merged,
        Map<SharedPartitionId, NavigableMap<Long, RangeReference>> stagedAlternates,
        List<RangeReference> alternateUpdates
    ) {
        RangeReference other = merged.objectId() != overlapping.objectId()
            ? overlapping : (incoming.objectId() != overlapping.objectId() ? incoming : null);
        if (other == null) {
            return;
        }
        // Only one equivalent fallback per logical batch. Retain the two
        // lowest object IDs after metadata replay, independent of order.
        SharedObjectRange range = incoming.range();
        NavigableMap<Long, RangeReference> alternatives = stagedAlternates.computeIfAbsent(
            range.partition(), this::copyExistingAlternates
        );
        RangeReference previous = alternatives.get(range.offsets().startOffset());
        RangeReference next = previous == null ? other : mergeEquivalentReference(previous, other);
        if (next != previous) {
            alternatives.put(range.offsets().startOffset(), next);
            alternateUpdates.add(next);
        }
    }

    private boolean isCurrentReference(RangeReference candidate) {
        NavigableMap<Long, RangeReference> ranges = byPartition.get(candidate.range().partition());
        if (ranges == null) {
            return false;
        }
        RangeReference current = ranges.get(candidate.range().offsets().startOffset());
        return current != null && current.objectId() == candidate.objectId();
    }

    private static RangeReference mergeEquivalentReference(RangeReference existing, RangeReference incoming) {
        if (existing.objectId() != incoming.objectId()) {
            // Equal logical Kafka batches may have multiple COMMITTED physical
            // copies. Select the same read reference after full replay,
            // regardless of each Broker's observation order.
            // This is NOT authority to retire the non-selected object.
            return incoming.objectId() < existing.objectId() ? incoming : existing;
        }
        if (!existing.range().equals(incoming.range())) {
            throw conflict(existing, incoming);
        }
        if (existing.hasObjectDescriptor() && incoming.hasObjectDescriptor()) {
            if (existing.objectSize() != incoming.objectSize() ||
                existing.objectChecksum() != incoming.objectChecksum()) {
                throw conflict(existing, incoming);
            }
            return existing;
        }
        if (!existing.hasObjectDescriptor() && incoming.hasObjectDescriptor()) {
            return incoming;
        }
        return existing;
    }

    public Optional<RangeReference> find(SharedPartitionId partition, long offset) {
        NavigableMap<Long, RangeReference> ranges = byPartition.get(partition);
        if (ranges == null) {
            return Optional.empty();
        }
        Map.Entry<Long, RangeReference> floor = ranges.floorEntry(offset);
        if (floor != null && floor.getValue().range().offsets().startOffset() <= offset &&
            offset < floor.getValue().range().offsets().endOffset()) {
            return Optional.of(floor.getValue());
        }
        return Optional.empty();
    }

    /**
     * Finds one already indexed, logically equivalent physical copy for a
     * failed remote read. Only COMMITTED metadata publication or durable local
     * checkpoint restore may populate these references. It is not a deletion
     * decision, and a returned alternative still requires normal checksum
     * verification in the object reader. Synchronized with add/restore to
     * snapshot the two independently published maps consistently when an
     * asynchronous read fails during authoritative COMMIT replay.
     * Successful primary reads remain lock-free.
     */
    public synchronized Optional<RangeReference> findEquivalentAlternative(RangeReference failed) {
        Objects.requireNonNull(failed, "failed");
        SharedObjectRange range = failed.range();
        long startOffset = range.offsets().startOffset();
        NavigableMap<Long, RangeReference> primaryRanges = byPartition.get(range.partition());
        if (primaryRanges != null) {
            RangeReference primary = primaryRanges.get(startOffset);
            if (isEquivalentDifferentObject(failed, primary)) {
                return Optional.of(primary);
            }
        }
        NavigableMap<Long, RangeReference> alternateRanges = alternates.get(range.partition());
        if (alternateRanges != null) {
            RangeReference alternate = alternateRanges.get(startOffset);
            if (isEquivalentDifferentObject(failed, alternate)) {
                return Optional.of(alternate);
            }
        }
        return Optional.empty();
    }

    private static boolean isEquivalentDifferentObject(RangeReference failed, RangeReference candidate) {
        return candidate != null && candidate.objectId() != failed.objectId()
            && sameLogicalRange(candidate.range(), failed.range());
    }

    public PartitionRemoteCoverage coverage(SharedPartitionId partition) {
        return coverage.computeIfAbsent(partition, ignored -> new PartitionRemoteCoverage());
    }

    public List<RangeReference> ranges(SharedPartitionId partition) {
        NavigableMap<Long, RangeReference> ranges = byPartition.get(partition);
        if (ranges == null) {
            return List.of();
        }
        return List.copyOf(new ArrayList<>(ranges.values()));
    }

    /**
     * Returns whether any current logical remote range reads from the supplied physical object.
     *
     * <p>Equivalent COMMITTED physical copies choose the lowest object ID
     * as a deterministic local read reference after full replay. A local
     * reference is not a Controller GC authorization, even when all known
     * replicas happen to agree on it.</p>
     */
    public synchronized boolean referencesObject(long objectId) {
        if (objectId <= 0) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        for (NavigableMap<Long, RangeReference> ranges : byPartition.values()) {
            for (RangeReference reference : ranges.values()) {
                if (reference.objectId() == objectId) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a per-partition generation that advances whenever the logical remote read view changes.
     *
     * <p>Callers can snapshot this while constructing a local logical view and later determine whether authoritative
     * metadata replay introduced ranges that must be rematerialized. Replaying an identical checkpoint/reference set
     * does not advance the generation.</p>
     */
    public long revision(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        AtomicLong revision = revisions.get(partition);
        return revision == null ? 0L : revision.get();
    }

    private NavigableMap<Long, RangeReference> copyExistingRanges(SharedPartitionId partition) {
        return copyExisting(byPartition.get(partition));
    }

    private NavigableMap<Long, RangeReference> copyExistingAlternates(SharedPartitionId partition) {
        return copyExisting(alternates.get(partition));
    }

    private static NavigableMap<Long, RangeReference> copyExisting(
        NavigableMap<Long, RangeReference> existing
    ) {
        NavigableMap<Long, RangeReference> staged = new TreeMap<>();
        if (existing != null) {
            staged.putAll(existing);
        }
        return staged;
    }

    private static RangeReference overlappingReference(
        NavigableMap<Long, RangeReference> ranges,
        OffsetRange offsets
    ) {
        Map.Entry<Long, RangeReference> floor = ranges.floorEntry(offsets.startOffset());
        if (floor != null && overlaps(floor.getValue().range().offsets(), offsets)) {
            return floor.getValue();
        }
        Map.Entry<Long, RangeReference> next = ranges.ceilingEntry(offsets.startOffset());
        if (next != null && overlaps(next.getValue().range().offsets(), offsets)) {
            return next.getValue();
        }
        return null;
    }

    private static boolean overlaps(OffsetRange left, OffsetRange right) {
        return left.startOffset() < right.endOffset() && right.startOffset() < left.endOffset();
    }

    private static boolean sameLogicalRange(SharedObjectRange left, SharedObjectRange right) {
        // Kafka RecordBatch byte length is part of the logical identity.
        // Equal offsets, epoch and checksum cannot make two differently
        // sized payloads interchangeable. LocalRemoteObjectCheckpoint
        // already applies this immutable range invariant.
        return left.offsets().equals(right.offsets()) &&
            left.leaderEpoch() == right.leaderEpoch() &&
            left.checksum() == right.checksum() &&
            left.objectLength() == right.objectLength();
    }

    private static RemoteMetadataConflictException conflict(RangeReference existing, RangeReference incoming) {
        return new RemoteMetadataConflictException(
            "Conflicting remote Kafka ranges: existing=" + existing + ", incoming=" + incoming);
    }

    public record PublicationResult(
        boolean readViewChanged,
        boolean objectReferenced
    ) {
    }

    public record RangeReference(
        long objectId,
        long objectSize,
        long objectChecksum,
        SharedObjectRange range
    ) {
        private static final long UNKNOWN_OBJECT_SIZE = -1L;

        public RangeReference(long objectId, SharedObjectRange range) {
            this(objectId, UNKNOWN_OBJECT_SIZE, 0L, range);
        }

        public RangeReference {
            if (objectId <= 0) {
                throw new IllegalArgumentException("objectId must be positive");
            }
            Objects.requireNonNull(range, "range");
            if (objectSize != UNKNOWN_OBJECT_SIZE && objectSize <= 0) {
                throw new IllegalArgumentException("objectSize must be positive or unknown");
            }
            if (objectSize > 0) {
                long rangeEnd;
                try {
                    rangeEnd = Math.addExact(range.objectPosition(), range.objectLength());
                } catch (ArithmeticException e) {
                    throw new IllegalArgumentException("object range end overflows", e);
                }
                if (rangeEnd > objectSize) {
                    throw new IllegalArgumentException("object range exceeds object size");
                }
            }
        }

        public boolean hasObjectDescriptor() {
            return objectSize > 0;
        }
    }
}
