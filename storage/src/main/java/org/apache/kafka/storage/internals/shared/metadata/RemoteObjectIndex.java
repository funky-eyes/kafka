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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Logical remote index. Physical objects may be duplicated after a leader race, but a logical Kafka offset range
 * has one content identity. Overlapping ranges are accepted only when the range and checksum are identical.
 *
 * <p>Uploaders are expected to emit metadata at Kafka RecordBatch boundaries. This makes conflict detection
 * deterministic even when multiple partitions are packed into different physical S3 objects.</p>
 */
public final class RemoteObjectIndex {
    private final ConcurrentHashMap<SharedPartitionId, ConcurrentNavigableMap<Long, RangeReference>> byPartition =
        new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SharedPartitionId, PartitionRemoteCoverage> coverage = new ConcurrentHashMap<>();

    /**
     * Validates every range in an object before publishing any of them.
     *
     * <p>A single S3 object can contain ranges from many partitions. A conflict in one partition must not leave ranges
     * from earlier partitions visible, otherwise a corruption event could expose a logically half-committed object.
     * Remote metadata commits are infrequent compared with reads, so serializing writers here is an acceptable cost;
     * readers remain lock-free over the concurrent range maps.</p>
     */
    public synchronized void add(SharedObjectMetadata object) {
        Objects.requireNonNull(object, "object");
        List<RangeReference> references = object.ranges().stream()
            .map(range -> new RangeReference(object.objectId(), range))
            .toList();
        addReferences(references);
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

    private void addReferences(List<RangeReference> references) {
        Map<SharedPartitionId, NavigableMap<Long, RangeReference>> stagedByPartition = new HashMap<>();
        List<RangeReference> newRanges = new ArrayList<>();

        for (RangeReference incoming : references) {
            Objects.requireNonNull(incoming, "range reference");
            SharedObjectRange range = incoming.range();
            SharedPartitionId partition = range.partition();
            NavigableMap<Long, RangeReference> staged = stagedByPartition.computeIfAbsent(
                partition,
                this::copyExistingRanges
            );
            RangeReference overlapping = overlappingReference(staged, range.offsets());
            if (overlapping != null) {
                if (sameLogicalRange(overlapping.range(), range)) {
                    continue;
                }
                throw conflict(overlapping, incoming);
            }
            staged.put(range.offsets().startOffset(), incoming);
            newRanges.add(incoming);
        }

        // No validation below this point can fail for ordinary metadata. Publish only after the whole batch validates.
        for (RangeReference reference : newRanges) {
            SharedObjectRange range = reference.range();
            byPartition
                .computeIfAbsent(range.partition(), ignored -> new ConcurrentSkipListMap<>())
                .put(range.offsets().startOffset(), reference);
        }
        for (RangeReference reference : references) {
            SharedObjectRange range = reference.range();
            coverage(range.partition()).add(range.offsets());
        }
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

    private NavigableMap<Long, RangeReference> copyExistingRanges(SharedPartitionId partition) {
        NavigableMap<Long, RangeReference> staged = new TreeMap<>();
        NavigableMap<Long, RangeReference> existing = byPartition.get(partition);
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
        return left.offsets().equals(right.offsets()) && left.checksum() == right.checksum();
    }

    private static RemoteMetadataConflictException conflict(RangeReference existing, RangeReference incoming) {
        return new RemoteMetadataConflictException(
            "Conflicting remote Kafka ranges: existing=" + existing + ", incoming=" + incoming);
    }

    public record RangeReference(long objectId, SharedObjectRange range) {
        public RangeReference {
            if (objectId <= 0) {
                throw new IllegalArgumentException("objectId must be positive");
            }
            Objects.requireNonNull(range, "range");
        }
    }
}
