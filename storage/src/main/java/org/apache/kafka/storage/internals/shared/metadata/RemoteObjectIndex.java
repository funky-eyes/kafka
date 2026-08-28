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
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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

    public void add(SharedObjectMetadata object) {
        for (SharedObjectRange range : object.ranges()) {
            addRange(object.objectId(), range);
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

    private void addRange(long objectId, SharedObjectRange range) {
        ConcurrentNavigableMap<Long, RangeReference> ranges =
            byPartition.computeIfAbsent(range.partition(), ignored -> new ConcurrentSkipListMap<>());
        synchronized (ranges) {
            RangeReference incoming = new RangeReference(objectId, range);
            Map.Entry<Long, RangeReference> floor = ranges.floorEntry(range.offsets().startOffset());
            if (floor != null && overlaps(floor.getValue().range().offsets(), range.offsets())) {
                if (sameLogicalRange(floor.getValue().range(), range)) {
                    coverage(range.partition()).add(range.offsets());
                    return;
                }
                throw conflict(floor.getValue(), incoming);
            }

            Map.Entry<Long, RangeReference> next = ranges.ceilingEntry(range.offsets().startOffset());
            if (next != null && overlaps(next.getValue().range().offsets(), range.offsets())) {
                if (sameLogicalRange(next.getValue().range(), range)) {
                    coverage(range.partition()).add(range.offsets());
                    return;
                }
                throw conflict(next.getValue(), incoming);
            }

            ranges.put(range.offsets().startOffset(), incoming);
            coverage(range.partition()).add(range.offsets());
        }
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
    }
}
