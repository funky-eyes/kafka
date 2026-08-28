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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Union of remotely committed half-open offset ranges for one partition.
 *
 * <p>The structure intentionally supports holes. A single remote-end-offset is insufficient when old and new
 * leaders finish independent S3 objects out of order.</p>
 */
public final class PartitionRemoteCoverage {
    private final NavigableMap<Long, Long> ranges = new TreeMap<>();

    public synchronized void add(OffsetRange range) {
        long start = range.startOffset();
        long end = range.endOffset();

        Map.Entry<Long, Long> floor = ranges.floorEntry(start);
        if (floor != null && floor.getValue() >= start) {
            start = floor.getKey();
            end = Math.max(end, floor.getValue());
            ranges.remove(floor.getKey());
        }

        Map.Entry<Long, Long> next = ranges.ceilingEntry(start);
        while (next != null && next.getKey() <= end) {
            end = Math.max(end, next.getValue());
            ranges.remove(next.getKey());
            next = ranges.ceilingEntry(start);
        }
        ranges.put(start, end);
    }

    public synchronized boolean covers(OffsetRange range) {
        Map.Entry<Long, Long> floor = ranges.floorEntry(range.startOffset());
        return floor != null && floor.getValue() >= range.endOffset();
    }

    public synchronized long contiguousEnd(long startOffset) {
        Map.Entry<Long, Long> floor = ranges.floorEntry(startOffset);
        if (floor != null && floor.getKey() <= startOffset && floor.getValue() > startOffset) {
            return floor.getValue();
        }
        Map.Entry<Long, Long> exact = ranges.ceilingEntry(startOffset);
        if (exact != null && exact.getKey() == startOffset) {
            return exact.getValue();
        }
        return startOffset;
    }

    public synchronized List<OffsetRange> missingRanges(long startOffset, long endOffset) {
        if (startOffset < 0 || endOffset < startOffset) {
            throw new IllegalArgumentException("Invalid query range [" + startOffset + ", " + endOffset + ")");
        }
        if (startOffset == endOffset) {
            return List.of();
        }

        List<OffsetRange> missing = new ArrayList<>();
        long cursor = startOffset;
        Map.Entry<Long, Long> floor = ranges.floorEntry(cursor);
        if (floor != null && floor.getValue() > cursor) {
            cursor = Math.min(endOffset, floor.getValue());
        }

        while (cursor < endOffset) {
            Map.Entry<Long, Long> next = ranges.ceilingEntry(cursor);
            if (next == null || next.getKey() >= endOffset) {
                missing.add(new OffsetRange(cursor, endOffset));
                break;
            }
            if (next.getKey() > cursor) {
                missing.add(new OffsetRange(cursor, Math.min(endOffset, next.getKey())));
            }
            cursor = Math.max(cursor, Math.min(endOffset, next.getValue()));
        }
        return Collections.unmodifiableList(missing);
    }

    public synchronized List<OffsetRange> snapshot() {
        List<OffsetRange> result = new ArrayList<>(ranges.size());
        ranges.forEach((start, end) -> result.add(new OffsetRange(start, end)));
        return Collections.unmodifiableList(result);
    }
}
