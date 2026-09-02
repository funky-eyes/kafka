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

import java.util.List;
import java.util.Objects;

public record SharedObjectMetadata(
    long objectId,
    long objectSize,
    long objectChecksum,
    List<SharedObjectRange> ranges
) {
    public SharedObjectMetadata {
        if (objectId <= 0 || objectSize <= 0) {
            throw new IllegalArgumentException("invalid object identity or size");
        }
        Objects.requireNonNull(ranges, "ranges");
        if (ranges.isEmpty()) {
            throw new IllegalArgumentException("shared object must contain at least one range");
        }
        ranges = List.copyOf(ranges);
        for (SharedObjectRange range : ranges) {
            Objects.requireNonNull(range, "range");
            long rangeEnd;
            try {
                rangeEnd = Math.addExact(range.objectPosition(), range.objectLength());
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("object range end overflows: " + range, e);
            }
            if (rangeEnd > objectSize) {
                throw new IllegalArgumentException("object range exceeds object size: " + range);
            }
        }
    }
}
