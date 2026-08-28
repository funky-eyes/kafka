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

/** Half-open Kafka offset range [startOffset, endOffset). */
public record OffsetRange(long startOffset, long endOffset) {
    public OffsetRange {
        if (startOffset < 0 || endOffset <= startOffset) {
            throw new IllegalArgumentException("Invalid offset range [" + startOffset + ", " + endOffset + ")");
        }
    }

    public boolean overlapsOrAdjacent(OffsetRange other) {
        return startOffset <= other.endOffset && other.startOffset <= endOffset;
    }

    public boolean contains(OffsetRange other) {
        return startOffset <= other.startOffset && endOffset >= other.endOffset;
    }
}
