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

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedObjectMetadataTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1, 2, 0);

    @Test
    void shouldRejectNonPositiveObjectId() {
        SharedObjectRange range = range(0, 10);
        assertThrows(IllegalArgumentException.class, () ->
            new SharedObjectMetadata(0, 10, 123, List.of(range)));
    }

    @Test
    void shouldRejectOverflowingPhysicalRange() {
        SharedObjectRange range = range(Long.MAX_VALUE - 1, 4);
        assertThrows(IllegalArgumentException.class, () ->
            new SharedObjectMetadata(1, Long.MAX_VALUE, 123, List.of(range)));
    }

    @Test
    void shouldAcceptRangeEndingExactlyAtObjectSize() {
        SharedObjectRange range = range(90, 10);
        SharedObjectMetadata metadata = new SharedObjectMetadata(1, 100, 123, List.of(range));
        assertEquals(100, range.objectPosition() + range.objectLength());
        assertEquals(List.of(range), metadata.ranges());
    }

    private static SharedObjectRange range(long objectPosition, int objectLength) {
        return new SharedObjectRange(
            PARTITION,
            new OffsetRange(0, 10),
            3,
            objectPosition,
            objectLength,
            456
        );
    }
}
