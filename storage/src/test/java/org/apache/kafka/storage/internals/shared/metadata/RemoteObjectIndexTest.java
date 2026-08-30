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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteObjectIndexTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1, 2, 0);
    private static final SharedPartitionId OTHER_PARTITION = new SharedPartitionId(3, 4, 1);

    @Test
    void shouldTrackHolesAndMergeCoverage() {
        PartitionRemoteCoverage coverage = new PartitionRemoteCoverage();
        coverage.add(new OffsetRange(0, 100));
        coverage.add(new OffsetRange(150, 200));
        coverage.add(new OffsetRange(200, 250));

        assertEquals(List.of(new OffsetRange(0, 100), new OffsetRange(150, 250)), coverage.snapshot());
        assertEquals(100, coverage.contiguousEnd(0));
        assertEquals(List.of(new OffsetRange(100, 150), new OffsetRange(250, 300)), coverage.missingRanges(0, 300));
    }

    @Test
    void shouldAcceptPhysicalDuplicateWithIdenticalLogicalContent() {
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(object(10, 0, 100, 777));
        index.add(object(11, 0, 100, 777));

        assertTrue(index.coverage(PARTITION).covers(new OffsetRange(0, 100)));
        assertEquals(1, index.ranges(PARTITION).size());
        assertEquals(10, index.find(PARTITION, 50).orElseThrow().objectId());
    }

    @Test
    void shouldRestoreDurableLocalReferencesBeforeAuthoritativeReplay() {
        RemoteObjectIndex index = new RemoteObjectIndex();
        SharedObjectRange first = range(PARTITION, 0, 100, 777);
        SharedObjectRange second = range(OTHER_PARTITION, 100, 200, 888);

        index.restore(List.of(
            new RemoteObjectIndex.RangeReference(10, first),
            new RemoteObjectIndex.RangeReference(11, second)
        ));

        assertTrue(index.coverage(PARTITION).covers(new OffsetRange(0, 100)));
        assertTrue(index.coverage(OTHER_PARTITION).covers(new OffsetRange(100, 200)));
        assertEquals(10, index.find(PARTITION, 50).orElseThrow().objectId());
        assertEquals(11, index.find(OTHER_PARTITION, 150).orElseThrow().objectId());

        // Later Kafka metadata replay of the same authoritative logical range is idempotent.
        index.add(object(99, 0, 100, 777));
        assertEquals(10, index.find(PARTITION, 50).orElseThrow().objectId());
    }

    @Test
    void shouldRejectConflictingDuplicateContent() {
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(object(10, 0, 100, 777));

        assertThrows(RemoteMetadataConflictException.class, () ->
            index.add(object(11, 0, 100, 778)));
    }

    @Test
    void shouldRejectPartiallyOverlappingRemoteRanges() {
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(object(10, 0, 100, 777));

        assertThrows(RemoteMetadataConflictException.class, () ->
            index.add(object(11, 50, 150, 777)));
    }

    @Test
    void objectConflictMustNotPublishEarlierRangesFromTheSameObject() {
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(object(10, OTHER_PARTITION, 0, 100, 900));

        SharedObjectMetadata conflicting = new SharedObjectMetadata(
            11,
            200,
            1234,
            List.of(
                range(PARTITION, 0, 100, 777),
                range(OTHER_PARTITION, 0, 100, 901)
            )
        );

        assertThrows(RemoteMetadataConflictException.class, () -> index.add(conflicting));
        assertTrue(index.ranges(PARTITION).isEmpty());
        assertFalse(index.coverage(PARTITION).covers(new OffsetRange(0, 100)));
        assertEquals(1, index.ranges(OTHER_PARTITION).size());
        assertEquals(10, index.find(OTHER_PARTITION, 50).orElseThrow().objectId());
    }

    private static SharedObjectMetadata object(long objectId, long start, long end, long checksum) {
        return object(objectId, PARTITION, start, end, checksum);
    }

    private static SharedObjectMetadata object(
        long objectId,
        SharedPartitionId partition,
        long start,
        long end,
        long checksum
    ) {
        int length = Math.toIntExact(end - start);
        return new SharedObjectMetadata(
            objectId,
            length,
            checksum,
            List.of(range(partition, start, end, checksum))
        );
    }

    private static SharedObjectRange range(
        SharedPartitionId partition,
        long start,
        long end,
        long checksum
    ) {
        int length = Math.toIntExact(end - start);
        return new SharedObjectRange(
            partition,
            new OffsetRange(start, end),
            3,
            0,
            length,
            checksum
        );
    }
}
