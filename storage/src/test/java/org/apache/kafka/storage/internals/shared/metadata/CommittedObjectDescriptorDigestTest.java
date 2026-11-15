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

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommittedObjectDescriptorDigestTest {
    private static final SharedPartitionId FIRST = new SharedPartitionId(10L, 20L, 0);
    private static final SharedPartitionId SECOND = new SharedPartitionId(10L, 21L, 1);

    private static SharedObjectRange range(SharedPartitionId partition, long start, long end,
                                           int epoch, long position, int length, long checksum) {
        return new SharedObjectRange(
            partition, new OffsetRange(start, end), epoch, position, length, checksum);
    }

    private static SharedObjectMetadata object(SharedObjectRange... ranges) {
        return new SharedObjectMetadata(501L, 4_096L, 17L, List.of(ranges));
    }

    @Test
    void stableAcrossEquivalentObjectReconstruction() {
        SharedObjectRange first = range(FIRST, 0L, 100L, 3, 0L, 100, 33L);
        SharedObjectRange second = range(SECOND, 10L, 60L, 4, 200L, 50, 44L);
        String left = CommittedObjectDescriptorDigest.sha256(object(first, second));
        String right = CommittedObjectDescriptorDigest.sha256(object(
            range(new SharedPartitionId(10L, 20L, 0), 0L, 100L, 3, 0L, 100, 33L),
            range(new SharedPartitionId(10L, 21L, 1), 10L, 60L, 4, 200L, 50, 44L)
        ));
        assertEquals(left, right);
        assertTrue(left.matches("[0-9a-f]{64}"));
    }

    @Test
    void objectIdentitySizeAndChecksumAreAllCommitted() {
        SharedObjectRange r = range(FIRST, 0L, 100L, 3, 0L, 100, 33L);
        String original = CommittedObjectDescriptorDigest.sha256(object(r));
        assertNotEquals(original, CommittedObjectDescriptorDigest.sha256(
            new SharedObjectMetadata(502L, 4_096L, 17L, List.of(r))));
        assertNotEquals(original, CommittedObjectDescriptorDigest.sha256(
            new SharedObjectMetadata(501L, 4_097L, 17L, List.of(r))));
        assertNotEquals(original, CommittedObjectDescriptorDigest.sha256(
            new SharedObjectMetadata(501L, 4_096L, 18L, List.of(r))));
    }

    @Test
    void everyPackedRangeFieldChangesDigest() {
        SharedObjectRange r = range(FIRST, 0L, 100L, 3, 0L, 100, 33L);
        String original = CommittedObjectDescriptorDigest.sha256(object(r));
        List<SharedObjectRange> variants = List.of(
            range(new SharedPartitionId(11L, 20L, 0), 0L, 100L, 3, 0L, 100, 33L),
            range(new SharedPartitionId(10L, 22L, 0), 0L, 100L, 3, 0L, 100, 33L),
            range(new SharedPartitionId(10L, 20L, 1), 0L, 100L, 3, 0L, 100, 33L),
            range(FIRST, 1L, 100L, 3, 0L, 100, 33L),
            range(FIRST, 0L, 101L, 3, 0L, 100, 33L),
            range(FIRST, 0L, 100L, 4, 0L, 100, 33L),
            range(FIRST, 0L, 100L, 3, 1L, 100, 33L),
            range(FIRST, 0L, 100L, 3, 0L, 101, 33L),
            range(FIRST, 0L, 100L, 3, 0L, 100, 34L)
        );
        for (SharedObjectRange variant : variants) {
            assertNotEquals(original, CommittedObjectDescriptorDigest.sha256(object(variant)),
                "Changing any packed range field must invalidate the digest: " + variant);
        }
    }

    @Test
    void secondPackedRangeAndOrderingAreNeverIgnored() {
        SharedObjectRange first = range(FIRST, 0L, 100L, 3, 0L, 100, 33L);
        SharedObjectRange second = range(SECOND, 10L, 60L, 4, 200L, 50, 44L);
        String full = CommittedObjectDescriptorDigest.sha256(object(first, second));
        assertNotEquals(full, CommittedObjectDescriptorDigest.sha256(object(first)));
        assertNotEquals(full, CommittedObjectDescriptorDigest.sha256(object(second, first)));
        assertNotEquals(full, CommittedObjectDescriptorDigest.sha256(object(
            first, range(SECOND, 10L, 61L, 4, 200L, 50, 44L))));
    }

    @Test
    void descriptorSnapshotCannotBeMutatedBehindItsDigest() {
        List<SharedObjectRange> mutable = new ArrayList<>();
        mutable.add(range(FIRST, 0L, 100L, 3, 0L, 100, 33L));
        SharedObjectMetadata snapshot = new SharedObjectMetadata(501L, 4_096L, 17L, mutable);
        String before = CommittedObjectDescriptorDigest.sha256(snapshot);
        mutable.add(range(SECOND, 10L, 60L, 4, 200L, 50, 44L));
        assertEquals(before, CommittedObjectDescriptorDigest.sha256(snapshot));
        assertThrows(UnsupportedOperationException.class,
            () -> snapshot.ranges().add(mutable.get(1)));
    }
}
