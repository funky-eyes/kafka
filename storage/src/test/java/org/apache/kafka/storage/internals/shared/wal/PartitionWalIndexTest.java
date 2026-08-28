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
package org.apache.kafka.storage.internals.shared.wal;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionWalIndexTest {
    @Test
    void shouldInvalidateEntriesAtAndAfterTruncateOffset() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(1L, 2L, 3);

        index.apply(WalRecord.data(1L, 2L, 3, 7, 10, 19, new byte[]{1}), new WalAppendResult(1, 0, 100));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 20, 29, new byte[]{2}), new WalAppendResult(1, 100, 100));
        index.apply(WalRecord.data(1L, 2L, 3, 7, 30, 39, new byte[]{3}), new WalAppendResult(1, 200, 100));

        index.apply(WalRecord.truncate(1L, 2L, 3, 8, 25), new WalAppendResult(1, 300, 60));

        assertTrue(index.find(key, 10).isPresent());
        assertTrue(index.find(key, 19).isPresent());
        assertFalse(index.find(key, 20).isPresent());
        assertFalse(index.find(key, 25).isPresent());
        assertFalse(index.find(key, 30).isPresent());
    }

    @Test
    void shouldReplaceLogicalRangeWithNewGeneration() {
        PartitionWalIndex index = new PartitionWalIndex();
        WalPartitionKey key = new WalPartitionKey(10L, 20L, 1);

        index.apply(WalRecord.data(10L, 20L, 1, 3, 100, 109, new byte[]{1}), new WalAppendResult(2, 0, 100));
        index.apply(WalRecord.truncate(10L, 20L, 1, 4, 100), new WalAppendResult(2, 100, 60));
        index.apply(WalRecord.data(10L, 20L, 1, 4, 100, 109, new byte[]{2}), new WalAppendResult(3, 0, 100));

        WalLocation location = index.find(key, 105).orElseThrow();
        assertEquals(3, location.segmentId());
        assertEquals(4, location.leaderEpoch());
    }
}
