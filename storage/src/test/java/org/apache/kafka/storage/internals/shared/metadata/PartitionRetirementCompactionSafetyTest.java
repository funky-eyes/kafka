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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression witnesses for why a single compacted log-start key is NOT a safe distributed
 * monotonic writer protocol. These are deliberately negative proofs, not writer tests.
 *
 * <p>Live replay sees conflicting order, but Kafka compaction is allowed to retain only
 * the last value for a key. A stale last writer can thereby erase the previously higher
 * watermark before a recovering broker replays it. The production writer is disabled.</p>
 */
class PartitionRetirementCompactionSafetyTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId RECREATED = new SharedPartitionId(1L, 3L, 0);

    @Test
    void liveReplayDetectsBackwardWriteButCompactedReplayCannotRecoverItsHistory() {
        byte[] key = SharedMetadataRecordCodec.partitionLogStartKey(PARTITION);
        byte[] higher = SharedMetadataRecordCodec.partitionLogStartValue(40L);
        byte[] lower = SharedMetadataRecordCodec.partitionLogStartValue(20L);

        SharedMetadataImage unCompacted = new SharedMetadataImage();
        unCompacted.applyFromMetadataLog(key, higher, 10L);
        assertThrows(IllegalStateException.class, () ->
            unCompacted.applyFromMetadataLog(key, lower, 11L)
        );
        assertEquals(SharedMetadataImage.State.FAILED, unCompacted.state());

        // Kafka compaction may legitimately remove offset 10: the remaining value at
        // offset 11 has no epoch/CAS proof and appears self-consistent on cold replay.
        // This MUST NOT be treated as an authorized retirement watermark.
        SharedMetadataImage afterCompaction = new SharedMetadataImage();
        afterCompaction.applyFromMetadataLog(key, lower, 11L);
        afterCompaction.markReady();
        assertEquals(20L, afterCompaction.partitionLogStartOffset(PARTITION));
        assertEquals(11L, afterCompaction.partitionLogStartEvidence(PARTITION).lastConsumedMetadataOffset());
    }

    @Test
    void compactionDoesNotSupplyAWriterEpochOrAnAuthenticCatchUpHorizon() {
        SharedMetadataImage afterCompaction = new SharedMetadataImage();
        afterCompaction.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(20L),
            11L
        );
        afterCompaction.markReady();

        // The highest consumed offset is not an authoritative Kafka last-stable
        // offset or source-leader epoch. An external required horizon must still
        // block an optimistic writer value precheck.
        assertEquals(
            PartitionLogStartAdvancePrecheck.Finding.METADATA_REPLAY_BEHIND,
            PartitionLogStartAdvancePrecheck.assess(afterCompaction, PARTITION, 21L, 21L, 12L)
        );
        assertTrue(afterCompaction.partitionLogStartEvidence(RECREATED).explicitLogStart().isEmpty());
    }

    @Test
    void topicRecreationKeepsDistinctCompactedKeys() {
        byte[] originalKey = SharedMetadataRecordCodec.partitionLogStartKey(PARTITION);
        byte[] recreatedKey = SharedMetadataRecordCodec.partitionLogStartKey(RECREATED);
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(originalKey, SharedMetadataRecordCodec.partitionLogStartValue(40L), 2L);
        image.applyFromMetadataLog(recreatedKey, SharedMetadataRecordCodec.partitionLogStartValue(0L), 3L);
        image.markReady();

        assertEquals(40L, image.partitionLogStartOffset(PARTITION));
        assertEquals(0L, image.partitionLogStartOffset(RECREATED));
    }
}
