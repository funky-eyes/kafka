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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRetirementAuthorityRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionRetirementControlManagerTest {
    private static final Key PARTITION = new Key(Uuid.randomUuid(), 0);

    @Test
    void controllerReplayRetainsTheHighestWatermarkAcrossEpochHandover() {
        PartitionRetirementControlManager manager = manager();
        manager.replay(record(PARTITION, new Value(0L, 3, 1, 0L, false)));
        manager.replay(record(PARTITION, new Value(4L, 4, 2, 70L, false)));
        assertEquals(new Value(4L, 4, 2, 70L, false), manager.get(PARTITION));
    }

    @Test
    void controllerReplayRejectsLateFormerBroker() {
        PartitionRetirementControlManager manager = manager();
        manager.replay(record(PARTITION, new Value(4L, 4, 2, 70L, false)));
        assertThrows(IllegalStateException.class, () ->
            manager.replay(record(PARTITION, new Value(5L, 3, 1, 70L, false)))
        );
        assertEquals(2, manager.get(PARTITION).brokerId());
    }

    @Test
    void terminalTopicDeleteCannotBeReauthorizedByReplayedRecord() {
        PartitionRetirementControlManager manager = manager();
        manager.replay(record(PARTITION, new Value(4L, 4, 2, 70L, false)));
        manager.replay(record(PARTITION, new Value(5L, 4, -1, 70L, true)));
        assertThrows(IllegalStateException.class, () ->
            manager.replay(record(PARTITION, new Value(6L, 5, 3, 70L, false)))
        );
    }

    @Test
    void timelineRollbackRevertsToThePriorAuthoritySnapshot() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        PartitionRetirementControlManager manager = new PartitionRetirementControlManager(registry);
        Value initial = new Value(0L, 3, 1, 0L, false);
        manager.replay(record(PARTITION, initial));
        registry.idempotentCreateSnapshot(0L);
        manager.replay(record(PARTITION, new Value(1L, 4, 2, 70L, false)));
        registry.idempotentCreateSnapshot(1L);
        assertEquals(70L, manager.get(PARTITION).logStartOffset());
        registry.revertToSnapshot(0L);
        assertEquals(initial, manager.get(PARTITION));
    }

    @Test
    void topicRecreationDoesNotReuseDeletedAuthority() {
        Key recreated = new Key(Uuid.randomUuid(), 0);
        PartitionRetirementControlManager manager = manager();
        manager.replay(record(PARTITION, new Value(10L, 5, -1, 60L, true)));
        manager.replay(record(recreated, new Value(11L, 0, 2, -1L, false)));
        assertEquals(-1, manager.get(PARTITION).brokerId());
        assertEquals(2, manager.get(recreated).brokerId());
        assertNull(manager.get(new Key(PARTITION.topicId(), 1)));
    }

    @Test
    void committedKRaftAppendOffsetMustMatchAuthorityOffset() {
        PartitionRetirementControlManager manager = manager();
        Value value = new Value(120L, 5, 1, 0L, false);
        manager.replayFromKRaft(record(PARTITION, value), Optional.empty(), 120L);
        assertEquals(value, manager.get(PARTITION));
    }

    @Test
    void forgedAuthorityOffsetCannotAdvanceFromOrdinaryLogReplay() {
        PartitionRetirementControlManager manager = manager();
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(
                record(PARTITION, new Value(9000L, 5, 1, 0L, false)),
                Optional.empty(), 120L
            )
        );
        assertNull(manager.get(PARTITION));
    }

    @Test
    void KRaftSnapshotMayRetainOlderAcceptedAuthorityOffset() {
        PartitionRetirementControlManager manager = manager();
        Value historical = new Value(80L, 4, -1, 40L, true);
        manager.replayFromKRaft(
            record(PARTITION, historical), Optional.of(new OffsetAndEpoch(200L, 9)), 199L
        );
        assertEquals(historical, manager.get(PARTITION));
    }

    @Test
    void snapshotCannotClaimAuthorityBeyondItsCommittedHorizon() {
        PartitionRetirementControlManager manager = manager();
        Value atHorizon = new Value(200L, 5, 1, 40L, false);
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(
                record(PARTITION, atHorizon), Optional.of(new OffsetAndEpoch(200L, 9)), 150L
            )
        );
        assertNull(manager.get(PARTITION));
    }

    @Test
    void wrongOffsetIsRejectedWithoutReplacingPreviouslyReplayedState() {
        PartitionRetirementControlManager manager = manager();
        Value previous = new Value(10L, 5, 1, 0L, false);
        manager.replayFromKRaft(record(PARTITION, previous), Optional.empty(), 10L);
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(
                record(PARTITION, new Value(40L, 6, 2, 0L, false)),
                Optional.empty(), 20L
            )
        );
        assertEquals(previous, manager.get(PARTITION));
    }

    private static PartitionRetirementControlManager manager() {
        return new PartitionRetirementControlManager(new SnapshotRegistry(new LogContext()));
    }

    private static PartitionRetirementAuthorityRecord record(Key key, Value value) {
        return PartitionRetirementAuthorityState.record(key, value);
    }
}
