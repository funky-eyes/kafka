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
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercise only the future-version atomic KRaft terminal deletion writer.
 * These terminal facts are never sufficient to delete S3 objects.
 */
class PartitionRetirementTopicDeletionRecordsTest {
    @Test
    void nativeTopicDeletionAndTerminalFactsShareOneKRaftAtomicBatch() {
        Uuid deleted = Uuid.randomUuid();
        ControllerResult<Map<Uuid, ApiError>> nativeDelete = nativeDelete(deleted);
        ControllerResult<Map<Uuid, ApiError>> augmented = PartitionRetirementTopicDeletionRecords.augment(
            nativeDelete,
            topicId -> Set.of(3, 1),
            key -> null,
            100L,
            10
        );

        assertTrue(augmented.isAtomic());
        assertEquals(nativeDelete.response(), augmented.response());
        assertEquals(3, augmented.records().size());
        assertTrue(augmented.records().get(0).message() instanceof RemoveTopicRecord);
        PartitionRetirementAuthorityRecord first =
            (PartitionRetirementAuthorityRecord) augmented.records().get(1).message();
        PartitionRetirementAuthorityRecord second =
            (PartitionRetirementAuthorityRecord) augmented.records().get(2).message();

        assertEquals(1, first.partitionId());
        assertEquals(3, second.partitionId());
        assertEquals(101L, first.authorityOffset());
        assertEquals(102L, second.authorityOffset());
        assertEquals(deleted, first.topicId());
        assertTrue(first.terminallyDeleted());

        PartitionRetirementControlManager manager =
            new PartitionRetirementControlManager(new SnapshotRegistry(new LogContext()));
        manager.replayFromKRaft(first, Optional.empty(), 101L);
        manager.replayFromKRaft(second, Optional.empty(), 102L);
        assertTrue(manager.get(new Key(deleted, 1)).terminallyDeleted());
        assertTrue(manager.get(new Key(deleted, 3)).terminallyDeleted());
        assertNull(manager.get(new Key(Uuid.randomUuid(), 1)));
    }

    @Test
    void preservesAcceptedAuthorityCoordinatesAndFencesRecreatedTopicId() {
        Uuid oldTopicId = Uuid.randomUuid();
        Uuid newTopicId = Uuid.randomUuid();
        Uuid incarnation = Uuid.randomUuid();
        Key oldKey = new Key(oldTopicId, 0);
        Value previous = new Value(15L, 3, 7, 2, 42L, false, 31L, incarnation);
        ControllerResult<Map<Uuid, ApiError>> nativeDelete = nativeDelete(oldTopicId);

        ControllerResult<Map<Uuid, ApiError>> augmented = PartitionRetirementTopicDeletionRecords.augment(
            nativeDelete, id -> Set.of(0), key -> key.equals(oldKey) ? previous : null, 20L, 10
        );
        PartitionRetirementAuthorityRecord terminal =
            (PartitionRetirementAuthorityRecord) augmented.records().get(1).message();
        assertEquals(21L, terminal.authorityOffset());
        assertEquals(3, terminal.sourceLeaderEpoch());
        assertEquals(7, terminal.sourcePartitionEpoch());
        assertEquals(42L, terminal.logStartOffset());
        assertEquals(-1, terminal.brokerId());
        assertTrue(terminal.terminallyDeleted());

        PartitionRetirementControlManager manager =
            new PartitionRetirementControlManager(new SnapshotRegistry(new LogContext()));
        manager.replayFromKRaft(
            org.apache.kafka.metadata.PartitionRetirementAuthorityState.record(oldKey, previous),
            Optional.empty(), 15L
        );
        manager.replayFromKRaft(terminal, Optional.empty(), 21L);
        assertTrue(manager.get(oldKey).terminallyDeleted());
        assertNull(manager.get(new Key(newTopicId, 0)));
    }

    @Test
    void multipleTopicsUseActualRecordPositionsAndSkipFailedOutcomes() {
        Uuid first = Uuid.randomUuid();
        Uuid second = Uuid.randomUuid();
        List<ApiMessageAndVersion> records = List.of(
            remove(first), remove(second)
        );
        ControllerResult<Map<Uuid, ApiError>> nativeDelete =
            ControllerResult.atomicOf(records, Map.of(first, ApiError.NONE));
        ControllerResult<Map<Uuid, ApiError>> augmented = PartitionRetirementTopicDeletionRecords.augment(
            nativeDelete, id -> id.equals(first) ? Set.of(1) : Set.of(3), key -> null, 200L, 5
        );
        assertEquals(3, augmented.records().size());
        PartitionRetirementAuthorityRecord terminal =
            (PartitionRetirementAuthorityRecord) augmented.records().get(2).message();
        assertEquals(first, terminal.topicId());
        assertEquals(202L, terminal.authorityOffset());
    }

    @Test
    void oversizedTopicCannotCausePartialRetirementOrBreakNativeDelete() {
        Uuid topicId = Uuid.randomUuid();
        ControllerResult<Map<Uuid, ApiError>> nativeDelete = nativeDelete(topicId);

        ControllerResult<Map<Uuid, ApiError>> result = PartitionRetirementTopicDeletionRecords.augment(
            nativeDelete, id -> Set.of(0, 1, 2), key -> null, 50L, 3
        );
        assertSame(nativeDelete, result);
        assertEquals(1, result.records().size());
    }

    @Test
    void alreadyTerminalAuthorityNeverEmitsAnotherMutableClaim() {
        Uuid topicId = Uuid.randomUuid();
        ControllerResult<Map<Uuid, ApiError>> nativeDelete = nativeDelete(topicId);
        Value deleted = new Value(20L, -1, -1, -1, -1L, true, -1L, Uuid.ZERO_UUID);

        ControllerResult<Map<Uuid, ApiError>> result = PartitionRetirementTopicDeletionRecords.augment(
            nativeDelete, id -> Set.of(0), key -> deleted, 30L, 10
        );
        assertSame(nativeDelete, result);
    }

    @Test
    void malformedInputIsRejectedWithoutProducingAnyRetirementRecords() {
        Uuid topicId = Uuid.randomUuid();
        ControllerResult<Map<Uuid, ApiError>> nativeDelete = nativeDelete(topicId);
        assertThrows(IllegalArgumentException.class, () ->
            PartitionRetirementTopicDeletionRecords.augment(
                nativeDelete, id -> Set.of(0), key -> null, -1L, 10
            ));
        assertSame(nativeDelete, PartitionRetirementTopicDeletionRecords.augment(
            nativeDelete, id -> Set.of(-1), key -> null, 30L, 10
        ));
        assertEquals(1, nativeDelete.records().size());
    }

    private static ApiMessageAndVersion remove(Uuid topicId) {
        return new ApiMessageAndVersion(new RemoveTopicRecord().setTopicId(topicId), (short) 0);
    }

    private static ControllerResult<Map<Uuid, ApiError>> nativeDelete(Uuid topicId) {
        return ControllerResult.atomicOf(List.of(remove(topicId)), Map.of(topicId, ApiError.NONE));
    }
}
