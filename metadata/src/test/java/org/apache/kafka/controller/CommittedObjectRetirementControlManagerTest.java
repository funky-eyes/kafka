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
import org.apache.kafka.common.metadata.CommittedObjectRetirementRecord;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.CommittedObjectRetirementState;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommittedObjectRetirementControlManagerTest {
    private static final long OBJECT_ID = 501L;
    private static final Uuid CATALOG = Uuid.randomUuid();
    private static final String DIGEST = "0123456789abcdef".repeat(4);

    @Test
    void firstIntentIsAtomicAndReplayedAtExactKRaftOffset() {
        CommittedObjectRetirementControlManager manager = manager();
        CommittedObjectRetirementRecord claim = record(0L, -1L, 1, -1L);
        ControllerResult<Boolean> planned = manager.recordRetirementIntent(claim, 11L);
        assertEquals(true, planned.response());
        assertEquals(true, planned.isAtomic());
        assertNull(manager.get(OBJECT_ID));
        assertEquals(0L, claim.acceptedKRaftOffset(), "Never mutate the caller's record");
        CommittedObjectRetirementRecord intent =
            (CommittedObjectRetirementRecord) planned.records().get(0).message();
        assertEquals(11L, intent.acceptedKRaftOffset());
        assertEquals(-1L, intent.previousKRaftOffset());
        assertEquals(-1L, intent.safeEpoch());
        assertEquals((byte) 1, intent.stage());
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(intent, Optional.empty(), 12L));
        assertNull(manager.get(OBJECT_ID));
        manager.replayFromKRaft(intent, Optional.empty(), 11L);
        assertEquals(CommittedObjectRetirementState.Stage.RETIRE_INTENT, manager.get(OBJECT_ID).stage());
        assertEquals(false, manager.recordRetirementIntent(claim, 12L).response());
        assertEquals(0, manager.recordRetirementIntent(claim, 12L).records().size());
    }

    @Test
    void firstIntentRejectsForgedLaterStagesAndInvalidImmutableIdentity() {
        CommittedObjectRetirementControlManager manager = manager();
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntent(record(0L, -1L, 2, 7L), 11L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntent(record(0L, 8L, 1, -1L), 11L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntent(record(0L, -1L, 1, 7L), 11L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntent(record(0L, -1L, 1, -1L).setDescriptorSha256("bad"), 11L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntent(record(0L, -1L, 1, -1L).setCatalogTopicId(Uuid.ZERO_UUID), 11L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntent(record(0L, -1L, 1, -1L), -1L));
        assertNull(manager.get(OBJECT_ID));
    }

    @Test
    void batchFirstIntentsAreAtomicAndReplayOnlyAtTheirActualOffsets() {
        CommittedObjectRetirementControlManager manager = manager();
        var first = record(0L, -1L, 1, -1L);
        var second = record(0L, -1L, 1, -1L).setObjectId(OBJECT_ID + 1L);
        var planned = manager.recordRetirementIntentBatch(java.util.List.of(first, second), 21L);
        assertEquals(2, planned.response());
        assertEquals(true, planned.isAtomic());
        assertNull(manager.get(OBJECT_ID));
        assertNull(manager.get(OBJECT_ID + 1L));
        var one = (CommittedObjectRetirementRecord) planned.records().get(0).message();
        var two = (CommittedObjectRetirementRecord) planned.records().get(1).message();
        assertEquals(21L, one.acceptedKRaftOffset());
        assertEquals(22L, two.acceptedKRaftOffset());
        assertEquals(-1L, one.safeEpoch());
        assertEquals(-1L, two.safeEpoch());
        assertEquals((byte) 1, one.stage());
        assertEquals((byte) 1, two.stage());
        assertEquals(0L, first.acceptedKRaftOffset());
        assertEquals(0L, second.acceptedKRaftOffset());
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(one, Optional.empty(), 20L));
        assertNull(manager.get(OBJECT_ID));
        manager.replayFromKRaft(one, Optional.empty(), 21L);
        manager.replayFromKRaft(two, Optional.empty(), 22L);
        assertEquals(CommittedObjectRetirementState.Stage.RETIRE_INTENT, manager.get(OBJECT_ID).stage());
        assertEquals(CommittedObjectRetirementState.Stage.RETIRE_INTENT, manager.get(OBJECT_ID + 1L).stage());
        assertEquals(0, manager.recordRetirementIntentBatch(
            java.util.List.of(first, second), 23L).records().size());
    }

    @Test
    void invalidAndDuplicateFirstIntentsVetoEntireBatch() {
        CommittedObjectRetirementControlManager manager = manager();
        var first = record(0L, -1L, 1, -1L);
        var second = record(0L, -1L, 1, -1L).setObjectId(OBJECT_ID + 1L);
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntentBatch(java.util.List.of(first, first), 20L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntentBatch(
                java.util.Collections.nCopies(65, first), 20L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.recordRetirementIntentBatch(java.util.List.of(first, second.setStage((byte) 2)), 20L));
        assertEquals(0, manager.recordRetirementIntentBatch(java.util.List.of(), 20L).response());
        assertNull(manager.get(OBJECT_ID));
        manager.replayFromKRaft(record(10L, -1L, 1, -1L).setObjectId(OBJECT_ID + 1L),
            Optional.empty(), 10L);
        assertEquals(0, manager.recordRetirementIntentBatch(
            java.util.List.of(first, record(0L, -1L, 1, -1L).setObjectId(OBJECT_ID + 1L)),
            20L).records().size());
        assertNull(manager.get(OBJECT_ID));
    }

    @Test
    void terminalBatchBuildsOneAtomicOrderedKRaftAppendWithoutPartialMutation() {
        CommittedObjectRetirementControlManager manager = manager();
        long otherObjectId = OBJECT_ID + 1L;
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        manager.replayFromKRaft(record(11L, 10L, 2, 7L), Optional.empty(), 11L);
        manager.replayFromKRaft(record(12L, 11L, 3, 7L), Optional.empty(), 12L);
        manager.replayFromKRaft(record(13L, -1L, 1, -1L).setObjectId(otherObjectId),
            Optional.empty(), 13L);
        manager.replayFromKRaft(record(14L, 13L, 2, 7L).setObjectId(otherObjectId),
            Optional.empty(), 14L);
        manager.replayFromKRaft(record(15L, 14L, 3, 7L).setObjectId(otherObjectId),
            Optional.empty(), 15L);

        var first = new CommittedObjectRetirementControlManager.TerminalDeletion(OBJECT_ID, 12L);
        var second = new CommittedObjectRetirementControlManager.TerminalDeletion(otherObjectId, 15L);
        ControllerResult<Integer> result =
            manager.finalizeRecordedDeletionBatch(java.util.List.of(first, second), 16L);
        assertEquals(true, result.isAtomic());
        assertEquals(2, result.response());
        assertEquals(2, result.records().size());
        CommittedObjectRetirementRecord one =
            (CommittedObjectRetirementRecord) result.records().get(0).message();
        CommittedObjectRetirementRecord two =
            (CommittedObjectRetirementRecord) result.records().get(1).message();
        assertEquals(16L, one.acceptedKRaftOffset());
        assertEquals(17L, two.acceptedKRaftOffset());
        assertEquals(12L, one.previousKRaftOffset());
        assertEquals(15L, two.previousKRaftOffset());
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_RECORDED, manager.get(OBJECT_ID).stage());
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_RECORDED, manager.get(otherObjectId).stage());

        manager.replayFromKRaft(one, Optional.empty(), 16L);
        manager.replayFromKRaft(two, Optional.empty(), 17L);
        assertEquals(CommittedObjectRetirementState.Stage.DELETED, manager.get(OBJECT_ID).stage());
        assertEquals(CommittedObjectRetirementState.Stage.DELETED, manager.get(otherObjectId).stage());
    }

    @Test
    void staleOrUnfinishedTerminalBatchVetoesEveryPhysicalObject() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        manager.replayFromKRaft(record(11L, 10L, 2, 7L), Optional.empty(), 11L);
        manager.replayFromKRaft(record(12L, 11L, 3, 7L), Optional.empty(), 12L);
        var first = new CommittedObjectRetirementControlManager.TerminalDeletion(OBJECT_ID, 12L);
        var missing = new CommittedObjectRetirementControlManager.TerminalDeletion(OBJECT_ID + 1, 13L);
        var stale = new CommittedObjectRetirementControlManager.TerminalDeletion(OBJECT_ID, 11L);
        assertEquals(0, manager.finalizeRecordedDeletionBatch(java.util.List.of(first, missing), 13L)
            .records().size());
        assertEquals(0, manager.finalizeRecordedDeletionBatch(java.util.List.of(stale), 13L)
            .records().size());
        assertThrows(IllegalArgumentException.class, () ->
            manager.finalizeRecordedDeletionBatch(java.util.List.of(first, first), 13L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.finalizeRecordedDeletionBatch(java.util.Collections.nCopies(65, first), 13L));
        assertThrows(IllegalStateException.class, () ->
            manager.finalizeRecordedDeletionBatch(java.util.List.of(first), 12L));
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_RECORDED, manager.get(OBJECT_ID).stage());
    }

    @Test
    void terminalWriterOnlyAppendsAfterMatchingRecordedPhysicalDelete() {
        CommittedObjectRetirementControlManager manager = manager();
        assertEquals(false, manager.finalizeRecordedDeletion(OBJECT_ID, 12L, 13L).response());
        assertEquals(0, manager.finalizeRecordedDeletion(OBJECT_ID, 12L, 13L).records().size());

        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        manager.replayFromKRaft(record(11L, 10L, 2, 7L), Optional.empty(), 11L);
        assertEquals(false, manager.finalizeRecordedDeletion(OBJECT_ID, 11L, 12L).response());
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_READY, manager.get(OBJECT_ID).stage());

        manager.replayFromKRaft(record(12L, 11L, 3, 7L), Optional.empty(), 12L);
        ControllerResult<Boolean> terminal = manager.finalizeRecordedDeletion(OBJECT_ID, 12L, 13L);
        assertEquals(true, terminal.response());
        assertEquals(true, terminal.isAtomic());
        assertEquals(1, terminal.records().size());
        CommittedObjectRetirementRecord generated =
            (CommittedObjectRetirementRecord) terminal.records().get(0).message();
        assertEquals(OBJECT_ID, generated.objectId());
        assertEquals(12L, generated.previousKRaftOffset());
        assertEquals(13L, generated.acceptedKRaftOffset());
        assertEquals((byte) 4, generated.stage());
        assertEquals(DIGEST, generated.descriptorSha256());
        assertEquals(CATALOG, generated.catalogTopicId());

        // The write result alone is not durable; only actual KRaft replay
        // can advance the timeline. An uncertain append must be retried via CAS.
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_RECORDED, manager.get(OBJECT_ID).stage());
        manager.replayFromKRaft(generated, Optional.empty(), 13L);
        assertEquals(CommittedObjectRetirementState.Stage.DELETED, manager.get(OBJECT_ID).stage());
        assertEquals(false, manager.finalizeRecordedDeletion(OBJECT_ID, 12L, 14L).response());
    }

    @Test
    void terminalWriterRejectsStaleOffsetAndNeverSkipsPhysicalDeleteStage() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        assertEquals(false, manager.finalizeRecordedDeletion(OBJECT_ID, 10L, 11L).response());
        manager.replayFromKRaft(record(11L, 10L, 2, 7L), Optional.empty(), 11L);
        assertEquals(false, manager.finalizeRecordedDeletion(OBJECT_ID, 11L, 12L).response());
        manager.replayFromKRaft(record(12L, 11L, 3, 7L), Optional.empty(), 12L);
        assertEquals(false, manager.finalizeRecordedDeletion(OBJECT_ID, 11L, 13L).response());
        assertThrows(IllegalStateException.class, () ->
            manager.finalizeRecordedDeletion(OBJECT_ID, 12L, 12L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.finalizeRecordedDeletion(0L, 12L, 13L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.finalizeRecordedDeletion(OBJECT_ID, -1L, 13L));

        // Exact KRaft append verification remains mandatory: a prepared
        // terminal record replayed at another offset is corruption.
        CommittedObjectRetirementRecord planned =
            (CommittedObjectRetirementRecord) manager.finalizeRecordedDeletion(
                OBJECT_ID, 12L, 13L).records().get(0).message();
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(planned, Optional.empty(), 14L));
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_RECORDED, manager.get(OBJECT_ID).stage());
    }

    @Test
    void exactKRaftOffsetsAndCasDriveEveryDurableStage() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        manager.replayFromKRaft(record(11L, 10L, 2, 7L), Optional.empty(), 11L);
        manager.replayFromKRaft(record(12L, 11L, 3, 7L), Optional.empty(), 12L);
        manager.replayFromKRaft(record(13L, 12L, 4, 7L), Optional.empty(), 13L);
        assertEquals(CommittedObjectRetirementState.Stage.DELETED, manager.get(OBJECT_ID).stage());
        assertEquals(13L, manager.get(OBJECT_ID).acceptedKRaftOffset());
    }

    @Test
    void firstTransitionMustBeAnIntentAndMustOccupyActualLogOffset() {
        CommittedObjectRetirementControlManager manager = manager();
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 11L));
        assertNull(manager.get(OBJECT_ID));
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(10L, -1L, 2, 7L), Optional.empty(), 10L));
        assertNull(manager.get(OBJECT_ID));
    }

    @Test
    void staleCasAndStageSkipCannotReplaceACommittedIntent() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(11L, 9L, 2, 7L), Optional.empty(), 11L));
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(11L, 10L, 3, 7L), Optional.empty(), 11L));
        assertEquals(CommittedObjectRetirementState.Stage.RETIRE_INTENT, manager.get(OBJECT_ID).stage());
    }

    @Test
    void immutableDescriptorAndCatalogIncarnationCannotChange() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(11L, 10L, 2, 7L).setDescriptorSha256("f".repeat(64)),
                Optional.empty(), 11L));
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(11L, 10L, 2, 7L).setCatalogTopicId(Uuid.randomUuid()),
                Optional.empty(), 11L));
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(11L, 10L, 2, 7L).setSealedCatalogPositionExclusive(13L),
                Optional.empty(), 11L));
    }

    @Test
    void readinessWithoutSafeEpochFailsClosed() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        assertThrows(IllegalArgumentException.class, () ->
            manager.replayFromKRaft(record(11L, 10L, 2, -1L), Optional.empty(), 11L));
        assertEquals(CommittedObjectRetirementState.Stage.RETIRE_INTENT, manager.get(OBJECT_ID).stage());
    }

    @Test
    void terminalDeletionBlocksFutureObjectIdResurrection() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 4, 7L), Optional.of(new OffsetAndEpoch(20L, 4)), 19L);
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(21L, 10L, 2, 7L), Optional.empty(), 21L));
    }

    @Test
    void trustedSnapshotMayRestoreOneTerminalEntryButNeverClaimFuturePosition() {
        CommittedObjectRetirementControlManager manager = manager();
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(20L, 19L, 4, 7L),
                Optional.of(new OffsetAndEpoch(20L, 3)), 19L));
        assertNull(manager.get(OBJECT_ID));
        manager.replayFromKRaft(record(20L, 19L, 4, 7L),
            Optional.of(new OffsetAndEpoch(21L, 3)), 20L);
        assertEquals(CommittedObjectRetirementState.Stage.DELETED, manager.get(OBJECT_ID).stage());
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(20L, 19L, 3, 7L),
                Optional.of(new OffsetAndEpoch(21L, 3)), 20L));
    }

    @Test
    void controllerTimelineRollbackRestoresPriorKRaftLifecycle() {
        SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        CommittedObjectRetirementControlManager manager = new CommittedObjectRetirementControlManager(registry);
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        registry.idempotentCreateSnapshot(10L);
        manager.replayFromKRaft(record(11L, 10L, 2, 7L), Optional.empty(), 11L);
        registry.idempotentCreateSnapshot(11L);
        assertEquals(CommittedObjectRetirementState.Stage.DELETE_READY, manager.get(OBJECT_ID).stage());
        registry.revertToSnapshot(10L);
        assertEquals(CommittedObjectRetirementState.Stage.RETIRE_INTENT, manager.get(OBJECT_ID).stage());
    }

    @Test
    void independentPhysicalObjectsAndCatalogTopicIncarnationsDoNotAlias() {
        CommittedObjectRetirementControlManager manager = manager();
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        manager.replayFromKRaft(record(11L, -1L, 1, -1L).setObjectId(OBJECT_ID + 1)
            .setCatalogTopicId(Uuid.randomUuid()), Optional.empty(), 11L);
        assertEquals(10L, manager.get(OBJECT_ID).acceptedKRaftOffset());
        assertEquals(11L, manager.get(OBJECT_ID + 1).acceptedKRaftOffset());
    }

    @Test
    void malformedDigestUnknownStageAndRegressingSafeEpochFailClosed() {
        CommittedObjectRetirementControlManager manager = manager();
        assertThrows(IllegalArgumentException.class, () ->
            manager.replayFromKRaft(record(10L, -1L, 1, -1L).setDescriptorSha256("NOT_SHA"),
                Optional.empty(), 10L));
        assertThrows(IllegalArgumentException.class, () ->
            manager.replayFromKRaft(record(10L, -1L, 1, -1L).setStage((byte) 9),
                Optional.empty(), 10L));
        manager.replayFromKRaft(record(10L, -1L, 1, -1L), Optional.empty(), 10L);
        manager.replayFromKRaft(record(11L, 10L, 2, 8L), Optional.empty(), 11L);
        assertThrows(IllegalStateException.class, () ->
            manager.replayFromKRaft(record(12L, 11L, 3, 7L), Optional.empty(), 12L));
    }

    private static CommittedObjectRetirementControlManager manager() {
        return new CommittedObjectRetirementControlManager(new SnapshotRegistry(new LogContext()));
    }

    private static CommittedObjectRetirementRecord record(long offset, long previous, int stage, long safeEpoch) {
        return new CommittedObjectRetirementRecord()
            .setObjectId(OBJECT_ID)
            .setAcceptedKRaftOffset(offset)
            .setPreviousKRaftOffset(previous)
            .setRetirementEpoch(7L)
            .setCatalogTopicId(CATALOG)
            .setCatalogCommitOffset(8L)
            .setSealedCatalogPositionExclusive(12L)
            .setObjectSize(4096L)
            .setObjectChecksum(42L)
            .setDescriptorSha256(DIGEST)
            .setStage((byte) stage)
            .setSafeEpoch(safeEpoch);
    }
}
