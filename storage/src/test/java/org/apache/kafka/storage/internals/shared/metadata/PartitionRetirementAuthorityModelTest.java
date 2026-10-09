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

import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Decision;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Outcome;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Snapshot;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Protocol reference witnesses only; they do not exercise a committed KRaft state machine.
 */
class PartitionRetirementAuthorityModelTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId RECREATED = new SharedPartitionId(1L, 3L, 0);

    @Test
    void initialStateHasNeitherImplicitZeroNorAnAuthoritativeLeader() {
        Snapshot initial = initial();
        assertEquals(-1L, initial.authorityOffset());
        assertEquals(-1, initial.maxSourceLeaderEpoch());
        assertEquals(-1, initial.activeBrokerId());
        assertTrue(initial.explicitLogStart().isEmpty());
        assertEquals(Outcome.NO_ACTIVE_LEADER, advance(initial, 1, 0, 0L, 0L, -1L, 0L).outcome());
    }

    @Test
    void higherEpochControllerElectionCreatesOnePartitionOwner() {
        Decision election = elect(initial(), 1, 10, -1L, 0L);
        assertEquals(Outcome.APPLIED, election.outcome());
        assertEquals(1, election.snapshot().activeBrokerId());
        assertEquals(10, election.snapshot().maxSourceLeaderEpoch());
        assertTrue(election.snapshot().explicitLogStart().isEmpty());
    }

    @Test
    void duplicateLeaderAtSameEpochCannotMintANewControllerGeneration() {
        Snapshot active = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Decision duplicate = elect(active, 1, 10, 0L, 1L);
        assertEquals(Outcome.NO_CHANGE, duplicate.outcome());
        assertSame(active, duplicate.snapshot());
    }

    @Test
    void equalEpochAfterFollowerCannotReestablishLeadership() {
        Snapshot elected = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Snapshot revoked = revoke(elected, 10, 0L, 1L).snapshot();
        Decision delayed = elect(revoked, 1, 10, 1L, 2L);
        assertEquals(Outcome.STALE_KAFKA_LEADER_EPOCH, delayed.outcome());
        assertSame(revoked, delayed.snapshot());
        assertEquals(-1, delayed.snapshot().activeBrokerId());
    }

    @Test
    void newLeaderEpochOnAnotherBrokerPreservesMonotonicWatermark() {
        Snapshot elected = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Snapshot zero = advance(elected, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        Snapshot watermark = advance(zero, 1, 10, 30L, 30L, 1L, 2L).snapshot();
        Snapshot revoked = revoke(watermark, 11, 2L, 3L).snapshot();
        Snapshot next = elect(revoked, 2, 12, 3L, 4L).snapshot();

        assertEquals(12, next.maxSourceLeaderEpoch());
        assertEquals(2, next.activeBrokerId());
        assertEquals(OptionalLong.of(30L), next.explicitLogStart());
        assertEquals(4L, next.authorityOffset());
    }

    @Test
    void oldBrokerCannotReinitializeToClaimANewerEpoch() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        state = revoke(state, 11, 0L, 1L).snapshot();
        state = elect(state, 2, 12, 1L, 2L).snapshot();
        Decision formerLeader = advance(state, 1, 10, 0L, 0L, 2L, 3L);
        Decision spoofedEpoch = advance(state, 1, 12, 0L, 0L, 2L, 3L);

        assertEquals(Outcome.STALE_KAFKA_LEADER_EPOCH, formerLeader.outcome());
        assertEquals(Outcome.WRONG_BROKER, spoofedEpoch.outcome());
        assertEquals(2L, state.authorityOffset());
    }

    @Test
    void staleExpectedAuthorityVersionCannotCommitOverNewerState() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        state = advance(state, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        Decision stale = advance(state, 1, 10, 40L, 40L, 0L, 2L);

        assertEquals(Outcome.STALE_AUTHORITY_SNAPSHOT, stale.outcome());
        assertEquals(OptionalLong.of(0L), stale.snapshot().explicitLogStart());
    }

    @Test
    void nonIncreasingAuthorityOffsetIsNotAccepted() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Decision invalid = advance(state, 1, 10, 0L, 0L, 0L, 0L);
        assertEquals(Outcome.NON_MONOTONIC_AUTHORITY_OFFSET, invalid.outcome());
        assertSame(state, invalid.snapshot());
    }

    @Test
    void firstWatermarkMustBeExplicitZeroForTheIncarnation() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Decision nonZero = advance(state, 1, 10, 10L, 10L, 0L, 1L);
        assertEquals(Outcome.INITIAL_ZERO_REQUIRED, nonZero.outcome());
        Decision zero = advance(state, 1, 10, 0L, 10L, 0L, 1L);
        assertEquals(Outcome.APPLIED, zero.outcome());
        assertEquals(OptionalLong.of(0L), zero.snapshot().explicitLogStart());
    }

    @Test
    void regressionFailsAndEqualityDoesNotAdvanceState() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        state = advance(state, 1, 10, 0L, 10L, 0L, 1L).snapshot();
        state = advance(state, 1, 10, 10L, 10L, 1L, 2L).snapshot();
        Decision same = advance(state, 1, 10, 10L, 10L, 2L, 3L);
        Decision lower = advance(state, 1, 10, 9L, 10L, 2L, 3L);
        assertEquals(Outcome.NO_CHANGE, same.outcome());
        assertEquals(Outcome.REGRESSED_WATERMARK, lower.outcome());
        assertSame(state, lower.snapshot());
    }

    @Test
    void sourceLogStartUpperBoundIsStillOnlyCallerEvidence() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Decision above = advance(state, 1, 10, 5L, 4L, 0L, 1L);
        assertEquals(Outcome.EXCEEDS_OBSERVED_SOURCE_START, above.outcome());
        assertTrue(above.snapshot().explicitLogStart().isEmpty());
    }

    @Test
    void noLeaderCannotAdvanceAnAlreadyRecordedWatermark() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        state = advance(state, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        state = revoke(state, 11, 1L, 2L).snapshot();
        Decision denied = advance(state, 1, 11, 0L, 0L, 2L, 3L);
        assertEquals(Outcome.NO_ACTIVE_LEADER, denied.outcome());
        assertEquals(OptionalLong.of(0L), denied.snapshot().explicitLogStart());
    }

    @Test
    void differentTopicIncarnationIsNotAuthorizedByAnExistingSnapshot() {
        Snapshot original = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Decision other = PartitionRetirementAuthorityModel.observeLeader(
            original, RECREATED, 1, 11, 0L, 1L
        );
        assertEquals(Outcome.TOPIC_PARTITION_MISMATCH, other.outcome());
        assertSame(original, other.snapshot());
        Snapshot fresh = PartitionRetirementAuthorityModel.initial(RECREATED);
        assertTrue(fresh.explicitLogStart().isEmpty());
    }

    @Test
    void compactionCannotMakeALowerProposalAdmissibleAgainstRetainedAuthority() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        state = advance(state, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        state = advance(state, 1, 10, 40L, 40L, 1L, 9L).snapshot();

        // Model the controller state reconstructed from an authoritative checkpoint:
        // the higher watermark remains in that checkpoint after log compaction.
        Snapshot restoredCheckpoint = new Snapshot(
            PARTITION, state.authorityOffset(), state.maxSourceLeaderEpoch(),
            state.activeBrokerId(), state.explicitLogStart(), false
        );
        Decision late = advance(restoredCheckpoint, 1, 10, 20L, 40L, 9L, 10L);
        assertEquals(Outcome.REGRESSED_WATERMARK, late.outcome());
        assertEquals(OptionalLong.of(40L), late.snapshot().explicitLogStart());
    }

    @Test
    void controllerOffsetsMaySkipUnrelatedLogEntriesButNeverGoBackward() {
        Snapshot state = elect(initial(), 1, 10, -1L, 100L).snapshot();
        state = advance(state, 1, 10, 0L, 0L, 100L, 104L).snapshot();
        Decision stale = revoke(state, 11, 104L, 103L);
        assertEquals(Outcome.NON_MONOTONIC_AUTHORITY_OFFSET, stale.outcome());
        assertEquals(104L, stale.snapshot().authorityOffset());
    }

    @Test
    void invalidArgumentsAndFabricatedInitialSnapshotFailFast() {
        Snapshot initial = initial();
        assertThrows(IllegalArgumentException.class, () -> elect(initial, -1, 10, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> elect(initial, 1, -1, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> advance(initial, 1, 0, -1L, 0L, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () -> advance(initial, 1, 0, 0L, -1L, -1L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, -1L, -1, 2, OptionalLong.empty(), false)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 10L, -1, -1, OptionalLong.empty(), false)
        );
        assertFalse(initial.explicitLogStart().isPresent());
    }

    @Test
    void deletingActiveTopicIrreversiblyFencesNewElections() {
        Snapshot active = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Decision deleted = delete(active, 0L, 1L);

        assertEquals(Outcome.APPLIED, deleted.outcome());
        assertTrue(deleted.snapshot().terminallyDeleted());
        assertEquals(-1, deleted.snapshot().activeBrokerId());
        Decision delayed = elect(deleted.snapshot(), 2, 99, 1L, 2L);
        assertEquals(Outcome.TERMINALLY_DELETED, delayed.outcome());
        assertSame(deleted.snapshot(), delayed.snapshot());
    }

    @Test
    void topicDeleteBeforeAnyElectionIsTerminal() {
        Decision deleted = delete(initial(), -1L, 0L);
        assertEquals(Outcome.APPLIED, deleted.outcome());
        assertEquals(-1, deleted.snapshot().maxSourceLeaderEpoch());
        assertTrue(deleted.snapshot().terminallyDeleted());
        Decision newEpoch = elect(deleted.snapshot(), 1, 0, 0L, 1L);
        assertEquals(Outcome.TERMINALLY_DELETED, newEpoch.outcome());
    }

    @Test
    void lateWatermarkAndNoLeaderCallbacksCannotModifyDeletion() {
        Snapshot active = elect(initial(), 1, 10, -1L, 0L).snapshot();
        active = advance(active, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        Snapshot deleted = delete(active, 1L, 2L).snapshot();
        assertEquals(
            Outcome.TERMINALLY_DELETED,
            advance(deleted, 1, 10, 0L, 0L, 2L, 3L).outcome()
        );
        assertEquals(Outcome.TERMINALLY_DELETED, revoke(deleted, 11, 2L, 3L).outcome());
        assertEquals(OptionalLong.of(0L), deleted.explicitLogStart());
    }

    @Test
    void staleDeleteRequestCannotOverrideNewAuthorityVersion() {
        Snapshot elected = elect(initial(), 1, 10, -1L, 0L).snapshot();
        Snapshot advanced = advance(elected, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        Decision stale = delete(advanced, 0L, 2L);
        assertEquals(Outcome.STALE_AUTHORITY_SNAPSHOT, stale.outcome());
        assertSame(advanced, stale.snapshot());
        assertFalse(stale.snapshot().terminallyDeleted());
    }

    @Test
    void duplicateDeleteIsIdempotentButDoesNotMintNewVersion() {
        Snapshot deleted = delete(initial(), -1L, 0L).snapshot();
        Decision repeated = delete(deleted, 0L, 1L);
        assertEquals(Outcome.NO_CHANGE, repeated.outcome());
        assertSame(deleted, repeated.snapshot());
        assertEquals(0L, repeated.snapshot().authorityOffset());
    }

    @Test
    void topicIdRecreationDoesNotInheritTerminalState() {
        Snapshot removed = delete(initial(), -1L, 0L).snapshot();
        assertEquals(
            Outcome.TOPIC_PARTITION_MISMATCH,
            PartitionRetirementAuthorityModel.observeLeader(removed, RECREATED, 2, 1, 0L, 1L).outcome()
        );
        Snapshot newTopic = PartitionRetirementAuthorityModel.initial(RECREATED);
        Decision elected = PartitionRetirementAuthorityModel.observeLeader(
            newTopic, RECREATED, 2, 1, -1L, 0L
        );
        assertEquals(Outcome.APPLIED, elected.outcome());
        assertFalse(elected.snapshot().terminallyDeleted());
    }

    @Test
    void terminalSnapshotRetainsWatermarkAcrossCheckpointRoundTrip() {
        Snapshot state = elect(initial(), 1, 10, -1L, 0L).snapshot();
        state = advance(state, 1, 10, 0L, 0L, 0L, 1L).snapshot();
        state = advance(state, 1, 10, 50L, 50L, 1L, 2L).snapshot();
        Snapshot deleted = delete(state, 2L, 3L).snapshot();
        Snapshot recovered = new Snapshot(
            deleted.partition(), deleted.authorityOffset(),
            deleted.maxSourceLeaderEpoch(), deleted.activeBrokerId(),
            deleted.explicitLogStart(), deleted.terminallyDeleted()
        );

        assertEquals(OptionalLong.of(50L), recovered.explicitLogStart());
        assertEquals(Outcome.TERMINALLY_DELETED, elect(recovered, 3, 11, 3L, 4L).outcome());
    }

    @Test
    void terminalStateConstructorRejectsRevivedLeader() {
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 4L, 12, 1, OptionalLong.empty(), true)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, -1L, -1, -1, OptionalLong.empty(), true)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 10L, -1, -1, OptionalLong.of(4L), true)
        );
    }

    @Test
    void initialAuthorityOffsetCannotCarryAnyClaimedGeneration() {
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, -1L, 10, -1, OptionalLong.empty(), false)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, -1L, -1, -1, OptionalLong.empty(), true)
        );
    }

    @Test
    void unknownEpochAtCommittedOffsetRequiresATerminalTombstone() {
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 0L, -1, -1, OptionalLong.empty(), false)
        );
        Snapshot deletedBeforeElection = new Snapshot(
            PARTITION, 0L, -1, -1, OptionalLong.empty(), true
        );
        assertTrue(deletedBeforeElection.terminallyDeleted());
    }

    @Test
    void terminalStateRetainsPriorWatermarkButNeverActiveBroker() {
        Snapshot deleted = new Snapshot(PARTITION, 20L, 9, -1, OptionalLong.of(50L), true);
        assertEquals(OptionalLong.of(50L), deleted.explicitLogStart());
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 20L, 9, 1, OptionalLong.of(50L), true)
        );
    }

    @Test
    void invalidNegativeDomainAndWatermarkValuesFailClosed() {
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, -2L, -1, -1, OptionalLong.empty(), false)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 2L, -2, -1, OptionalLong.empty(), false)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 2L, 3, -2, OptionalLong.empty(), false)
        );
        assertThrows(IllegalArgumentException.class, () ->
            new Snapshot(PARTITION, 2L, 3, 1, OptionalLong.of(-1L), false)
        );
    }

    private static Decision delete(Snapshot state, long expected, long next) {
        return PartitionRetirementAuthorityModel.observeTopicDeleted(
            state, PARTITION, expected, next
        );
    }

    private static Snapshot initial() {
        return PartitionRetirementAuthorityModel.initial(PARTITION);
    }

    private static Decision elect(
        Snapshot state, int broker, int epoch, long expected, long next
    ) {
        return PartitionRetirementAuthorityModel.observeLeader(
            state, PARTITION, broker, epoch, expected, next
        );
    }

    private static Decision revoke(
        Snapshot state, int epoch, long expected, long next
    ) {
        return PartitionRetirementAuthorityModel.observeNoLeader(
            state, PARTITION, epoch, expected, next
        );
    }

    private static Decision advance(
        Snapshot state, int broker, int epoch, long proposed, long sourceStart,
        long expected, long next
    ) {
        return PartitionRetirementAuthorityModel.advanceLogStart(
            state, PARTITION, broker, epoch, proposed, sourceStart, expected, next
        );
    }
}
