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

import java.util.Objects;
import java.util.OptionalLong;

/**
 * Pure reference reducer for a FUTURE controller-serialized partition retirement authority.
 *
 * <p>This is deliberately not an authority service: its inputs and snapshots are supplied
 * by the caller and cannot prove an authenticated controller, a fresh source log start,
 * a durable KRaft write, or mixed-version support. It has no Kafka producer, object store,
 * remote index or deletion capability and MUST NOT be used as write authorization.</p>
 *
 * <p>The reference protocol assumes an external, truly serialized authority validates
 * against its latest durable snapshot at the commit boundary. Kafka's existing
 * last-writer-wins compacted log-start key does not supply this property.</p>
 */
public final class PartitionRetirementAuthorityModel {
    private static final int NO_LEADER = -1;
    private static final int UNKNOWN_EPOCH = -1;
    private static final long NO_AUTHORITY_OFFSET = -1L;

    private PartitionRetirementAuthorityModel() {
    }

    public static Snapshot initial(SharedPartitionId partition) {
        return new Snapshot(partition, NO_AUTHORITY_OFFSET, UNKNOWN_EPOCH, NO_LEADER, OptionalLong.empty(), false);
    }

    /**
     * Models a leader transition already validated by a FUTURE authoritative controller.
     * Repeated equal-epoch leader observations cannot bypass an earlier demotion.
     */
    public static Decision observeLeader(
        Snapshot previous,
        SharedPartitionId partition,
        int brokerId,
        int sourceLeaderEpoch,
        long expectedAuthorityOffset,
        long nextAuthorityOffset
    ) {
        requireBrokerAndEpoch(brokerId, sourceLeaderEpoch);
        Outcome precondition = checkVersion(previous, partition, expectedAuthorityOffset, nextAuthorityOffset);
        if (precondition != null) {
            return new Decision(precondition, previous);
        }
        if (previous.terminallyDeleted()) {
            return new Decision(Outcome.TERMINALLY_DELETED, previous);
        }
        if (sourceLeaderEpoch < previous.maxSourceLeaderEpoch()) {
            return new Decision(Outcome.STALE_KAFKA_LEADER_EPOCH, previous);
        }
        if (sourceLeaderEpoch == previous.maxSourceLeaderEpoch()) {
            if (previous.activeBrokerId() == brokerId) {
                return new Decision(Outcome.NO_CHANGE, previous);
            }
            return new Decision(Outcome.STALE_KAFKA_LEADER_EPOCH, previous);
        }
        return new Decision(
            Outcome.APPLIED,
            new Snapshot(
                partition,
                nextAuthorityOffset,
                sourceLeaderEpoch,
                brokerId,
                previous.explicitLogStart(),
                false
            )
        );
    }

    /** Models a controller-serialized transition to no active leader, preserving the max source epoch. */
    public static Decision observeNoLeader(
        Snapshot previous,
        SharedPartitionId partition,
        int sourceLeaderEpoch,
        long expectedAuthorityOffset,
        long nextAuthorityOffset
    ) {
        if (sourceLeaderEpoch < 0) {
            throw new IllegalArgumentException("sourceLeaderEpoch must be non-negative");
        }
        Outcome precondition = checkVersion(previous, partition, expectedAuthorityOffset, nextAuthorityOffset);
        if (precondition != null) {
            return new Decision(precondition, previous);
        }
        if (previous.terminallyDeleted()) {
            return new Decision(Outcome.TERMINALLY_DELETED, previous);
        }
        if (sourceLeaderEpoch < previous.maxSourceLeaderEpoch()) {
            return new Decision(Outcome.STALE_KAFKA_LEADER_EPOCH, previous);
        }
        if (sourceLeaderEpoch == previous.maxSourceLeaderEpoch() && previous.activeBrokerId() == NO_LEADER) {
            return new Decision(Outcome.NO_CHANGE, previous);
        }
        return new Decision(
            Outcome.APPLIED,
            new Snapshot(
                partition,
                nextAuthorityOffset,
                sourceLeaderEpoch,
                NO_LEADER,
                previous.explicitLogStart(),
                false
            )
        );
    }

    /**
     * Models a monotonic advance against the *latest* serialized state. Neither the
     * source-log-start argument nor expected offset is independently authenticated here.
     */
    public static Decision advanceLogStart(
        Snapshot previous,
        SharedPartitionId partition,
        int brokerId,
        int sourceLeaderEpoch,
        long requestedLogStart,
        long observedSourceLogStart,
        long expectedAuthorityOffset,
        long nextAuthorityOffset
    ) {
        requireBrokerAndEpoch(brokerId, sourceLeaderEpoch);
        if (requestedLogStart < 0L || observedSourceLogStart < 0L) {
            throw new IllegalArgumentException("log-start offsets must be non-negative");
        }
        Outcome precondition = checkVersion(previous, partition, expectedAuthorityOffset, nextAuthorityOffset);
        if (precondition != null) {
            return new Decision(precondition, previous);
        }
        if (previous.terminallyDeleted()) {
            return new Decision(Outcome.TERMINALLY_DELETED, previous);
        }
        Outcome finding = classifyAdvance(previous, brokerId, sourceLeaderEpoch,
            requestedLogStart, observedSourceLogStart);
        if (finding != Outcome.APPLIED) {
            return new Decision(finding, previous);
        }
        return new Decision(
            Outcome.APPLIED,
            new Snapshot(
                partition,
                nextAuthorityOffset,
                previous.maxSourceLeaderEpoch(),
                brokerId,
                OptionalLong.of(requestedLogStart),
                false
            )
        );
    }


    /**
     * Models a controller-committed terminal Topic ID fence. It must be applied
     * by a future authoritative controller after validating a real RemoveTopicRecord
     * for this exact topic incarnation, and survives all later controller snapshots.
     * Merely invoking this pure function does not persist or authorize anything.
     */
    public static Decision observeTopicDeleted(
        Snapshot previous,
        SharedPartitionId partition,
        long expectedAuthorityOffset,
        long nextAuthorityOffset
    ) {
        Outcome precondition = checkVersion(previous, partition, expectedAuthorityOffset, nextAuthorityOffset);
        if (precondition != null) {
            return new Decision(precondition, previous);
        }
        if (previous.terminallyDeleted()) {
            return new Decision(Outcome.NO_CHANGE, previous);
        }
        return new Decision(
            Outcome.APPLIED,
            new Snapshot(
                partition,
                nextAuthorityOffset,
                previous.maxSourceLeaderEpoch(),
                NO_LEADER,
                previous.explicitLogStart(),
                true
            )
        );
    }

    private static Outcome classifyAdvance(
        Snapshot previous,
        int brokerId,
        int sourceLeaderEpoch,
        long requestedLogStart,
        long observedSourceLogStart
    ) {
        if (previous.activeBrokerId() == NO_LEADER) {
            return Outcome.NO_ACTIVE_LEADER;
        }
        if (sourceLeaderEpoch != previous.maxSourceLeaderEpoch()) {
            return Outcome.STALE_KAFKA_LEADER_EPOCH;
        }
        if (brokerId != previous.activeBrokerId()) {
            return Outcome.WRONG_BROKER;
        }
        if (requestedLogStart > observedSourceLogStart) {
            return Outcome.EXCEEDS_OBSERVED_SOURCE_START;
        }
        OptionalLong recorded = previous.explicitLogStart();
        if (recorded.isEmpty() && requestedLogStart != 0L) {
            return Outcome.INITIAL_ZERO_REQUIRED;
        }
        if (recorded.isPresent() && requestedLogStart < recorded.getAsLong()) {
            return Outcome.REGRESSED_WATERMARK;
        }
        if (recorded.isPresent() && requestedLogStart == recorded.getAsLong()) {
            return Outcome.NO_CHANGE;
        }
        return Outcome.APPLIED;
    }

    private static Outcome checkVersion(
        Snapshot previous,
        SharedPartitionId partition,
        long expectedAuthorityOffset,
        long nextAuthorityOffset
    ) {
        Objects.requireNonNull(previous, "previous");
        Objects.requireNonNull(partition, "partition");
        if (expectedAuthorityOffset < NO_AUTHORITY_OFFSET || nextAuthorityOffset < 0L) {
            throw new IllegalArgumentException("Invalid authority offset");
        }
        if (!previous.partition().equals(partition)) {
            return Outcome.TOPIC_PARTITION_MISMATCH;
        }
        if (previous.authorityOffset() != expectedAuthorityOffset) {
            return Outcome.STALE_AUTHORITY_SNAPSHOT;
        }
        if (nextAuthorityOffset <= previous.authorityOffset()) {
            return Outcome.NON_MONOTONIC_AUTHORITY_OFFSET;
        }
        return null;
    }

    private static void requireBrokerAndEpoch(int brokerId, int sourceLeaderEpoch) {
        if (brokerId < 0 || sourceLeaderEpoch < 0) {
            throw new IllegalArgumentException("brokerId and sourceLeaderEpoch must be non-negative");
        }
    }

    public enum Outcome {
        APPLIED,
        NO_CHANGE,
        TERMINALLY_DELETED,
        TOPIC_PARTITION_MISMATCH,
        STALE_AUTHORITY_SNAPSHOT,
        NON_MONOTONIC_AUTHORITY_OFFSET,
        STALE_KAFKA_LEADER_EPOCH,
        NO_ACTIVE_LEADER,
        WRONG_BROKER,
        INITIAL_ZERO_REQUIRED,
        EXCEEDS_OBSERVED_SOURCE_START,
        REGRESSED_WATERMARK
    }

    /** An immutable *hypothetical* controller-owned snapshot, never proof of durable authority by itself. */
    public record Snapshot(
        SharedPartitionId partition,
        long authorityOffset,
        int maxSourceLeaderEpoch,
        int activeBrokerId,
        OptionalLong explicitLogStart,
        boolean terminallyDeleted
    ) {
        public Snapshot {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(explicitLogStart, "explicitLogStart");
            validateSnapshotDomain(authorityOffset, maxSourceLeaderEpoch, activeBrokerId);
            validateSnapshotInitialState(
                authorityOffset, maxSourceLeaderEpoch, activeBrokerId, explicitLogStart, terminallyDeleted
            );
            validateSnapshotLeadership(
                maxSourceLeaderEpoch, activeBrokerId, explicitLogStart, terminallyDeleted
            );
            validateSnapshotWatermark(explicitLogStart);
        }

        private static void validateSnapshotDomain(long offset, int leaderEpoch, int brokerId) {
            if (offset < NO_AUTHORITY_OFFSET || leaderEpoch < UNKNOWN_EPOCH || brokerId < NO_LEADER) {
                throw new IllegalArgumentException("Invalid reference authority snapshot");
            }
        }

        private static void validateSnapshotInitialState(
            long offset,
            int leaderEpoch,
            int brokerId,
            OptionalLong watermark,
            boolean deleted
        ) {
            if (offset == NO_AUTHORITY_OFFSET
                && (leaderEpoch != UNKNOWN_EPOCH || brokerId != NO_LEADER || watermark.isPresent() || deleted)) {
                throw new IllegalArgumentException("Unknown authority offset requires an empty initial snapshot");
            }
            if (offset >= 0L && leaderEpoch == UNKNOWN_EPOCH && !deleted) {
                throw new IllegalArgumentException("Only terminal deletion may precede a first election");
            }
        }

        private static void validateSnapshotLeadership(
            int leaderEpoch,
            int brokerId,
            OptionalLong watermark,
            boolean deleted
        ) {
            if (leaderEpoch == UNKNOWN_EPOCH && (brokerId != NO_LEADER || watermark.isPresent())) {
                throw new IllegalArgumentException("Unknown source epoch cannot have a leader or watermark");
            }
            if (deleted && brokerId != NO_LEADER) {
                throw new IllegalArgumentException("Terminally deleted topic cannot retain a leader");
            }
        }

        private static void validateSnapshotWatermark(OptionalLong watermark) {
            if (watermark.isPresent() && watermark.getAsLong() < 0L) {
                throw new IllegalArgumentException("Persisted log start must be non-negative");
            }
        }
    }

    public record Decision(Outcome outcome, Snapshot snapshot) {
        public Decision {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(snapshot, "snapshot");
        }
    }
}
