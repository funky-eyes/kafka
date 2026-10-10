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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRetirementAuthorityRecord;

import java.util.Objects;

/**
 * KRaft record value and replay-time invariants for future partition-retirement authority.
 *
 * <p>These immutable values are NOT a grant of permission. In particular, a record may
 * only be emitted by a future controller event that validates broker identity, leader
 * epoch, source-log evidence and expected authority version at the KRaft commit boundary.
 * No such event or emitter is enabled by this class.</p>
 */
public final class PartitionRetirementAuthorityState {
    private PartitionRetirementAuthorityState() {
    }

    public record Key(Uuid topicId, int partitionId) {
        public Key {
            Objects.requireNonNull(topicId, "topicId");
            if (partitionId < 0) {
                throw new IllegalArgumentException("Partition must be nonnegative");
            }
        }
    }

    public record Value(
        long authorityOffset,
        int sourceLeaderEpoch,
        int brokerId,
        long logStartOffset,
        boolean terminallyDeleted,
        long brokerEpoch,
        Uuid brokerIncarnationId
    ) {
        /**
         * Compatibility constructor for existing non-emitting reference models.
         * Unbound broker IDs are not authorized for future KRaft writes.
         */
        public Value(long offset, int leaderEpoch, int broker, long watermark, boolean deleted) {
            this(offset, leaderEpoch, broker, watermark, deleted, -1L, Uuid.ZERO_UUID);
        }

        public boolean hasBrokerIdentityProof() {
            return brokerId >= 0 && brokerEpoch >= 0 && !Uuid.ZERO_UUID.equals(brokerIncarnationId);
        }

        public Value {
            Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
            validateCoordinates(authorityOffset, sourceLeaderEpoch, brokerId, logStartOffset);
            validateLeadership(sourceLeaderEpoch, brokerId, logStartOffset, terminallyDeleted);
            validateBrokerIdentity(brokerId, brokerEpoch, brokerIncarnationId);
        }

        private static void validateCoordinates(long offset, int leaderEpoch, int broker, long watermark) {
            if (offset < 0 || leaderEpoch < -1 || broker < -1 || watermark < -1) {
                throw new IllegalArgumentException("Invalid retirement authority record values");
            }
        }

        private static void validateLeadership(
            int leaderEpoch, int brokerId, long watermark, boolean deleted
        ) {
            if (leaderEpoch == -1 && (!deleted || brokerId != -1 || watermark != -1)) {
                throw new IllegalArgumentException("Unknown source epoch only permits terminal deletion");
            }
            if (deleted && brokerId != -1) {
                throw new IllegalArgumentException("Deleted partition cannot retain an active broker");
            }
        }

        private static void validateBrokerIdentity(int brokerId, long epoch, Uuid incarnation) {
            if (epoch < -1) {
                throw new IllegalArgumentException("Broker registration epoch must be nonnegative or unknown");
            }
            if (brokerId == -1 && (epoch != -1 || !Uuid.ZERO_UUID.equals(incarnation))) {
                throw new IllegalArgumentException("A demoted or deleted partition cannot retain broker identity");
            }
            boolean hasBrokerEpoch = epoch >= 0;
            boolean hasIncarnation = !Uuid.ZERO_UUID.equals(incarnation);
            if (hasBrokerEpoch != hasIncarnation) {
                throw new IllegalArgumentException("Broker epoch and incarnation must be bound together");
            }
        }
    }

    public static Key key(PartitionRetirementAuthorityRecord record) {
        Objects.requireNonNull(record, "record");
        return new Key(record.topicId(), record.partitionId());
    }

    public static Value value(PartitionRetirementAuthorityRecord record) {
        Objects.requireNonNull(record, "record");
        return new Value(
            record.authorityOffset(),
            record.sourceLeaderEpoch(),
            record.brokerId(),
            record.logStartOffset(),
            record.terminallyDeleted(),
            record.brokerEpoch(),
            record.brokerIncarnationId()
        );
    }

    public static PartitionRetirementAuthorityRecord record(Key key, Value value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        return new PartitionRetirementAuthorityRecord()
            .setTopicId(key.topicId())
            .setPartitionId(key.partitionId())
            .setAuthorityOffset(value.authorityOffset())
            .setSourceLeaderEpoch(value.sourceLeaderEpoch())
            .setBrokerId(value.brokerId())
            .setBrokerEpoch(value.brokerEpoch())
            .setBrokerIncarnationId(value.brokerIncarnationId())
            .setLogStartOffset(value.logStartOffset())
            .setTerminallyDeleted(value.terminallyDeleted());
    }

    /**
     * Checks the entire previous authoritative state, not just a compacted key's last value.
     * This is a replay defence. It is NOT a replacement for serialized controller validation
     * before generating the record, and the supplied authority offset is not authenticated.
     */
    public static void validateAdvance(Value previous, Value next) {
        Objects.requireNonNull(next, "next");
        if (previous == null) {
            return;
        }
        validateMonotonicity(previous, next);
        validateBrokerContinuity(previous, next);
    }

    private static void validateMonotonicity(Value previous, Value next) {
        if (previous.terminallyDeleted()) {
            throw new IllegalStateException("Retirement authority may not revive a deleted Topic ID");
        }
        if (next.authorityOffset() <= previous.authorityOffset()) {
            throw new IllegalStateException("Retirement authority offset is not increasing");
        }
        if (next.sourceLeaderEpoch() < previous.sourceLeaderEpoch()) {
            throw new IllegalStateException("Retirement source leader epoch regressed");
        }
        if (next.logStartOffset() < previous.logStartOffset()) {
            throw new IllegalStateException("Retirement log-start watermark regressed");
        }
    }

    private static void validateBrokerContinuity(Value previous, Value next) {
        if (next.brokerId() < 0) {
            return;
        }
        if (previous.hasBrokerIdentityProof() && !next.hasBrokerIdentityProof()) {
            throw new IllegalStateException("Retirement record discarded authenticated broker identity");
        }
        if (next.sourceLeaderEpoch() == previous.sourceLeaderEpoch()) {
            validateSameEpochBroker(previous, next);
        }
    }

    private static void validateSameEpochBroker(Value previous, Value next) {
        if (previous.brokerId() != next.brokerId()) {
            throw new IllegalStateException("Retirement leader promoted or changed without a new source epoch");
        }
        if (previous.hasBrokerIdentityProof()
            && (previous.brokerEpoch() != next.brokerEpoch()
                || !previous.brokerIncarnationId().equals(next.brokerIncarnationId()))) {
            throw new IllegalStateException("Broker incarnation changed without a fresh source leader epoch");
        }
    }
}
