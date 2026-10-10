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
        boolean terminallyDeleted
    ) {
        public Value {
            if (authorityOffset < 0 || sourceLeaderEpoch < -1 || brokerId < -1 || logStartOffset < -1) {
                throw new IllegalArgumentException("Invalid retirement authority record values");
            }
            if (sourceLeaderEpoch == -1 && (!terminallyDeleted || brokerId != -1 || logStartOffset != -1)) {
                throw new IllegalArgumentException("Unknown source epoch only permits terminal deletion");
            }
            if (terminallyDeleted && brokerId != -1) {
                throw new IllegalArgumentException("Deleted partition cannot retain an active broker");
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
            record.terminallyDeleted()
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
        if (next.sourceLeaderEpoch() == previous.sourceLeaderEpoch()
            && next.brokerId() >= 0 && previous.brokerId() != next.brokerId()) {
            throw new IllegalStateException("Retirement leader promoted or changed without a new source epoch");
        }
    }
}
