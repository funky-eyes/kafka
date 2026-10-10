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

import org.apache.kafka.common.metadata.PartitionRetirementAuthorityRecord;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.Optional;

import java.util.Objects;

/**
 * Controller-event-queue read model for a future KRaft retirement authority protocol.
 *
 * <p>KRaft replay and SnapshotRegistry rollback maintain the observed values. This
 * manager CANNOT emit, accept, or commit authority transitions: no RPC, controller
 * write event or producer is exposed. Production emission requires a separately
 * version-gated commit-time controller authorization implementation.</p>
 */
final class PartitionRetirementControlManager {
    private final TimelineHashMap<Key, Value> states;

    PartitionRetirementControlManager(SnapshotRegistry registry) {
        states = new TimelineHashMap<>(Objects.requireNonNull(registry, "registry"), 0);
    }

    void replay(PartitionRetirementAuthorityRecord record) {
        Key key = PartitionRetirementAuthorityState.key(record);
        Value next = PartitionRetirementAuthorityState.value(record);
        PartitionRetirementAuthorityState.validateAdvance(states.get(key), next);
        states.put(key, next);
    }

    /**
     * Validates the record's claimed authority offset against the KRaft source
     * position. Log replay must match the actual append offset exactly. Snapshot
     * replay retains the historical accepted offset, which must be strictly
     * earlier than the snapshot's next-offset boundary.
     *
     * This authenticates the position within a trusted KRaft reader only; it
     * does not independently validate the broker or source-log-start proof.
     */
    void replayFromKRaft(
        PartitionRetirementAuthorityRecord record,
        Optional<OffsetAndEpoch> snapshotId,
        long recordOffset
    ) {
        Objects.requireNonNull(snapshotId, "snapshotId");
        Objects.requireNonNull(record, "record");
        if (snapshotId.isPresent()) {
            if (record.authorityOffset() >= snapshotId.get().offset()) {
                throw new IllegalStateException("Retirement authority is ahead of its KRaft snapshot horizon");
            }
        } else if (record.authorityOffset() != recordOffset) {
            throw new IllegalStateException("Retirement authority offset does not match its KRaft record offset");
        }
        replay(record);
    }

    Value get(Key key) {
        return states.get(key);
    }
}
