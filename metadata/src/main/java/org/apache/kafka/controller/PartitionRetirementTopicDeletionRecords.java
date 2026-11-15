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
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Future-version, KRaft-native terminal topic-ID retirement records.
 *
 * <p>Derive a terminal fact from an actual native RemoveTopicRecord in the
 * SAME atomic Controller event. This does not authenticate a broker-reported
 * log-start, remote object references or drained read leases; it is NOT a
 * physical GC grant. The caller must guard all emission with the negotiated
 * partition.retirement.authority.version and its metadata.version dependency.</p>
 *
 * <p>When a native DeleteTopics request would exceed the configured atomic
 * record limit after adding retirement entries, retain the original native
 * result, emitting NO partial retirement entries. This sacrifices future GC
 * eligibility for that request instead of making Kafka Topic deletion fail
 * due to an experimental lifecycle feature. No GC worker is wired.</p>
 */
final class PartitionRetirementTopicDeletionRecords {
    private PartitionRetirementTopicDeletionRecords() {
    }

    static ControllerResult<Map<Uuid, ApiError>> augment(
        ControllerResult<Map<Uuid, ApiError>> nativeDeletion,
        Function<Uuid, Set<Integer>> partitionsForTopic,
        Function<Key, Value> replayedAuthority,
        long nextKRaftOffset,
        int maxAtomicRecords
    ) {
        Objects.requireNonNull(nativeDeletion, "nativeDeletion");
        Objects.requireNonNull(partitionsForTopic, "partitionsForTopic");
        Objects.requireNonNull(replayedAuthority, "replayedAuthority");
        if (nextKRaftOffset < 0L || maxAtomicRecords <= 0) {
            throw new IllegalArgumentException("Invalid KRaft append position or atomic batch limit");
        }
        if (!nativeDeletion.isAtomic()) {
            throw new IllegalArgumentException("Native Topic deletion must use one atomic KRaft batch");
        }

        List<ApiMessageAndVersion> added = new ArrayList<>();
        Set<Uuid> seen = new HashSet<>();
        for (ApiMessageAndVersion message : nativeDeletion.records()) {
            if (!(message.message() instanceof RemoveTopicRecord deleted)) {
                continue;
            }
            Uuid topicId = deleted.topicId();
            ApiError result = nativeDeletion.response().get(topicId);
            if (result == null || result.isFailure() || !seen.add(topicId)) {
                continue;
            }
            Set<Integer> partitions = Objects.requireNonNull(
                partitionsForTopic.apply(topicId), "Current KRaft topic partitions"
            );
            List<Integer> sorted = partitions.stream().sorted(Comparator.naturalOrder()).toList();
            if (sorted.size() > maxAtomicRecords - nativeDeletion.records().size() - added.size()) {
                return nativeDeletion;
            }
            for (Integer partition : sorted) {
                if (partition == null || partition < 0) {
                    return nativeDeletion;
                }
                Key key = new Key(topicId, partition);
                Value previous = replayedAuthority.apply(key);
                if (previous != null && previous.terminallyDeleted()) {
                    return nativeDeletion;
                }
                long recordOffset = Math.addExact(
                    nextKRaftOffset, nativeDeletion.records().size() + (long) added.size()
                );
                Value terminal = terminalValue(previous, recordOffset);
                PartitionRetirementAuthorityState.validateAdvance(previous, terminal);
                added.add(new ApiMessageAndVersion(
                    PartitionRetirementAuthorityState.record(key, terminal), (short) 0
                ));
            }
        }
        if (added.isEmpty()) {
            return nativeDeletion;
        }
        List<ApiMessageAndVersion> combined =
            new ArrayList<>(nativeDeletion.records().size() + added.size());
        combined.addAll(nativeDeletion.records());
        combined.addAll(added);
        return ControllerResult.atomicOf(combined, nativeDeletion.response());
    }

    private static Value terminalValue(Value previous, long writeOffset) {
        return new Value(
            writeOffset,
            previous == null ? -1 : previous.sourceLeaderEpoch(),
            previous == null ? -1 : previous.sourcePartitionEpoch(),
            -1,
            previous == null ? -1L : previous.logStartOffset(),
            true,
            -1L,
            Uuid.ZERO_UUID
        );
    }
}
