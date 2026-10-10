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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * A read-only broker-local source observation, never an authority to write
 * KRaft metadata or retire remote objects.
 *
 * <p>Captures an epoch-aware leadership ticket, verifies that the actual
 * SharedUnifiedLog has the SAME immutable Kafka Topic ID and partition, reads
 * logStart/HW/LEO under Kafka's native log lock, then rechecks both
 * source identity and the leadership ticket.
 * Broker registration/incarnation, WAL durable horizon, controller quorum
 * commit and cross-broker replay are NOT proved by this process-local view.</p>
 */
final class SourceLogStartObservation {
    private SourceLogStartObservation() {
    }

    static Optional<LocalObservation> capture(
        SharedPartitionRoleListener roleListener,
        SharedUnifiedLog log,
        SharedPartitionId partition
    ) throws IOException {
        Objects.requireNonNull(roleListener, "roleListener");
        Objects.requireNonNull(log, "log");
        Objects.requireNonNull(partition, "partition");
        if (!matchesSourcePartition(log, partition)) {
            return Optional.empty();
        }
        var ticket = roleListener.captureEpochRetirementLeader(partition);
        if (ticket.isEmpty()) {
            return Optional.empty();
        }
        Optional<SharedUnifiedLog.NativeSourceWindow> window = log.captureNativeSourceWindow();
        // The native window and Topic ID are read separately. A source log
        // replacement must not associate offsets with the first identity.
        if (window.isEmpty() || !matchesSourcePartition(log, partition)
            || !roleListener.stillRetirementLeader(ticket.get())) {
            return Optional.empty();
        }
        return Optional.of(new LocalObservation(
            partition, ticket.get().leaderEpoch(), ticket.get().localGeneration(), window.get()
        ));
    }

    private static boolean matchesSourcePartition(SharedUnifiedLog log, SharedPartitionId partition) {
        Uuid expected = new Uuid(partition.topicIdHigh(), partition.topicIdLow());
        Optional<Uuid> actualTopicId = log.topicId();
        if (actualTopicId == null || actualTopicId.isEmpty() || !expected.equals(actualTopicId.get())) {
            return false;
        }
        var actualPartition = log.topicPartition();
        return actualPartition != null && actualPartition.partition() == partition.partition();
    }

    record LocalObservation(
        SharedPartitionId partition,
        int sourceLeaderEpoch,
        long localGeneration,
        SharedUnifiedLog.NativeSourceWindow offsets
    ) {
        LocalObservation {
            Objects.requireNonNull(partition, "partition");
            Objects.requireNonNull(offsets, "offsets");
            if (sourceLeaderEpoch < 0 || localGeneration <= 0L) {
                throw new IllegalArgumentException("Source observation requires an epoch-aware leader ticket");
            }
        }
    }
}
