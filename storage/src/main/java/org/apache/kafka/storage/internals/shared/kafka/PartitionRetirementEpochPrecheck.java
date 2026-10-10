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

import org.apache.kafka.storage.internals.shared.metadata.PartitionLogStartAdvancePrecheck;
import org.apache.kafka.storage.internals.shared.metadata.SharedMetadataImage;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Read-only join of a local KRaft epoch notification with the metadata replay value precheck.
 *
 * <p>A matching local epoch is not a distributed lease, an authoritative latest
 * KRaft epoch, a transaction generation fence, or permission to write a
 * compacted watermark record. Callers must never use an advisory result as
 * a retirement or MinIO-delete authorization.</p>
 */
public final class PartitionRetirementEpochPrecheck {
    private PartitionRetirementEpochPrecheck() {
    }

    /**
     * @param observedKafkaLeaderEpoch freshly observed leader epoch from the Kafka source partition,
     *                                 or -1 when unavailable
     * @param observedKafkaLogStart freshly observed inclusive Kafka source log start
     * @param requiredMetadataOffset last record in an external read-committed replay horizon
     */
    public static Assessment assess(
        LocalRetirementLeadershipFence fence,
        LocalRetirementLeadershipFence.LeaderTicket ticket,
        int observedKafkaLeaderEpoch,
        SharedMetadataImage image,
        long requestedLogStart,
        long observedKafkaLogStart,
        long requiredMetadataOffset
    ) {
        Objects.requireNonNull(fence, "fence");
        Objects.requireNonNull(ticket, "ticket");
        Objects.requireNonNull(image, "image");
        if (!fence.stillLeader(ticket)) {
            return new Assessment(Status.LOCAL_TICKET_STALE, Optional.empty());
        }
        if (ticket.leaderEpoch() < 0) {
            return new Assessment(Status.KAFKA_EPOCH_UNKNOWN, Optional.empty());
        }
        if (observedKafkaLeaderEpoch != ticket.leaderEpoch()) {
            return new Assessment(Status.KAFKA_EPOCH_MISMATCH, Optional.empty());
        }
        PartitionLogStartAdvancePrecheck.Finding value = PartitionLogStartAdvancePrecheck.assess(
            image, ticket.partition(), requestedLogStart, observedKafkaLogStart, requiredMetadataOffset
        );
        // Metadata replay can overlap a demotion even when the first ticket
        // check succeeds. Return no advisory result from a stale generation.
        if (!fence.stillLeader(ticket)) {
            return new Assessment(Status.LOCAL_TICKET_STALE, Optional.empty());
        }
        return new Assessment(Status.LOCAL_EPOCH_OBSERVATION_MATCH, Optional.of(value));
    }

    /**
     * Consults the actual local Kafka UnifiedLog instead of trusting a caller's
     * supplied epoch or LogStart coordinates. This stays a read-only advisory
     * check: the local ticket is not a broker-incarnation certificate, and the
     * metadata-consumer offset is not a quorum-wide durability barrier.
     *
     * <p>Kafka log locks are acquired only by native observation. Neither that
     * lock nor the role listener's callback lock is held during metadata lookup.</p>
     */
    static Assessment assessNativeSource(
        SharedPartitionRoleListener roleListener,
        SharedUnifiedLog sourceLog,
        SharedPartitionId partition,
        SharedMetadataImage image,
        long requestedLogStart,
        long requiredMetadataOffset
    ) throws IOException {
        Objects.requireNonNull(roleListener, "roleListener");
        Objects.requireNonNull(sourceLog, "sourceLog");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(image, "image");
        if (requestedLogStart < 0L || requiredMetadataOffset < -1L) {
            throw new IllegalArgumentException("Invalid native retirement preflight offsets");
        }
        var captured = SourceLogStartObservation.capture(roleListener, sourceLog, partition);
        if (captured.isEmpty()) {
            return new Assessment(Status.NATIVE_SOURCE_WINDOW_UNKNOWN, Optional.empty());
        }
        SourceLogStartObservation.LocalObservation source = captured.get();
        PartitionLogStartAdvancePrecheck.Finding value = PartitionLogStartAdvancePrecheck.assess(
            image, partition, requestedLogStart, source.offsets().logStartOffset(), requiredMetadataOffset
        );
        // Metadata replay may race with Kafka demotion, reassignment or removal.
        // The previously captured local generation must not be reused.
        var currentTicket = roleListener.captureEpochRetirementLeader(partition);
        if (currentTicket.isEmpty()
            || currentTicket.get().leaderEpoch() != source.sourceLeaderEpoch()
            || currentTicket.get().localGeneration() != source.localGeneration()) {
            return new Assessment(Status.LOCAL_ROLE_CHANGED_DURING_METADATA_CHECK, Optional.empty());
        }
        return new Assessment(Status.LOCAL_EPOCH_OBSERVATION_MATCH, Optional.of(value));
    }

    public enum Status {
        LOCAL_TICKET_STALE,
        KAFKA_EPOCH_UNKNOWN,
        KAFKA_EPOCH_MISMATCH,
        LOCAL_EPOCH_OBSERVATION_MATCH,
        NATIVE_SOURCE_WINDOW_UNKNOWN,
        LOCAL_ROLE_CHANGED_DURING_METADATA_CHECK
    }

    /** The value classification exists only for matching local epoch evidence. */
    public record Assessment(
        Status status,
        Optional<PartitionLogStartAdvancePrecheck.Finding> valueFinding
    ) {
        public Assessment {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(valueFinding, "valueFinding");
            if ((status == Status.LOCAL_EPOCH_OBSERVATION_MATCH) != valueFinding.isPresent()) {
                throw new IllegalArgumentException("Value finding requires matched local epoch evidence");
            }
        }
    }
}
