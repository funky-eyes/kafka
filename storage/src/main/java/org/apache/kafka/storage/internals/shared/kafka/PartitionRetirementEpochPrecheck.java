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
        return new Assessment(
            Status.LOCAL_EPOCH_OBSERVATION_MATCH,
            Optional.of(PartitionLogStartAdvancePrecheck.assess(
                image,
                ticket.partition(),
                requestedLogStart,
                observedKafkaLogStart,
                requiredMetadataOffset
            ))
        );
    }

    public enum Status {
        LOCAL_TICKET_STALE,
        KAFKA_EPOCH_UNKNOWN,
        KAFKA_EPOCH_MISMATCH,
        LOCAL_EPOCH_OBSERVATION_MATCH
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
