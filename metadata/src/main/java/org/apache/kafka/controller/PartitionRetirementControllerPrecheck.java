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
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.Replicas;

import java.util.Objects;

/**
 * Read-only preflight grounded in the controller's current KRaft partition registration.
 *
 * <p>This must be invoked from the controller event queue against the latest metadata
 * image. An affirmative result is NOT an authorization token, KRaft append, durable
 * generation claim, or proof of a current source log start. The registration can change
 * immediately after this method returns; a future writer must validate again atomically
 * with its authoritative controller commit. Native replica reassignment
 * must be complete; even a matching leaderEpoch is insufficient to fence
 * a changed ISR/replica topology because Kafka partitionEpoch advances
 * independently. Nothing here emits retirement metadata.</p>
 */
final class PartitionRetirementControllerPrecheck {
    private PartitionRetirementControllerPrecheck() {
    }

    static Finding assess(
        ReplicationControlManager replicationControl,
        Uuid topicId,
        int partitionId,
        int brokerId,
        int sourceLeaderEpoch
    ) {
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(topicId, "topicId");
        if (partitionId < 0 || brokerId < 0 || sourceLeaderEpoch < 0) {
            throw new IllegalArgumentException("Partition, broker and source leader epoch must be non-negative");
        }
        // The immutable Topic ID lookup fails closed when the topic was deleted and
        // recreated under the same human-readable name.
        return assessRegistration(
            replicationControl.getPartition(topicId, partitionId),
            brokerId,
            sourceLeaderEpoch
        );
    }

    /**
     * Isolated value classifier for regression testing. Only the controller lookup
     * overload above can establish that a registration came from the current KRaft image.
     */
    static Finding assessRegistration(
        PartitionRegistration registration,
        int brokerId,
        int sourceLeaderEpoch
    ) {
        if (brokerId < 0 || sourceLeaderEpoch < 0) {
            throw new IllegalArgumentException("Broker and leader epoch must be non-negative");
        }
        if (registration == null) {
            return Finding.PARTITION_NOT_PRESENT;
        }
        Finding leader = assessLeader(registration, brokerId, sourceLeaderEpoch);
        if (leader != Finding.CONTROLLER_IMAGE_MATCH) {
            return leader;
        }
        return assessReplicaTopology(registration, brokerId);
    }

    /**
     * Strict KRaft partition-generation fence. Kafka can update ISR or
     * replica assignments without changing leaderEpoch. The eventual
     * retirement writer must revalidate this in its serialized event.
     */
    static Finding assessRegistration(
        PartitionRegistration registration,
        int brokerId,
        int sourceLeaderEpoch,
        int expectedPartitionEpoch
    ) {
        if (expectedPartitionEpoch < 0) {
            throw new IllegalArgumentException("Partition epoch must be non-negative");
        }
        Finding leaderAndTopology = assessRegistration(registration, brokerId, sourceLeaderEpoch);
        if (leaderAndTopology != Finding.CONTROLLER_IMAGE_MATCH) {
            return leaderAndTopology;
        }
        return registration.partitionEpoch == expectedPartitionEpoch
            ? Finding.CONTROLLER_IMAGE_MATCH : Finding.KAFKA_PARTITION_EPOCH_MISMATCH;
    }

    private static Finding assessLeader(
        PartitionRegistration registration,
        int brokerId,
        int sourceLeaderEpoch
    ) {
        if (registration.leader < 0) {
            return Finding.NO_ACTIVE_LEADER;
        }
        if (registration.leaderEpoch != sourceLeaderEpoch) {
            return Finding.KAFKA_LEADER_EPOCH_MISMATCH;
        }
        if (registration.leader != brokerId) {
            return Finding.BROKER_NOT_LEADER;
        }
        if (registration.leaderRecoveryState != LeaderRecoveryState.RECOVERED) {
            return Finding.LEADER_STILL_RECOVERING;
        }
        return Finding.CONTROLLER_IMAGE_MATCH;
    }

    private static Finding assessReplicaTopology(PartitionRegistration registration, int brokerId) {
        if (!Replicas.contains(registration.replicas, brokerId)
            || !Replicas.contains(registration.isr, brokerId)) {
            return Finding.LEADER_NOT_IN_ISR;
        }
        // Kafka PartitionChangeRecord can change replica membership without
        // incrementing leaderEpoch. Until all adding/removing replicas have
        // finished, source ownership and reader coverage are not stationary.
        if (registration.addingReplicas.length != 0 || registration.removingReplicas.length != 0) {
            return Finding.REPLICA_REASSIGNMENT_IN_PROGRESS;
        }
        return Finding.CONTROLLER_IMAGE_MATCH;
    }

    enum Finding {
        PARTITION_NOT_PRESENT,
        NO_ACTIVE_LEADER,
        KAFKA_LEADER_EPOCH_MISMATCH,
        BROKER_NOT_LEADER,
        LEADER_STILL_RECOVERING,
        LEADER_NOT_IN_ISR,
        REPLICA_REASSIGNMENT_IN_PROGRESS,
        KAFKA_PARTITION_EPOCH_MISMATCH,
        CONTROLLER_IMAGE_MATCH
    }
}
