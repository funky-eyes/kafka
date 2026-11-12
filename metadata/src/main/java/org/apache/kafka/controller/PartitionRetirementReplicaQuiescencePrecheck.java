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
import org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Candidate;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Read-only Controller precheck for the full assigned-replica IO drain boundary.
 *
 * <p>Replica membership comes from the current KRaft partition registration,
 * NOT only the ISR. Even a replica outside ISR may have a local reader, an
 * in-flight S3 upload, or a stale object reference. Each assigned replica
 * needs an exact identity- and generation-bound reader/writer observation.
 * Replica reassignment and changed KRaft partition epochs fail closed.</p>
 *
 * <p>The expected fence-round UUID separates distinct observations for the
 * same unchanged KRaft generation. It is only an untrusted correlation token:
 * the caller must not treat an echoed UUID as an authenticated challenge
 * response. Broker-local fence generations may differ across brokers.</p>
 *
 * <p>Reports are deliberately UNAUTHENTICATED value objects. Even a complete
 * set of matching observations is NOT an authenticated quiescence certificate,
 * a durable KRaft generation, a multi-partition packed-object proof, or
 * permission to remove a COMMITTED object. No network listener, metadata
 * emitter, object reference eviction, or S3 delete path calls this precheck.
 * A future Controller writer must independently authenticate every report,
 * issue and consume one-time rounds, and revalidate all KRaft state inside its
 * serialized commit event.</p>
 */
final class PartitionRetirementReplicaQuiescencePrecheck {
    private PartitionRetirementReplicaQuiescencePrecheck() {
    }

    record Report(
        Key key,
        long objectId,
        int brokerId,
        long brokerEpoch,
        Uuid brokerIncarnationId,
        int sourceLeaderEpoch,
        int sourcePartitionEpoch,
        long expectedPreviousAuthorityOffset,
        long nextAuthorityOffset,
        long localFenceGeneration,
        Uuid fenceRoundId,
        boolean readerDrained,
        boolean writerDrained
    ) {
        Report {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
            Objects.requireNonNull(fenceRoundId, "fenceRoundId");
            if (objectId <= 0L || brokerId < 0 || brokerEpoch < 0L
                || Uuid.ZERO_UUID.equals(brokerIncarnationId)) {
                throw new IllegalArgumentException("A replica report needs a physical object and broker incarnation");
            }
            if (sourceLeaderEpoch < 0 || sourcePartitionEpoch < 0
                || expectedPreviousAuthorityOffset < -1L || nextAuthorityOffset < 0L) {
                throw new IllegalArgumentException("A replica report needs valid epoch, CAS and fence coordinates");
            }
            if (nextAuthorityOffset <= expectedPreviousAuthorityOffset
                || localFenceGeneration <= 0L || Uuid.ZERO_UUID.equals(fenceRoundId)) {
                throw new IllegalArgumentException("A replica report needs valid epoch, CAS and fence coordinates");
            }
        }
    }

    enum Finding {
        KRAFT_PARTITION_CHANGED,
        MISSING_OR_FOREIGN_REPLICA_REPORT,
        DUPLICATE_REPLICA_REPORT,
        OBJECT_OR_AUTHORITY_MISMATCH,
        SOURCE_EPOCH_MISMATCH,
        FENCE_ROUND_MISMATCH,
        BROKER_NOT_ELIGIBLE,
        BROKER_IDENTITY_MISMATCH,
        LOCAL_IO_NOT_DRAINED,
        MATCHED_UNTRUSTED
    }

    static Finding assess(
        ReplicationControlManager replicationControl,
        ClusterControlManager clusterControl,
        Candidate candidate,
        long objectId,
        Uuid expectedFenceRoundId,
        Collection<Report> reports
    ) {
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(clusterControl, "clusterControl");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(expectedFenceRoundId, "expectedFenceRoundId");
        Objects.requireNonNull(reports, "reports");
        if (objectId <= 0L || Uuid.ZERO_UUID.equals(expectedFenceRoundId)) {
            throw new IllegalArgumentException("Physical object and nonzero fence round are required");
        }

        PartitionRegistration partition = replicationControl.getPartition(
            candidate.key().topicId(), candidate.key().partitionId()
        );
        if (PartitionRetirementControllerPrecheck.assessRegistration(
            partition,
            candidate.brokerId(),
            candidate.sourceLeaderEpoch(),
            candidate.sourcePartitionEpoch()
        ) != PartitionRetirementControllerPrecheck.Finding.CONTROLLER_IMAGE_MATCH) {
            return Finding.KRAFT_PARTITION_CHANGED;
        }

        return assessAssignedReplicas(partition, clusterControl, candidate,
            objectId, expectedFenceRoundId, reports);
    }

    private static Finding assessAssignedReplicas(
        PartitionRegistration partition,
        ClusterControlManager clusterControl,
        Candidate candidate,
        long objectId,
        Uuid expectedFenceRoundId,
        Collection<Report> reports
    ) {
        // A replica outside ISR still has a physical copy or in-flight I/O.
        if (partition.replicas.length == 0 || reports.size() != partition.replicas.length) {
            return Finding.MISSING_OR_FOREIGN_REPLICA_REPORT;
        }

        Map<Integer, Report> byBroker = new HashMap<>();
        for (Report report : reports) {
            if (report == null) {
                return Finding.MISSING_OR_FOREIGN_REPLICA_REPORT;
            }
            if (byBroker.putIfAbsent(report.brokerId(), report) != null) {
                return Finding.DUPLICATE_REPLICA_REPORT;
            }
        }

        for (int brokerId : partition.replicas) {
            Report report = byBroker.remove(brokerId);
            if (report == null) {
                return Finding.MISSING_OR_FOREIGN_REPLICA_REPORT;
            }
            Finding check = assessReplica(clusterControl, candidate, objectId,
                expectedFenceRoundId, brokerId, report);
            if (check != Finding.MATCHED_UNTRUSTED) {
                return check;
            }
        }
        return byBroker.isEmpty()
            ? Finding.MATCHED_UNTRUSTED : Finding.MISSING_OR_FOREIGN_REPLICA_REPORT;
    }

    private static Finding assessReplica(
        ClusterControlManager clusterControl,
        Candidate candidate,
        long objectId,
        Uuid expectedFenceRoundId,
        int brokerId,
        Report report
    ) {
        Finding identity = assessReportCoordinates(candidate, objectId, expectedFenceRoundId, report);
        if (identity != Finding.MATCHED_UNTRUSTED) {
            return identity;
        }
        return assessBrokerRegistration(clusterControl.registration(brokerId), candidate, brokerId, report);
    }

    private static Finding assessReportCoordinates(
        Candidate candidate,
        long objectId,
        Uuid expectedFenceRoundId,
        Report report
    ) {
        if (!report.key().equals(candidate.key()) || report.objectId() != objectId
            || report.expectedPreviousAuthorityOffset() != candidate.expectedPreviousAuthorityOffset()
            || report.nextAuthorityOffset() != candidate.nextAuthorityOffset()) {
            return Finding.OBJECT_OR_AUTHORITY_MISMATCH;
        }
        if (report.sourceLeaderEpoch() != candidate.sourceLeaderEpoch()
            || report.sourcePartitionEpoch() != candidate.sourcePartitionEpoch()) {
            return Finding.SOURCE_EPOCH_MISMATCH;
        }
        if (!expectedFenceRoundId.equals(report.fenceRoundId())) {
            return Finding.FENCE_ROUND_MISMATCH;
        }
        return Finding.MATCHED_UNTRUSTED;
    }

    private static Finding assessBrokerRegistration(
        BrokerRegistration broker,
        Candidate candidate,
        int brokerId,
        Report report
    ) {
        if (!isEligibleBroker(broker, brokerId)) {
            return Finding.BROKER_NOT_ELIGIBLE;
        }
        if (broker.epoch() != report.brokerEpoch()
            || !broker.incarnationId().equals(report.brokerIncarnationId())) {
            return Finding.BROKER_IDENTITY_MISMATCH;
        }
        if (brokerId == candidate.brokerId()
            && (broker.epoch() != candidate.brokerEpoch()
                || !broker.incarnationId().equals(candidate.brokerIncarnationId()))) {
            return Finding.BROKER_IDENTITY_MISMATCH;
        }
        if (!report.readerDrained() || !report.writerDrained()) {
            return Finding.LOCAL_IO_NOT_DRAINED;
        }
        // A self-reported matching round is still not authenticated evidence.
        return Finding.MATCHED_UNTRUSTED;
    }

    private static boolean isEligibleBroker(BrokerRegistration broker, int brokerId) {
        return broker != null && broker.id() == brokerId && !broker.fenced()
            && !broker.inControlledShutdown() && broker.incarnationId() != null
            && !Uuid.ZERO_UUID.equals(broker.incarnationId());
    }
}
