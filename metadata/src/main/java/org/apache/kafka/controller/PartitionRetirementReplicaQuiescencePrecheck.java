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
 * <p>Reports are deliberately UNAUTHENTICATED value objects. Even a complete
 * set of matching observations is NOT an authenticated quiescence certificate,
 * a durable KRaft generation, a multi-partition packed-object proof, or
 * permission to remove a COMMITTED object. No network listener, metadata
 * emitter, object reference eviction, or S3 delete path calls this precheck.
 * A future Controller writer must independently authenticate every report
 * and revalidate all KRaft state inside its serialized commit event.</p>
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
        boolean readerDrained,
        boolean writerDrained
    ) {
        Report {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
            if (objectId <= 0L || brokerId < 0 || brokerEpoch < 0L
                || Uuid.ZERO_UUID.equals(brokerIncarnationId)) {
                throw new IllegalArgumentException("A replica report needs a physical object and broker incarnation");
            }
            if (sourceLeaderEpoch < 0 || sourcePartitionEpoch < 0
                || expectedPreviousAuthorityOffset < -1L || nextAuthorityOffset < 0L
                || nextAuthorityOffset <= expectedPreviousAuthorityOffset
                || localFenceGeneration <= 0L) {
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
        Collection<Report> reports
    ) {
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(clusterControl, "clusterControl");
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(reports, "reports");
        if (objectId <= 0L) {
            throw new IllegalArgumentException("Physical object ID must be positive");
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

        // Never substitute ISR for the full assigned replica set. Insist on
        // exactly one report per assigned replica, including out-of-ISR ones.
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
            if (!report.key().equals(candidate.key()) || report.objectId() != objectId
                || report.expectedPreviousAuthorityOffset() != candidate.expectedPreviousAuthorityOffset()
                || report.nextAuthorityOffset() != candidate.nextAuthorityOffset()) {
                return Finding.OBJECT_OR_AUTHORITY_MISMATCH;
            }
            if (report.sourceLeaderEpoch() != candidate.sourceLeaderEpoch()
                || report.sourcePartitionEpoch() != candidate.sourcePartitionEpoch()) {
                return Finding.SOURCE_EPOCH_MISMATCH;
            }

            BrokerRegistration broker = clusterControl.registration(brokerId);
            if (broker == null || broker.id() != brokerId || broker.fenced()
                || broker.inControlledShutdown() || broker.incarnationId() == null
                || Uuid.ZERO_UUID.equals(broker.incarnationId())) {
                return Finding.BROKER_NOT_ELIGIBLE;
            }
            if (broker.epoch() != report.brokerEpoch()
                || !broker.incarnationId().equals(report.brokerIncarnationId())
                || (brokerId == candidate.brokerId()
                    && (broker.epoch() != candidate.brokerEpoch()
                        || !broker.incarnationId().equals(candidate.brokerIncarnationId())))) {
                return Finding.BROKER_IDENTITY_MISMATCH;
            }
            if (!report.readerDrained() || !report.writerDrained()) {
                return Finding.LOCAL_IO_NOT_DRAINED;
            }
        }
        // A matching self-report proves neither channel authentication nor
        // durability across controller failover, broker restart or S3 PUT races.
        return byBroker.isEmpty()
            ? Finding.MATCHED_UNTRUSTED : Finding.MISSING_OR_FOREIGN_REPLICA_REPORT;
    }
}
