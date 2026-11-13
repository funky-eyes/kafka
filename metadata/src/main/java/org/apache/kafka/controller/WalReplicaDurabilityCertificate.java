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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Pure, fail-closed validation of a proposed WAL replica durability certificate.
 * This does not persist evidence, authorize a produce acknowledgement, or enable GC.
 */
final class WalReplicaDurabilityCertificate {
    record Coordinate(Uuid topicId, int partition, int leaderEpoch, int partitionEpoch,
                      long startOffset, long endOffset, long controllerEpoch, long kraftOffset) {
        Coordinate {
            Objects.requireNonNull(topicId);
            if (Uuid.ZERO_UUID.equals(topicId)
                || partition < 0 || leaderEpoch < 0 || partitionEpoch < 0) {
                throw new IllegalArgumentException("invalid WAL durability identity");
            }
            if (startOffset < 0 || endOffset <= startOffset
                || controllerEpoch < 0 || kraftOffset < 0) {
                throw new IllegalArgumentException("invalid WAL durability coordinate");
            }
        }
    }

    record Receipt(int brokerId, long brokerEpoch, Uuid incarnationId,
                   long storageGeneration, long durableEndOffset, boolean authenticated,
                   boolean checksumVerified) {
        Receipt {
            Objects.requireNonNull(incarnationId);
            if (Uuid.ZERO_UUID.equals(incarnationId)
                || brokerId < 0 || brokerEpoch < 0 || storageGeneration <= 0) {
                throw new IllegalArgumentException("invalid replica identity");
            }
            if (durableEndOffset < 0) {
                throw new IllegalArgumentException("invalid replica durability boundary");
            }
        }
    }

    /**
     * Controller-supplied snapshot of one current Broker registration plus
     * its independently verified local WAL storage generation. This record
     * is not itself proof that the Broker flushed any bytes.
     */
    record CurrentRegistration(long brokerEpoch, Uuid incarnationId, long storageGeneration,
                               boolean fenced, boolean inControlledShutdown) {
        CurrentRegistration {
            Objects.requireNonNull(incarnationId);
            if (brokerEpoch < 0 || Uuid.ZERO_UUID.equals(incarnationId) || storageGeneration <= 0) {
                throw new IllegalArgumentException("invalid current WAL replica registration");
            }
        }
    }

    private WalReplicaDurabilityCertificate() {
    }

    /**
     * This is a necessary validation precheck, never sufficient evidence for ACK.
     * The caller must independently verify current KRaft topology, ISR/HW,
     * storage durability, range continuity, replay and controller leadership.
     */
    static boolean hasIndependentCompleteReceipts(Coordinate coordinate, List<Receipt> receipts,
                                                   Set<Integer> currentIsr, int requiredReplicas) {
        if (coordinate == null || receipts == null || currentIsr == null) {
            return false;
        }
        if (requiredReplicas < 2 || currentIsr.size() < requiredReplicas
            || receipts.size() < requiredReplicas) {
            return false;
        }
        Set<Integer> seen = new HashSet<>();
        for (Receipt receipt : receipts) {
            if (receipt == null || !receipt.authenticated() || !receipt.checksumVerified()) {
                return false;
            }
            if (!currentIsr.contains(receipt.brokerId()) || !seen.add(receipt.brokerId())
                || receipt.durableEndOffset() < coordinate.endOffset()) {
                return false;
            }
        }
        return seen.size() >= requiredReplicas;
    }

    /**
     * Stronger necessary precheck against an independently captured current
     * Broker identity snapshot. In particular, a receipt from an earlier
     * registration, restarted Broker process or replaced disk cannot be
     * carried forward merely because its Broker ID remains in ISR.
     *
     * This does not prove durable contiguous WAL bytes, a KRaft-committed
     * certificate, Controller replay validity or Kafka ACK eligibility.
     */
    static boolean hasCurrentRegisteredCompleteReceipts(
        Coordinate coordinate,
        List<Receipt> receipts,
        Set<Integer> currentIsr,
        Map<Integer, CurrentRegistration> registrations,
        int requiredReplicas
    ) {
        if (registrations == null
            || !hasIndependentCompleteReceipts(coordinate, receipts, currentIsr, requiredReplicas)) {
            return false;
        }
        Set<Uuid> seenIncarnations = new HashSet<>();
        for (Receipt receipt : receipts) {
            CurrentRegistration current = registrations.get(receipt.brokerId());
            if (current == null || current.fenced() || current.inControlledShutdown()
                || current.brokerEpoch() != receipt.brokerEpoch()
                || !current.incarnationId().equals(receipt.incarnationId())
                || current.storageGeneration() != receipt.storageGeneration()
                || !seenIncarnations.add(receipt.incarnationId())) {
                return false;
            }
        }
        return true;
    }
}
