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

    /**
     * Coordinates supplied by a trusted KRaft record or snapshot reader and
     * the current Controller's committed metadata log horizon. Both horizons
     * are exclusive. -1 selects the absent replay source; exactly one of
     * logRecordOffset and snapshotNextOffset must be present.
     *
     * Constructing this value cannot authenticate a KRaft source. Callers
     * must obtain these fields from Controller-owned replay state.
     */
    record KRaftReplayContext(
        Uuid topicId,
        int partition,
        int leaderEpoch,
        int partitionEpoch,
        long currentControllerEpoch,
        long committedNextOffset,
        long logRecordOffset,
        long snapshotNextOffset
    ) {
        KRaftReplayContext {
            requireValidPartitionIdentity(topicId, partition, leaderEpoch, partitionEpoch);
            requireValidReplayHorizon(currentControllerEpoch, committedNextOffset,
                logRecordOffset, snapshotNextOffset);
            requireExclusiveReplaySource(committedNextOffset, logRecordOffset, snapshotNextOffset);
        }

        private static void requireValidPartitionIdentity(
            Uuid topicId, int partition, int leaderEpoch, int partitionEpoch
        ) {
            Objects.requireNonNull(topicId);
            if (Uuid.ZERO_UUID.equals(topicId) || partition < 0
                || leaderEpoch < 0 || partitionEpoch < 0) {
                throw new IllegalArgumentException("invalid replay partition identity");
            }
        }

        private static void requireValidReplayHorizon(
            long currentControllerEpoch, long committedNextOffset, long logRecordOffset, long snapshotNextOffset
        ) {
            if (currentControllerEpoch < 0 || committedNextOffset <= 0
                || logRecordOffset < -1 || snapshotNextOffset < -1) {
                throw new IllegalArgumentException("invalid KRaft replay horizon");
            }
        }

        private static void requireExclusiveReplaySource(
            long committedNextOffset, long logRecordOffset, long snapshotNextOffset
        ) {
            boolean logReplay = logRecordOffset >= 0;
            boolean snapshotReplay = snapshotNextOffset >= 0;
            if (logReplay == snapshotReplay) {
                throw new IllegalArgumentException("exactly one KRaft replay source is required");
            }
            if (logReplay && logRecordOffset >= committedNextOffset) {
                throw new IllegalArgumentException("uncommitted KRaft record offset");
            }
            if (snapshotReplay && (snapshotNextOffset == 0
                || snapshotNextOffset > committedNextOffset)) {
                throw new IllegalArgumentException("snapshot is beyond committed KRaft horizon");
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
            if (!matchesCurrentRegistration(receipt, current)
                || !seenIncarnations.add(receipt.incarnationId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks structural conditions for matching a candidate to current KRaft
     * replay provenance. Log replay must match the exact record offset; a
     * snapshot replay may only contain an earlier offset than its exclusive
     * snapshot boundary. The claimed offset must be committed in either case.
     *
     * This is a necessary precheck, not a KRaft writer, certificate verifier,
     * authoritative replay event or producer ACK permission. In particular,
     * the supplied context must be independently verified and authenticated.
     */
    static boolean hasKRaftReplayPreconditions(
        Coordinate coordinate,
        List<Receipt> receipts,
        Set<Integer> currentIsr,
        Map<Integer, CurrentRegistration> registrations,
        KRaftReplayContext replay,
        int requiredReplicas
    ) {
        if (replay == null || !hasCurrentRegisteredCompleteReceipts(
            coordinate, receipts, currentIsr, registrations, requiredReplicas)) {
            return false;
        }
        if (coordinate.controllerEpoch() != replay.currentControllerEpoch()
            || coordinate.kraftOffset() >= replay.committedNextOffset()) {
            return false;
        }
        if (!coordinate.topicId().equals(replay.topicId())
            || coordinate.partition() != replay.partition()
            || coordinate.leaderEpoch() != replay.leaderEpoch()
            || coordinate.partitionEpoch() != replay.partitionEpoch()) {
            return false;
        }
        return replay.logRecordOffset() >= 0
            ? coordinate.kraftOffset() == replay.logRecordOffset()
            : coordinate.kraftOffset() < replay.snapshotNextOffset();
    }

    private static boolean matchesCurrentRegistration(Receipt receipt, CurrentRegistration current) {
        if (current == null || current.fenced() || current.inControlledShutdown()) {
            return false;
        }
        return current.brokerEpoch() == receipt.brokerEpoch()
            && current.incarnationId().equals(receipt.incarnationId())
            && current.storageGeneration() == receipt.storageGeneration();
    }
}
