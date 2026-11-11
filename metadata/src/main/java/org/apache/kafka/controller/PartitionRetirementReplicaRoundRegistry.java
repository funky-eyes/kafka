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
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Finding;
import org.apache.kafka.controller.PartitionRetirementReplicaQuiescencePrecheck.Report;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Bounded, one-use, controller-volatile correlation of assigned-replica drain
 * diagnostics for a physical object and one KRaft partition generation.
 *
 * <p>Raw report collection remains a package-local diagnostic only.
 * An authenticated-ingress preflight additionally checks the actual
 * Controller request context, unique Broker principal mapping, live Broker
 * registration, and current KRaft partition topology. Neither path can
 * certify remote object durability or grant COMMITTED S3 deletion.
 * No KRaft records, network protocol, or S3 operations are exposed here.</p>
 *
 * <p>Call from the Controller event queue. Replace or clear the registry on
 * leadership loss; it does not survive restart, KRaft replay or snapshot load.
 * An eventual production protocol must bind requests and replies to verified
 * broker channel principals, enforce incarnation/epoch ownership, certify
 * every packed range, and recheck authority in a serialized write event.</p>
 */
final class PartitionRetirementReplicaRoundRegistry {
    static final long MAX_AGE_NS = TimeUnit.SECONDS.toNanos(5);
    static final int MAX_PENDING = 128;
    static final int MAX_PENDING_PER_SOURCE = 16;
    static final int MAX_ASSIGNED_REPLICAS = 256;

    record Challenge(Uuid roundId, Candidate candidate, long objectId, Set<Integer> assignedReplicas) {
        Challenge {
            Objects.requireNonNull(roundId, "roundId");
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(assignedReplicas, "assignedReplicas");
            if (Uuid.ZERO_UUID.equals(roundId) || objectId <= 0L || assignedReplicas.isEmpty()) {
                throw new IllegalArgumentException("A challenge requires a round, object, and assigned replicas");
            }
            assignedReplicas = Set.copyOf(assignedReplicas);
        }
    }

    private record Pending(Challenge challenge, long issuedAtNs, Map<Integer, Report> reports) {
    }

    private final Map<Uuid, Pending> pending = new LinkedHashMap<>();

    /**
     * Capture the Controller's complete assigned-replica set, never only ISR.
     * This method does not authenticate a Broker, claim a KRaft offset, or
     * issue any network challenge. Retrying the same object revokes older rounds.
     */
    Optional<Challenge> issue(
        ReplicationControlManager replicationControl,
        Candidate candidate,
        long objectId,
        long nowNs
    ) {
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(candidate, "candidate");
        if (objectId <= 0L) {
            throw new IllegalArgumentException("Physical Object ID must be positive");
        }
        evictExpired(nowNs);
        PartitionRegistration partition = replicationControl.getPartition(
            candidate.key().topicId(), candidate.key().partitionId());
        if (!matchesCurrentPartition(partition, candidate)) {
            return Optional.empty();
        }
        Set<Integer> assigned = assignedReplicaIds(partition);
        if (assigned.isEmpty()) {
            return Optional.empty();
        }
        // Re-issuing an object cannot collect receipts from an earlier round.
        pending.entrySet().removeIf(entry -> samePhysicalObject(entry.getValue(), candidate.key(), objectId));
        if (!hasCapacity(candidate.brokerId())) {
            return Optional.empty();
        }
        Uuid nonce;
        do {
            nonce = Uuid.randomUuid();
        } while (Uuid.ZERO_UUID.equals(nonce) || pending.containsKey(nonce));
        Challenge challenge = new Challenge(nonce, candidate, objectId, assigned);
        pending.put(challenge.roundId(), new Pending(challenge, nowNs, new HashMap<>()));
        return Optional.of(challenge);
    }

    /**
     * Store only a complete, round-bound value; duplicate or foreign replies
     * are rejected without changing the accepted receipt. This is NOT proof
     * that the purported broker sent the report.
     */
    boolean recordReport(Uuid roundId, Report report, long nowNs) {
        Objects.requireNonNull(roundId, "roundId");
        Objects.requireNonNull(report, "report");
        Pending state = pending.get(roundId);
        if (state == null) {
            return false;
        }
        if (!fresh(state.issuedAtNs(), nowNs)) {
            pending.remove(roundId);
            return false;
        }
        Challenge challenge = state.challenge();
        if (!challenge.assignedReplicas().contains(report.brokerId())
            || !matchesChallenge(challenge, report)) {
            return false;
        }
        return state.reports().putIfAbsent(report.brokerId(), report) == null;
    }

    /**
     * Non-emitting preflight for an eventual direct-Broker report transport.
     *
     * <p>The context must come from the real Controller network request, not
     * a constructed principal/header; exact BrokerHeartbeat v3 is required.
     * Only a unique, trusted Broker principal can submit the reply for its
     * current registered epoch and incarnation. No such report is currently
     * carried over the production heartbeat protocol. This method does NOT
     * issue an RPC, authenticate S3 bytes, or confer GC authority.</p>
     *
     * <p>A rejected, foreign or forwarded request cannot consume another
     * Broker's receipt slot. Consumption later rechecks KRaft registration.</p>
     */
    boolean recordAuthenticated(
        Uuid roundId,
        Report report,
        ControllerRequestContext context,
        PartitionRetirementBrokerPrincipalPolicy principalPolicy,
        ReplicationControlManager replicationControl,
        ClusterControlManager clusterControl,
        long nowNs
    ) {
        Objects.requireNonNull(roundId, "roundId");
        Objects.requireNonNull(report, "report");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(principalPolicy, "principalPolicy");
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(clusterControl, "clusterControl");
        Pending state = pending.get(roundId);
        if (state == null) {
            return false;
        }
        if (!fresh(state.issuedAtNs(), nowNs)) {
            pending.remove(roundId);
            return false;
        }
        if (!trustedIngress(context, principalPolicy, report.brokerId())) {
            return false;
        }
        Challenge challenge = state.challenge();
        if (!challenge.assignedReplicas().contains(report.brokerId())
            || !matchesChallenge(challenge, report)) {
            return false;
        }
        PartitionRegistration partition = replicationControl.getPartition(
            challenge.candidate().key().topicId(), challenge.candidate().key().partitionId());
        if (!matchesCurrentPartition(partition, challenge.candidate())
            || !challenge.assignedReplicas().equals(assignedReplicaIds(partition))) {
            return false;
        }
        BrokerRegistration broker = clusterControl.registration(report.brokerId());
        if (!matchesRegisteredBroker(broker, report, challenge.candidate())) {
            return false;
        }
        return state.reports().putIfAbsent(report.brokerId(), report) == null;
    }

    private static boolean trustedIngress(
        ControllerRequestContext context,
        PartitionRetirementBrokerPrincipalPolicy principalPolicy,
        int brokerId
    ) {
        return context.isDirectAuthenticatedBrokerChannel()
            && PartitionRetirementHeartbeatReportGate.isVersionThreeHeartbeat(context)
            && principalPolicy.matches(context.principal(), brokerId);
    }

    private static boolean matchesRegisteredBroker(
        BrokerRegistration broker,
        Report report,
        Candidate candidate
    ) {
        if (broker == null || broker.id() != report.brokerId() || broker.fenced()
            || broker.inControlledShutdown() || broker.incarnationId() == null) {
            return false;
        }
        if (broker.epoch() != report.brokerEpoch()
            || !broker.incarnationId().equals(report.brokerIncarnationId())) {
            return false;
        }
        return report.brokerId() != candidate.brokerId()
            || (broker.epoch() == candidate.brokerEpoch()
                && broker.incarnationId().equals(candidate.brokerIncarnationId()));
    }

    /**
     * Consume the round even when incomplete, stale, or internally inconsistent.
     * Only a matching collection returns MATCHED_UNTRUSTED, never authorization.
     * Missing, consumed, or expired rounds return Optional.empty().
     */
    Optional<Finding> consume(
        Uuid roundId,
        ReplicationControlManager replicationControl,
        ClusterControlManager clusterControl,
        long nowNs
    ) {
        Objects.requireNonNull(roundId, "roundId");
        Objects.requireNonNull(replicationControl, "replicationControl");
        Objects.requireNonNull(clusterControl, "clusterControl");
        Pending state = pending.remove(roundId);
        if (state == null || !fresh(state.issuedAtNs(), nowNs)) {
            return Optional.empty();
        }
        Challenge challenge = state.challenge();
        if (state.reports().size() != challenge.assignedReplicas().size()) {
            return Optional.of(Finding.MISSING_OR_FOREIGN_REPLICA_REPORT);
        }
        PartitionRegistration current = replicationControl.getPartition(
            challenge.candidate().key().topicId(), challenge.candidate().key().partitionId());
        if (!matchesCurrentPartition(current, challenge.candidate())
            || !challenge.assignedReplicas().equals(assignedReplicaIds(current))) {
            return Optional.of(Finding.KRAFT_PARTITION_CHANGED);
        }
        Collection<Report> evidence = state.reports().values();
        return Optional.of(PartitionRetirementReplicaQuiescencePrecheck.assess(
            replicationControl, clusterControl, challenge.candidate(),
            challenge.objectId(), challenge.roundId(), evidence));
    }

    /**
     * Revoke every unfinished round that includes the changed Broker,
     * including out-of-ISR assigned replicas. Do not limit this to the
     * original source leader. Invoke only AFTER successful KRaft replay on
     * the serialized Controller event queue.
     */
    void invalidateBroker(int brokerId) {
        if (brokerId < 0) {
            throw new IllegalArgumentException("Broker ID must be nonnegative");
        }
        pending.entrySet().removeIf(entry ->
            entry.getValue().challenge().assignedReplicas().contains(brokerId));
    }

    /** Topic IDs, rather than human-readable topic names, fence delete/recreate ABA. */
    void invalidateTopic(Uuid topicId) {
        Objects.requireNonNull(topicId, "topicId");
        pending.entrySet().removeIf(entry ->
            entry.getValue().challenge().candidate().key().topicId().equals(topicId));
    }

    /**
     * Revoke only the exact Topic ID / partition after a metadata mutation
     * or an authority CAS advance. Other partitions remain independent.
     */
    void invalidatePartition(Uuid topicId, int partitionId) {
        Objects.requireNonNull(topicId, "topicId");
        if (partitionId < 0) {
            throw new IllegalArgumentException("Partition ID must be nonnegative");
        }
        pending.entrySet().removeIf(entry -> {
            Key key = entry.getValue().challenge().candidate().key();
            return key.topicId().equals(topicId) && key.partitionId() == partitionId;
        });
    }

    /**
     * Only the uniquely authenticated original source may consume a round
     * through the Controller event queue. Foreign/anonymous requests cannot
     * consume or burn the owner's pending observations.
     */
    Optional<Finding> consumeAuthenticated(
        Uuid roundId,
        ControllerRequestContext context,
        PartitionRetirementBrokerPrincipalPolicy policy,
        ReplicationControlManager replicationControl,
        ClusterControlManager clusterControl,
        long nowNs
    ) {
        Objects.requireNonNull(roundId, "roundId");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(policy, "policy");
        Pending state = pending.get(roundId);
        if (state == null
            || !trustedIngress(context, policy, state.challenge().candidate().brokerId())) {
            return Optional.empty();
        }
        return consume(roundId, replicationControl, clusterControl, nowNs);
    }

    /** A leadership transition or feature negotiation invalidates all volatile evidence. */
    void invalidateAll() {
        pending.clear();
    }

    int pendingCount() {
        return pending.size();
    }

    private void evictExpired(long nowNs) {
        pending.entrySet().removeIf(entry -> !fresh(entry.getValue().issuedAtNs(), nowNs));
    }

    private boolean hasCapacity(int brokerId) {
        if (pending.size() >= MAX_PENDING) {
            return false;
        }
        long sourceRounds = pending.values().stream()
            .filter(value -> value.challenge().candidate().brokerId() == brokerId)
            .count();
        return sourceRounds < MAX_PENDING_PER_SOURCE;
    }

    private static Set<Integer> assignedReplicaIds(PartitionRegistration partition) {
        Set<Integer> assigned = new HashSet<>();
        if (partition == null || partition.replicas.length == 0
            || partition.replicas.length > MAX_ASSIGNED_REPLICAS) {
            return Set.of();
        }
        for (int brokerId : partition.replicas) {
            if (brokerId < 0 || !assigned.add(brokerId)) {
                return Set.of();
            }
        }
        return Set.copyOf(assigned);
    }

    private static boolean matchesCurrentPartition(PartitionRegistration partition, Candidate candidate) {
        return PartitionRetirementControllerPrecheck.assessRegistration(
            partition, candidate.brokerId(), candidate.sourceLeaderEpoch(),
            candidate.sourcePartitionEpoch()) == PartitionRetirementControllerPrecheck.Finding.CONTROLLER_IMAGE_MATCH;
    }

    private static boolean samePhysicalObject(Pending state, Key key, long objectId) {
        return state.challenge().objectId() == objectId
            && state.challenge().candidate().key().equals(key);
    }

    private static boolean matchesChallenge(Challenge challenge, Report report) {
        Candidate candidate = challenge.candidate();
        if (!challenge.roundId().equals(report.fenceRoundId())
            || challenge.objectId() != report.objectId() || !candidate.key().equals(report.key())) {
            return false;
        }
        if (candidate.sourceLeaderEpoch() != report.sourceLeaderEpoch()
            || candidate.sourcePartitionEpoch() != report.sourcePartitionEpoch()) {
            return false;
        }
        return candidate.expectedPreviousAuthorityOffset() == report.expectedPreviousAuthorityOffset()
            && candidate.nextAuthorityOffset() == report.nextAuthorityOffset();
    }

    private static boolean fresh(long issuedAtNs, long nowNs) {
        // System.nanoTime has an arbitrary signed origin. Subtracting before
        // comparison handles crossing Long.MAX_VALUE to Long.MIN_VALUE.
        long elapsedNs = nowNs - issuedAtNs;
        return elapsedNs >= 0L && elapsedNs < MAX_AGE_NS;
    }
}
