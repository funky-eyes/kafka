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
import org.apache.kafka.common.message.BrokerHeartbeatResponseData.SourceWalProbeChallenge;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.controller.PartitionRetirementCandidatePrecheck.Candidate;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * One-time, bounded, controller-volatile Source/WAL probe correlation.
 *
 * <p>Must be accessed on the controller event thread; no thread-safe promise
 * or persisted lease is made. A matched probe is NOT a valid source or WAL
 * certificate: the payload remains caller-reported and not remotely verified.
 * Controller leadership change loses this map and intentionally rejects old
 * responses. Global and per-Broker limits prevent a single authenticated
 * diagnostic source from monopolizing all outstanding challenges. There is
 * no KRaft emitter or remote GC capability.</p>
 */
final class PartitionRetirementSourceProbeRegistry {
    static final long MAX_AGE_NS = TimeUnit.SECONDS.toNanos(5);
    static final int MAX_PENDING = 128;
    static final int MAX_PENDING_PER_BROKER = 16;

    record Challenge(Uuid nonce, String clusterId, Candidate candidate) {
        Challenge {
            Objects.requireNonNull(nonce, "nonce");
            Objects.requireNonNull(clusterId, "clusterId");
            Objects.requireNonNull(candidate, "candidate");
            if (Uuid.ZERO_UUID.equals(nonce) || clusterId.isBlank()) {
                throw new IllegalArgumentException("Invalid source probe challenge identity");
            }
        }

        PartitionRetirementSourceProbe.Request toProtocolRequest() {
            return new PartitionRetirementSourceProbe.Request(
                nonce, clusterId, candidate.key().topicId(), candidate.key().partitionId(),
                candidate.brokerId(), candidate.brokerEpoch(), candidate.brokerIncarnationId(),
                candidate.sourceLeaderEpoch(), candidate.expectedPreviousAuthorityOffset(),
                candidate.nextAuthorityOffset(), candidate.claimedLogStartOffset()
            );
        }

        static Challenge fromProtocolRequest(PartitionRetirementSourceProbe.Request request) {
            Objects.requireNonNull(request, "request");
            Candidate candidate = new Candidate(
                new Key(request.topicId(), request.partitionId()),
                request.brokerId(), request.brokerEpoch(), request.brokerIncarnationId(),
                request.sourceLeaderEpoch(), request.expectedPreviousAuthorityOffset(),
                request.nextAuthorityOffset(), request.claimedLogStartOffset()
            );
            return new Challenge(request.nonce(), request.clusterId(), candidate);
        }

        /**
         * Encode this diagnostic Challenge using the unstable heartbeat v3
         * response field. Encoding is not issuance or broker authentication.
         */
        SourceWalProbeChallenge toHeartbeatChallenge() {
            return new SourceWalProbeChallenge()
                .setNonce(nonce)
                .setClusterId(clusterId)
                .setTopicId(candidate.key().topicId())
                .setPartitionId(candidate.key().partitionId())
                .setBrokerId(candidate.brokerId())
                .setBrokerEpoch(candidate.brokerEpoch())
                .setBrokerIncarnationId(candidate.brokerIncarnationId())
                .setSourceLeaderEpoch(candidate.sourceLeaderEpoch())
                .setExpectedPreviousAuthorityOffset(candidate.expectedPreviousAuthorityOffset())
                .setNextAuthorityOffset(candidate.nextAuthorityOffset())
                .setClaimedLogStartOffset(candidate.claimedLogStartOffset());
        }

        /**
         * Decode an untrusted heartbeat field. The returned Challenge still
         * requires a one-time nonce consume on the Controller event thread.
         */
        static Challenge fromHeartbeatChallenge(SourceWalProbeChallenge wire) {
            Objects.requireNonNull(wire, "wire");
            if (wire.nonce() == null || wire.clusterId() == null || wire.topicId() == null
                || wire.brokerIncarnationId() == null) {
                throw new IllegalArgumentException("Missing Source/WAL challenge identity");
            }
            PartitionRetirementSourceProbe.Request request = new PartitionRetirementSourceProbe.Request(
                wire.nonce(), wire.clusterId(), wire.topicId(), wire.partitionId(),
                wire.brokerId(), wire.brokerEpoch(), wire.brokerIncarnationId(),
                wire.sourceLeaderEpoch(), wire.expectedPreviousAuthorityOffset(),
                wire.nextAuthorityOffset(), wire.claimedLogStartOffset()
            );
            return fromProtocolRequest(request);
        }

    }

    enum Finding {
        UNKNOWN_OR_ALREADY_USED,
        EXPIRED,
        ENVELOPE_MISMATCH,
        PRINCIPAL_MISMATCH,
        VERSION_NOT_NEGOTIATED,
        CANDIDATE_CHANGED,
        MATCHED_UNTRUSTED
    }

    private record Pending(Challenge challenge, KafkaPrincipal principal, long issuedAtNs) {
    }

    private final String clusterId;
    // Insertion order makes v3 heartbeat offers deterministic when several
    // one-shot probes are pending for the same Broker. Never expose a
    // non-deterministically selected nonce from the Controller map.
    private final Map<Uuid, Pending> outstanding = new LinkedHashMap<>();

    PartitionRetirementSourceProbeRegistry(String clusterId) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        if (clusterId.isBlank()) {
            throw new IllegalArgumentException("A source probe must bind to a real cluster");
        }
    }

    Optional<Challenge> issue(
        Candidate candidate,
        KafkaPrincipal authenticatedPrincipal,
        PartitionRetirementBrokerPrincipalPolicy principalPolicy,
        long nowNs
    ) {
        Objects.requireNonNull(candidate, "candidate");
        Objects.requireNonNull(principalPolicy, "principalPolicy");
        // Both an unbound broker and a reused shared broker principal fail closed.
        if (!principalPolicy.matches(authenticatedPrincipal, candidate.brokerId())) {
            return Optional.empty();
        }
        outstanding.entrySet().removeIf(entry ->
            !fresh(entry.getValue().issuedAtNs(), nowNs)
                || supersededBrokerRegistration(entry.getValue(), candidate));
        // A successful current-epoch issuance fences a prior BrokerEpoch or
        // incarnation immediately. Otherwise old unused diagnostics can hold
        // all per-Broker slots until their TTL after a fast re-registration.
        // The global limit is not a fairness boundary by itself: one
        // authenticated Broker must not exhaust diagnostic capacity for
        // every other Broker. Count live challenges only, after pruning.
        long issuedToBroker = outstanding.values().stream()
            .filter(pending -> pending.challenge().candidate().brokerId() == candidate.brokerId())
            .count();
        if (issuedToBroker >= MAX_PENDING_PER_BROKER || outstanding.size() >= MAX_PENDING) {
            return Optional.empty();
        }
        Uuid nonce;
        do {
            nonce = Uuid.randomUuid();
        } while (Uuid.ZERO_UUID.equals(nonce) || outstanding.containsKey(nonce));
        Challenge challenge = new Challenge(nonce, clusterId, candidate);
        outstanding.put(nonce, new Pending(challenge, authenticatedPrincipal, nowNs));
        return Optional.of(challenge);
    }

    /**
     * Read-only view of an already issued, unexpired challenge for one
     * authenticated Broker heartbeat. Called only on the Controller event
     * thread. It never mints a nonce, extends its expiry, or consumes it.
     *
     * <p>The live predicate is evaluated only for matching Broker principals
     * and epochs; it must recheck the current BrokerRegistration, partition
     * leader and KRaft CAS on the same serialized Controller event. A stale
     * oldest nonce must not starve a newer, still-current challenge. Neither
     * skipped challenges nor selected challenges are consumed here.</p>
     */
    Optional<Challenge> pendingForHeartbeat(
        int brokerId,
        long brokerEpoch,
        KafkaPrincipal authenticatedPrincipal,
        long nowNs,
        Predicate<Candidate> stillCurrent
    ) {
        Objects.requireNonNull(stillCurrent, "stillCurrent");
        if (brokerId < 0 || brokerEpoch < 0L || authenticatedPrincipal == null) {
            return Optional.empty();
        }
        // LinkedHashMap preserves the earliest-issued, still-live nonce
        // until it is consumed. Heartbeat retries re-offer the same challenge.
        return outstanding.values().stream()
            .filter(pending -> pending.principal().equals(authenticatedPrincipal))
            .filter(pending -> fresh(pending.issuedAtNs(), nowNs))
            .map(Pending::challenge)
            .filter(challenge -> challenge.candidate().brokerId() == brokerId)
            .filter(challenge -> challenge.candidate().brokerEpoch() == brokerEpoch)
            .filter(challenge -> stillCurrent.test(challenge.candidate()))
            .findFirst();
    }

    /**
     * Consume a Controller-issued nonce only after checking the actual
     * authenticated request channel and its uniquely bound Broker identity.
     *
     * <p>A foreign authenticated Broker cannot burn another Broker's nonce:
     * the pending owner is checked before the destructive removal. Once the
     * owner and transport match, even malformed/replayed reports burn their
     * nonce to preserve the one-shot replay fence.</p>
     */
    Finding consumeAuthenticated(
        Challenge reported,
        Candidate currentCandidate,
        ControllerRequestContext context,
        PartitionRetirementBrokerPrincipalPolicy principalPolicy,
        long nowNs
    ) {
        Objects.requireNonNull(currentCandidate, "currentCandidate");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(principalPolicy, "principalPolicy");
        if (!context.isDirectAuthenticatedBrokerChannel()
            || !principalPolicy.matches(context.principal(), currentCandidate.brokerId())) {
            return Finding.PRINCIPAL_MISMATCH;
        }
        // Do not rely solely on the network ingress caller to check the
        // unstable protocol: the destructive nonce API must fail closed
        // even when invoked by a future internal Controller call site.
        if (!PartitionRetirementHeartbeatReportGate.isVersionThreeHeartbeat(context)) {
            return Finding.VERSION_NOT_NEGOTIATED;
        }
        if (reported != null) {
            Pending owner = outstanding.get(reported.nonce());
            if (owner != null && !owner.principal().equals(context.principal())) {
                return Finding.PRINCIPAL_MISMATCH;
            }
        }
        return consume(reported, currentCandidate, context.principal(), nowNs);
    }

    /**
     * Low-level, single-use nonce state transition. Call the authenticated
     * entrypoint from Controller runtime; package-local tests can also use
     * this primitive for deterministic expiry and envelope regressions.
     */
    Finding consume(
        Challenge reported,
        Candidate currentCandidate,
        KafkaPrincipal authenticatedPrincipal,
        long nowNs
    ) {
        Objects.requireNonNull(currentCandidate, "currentCandidate");
        if (reported == null) {
            return Finding.UNKNOWN_OR_ALREADY_USED;
        }
        // Remove before any comparison: even a forged or expired response burns
        // the nonce and can never be retried as a usable certificate.
        Pending pending = outstanding.remove(reported.nonce());
        if (pending == null) {
            return Finding.UNKNOWN_OR_ALREADY_USED;
        }
        if (!fresh(pending.issuedAtNs(), nowNs)) {
            return Finding.EXPIRED;
        }
        if (!pending.challenge().equals(reported)) {
            return Finding.ENVELOPE_MISMATCH;
        }
        if (!pending.principal().equals(authenticatedPrincipal)) {
            return Finding.PRINCIPAL_MISMATCH;
        }
        if (!pending.challenge().candidate().equals(currentCandidate)) {
            return Finding.CANDIDATE_CHANGED;
        }
        return Finding.MATCHED_UNTRUSTED;
    }

    private static boolean supersededBrokerRegistration(Pending pending, Candidate candidate) {
        Candidate issuedFor = pending.challenge().candidate();
        if (issuedFor.brokerId() != candidate.brokerId()) {
            return false;
        }
        // Different topics/partitions and source CAS windows under the SAME
        // registration remain independently valid.
        return issuedFor.brokerEpoch() != candidate.brokerEpoch()
            || !issuedFor.brokerIncarnationId().equals(candidate.brokerIncarnationId());
    }


    /**
     * Revoke every outstanding source/WAL diagnostic for a Broker whose
     * KRaft registration state changed. Called only on the serialized
     * Controller event thread, AFTER successful metadata record replay.
     *
     * <p>Do not wait for another issuance or for five-second expiry: a
     * registered incarnation can be replaced, fenced, unfenced or removed
     * without ever requesting another diagnostic. Other Brokers' pending
     * challenges, insertion order and quotas must remain unchanged.</p>
     */
    void invalidateBroker(int brokerId) {
        if (brokerId < 0) {
            throw new IllegalArgumentException("Invalid Broker ID for diagnostic revocation");
        }
        outstanding.entrySet().removeIf(
            entry -> entry.getValue().challenge().candidate().brokerId() == brokerId
        );
    }

    /** Revoke every volatile probe when the controller leadership epoch changes. */
    void invalidateAll() {
        outstanding.clear();
    }

    private static boolean fresh(long issuedAtNs, long nowNs) {
        // nanoTime has an arbitrary signed origin; subtraction, rather than
        // now >= issuedAt, is the correct comparison for bounded durations.
        long elapsedNs = nowNs - issuedAtNs;
        return elapsedNs >= 0L && elapsedNs < MAX_AGE_NS;
    }
}
