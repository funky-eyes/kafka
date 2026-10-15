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
package org.apache.kafka.server;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe.Report;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe.Request;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.BrokerLifecycleManager.SourceWalProbeCaptureState;
import org.apache.kafka.server.BrokerLifecycleManager.SourceWalProbeCaptureState.Capture;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Strictly local asynchronous Source/WAL diagnostic staging. No test enables
 * sending a SourceWalProbeReport or writing any KRaft retirement authority.
 */
class BrokerHeartbeatSourceProbeCaptureStateTest {
    private static final Uuid INCARNATION = new Uuid(5L, 6L);

    @Test
    void successfulCaptureIsAvailableExactlyOnce() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertNotNull(capture);
        Report observed = report(request);
        assertTrue(complete(state, capture, Optional.of(observed), null, 101L));
        assertEquals(Optional.of(observed), take(state, 102L));
        assertTrue(take(state, 103L).isEmpty());
    }

    @Test
    void duplicateNonceCannotResampleOrOverwritePriorEvidence() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture started = state.begin(request, 100L);
        assertNotNull(started);
        assertNull(state.begin(request, 101L));
        assertNull(state.begin(new Request(
            request.nonce(), "cluster-b", request.topicId(), request.partitionId(),
            request.brokerId(), request.brokerEpoch(), request.brokerIncarnationId(),
            request.sourceLeaderEpoch(), request.expectedPreviousAuthorityOffset(),
            request.nextAuthorityOffset(), request.claimedLogStartOffset()
        ), 102L));
        assertTrue(complete(state, started, Optional.of(report(request)), null, 103L));
        assertTrue(take(state, 104L).isPresent());
    }

    @Test
    void interleavedChallengesCannotReplayAnEarlierNonce() {
        var state = new SourceWalProbeCaptureState();
        Request first = challenge(1L);
        Capture firstCapture = state.begin(first, 100L);
        assertFalse(complete(state, firstCapture, Optional.empty(), null, 101L));
        Capture secondCapture = state.begin(challenge(2L), 102L);
        assertNotNull(secondCapture);
        assertFalse(complete(state, secondCapture, Optional.empty(), null, 103L));
        // A one-element "last nonce" fence would incorrectly accept this.
        assertNull(state.begin(first, 104L));
    }

    @Test
    void saturatedNonceHistoryRejectsNewChallengesWithoutEvictingLiveFences() {
        var state = new SourceWalProbeCaptureState();
        for (int i = 1; i <= SourceWalProbeCaptureState.MAX_RECENT_NONCES; i++) {
            Capture capture = state.begin(challenge(i), 100L + i);
            assertNotNull(capture);
            state.abandon(capture);
        }
        assertNull(state.begin(challenge(65L), 200L));
        // After the oldest five-second TTL elapses a slot can be reused.
        assertNotNull(state.begin(challenge(65L), SourceWalProbeCaptureState.MAX_AGE_NS + 101L));
    }

    @Test
    void revokedChallengesRetainReplayFencesAcrossProviderReplacement() {
        var state = new SourceWalProbeCaptureState();
        Request first = challenge(1L);
        Capture old = state.begin(first, 100L);
        state.clear();
        assertFalse(complete(state, old, Optional.empty(), null, 101L));
        Capture next = state.begin(challenge(2L), 102L);
        assertNotNull(next);
        state.abandon(next);
        assertNull(state.begin(first, 103L));
    }

    @Test
    void nonceHistoryHandlesMonotonicClockWrapWithoutPrematureEviction() {
        var state = new SourceWalProbeCaptureState();
        long start = Long.MAX_VALUE - 1L;
        Request request = challenge(1L);
        Capture initial = state.begin(request, start);
        state.abandon(initial);
        assertNull(state.begin(request, Long.MIN_VALUE + 2L));
        assertNotNull(state.begin(challenge(2L), Long.MIN_VALUE + 3L));
    }

    @Test
    void laterNonceFencesOutOfOrderCompletionFromOlderCapture() {
        var state = new SourceWalProbeCaptureState();
        Request first = challenge(1L);
        Request second = challenge(2L);
        Capture old = state.begin(first, 100L);
        // A generation fence cancels the old pending slot without waiting for
        // a delayed provider, which may still complete after a new capture.
        state.clear();
        assertNull(state.begin(second, 101L));
        assertFalse(complete(state, old, Optional.of(report(first)), null, 102L));
        Capture replacement = state.begin(second, 103L);
        assertNotNull(replacement);
        assertTrue(take(state, 103L).isEmpty());
        assertTrue(complete(state, replacement, Optional.of(report(second)), null, 104L));
        assertEquals(second, take(state, 105L).orElseThrow().request());
    }

    @Test
    void singleFlightPreventsAnUnboundedQueueOfNativeCaptureTasks() {
        var state = new SourceWalProbeCaptureState();
        Request first = challenge(1L);
        Capture capture = state.begin(first, 100L);
        assertNotNull(capture);
        // Expiration alone cannot authorize a second concurrent capture:
        // the old producer Future may still be executing.
        assertNull(state.begin(challenge(2L), 101L));
        assertNull(state.begin(challenge(3L), SourceWalProbeCaptureState.MAX_AGE_NS + 100L));
        assertFalse(complete(state, capture, Optional.empty(), null, 102L));
        assertNotNull(state.begin(challenge(2L), 103L));
    }

    @Test
    void providerReconfigurationRevokesPendingAndStagedCaptures() {
        var state = new SourceWalProbeCaptureState();
        Request oldRequest = challenge(1L);
        Capture old = state.begin(oldRequest, 100L);
        state.clear();
        assertNull(state.begin(challenge(2L), 101L));
        assertFalse(complete(state, old, Optional.of(report(oldRequest)), null, 101L));
        assertNull(state.begin(oldRequest, 102L));
        Request fresh = challenge(2L);
        Capture capture = state.begin(fresh, 103L);
        assertTrue(complete(state, capture, Optional.of(report(fresh)), null, 104L));
        state.clear();
        assertTrue(take(state, 105L).isEmpty());
    }

    @Test
    void synchronousProviderFailureReleasesIOLockButNotNonceReplayFence() {
        var state = new SourceWalProbeCaptureState();
        Capture abandoned = state.begin(challenge(1L), 100L);
        state.abandon(abandoned);
        assertNull(state.begin(challenge(1L), 101L));
        Capture fresh = state.begin(challenge(2L), 102L);
        assertNotNull(fresh);
        assertFalse(complete(state, abandoned, Optional.of(report(challenge(1L))), null, 103L));
        assertTrue(complete(state, fresh, Optional.of(report(challenge(2L))), null, 104L));
    }

    @Test
    void captureForWrongBrokerOrChangedEpochCannotBeStaged() {
        var wrongBroker = new SourceWalProbeCaptureState();
        Capture capture = wrongBroker.begin(challenge(1L), 100L);
        assertFalse(wrongBroker.complete(capture, Optional.of(report(challenge(1L))),
            null, 101L, 4, 54L, INCARNATION, BrokerState.RUNNING, true));
        var newEpoch = new SourceWalProbeCaptureState();
        Capture next = newEpoch.begin(challenge(2L), 100L);
        assertFalse(newEpoch.complete(next, Optional.of(report(challenge(2L))),
            null, 101L, 3, 55L, INCARNATION, BrokerState.RUNNING, true));
    }

    @Test
    void brokerIncarnationChangeInvalidatesAsyncCompletion() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertFalse(state.complete(capture, Optional.of(report(request)), null,
            101L, 3, 54L, new Uuid(5L, 7L), BrokerState.RUNNING, true));
        assertTrue(take(state, 102L).isEmpty());
    }

    @Test
    void fencingOrUnregistrationInvalidatesAsyncCompletion() {
        var fenced = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture first = fenced.begin(request, 100L);
        assertFalse(fenced.complete(first, Optional.of(report(request)), null,
            101L, 3, 54L, INCARNATION, BrokerState.RECOVERY, true));
        var unregistered = new SourceWalProbeCaptureState();
        Capture second = unregistered.begin(challenge(2L), 100L);
        assertFalse(unregistered.complete(second, Optional.of(report(challenge(2L))), null,
            101L, 3, 54L, INCARNATION, BrokerState.RUNNING, false));
    }

    @Test
    void emptyNullOrFailedProviderDoesNotProduceReport() {
        var empty = new SourceWalProbeCaptureState();
        assertFalse(complete(empty, empty.begin(challenge(1L), 100L), Optional.empty(), null, 101L));
        var missing = new SourceWalProbeCaptureState();
        assertFalse(complete(missing, missing.begin(challenge(2L), 100L), null, null, 101L));
        var failed = new SourceWalProbeCaptureState();
        assertFalse(complete(failed, failed.begin(challenge(3L), 100L),
            Optional.of(report(challenge(3L))), new IllegalStateException("unavailable"), 101L));
        assertTrue(take(failed, 102L).isEmpty());
    }

    @Test
    void reportForOtherNonceOrSourceCannotBeSubstituted() {
        var state = new SourceWalProbeCaptureState();
        Capture capture = state.begin(challenge(1L), 100L);
        assertFalse(complete(state, capture, Optional.of(report(challenge(2L))), null, 101L));
        assertTrue(take(state, 102L).isEmpty());
    }

    @Test
    void expiryAtExactMaxAgeInvalidatesCompletionAndStagedResult() {
        long expiry = 100L + SourceWalProbeCaptureState.MAX_AGE_NS;
        var expired = new SourceWalProbeCaptureState();
        Capture first = expired.begin(challenge(1L), 100L);
        assertFalse(complete(expired, first, Optional.of(report(challenge(1L))), null, expiry));
        var alreadyReady = new SourceWalProbeCaptureState();
        Capture second = alreadyReady.begin(challenge(2L), 100L);
        assertTrue(complete(alreadyReady, second, Optional.of(report(challenge(2L))), null, 101L));
        assertTrue(take(alreadyReady, expiry).isEmpty());
    }

    @Test
    void monotonicTimerCanCrossSignedLongBoundary() {
        var state = new SourceWalProbeCaptureState();
        long start = Long.MAX_VALUE - 1L;
        Request request = challenge(1L);
        Capture capture = state.begin(request, start);
        assertTrue(complete(state, capture, Optional.of(report(request)), null, Long.MIN_VALUE + 3L));
        assertTrue(take(state, Long.MIN_VALUE + 4L).isPresent());
    }

    @Test
    void backwardClockObservationCannotMakeOldCaptureFresh() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertFalse(complete(state, capture, Optional.of(report(request)), null, 99L));
    }

    @Test
    void stagedReportMapsToExactUnstableHeartbeatWireFields() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertTrue(complete(state, capture, Optional.of(report(request)), null, 101L));
        var wire = takeWire(state, 102L).orElseThrow();
        assertEquals(request.nonce(), wire.nonce());
        assertEquals(request.clusterId(), wire.clusterId());
        assertEquals(request.topicId(), wire.topicId());
        assertEquals(request.partitionId(), wire.partitionId());
        assertEquals(request.brokerIncarnationId(), wire.brokerIncarnationId());
        assertEquals(request.sourceLeaderEpoch(), wire.sourceLeaderEpoch());
        assertEquals(request.expectedPreviousAuthorityOffset(), wire.expectedPreviousAuthorityOffset());
        assertEquals(request.nextAuthorityOffset(), wire.nextAuthorityOffset());
        assertEquals(2L, wire.localLeadershipGeneration());
        assertEquals(0L, wire.nativeLogStartOffset());
        assertEquals(10L, wire.nativeHighWatermark());
        assertEquals(10L, wire.nativeLogEndOffset());
        assertEquals(12L, wire.walMutationRevision());
        assertEquals(13L, wire.walAdmissionRevision());
    }

    @Test
    void diagnosticWireReportSurvivesVThreeEncodeDecodeWithoutAuthorizingGc() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertTrue(complete(state, capture, Optional.of(report(request)), null, 101L));
        var heartbeat = new BrokerHeartbeatRequestData()
            .setBrokerId(request.brokerId())
            .setBrokerEpoch(request.brokerEpoch())
            .setSourceWalProbeReport(takeWire(state, 102L).orElseThrow());
        var parsed = new BrokerHeartbeatRequestData(
            MessageUtil.toByteBufferAccessor(heartbeat, (short) 3), (short) 3
        );
        assertEquals(heartbeat, parsed);
        assertEquals(request.nonce(), parsed.sourceWalProbeReport().nonce());
        assertEquals(request.brokerId(), parsed.brokerId());
        assertEquals(request.brokerEpoch(), parsed.brokerEpoch());
        assertTrue(takeWire(state, 103L).isEmpty());
    }

    @Test
    void localWireReportNeverSilentlyDowngradesToStableVTwo() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertTrue(complete(state, capture, Optional.of(report(request)), null, 101L));
        var heartbeat = new BrokerHeartbeatRequestData()
            .setBrokerId(request.brokerId())
            .setBrokerEpoch(request.brokerEpoch())
            .setSourceWalProbeReport(takeWire(state, 102L).orElseThrow());
        assertThrows(UnsupportedVersionException.class,
            () -> MessageUtil.toByteBufferAccessor(heartbeat, (short) 2));
    }

    @Test
    void wireAndTypedViewsShareTheSameOneShotResult() {
        var state = new SourceWalProbeCaptureState();
        Request first = challenge(1L);
        Capture capture = state.begin(first, 100L);
        assertTrue(complete(state, capture, Optional.of(report(first)), null, 101L));
        assertTrue(takeWire(state, 102L).isPresent());
        assertTrue(take(state, 103L).isEmpty());

        Request second = challenge(2L);
        Capture next = state.begin(second, 104L);
        assertTrue(complete(state, next, Optional.of(report(second)), null, 105L));
        assertTrue(take(state, 106L).isPresent());
        assertTrue(takeWire(state, 107L).isEmpty());
    }

    @Test
    void wireReportIsSuppressedAfterEpochChangeOrExpiry() {
        var changedEpoch = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture first = changedEpoch.begin(request, 100L);
        assertTrue(complete(changedEpoch, first, Optional.of(report(request)), null, 101L));
        assertTrue(changedEpoch.takeWireOnce(
            102L, 3, 55L, INCARNATION, BrokerState.RUNNING, true
        ).isEmpty());
        var expired = new SourceWalProbeCaptureState();
        Capture next = expired.begin(request, 100L);
        assertTrue(complete(expired, next, Optional.of(report(request)), null, 101L));
        assertTrue(takeWire(expired, 100L + SourceWalProbeCaptureState.MAX_AGE_NS).isEmpty());
    }

    @Test
    void revokingBrokerRegistrationDiscardsWireReportBeforeEncoding() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertTrue(complete(state, capture, Optional.of(report(request)), null, 101L));
        state.clear();
        assertTrue(takeWire(state, 102L).isEmpty());
        assertNull(state.begin(request, 103L));
    }

    @Test
    void reportDispatchRequiresIndependentlyVerifiedBrokerChannelIdentity() {
        var state = readyDispatchReport();
        assertTrue(dispatch(state, regularHeartbeat(), false, (short) 3, 102L,
            BrokerState.RUNNING, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3, 102L,
            BrokerState.RUNNING, true).isPresent());
        assertTrue(takeWire(state, 103L).isEmpty());
    }

    @Test
    void versionTwoControllerCannotDrainStagedWalReport() {
        var state = readyDispatchReport();
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 2, 102L,
            BrokerState.RUNNING, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3, 103L,
            BrokerState.RUNNING, true).isPresent());
    }

    @Test
    void missingControllerApiNegotiationCannotDrainStagedReport() {
        var state = readyDispatchReport();
        assertTrue(BrokerHeartbeatSourceWalReportDispatch.prepareOnce(
            state, regularHeartbeat(), Optional.empty(), true,
            102L, INCARNATION, BrokerState.RUNNING, true
        ).isEmpty());
        assertTrue(takeWire(state, 103L).isPresent());
    }

    @Test
    void explicitVThreeDispatchPreservesOrdinaryHeartbeatAndIsSingleUse() {
        var state = readyDispatchReport();
        var base = regularHeartbeat().setCurrentMetadataOffset(42L);
        var builder = dispatch(state, base, true, (short) 3, 102L,
            BrokerState.RUNNING, true).orElseThrow();
        assertEquals(3, builder.oldestAllowedVersion());
        assertEquals(3, builder.latestAllowedVersion());
        assertEquals(3, builder.build().version());
        var diagnostic = builder.build().data();
        assertEquals(42L, diagnostic.currentMetadataOffset());
        assertEquals(challenge(1L).nonce(), diagnostic.sourceWalProbeReport().nonce());
        assertEquals(54L, diagnostic.brokerEpoch());
        assertNull(base.sourceWalProbeReport());
        assertTrue(dispatch(state, base, true, (short) 3, 103L,
            BrokerState.RUNNING, true).isEmpty());
    }

    @Test
    void fencingShutdownOrOfflineLogDirCannotReportWalDurability() {
        var state = readyDispatchReport();
        assertTrue(dispatch(state, regularHeartbeat().setWantFence(true),
            true, (short) 3, 102L, BrokerState.RUNNING, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat().setWantShutDown(true),
            true, (short) 3, 102L, BrokerState.RUNNING, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat().setOfflineLogDirs(
            java.util.List.of(new Uuid(15L, 16L))
        ), true, (short) 3, 102L, BrokerState.RUNNING, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3, 103L,
            BrokerState.RUNNING, true).isPresent());
    }

    @Test
    void replacedBrokerEpochCannotSendOrReuseStagedSourceReport() {
        var state = readyDispatchReport();
        var changedEpoch = regularHeartbeat().setBrokerEpoch(55L);
        assertTrue(dispatch(state, changedEpoch, true, (short) 3, 102L,
            BrokerState.RUNNING, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3, 103L,
            BrokerState.RUNNING, true).isEmpty());
    }

    @Test
    void unregisteredOrExpiredCaptureCannotCreateAReportHeartbeat() {
        var state = readyDispatchReport();
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3, 102L,
            BrokerState.RUNNING, false).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3, 102L,
            BrokerState.RECOVERY, true).isEmpty());
        assertTrue(dispatch(state, regularHeartbeat(), true, (short) 3,
            100L + SourceWalProbeCaptureState.MAX_AGE_NS,
            BrokerState.RUNNING, true).isEmpty());
    }

    private static SourceWalProbeCaptureState readyDispatchReport() {
        var state = new SourceWalProbeCaptureState();
        Request request = challenge(1L);
        Capture capture = state.begin(request, 100L);
        assertTrue(complete(state, capture, Optional.of(report(request)), null, 101L));
        return state;
    }

    private static BrokerHeartbeatRequestData regularHeartbeat() {
        return new BrokerHeartbeatRequestData()
            .setBrokerId(3).setBrokerEpoch(54L).setCurrentMetadataOffset(100L);
    }

    private static Optional<BrokerHeartbeatRequest.Builder> dispatch(
        SourceWalProbeCaptureState state,
        BrokerHeartbeatRequestData heartbeat,
        boolean uniqueIdentityVerified,
        short controllerMaxVersion,
        long nowNs,
        BrokerState brokerState,
        boolean registered
    ) {
        return BrokerHeartbeatSourceWalReportDispatch.prepareOnce(
            state, heartbeat,
            Optional.of(NodeApiVersions.create(
                ApiKeys.BROKER_HEARTBEAT.id, (short) 0, controllerMaxVersion
            )),
            uniqueIdentityVerified, nowNs, INCARNATION, brokerState, registered
        );
    }

    private static Optional<BrokerHeartbeatRequestData.SourceWalProbeReport> takeWire(
        SourceWalProbeCaptureState state, long nowNs
    ) {
        return state.takeWireOnce(nowNs, 3, 54L, INCARNATION, BrokerState.RUNNING, true);
    }

    private static boolean complete(
        SourceWalProbeCaptureState state,
        Capture capture,
        Optional<Report> report,
        Throwable error,
        long nowNs
    ) {
        return state.complete(capture, report, error, nowNs,
            3, 54L, INCARNATION, BrokerState.RUNNING, true);
    }

    private static Optional<Report> take(SourceWalProbeCaptureState state, long nowNs) {
        return state.takeOnce(nowNs, 3, 54L, INCARNATION, BrokerState.RUNNING, true);
    }

    private static Request challenge(long nonceLo) {
        return new PartitionRetirementSourceProbe.Request(
            new Uuid(1L, nonceLo), "cluster-a", new Uuid(3L, 4L), 0,
            3, 54L, INCARNATION, 8, -1L, 100L, 0L
        );
    }

    private static Report report(Request request) {
        return new PartitionRetirementSourceProbe.Report(
            request, 2L, 0L, 10L, 10L, 12L, 13L
        );
    }
}
