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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.shared.CurrentLocalWalCoveragePrecheck;
import org.apache.kafka.storage.internals.shared.LocalWalDurabilityObservation;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BrokerLocalStartupReplayPrecheckTest {
    private static final Uuid TOPIC_ID = new Uuid(1L, 2L);
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final TopicIdPartition KAFKA_PARTITION =
        new TopicIdPartition(TOPIC_ID, new TopicPartition("shared-topic", 0));

    @TempDir
    Path tempDir;

    @Test
    void recoveredRingWalWindowAndRealBrokerReadyIdentityCanBeClassifiedLocally() throws Exception {
        try (SharedStorageExtension extension = started("complete", true, true)) {
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.REPLAY_WINDOW_CLASSIFIED, result.status());
            assertEquals(StartupWalReplayCoveragePrecheck.Finding.RECOVERED_BATCH_WINDOW,
                result.finding().orElseThrow());
        }
    }

    @Test
    void missingBrokerReadyRegistrationCannotBorrowRecoveredWalBytes() throws Exception {
        try (SharedStorageExtension extension = started("unbound", true, false)) {
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.BROKER_IDENTITY_UNAVAILABLE, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void emptyRecoveredWalCannotClaimANonemptyKafkaSourceWindow() throws Exception {
        try (SharedStorageExtension extension = started("empty", false, true)) {
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.REPLAY_WINDOW_CLASSIFIED, result.status());
            assertEquals(StartupWalReplayCoveragePrecheck.Finding.NO_REPLAYED_DATA,
                result.finding().orElseThrow());
        }
    }

    @Test
    void appendAfterReplayCannotBecomePartOfRecoveredSourceWindow() throws Exception {
        try (SharedStorageExtension extension = started("later", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), source(0L, 20L, 20L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.REPLAY_WINDOW_CLASSIFIED, result.status());
            assertEquals(StartupWalReplayCoveragePrecheck.Finding.POST_START_WAL_BATCH,
                result.finding().orElseThrow());
        }
    }

    @Test
    void nativeKafkaSourceRebaseDuringIndexScanInvalidatesTheDiagnostic() throws Exception {
        try (SharedStorageExtension extension = started("rebase", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.captureNativeSourceWindow()).thenReturn(
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L)),
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 11L, 11L))
            );
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.NATIVE_SOURCE_CHANGED, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void brokerIncarnationAbaDuringNativeCaptureRevokesTheRecoveryPairing() throws Exception {
        try (SharedStorageExtension extension = started("incarnation-aba", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger samples = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (samples.getAndIncrement() == 0) {
                    extension.onBrokerReady(registered(55L, new Uuid(14L, 15L))).join();
                    extension.onBrokerReady(registered(54L, new Uuid(10L, 11L))).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.BROKER_IDENTITY_REVOKED, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void localLeaderDemotionDuringCaptureDiscardsTheRecoveredWindow() throws Exception {
        try (SharedStorageExtension extension = started("demoted", true, true)) {
            SharedPartitionRoleListener roles = leader(extension);
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                roles.onLeadershipChangeWithEpochs(Map.of(), Map.of(KAFKA_PARTITION, 9));
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            var result = BrokerLocalStartupReplayPrecheck.assess(extension, roles, log, PARTITION);
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.NATIVE_SOURCE_UNAVAILABLE, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void walAdmissionDuringNativeSourceReadInvalidatesRecoveryObservation() throws Exception {
        try (SharedStorageExtension extension = started("wal-race", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger samples = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (samples.getAndIncrement() == 0) {
                    extension.storage().appendData(
                        PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{3})
                    ).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.LOCAL_WAL_CHANGED, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void changedBackendCompletionSnapshotRejectsOtherwiseStableWalGenerations() throws Exception {
        try (SharedStorageExtension runtime = started("live-snapshot-drift", false, true)) {
            SharedStorageExtension extension = mock(SharedStorageExtension.class);
            SharedStorageEngine storage = mock(SharedStorageEngine.class);
            var identity = runtime.brokerStartupReplayObservation().orElseThrow();
            when(extension.brokerStartupReplayObservation()).thenReturn(Optional.of(identity));
            when(extension.storage()).thenReturn(storage);
            when(storage.startupWalReplayEvidence()).thenReturn(identity.replay());
            when(storage.walMutationRevision(PARTITION)).thenReturn(0L);
            when(storage.walAdmissionRevision(PARTITION)).thenReturn(6L);
            var changed = mock(LocalWalDurabilityObservation.Snapshot.class);
            when(storage.liveWalDurabilityObservation(PARTITION)).thenReturn(
                LocalWalDurabilityObservation.Snapshot.EMPTY, changed
            );

            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(runtime), source(0L, 0L, 0L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.LOCAL_WAL_CHANGED, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void kafkaTopicIncarnationMismatchCannotClaimReplayedPartition() throws Exception {
        try (SharedStorageExtension extension = started("topic-reuse", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.topicId()).thenReturn(Optional.of(new Uuid(3L, 4L)));
            var result = BrokerLocalStartupReplayPrecheck.assess(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.NATIVE_SOURCE_UNAVAILABLE, result.status());
            assertTrue(result.finding().isEmpty());
        }
    }

    @Test
    void startupOnlyAndCurrentDurableClassificationsStayDistinctForMixedWal() throws Exception {
        try (SharedStorageExtension extension = started("mixed-live-current", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            SharedPartitionRoleListener roles = leader(extension);
            SharedUnifiedLog log = source(0L, 20L, 20L);
            var startup = BrokerLocalStartupReplayPrecheck.assess(extension, roles, log, PARTITION);
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.REPLAY_WINDOW_CLASSIFIED, startup.status());
            assertEquals(StartupWalReplayCoveragePrecheck.Finding.POST_START_WAL_BATCH,
                startup.finding().orElseThrow());

            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(extension, roles, log, PARTITION);
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.CURRENT_WINDOW_CLASSIFIED, current.status());
            assertEquals(CurrentLocalWalCoveragePrecheck.Finding.INDEXED_BATCH_WINDOW,
                current.finding().orElseThrow());
        }
    }

    @Test
    void currentDurableCoverageCanContainOnlySuccessfulPostStartWrites() throws Exception {
        try (SharedStorageExtension extension = started("only-live-current", false, true)) {
            extension.storage().appendOwnedData(
                PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.CURRENT_WINDOW_CLASSIFIED, current.status());
            assertEquals(CurrentLocalWalCoveragePrecheck.Finding.INDEXED_BATCH_WINDOW,
                current.finding().orElseThrow());
            assertEquals(0L, extension.storage().startupWalReplayEvidence().replayedRecords());
        }
    }

    @Test
    void currentDurableFindingCannotBorrowUnwrittenKafkaOffsetGaps() throws Exception {
        try (SharedStorageExtension extension = started("gap-live-current", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 20L, 29L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(
                extension, leader(extension), source(0L, 30L, 30L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.CURRENT_WINDOW_CLASSIFIED, current.status());
            assertEquals(CurrentLocalWalCoveragePrecheck.Finding.KAFKA_BATCH_GAP_OR_OVERLAP,
                current.finding().orElseThrow());
        }
    }

    @Test
    void nativeSourceChangeRevokesCurrentDurableWindowBeforeReturning() throws Exception {
        try (SharedStorageExtension extension = started("current-native-change", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.captureNativeSourceWindow()).thenReturn(
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L)),
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 11L))
            );
            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.NATIVE_SOURCE_CHANGED, current.status());
            assertTrue(current.finding().isEmpty());
        }
    }

    @Test
    void incarnationAbaCannotAuthorizeCurrentLocalWalCoverage() throws Exception {
        try (SharedStorageExtension extension = started("current-aba", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger captures = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (captures.getAndIncrement() == 0) {
                    extension.onBrokerReady(registered(55L, new Uuid(15L, 16L))).join();
                    extension.onBrokerReady(registered(54L, new Uuid(10L, 11L))).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.BROKER_IDENTITY_REVOKED, current.status());
            assertTrue(current.finding().isEmpty());
        }
    }

    @Test
    void completedWalAppendDuringNativeCaptureInvalidatesCurrentFinding() throws Exception {
        try (SharedStorageExtension extension = started("current-wal-drift", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger captures = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (captures.getAndIncrement() == 0) {
                    extension.storage().appendData(
                        PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
                    ).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(
                extension, leader(extension), log, PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.LOCAL_WAL_CHANGED, current.status());
            assertTrue(current.finding().isEmpty());
        }
    }

    @Test
    void currentCoverageRequiresBrokerReadyIdentityEvenWhenWalIndexIsComplete() throws Exception {
        try (SharedStorageExtension extension = started("current-unbound", true, false)) {
            var current = BrokerLocalStartupReplayPrecheck.assessCurrent(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            );
            assertEquals(BrokerLocalStartupReplayPrecheck.Status.BROKER_IDENTITY_UNAVAILABLE, current.status());
            assertTrue(current.finding().isEmpty());
        }
    }

    @Test
    void stableReplayedWalAndNativeSourceYieldOnlyAProcessLocalClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-replay", true, true)) {
            var claim = BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            ).orElseThrow();
            assertEquals("cluster-a", claim.clusterId());
            assertEquals(TOPIC_ID, claim.topicId());
            assertEquals(0, claim.partitionId());
            assertEquals(3, claim.brokerId());
            assertEquals(54L, claim.brokerEpoch());
            assertEquals(new Uuid(10L, 11L), claim.brokerIncarnationId());
            assertEquals(8, claim.sourceLeaderEpoch());
            assertTrue(claim.localLeadershipGeneration() > 0L);
            assertEquals(0L, claim.nativeLogStartOffset());
            assertEquals(10L, claim.nativeHighWatermark());
            assertEquals(10L, claim.nativeLogEndOffset());
            assertEquals(extension.storage().walMutationRevision(PARTITION), claim.observedWalMutationRevision());
            assertEquals(extension.storage().walAdmissionRevision(PARTITION), claim.observedWalAdmissionRevision());
        }
    }

    @Test
    void mixedReplayAndDurableLiveWritesCanProduceCurrentLocalClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-mixed", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            var claim = BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), source(0L, 20L, 20L), PARTITION
            ).orElseThrow();
            assertEquals(0L, claim.nativeLogStartOffset());
            assertEquals(20L, claim.nativeHighWatermark());
        }
    }

    @Test
    void unregisteredBrokerCannotCreateAClaimFromRecoveredWal() throws Exception {
        try (SharedStorageExtension extension = started("claim-unbound", true, false)) {
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION
            ).isEmpty());
        }
    }

    @Test
    void missingKafkaRecordBatchCannotCreatePositiveClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-gap", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 20L, 29L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), source(0L, 30L, 30L), PARTITION
            ).isEmpty());
        }
    }

    @Test
    void emptySourceWindowCannotMasqueradeAsDurableDataClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-empty", false, true)) {
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), source(0L, 0L, 0L), PARTITION
            ).isEmpty());
        }
    }

    @Test
    void sourceLogEndChangeDuringCaptureRevokesTheClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-source-drift", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.captureNativeSourceWindow()).thenReturn(
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L)),
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 11L))
            );
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), log, PARTITION
            ).isEmpty());
        }
    }

    @Test
    void brokerIncarnationAbaDuringNativeCaptureRevokesTheClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-aba", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger samples = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (samples.getAndIncrement() == 0) {
                    extension.onBrokerReady(registered(55L, new Uuid(14L, 15L))).join();
                    extension.onBrokerReady(registered(54L, new Uuid(10L, 11L))).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), log, PARTITION
            ).isEmpty());
        }
    }

    @Test
    void completedWalAppendDuringNativeCaptureRevokesTheClaim() throws Exception {
        try (SharedStorageExtension extension = started("claim-wal-change", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger samples = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (samples.getAndIncrement() == 0) {
                    extension.storage().appendData(
                        PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
                    ).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalClaim(
                extension, leader(extension), log, PARTITION
            ).isEmpty());
        }
    }

    @Test
    void callerConstructedMalformedClaimIsRejectedWithoutTrustingItsFields() {
        assertThrows(IllegalArgumentException.class, () ->
            new BrokerLocalStartupReplayPrecheck.CurrentSourceWalClaim(
                "", TOPIC_ID, 0, 3, 54L, new Uuid(10L, 11L),
                8, 2L, 0L, 10L, 10L, 0L, 0L
            ));
        assertThrows(IllegalArgumentException.class, () ->
            new BrokerLocalStartupReplayPrecheck.CurrentSourceWalClaim(
                "cluster-a", TOPIC_ID, 0, 3, 54L, new Uuid(10L, 11L),
                8, 2L, 0L, 0L, 0L, 0L, 0L
            ));
        assertThrows(IllegalArgumentException.class, () ->
            new BrokerLocalStartupReplayPrecheck.CurrentSourceWalClaim(
                "cluster-a", Uuid.ZERO_UUID, 0, 3, 54L, new Uuid(10L, 11L),
                8, 2L, 0L, 10L, 10L, 0L, 0L
            ));
    }

    @Test
    void currentSourceReportEchoesOnlyTheExactControllerChallenge() throws Exception {
        try (SharedStorageExtension extension = started("probe-positive", true, true)) {
            var request = probe();
            var result = BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION, request
            ).orElseThrow();
            assertEquals(request, result.request());
            assertEquals(0L, result.nativeLogStartOffset());
            assertEquals(10L, result.nativeHighWatermark());
            assertEquals(10L, result.nativeLogEndOffset());
            assertTrue(result.localLeadershipGeneration() > 0L);
        }
    }

    @Test
    void challengeBoundNativeWalCaptureSerializesAsHeartbeatVThreeReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-wire-positive", true, true)) {
            var wire = BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalHeartbeatReport(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION, probe()
            ).orElseThrow();
            assertEquals(probe().nonce(), wire.nonce());
            assertEquals(TOPIC_ID, wire.topicId());
            assertEquals(0, wire.partitionId());
            assertEquals(8, wire.sourceLeaderEpoch());
            assertEquals(10L, wire.nativeHighWatermark());
            assertEquals(10L, wire.nativeLogEndOffset());
            BrokerHeartbeatRequestData heartbeat = new BrokerHeartbeatRequestData()
                .setBrokerId(3)
                .setBrokerEpoch(54L)
                .setCurrentMetadataOffset(100L)
                .setSourceWalProbeReport(wire);
            var parsed = new BrokerHeartbeatRequestData(
                MessageUtil.toByteBufferAccessor(heartbeat, (short) 3), (short) 3
            );
            assertEquals(wire, parsed.sourceWalProbeReport());
        }
    }

    @Test
    void staleBrokerIncarnationCannotEncodeHeartbeatSourceEvidence() throws Exception {
        try (SharedStorageExtension extension = started("probe-wire-stale", true, true)) {
            var stale = request(TOPIC_ID, 0, 3, 54L,
                new Uuid(9L, 11L), 8, "cluster-a", 0L);
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalHeartbeatReport(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION, stale
            ).isEmpty());
        }
    }

    @Test
    void liveDurableWalAndStartupReplayMayJointlyProduceProbeWindow() throws Exception {
        try (SharedStorageExtension extension = started("probe-mixed", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            var request = probe();
            var report = BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), source(0L, 20L, 20L), PARTITION, request
            ).orElseThrow();
            assertEquals(20L, report.nativeHighWatermark());
        }
    }

    @Test
    void probeWithAnotherTopicOrPartitionCannotReadLocalSource() throws Exception {
        try (SharedStorageExtension extension = started("probe-other-topic", true, true)) {
            var wrongTopic = request(new Uuid(7L, 8L), 0, 3, 54L,
                new Uuid(10L, 11L), 8, "cluster-a", 0L);
            var wrongPartition = request(TOPIC_ID, 1, 3, 54L,
                new Uuid(10L, 11L), 8, "cluster-a", 0L);
            var roles = leader(extension);
            var log = source(0L, 10L, 10L);
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongTopic).isEmpty());
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongPartition).isEmpty());
        }
    }

    @Test
    void staleBrokerEpochOrIncarnationCannotReuseReplayedWalEvidence() throws Exception {
        try (SharedStorageExtension extension = started("probe-old-broker", true, true)) {
            var wrongEpoch = request(TOPIC_ID, 0, 3, 53L,
                new Uuid(10L, 11L), 8, "cluster-a", 0L);
            var wrongIncarnation = request(TOPIC_ID, 0, 3, 54L,
                new Uuid(10L, 12L), 8, "cluster-a", 0L);
            var roles = leader(extension);
            var log = source(0L, 10L, 10L);
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongEpoch).isEmpty());
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongIncarnation).isEmpty());
        }
    }

    @Test
    void wrongBrokerIdOrClusterCannotBorrowSourceWalReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-foreign-broker", true, true)) {
            var wrongBroker = request(TOPIC_ID, 0, 4, 54L,
                new Uuid(10L, 11L), 8, "cluster-a", 0L);
            var wrongCluster = request(TOPIC_ID, 0, 3, 54L,
                new Uuid(10L, 11L), 8, "cluster-b", 0L);
            var roles = leader(extension);
            var log = source(0L, 10L, 10L);
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongBroker).isEmpty());
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongCluster).isEmpty());
        }
    }

    @Test
    void oldLeaderEpochOrIncorrectLogStartCannotBecomeAProbeReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-old-source", true, true)) {
            var wrongLeader = request(TOPIC_ID, 0, 3, 54L,
                new Uuid(10L, 11L), 7, "cluster-a", 0L);
            var wrongStart = request(TOPIC_ID, 0, 3, 54L,
                new Uuid(10L, 11L), 8, "cluster-a", 1L);
            var roles = leader(extension);
            var log = source(0L, 10L, 10L);
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongLeader).isEmpty());
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, roles, log, PARTITION, wrongStart).isEmpty());
        }
    }

    @Test
    void nativeKafkaSourceDriftDuringReadInvalidatesTheProbeReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-native-drift", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.captureNativeSourceWindow()).thenReturn(
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L)),
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 11L))
            );
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), log, PARTITION, probe()).isEmpty());
        }
    }

    @Test
    void incompleteWalBatchCoverageCannotBecomeAProbeReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-wal-gap", true, true)) {
            extension.storage().appendData(
                PARTITION, 8, 20L, 29L, ByteBuffer.wrap(new byte[]{2})
            ).join();
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), source(0L, 30L, 30L), PARTITION, probe()).isEmpty());
        }
    }

    @Test
    void unregisteredBrokerCannotManufactureChallengeBoundWalReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-no-registration", true, false)) {
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), source(0L, 10L, 10L), PARTITION, probe()).isEmpty());
        }
    }

    @Test
    void concurrentWalAppendDuringNativeCaptureInvalidatesProbeReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-wal-race", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger samples = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (samples.getAndIncrement() == 0) {
                    extension.storage().appendData(
                        PARTITION, 8, 10L, 19L, ByteBuffer.wrap(new byte[]{2})
                    ).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), log, PARTITION, probe()).isEmpty());
        }
    }

    @Test
    void brokerIncarnationAbaDuringNativeCaptureInvalidatesProbeReport() throws Exception {
        try (SharedStorageExtension extension = started("probe-incarnation-aba", true, true)) {
            SharedUnifiedLog log = source(0L, 10L, 10L);
            AtomicInteger samples = new AtomicInteger();
            when(log.captureNativeSourceWindow()).thenAnswer(invocation -> {
                if (samples.getAndIncrement() == 0) {
                    extension.onBrokerReady(registered(55L, new Uuid(15L, 16L))).join();
                    extension.onBrokerReady(registered(54L, new Uuid(10L, 11L))).join();
                }
                return Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L));
            });
            assertTrue(BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
                extension, leader(extension), log, PARTITION, probe()).isEmpty());
        }
    }

    @Test
    void reconstructedProbeAndReportMustUseValidBoundedCoordinates() {
        assertThrows(IllegalArgumentException.class, () ->
            request(Uuid.ZERO_UUID, 0, 3, 54L, new Uuid(10L, 11L), 8, "cluster-a", 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementSourceProbe.Request(
                Uuid.ZERO_UUID, "cluster-a", TOPIC_ID, 0, 3, 54L,
                new Uuid(10L, 11L), 8, -1L, 20L, 0L
            ));
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementSourceProbe.Report(probe(), 1L, 0L, 0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementSourceProbe.Report(probe(), 1L, 1L, 10L, 10L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementSourceProbe.Report(probe(), 1L, 0L, 10L, 10L, -1L, 0L));
    }

    @Test
    void extensionFacadeSamplesNativeKafkaSourceAndDurableWalOnly() throws Exception {
        try (SharedStorageExtension extension = started("adapter-positive", true, true)) {
            leader(extension);
            var observed = extension.captureSourceWalProbe(probe(), source(0L, 10L, 10L));
            assertEquals(probe(), observed.orElseThrow().request());
            assertEquals(10L, observed.orElseThrow().nativeHighWatermark());
            assertEquals(10L, observed.orElseThrow().nativeLogEndOffset());
        }
    }

    @Test
    void classicKafkaLogsAndMissingBrokerReadyIdentityFailClosed() throws Exception {
        try (SharedStorageExtension extension = started("adapter-classic", true, true)) {
            leader(extension);
            assertTrue(extension.captureSourceWalProbe(probe(), mock(UnifiedLog.class)).isEmpty());
            assertTrue(extension.captureSourceWalProbe(probe(), null).isEmpty());
        }
        try (SharedStorageExtension extension = started("adapter-unbound", true, false)) {
            leader(extension);
            assertTrue(extension.captureSourceWalProbe(probe(), source(0L, 10L, 10L)).isEmpty());
        }
    }

    @Test
    void extensionFacadeRejectsStaleBrokerOrTopicIncarnation() throws Exception {
        try (SharedStorageExtension extension = started("adapter-stale", true, true)) {
            leader(extension);
            var wrongBroker = request(TOPIC_ID, 0, 3, 55L,
                new Uuid(10L, 11L), 8, "cluster-a", 0L);
            var wrongTopic = request(new Uuid(31L, 32L), 0, 3, 54L,
                new Uuid(10L, 11L), 8, "cluster-a", 0L);
            var log = source(0L, 10L, 10L);
            assertTrue(extension.captureSourceWalProbe(wrongBroker, log).isEmpty());
            assertTrue(extension.captureSourceWalProbe(wrongTopic, log).isEmpty());
        }
    }

    @Test
    void asyncSamplerDefersNativeWalReadsUntilExecutorRuns() throws Exception {
        try (SharedStorageExtension extension = started("adapter-deferred", true, true)) {
            leader(extension);
            var tasks = new ArrayDeque<Runnable>();
            AtomicInteger lookups = new AtomicInteger();
            var sampler = extension.readOnlySourceWalSampler(challenge -> {
                lookups.incrementAndGet();
                return Optional.of(sourceUnchecked(0L, 10L, 10L));
            }, tasks::add);
            var future = sampler.apply(probe());
            assertFalse(future.isDone());
            assertEquals(0, lookups.get());
            assertEquals(1, tasks.size());
            tasks.removeFirst().run();
            assertEquals(1, lookups.get());
            assertEquals(probe(), future.join().orElseThrow().request());
        }
    }

    @Test
    void asyncSamplerNeverTurnsMissingLogOrClosedExtensionIntoEvidence() throws Exception {
        try (SharedStorageExtension extension = started("adapter-missing", true, true)) {
            leader(extension);
            var sampler = extension.readOnlySourceWalSampler(
                challenge -> Optional.empty(), Runnable::run
            );
            assertTrue(sampler.apply(probe()).join().isEmpty());
            var closedSampler = extension.readOnlySourceWalSampler(
                challenge -> Optional.of(sourceUnchecked(0L, 10L, 10L)), Runnable::run
            );
            extension.close();
            assertTrue(closedSampler.apply(probe()).join().isEmpty());
        }
    }

    @Test
    void asyncExecutorRejectionCannotFallBackToHeartbeatThread() throws Exception {
        try (SharedStorageExtension extension = started("adapter-reject", true, true)) {
            leader(extension);
            AtomicInteger lookups = new AtomicInteger();
            Executor rejected = command -> {
                throw new RejectedExecutionException("bounded worker is saturated");
            };
            var sampler = extension.readOnlySourceWalSampler(challenge -> {
                lookups.incrementAndGet();
                return Optional.of(sourceUnchecked(0L, 10L, 10L));
            }, rejected);
            assertThrows(RejectedExecutionException.class, () -> sampler.apply(probe()));
            assertEquals(0, lookups.get());
        }
    }

    @Test
    void asynchronousSamplerStillRejectsNativeKafkaSourceDrift() throws Exception {
        try (SharedStorageExtension extension = started("adapter-drift", true, true)) {
            leader(extension);
            SharedUnifiedLog log = source(0L, 10L, 10L);
            when(log.captureNativeSourceWindow()).thenReturn(
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 10L, 10L)),
                Optional.of(new SharedUnifiedLog.NativeSourceWindow(0L, 11L, 11L))
            );
            var sampler = extension.readOnlySourceWalSampler(
                challenge -> Optional.of(log), Runnable::run
            );
            assertTrue(sampler.apply(probe()).join().isEmpty());
        }
    }

    private static SharedUnifiedLog sourceUnchecked(long start, long hw, long leo) {
        try {
            return source(start, hw, leo);
        } catch (Exception failure) {
            throw new CompletionException(failure);
        }
    }

    private static PartitionRetirementSourceProbe.Request probe() {
        return request(TOPIC_ID, 0, 3, 54L, new Uuid(10L, 11L), 8, "cluster-a", 0L);
    }

    private static PartitionRetirementSourceProbe.Request request(
        Uuid topicId, int partitionId, int brokerId, long brokerEpoch,
        Uuid incarnation, int leaderEpoch, String clusterId, long logStart
    ) {
        return new PartitionRetirementSourceProbe.Request(
            new Uuid(21L, 22L), clusterId, topicId, partitionId, brokerId, brokerEpoch,
            incarnation, leaderEpoch, -1L, 20L, logStart
        );
    }

    private SharedStorageExtension started(String suffix, boolean recoveredData, boolean registered)
        throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("log-" + suffix));
        Path walDir = tempDir.resolve("wal-" + suffix);
        if (recoveredData) {
            try (SharedStorageExtension original = new SharedStorageExtension()) {
                original.start(context(logDir, walDir));
                original.storage().appendData(
                    PARTITION, 8, 0L, 9L, ByteBuffer.wrap(new byte[]{1})
                ).join();
            }
        }
        SharedStorageExtension extension = new SharedStorageExtension();
        extension.start(context(logDir, walDir));
        if (registered) {
            extension.onBrokerReady(registered(54L, new Uuid(10L, 11L))).join();
        }
        return extension;
    }

    private static SharedPartitionRoleListener leader(SharedStorageExtension extension) {
        SharedPartitionRoleListener roles =
            (SharedPartitionRoleListener) extension.partitionRoleListener();
        roles.onLeadershipChangeWithEpochs(Map.of(KAFKA_PARTITION, 8), Map.of());
        return roles;
    }

    private static SharedUnifiedLog source(long start, long hw, long leo) throws Exception {
        SharedUnifiedLog log = mock(SharedUnifiedLog.class);
        when(log.topicId()).thenReturn(Optional.of(TOPIC_ID));
        when(log.topicPartition()).thenReturn(new TopicPartition("shared-topic", 0));
        when(log.captureNativeSourceWindow()).thenReturn(
            Optional.of(new SharedUnifiedLog.NativeSourceWindow(start, hw, leo))
        );
        return log;
    }

    private static StorageExtensionBrokerContext registered(long epoch, Uuid incarnation) {
        return new StorageExtensionBrokerContext(
            "cluster-a", 3,
            List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9092)),
            Map.of(), Time.SYSTEM, epoch, incarnation
        );
    }

    private static StorageExtensionContext context(Path logDir, Path walDir) {
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString());
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, 1024L * 1024L);
        return new StorageExtensionContext(
            originals, List.of(logDir.toFile()), 3, Time.SYSTEM
        );
    }
}
