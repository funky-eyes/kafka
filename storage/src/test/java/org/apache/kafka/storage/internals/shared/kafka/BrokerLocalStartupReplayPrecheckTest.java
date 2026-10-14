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
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
