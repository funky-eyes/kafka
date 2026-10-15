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
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.SourceWalProbeReport;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.controller.PartitionRetirementHeartbeatReportGate.Finding;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementHeartbeatReportGateTest {
    private static final KafkaPrincipal BROKER_THREE =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unique-broker-three");
    private static final KafkaPrincipal BROKER_FOUR =
        new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "unique-broker-four");
    private static final PartitionRetirementBrokerPrincipalPolicy BOUND =
        new PartitionRetirementBrokerPrincipalPolicy(Map.of(3, BROKER_THREE));

    @Test
    void absentReportPreservesLegacyHeartbeatWithoutBrokerPrincipal() {
        assertEquals(Finding.ABSENT, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 2, KafkaPrincipal.ANONYMOUS),
            new BrokerHeartbeatRequestData().setBrokerId(3).setBrokerEpoch(54L),
            PartitionRetirementBrokerPrincipalPolicy.DENY_ALL
        ));
    }

    @Test
    void productionDenyAllRejectsEvenUniquelyNamedBroker() {
        assertEquals(Finding.PRINCIPAL_UNBOUND, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 3, BROKER_THREE), heartbeat(),
            PartitionRetirementBrokerPrincipalPolicy.DENY_ALL
        ));
    }

    @Test
    void anonymousAndForeignBrokerPrincipalsCannotReport() {
        assertEquals(Finding.PRINCIPAL_UNBOUND, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 3, KafkaPrincipal.ANONYMOUS), heartbeat(), BOUND
        ));
        assertEquals(Finding.PRINCIPAL_UNBOUND, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 3, BROKER_FOUR), heartbeat(), BOUND
        ));
    }

    @Test
    void vTwoListenerCannotAcceptVThreeTaggedReport() {
        assertEquals(Finding.VERSION_NOT_NEGOTIATED, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 2, BROKER_THREE), heartbeat(), BOUND
        ));
    }

    @Test
    void unknownFutureProtocolCannotImplicitlyAuthorizeVThreeDiagnostic() {
        assertEquals(Finding.VERSION_NOT_NEGOTIATED, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 4, BROKER_THREE), heartbeat(), BOUND
        ));
    }

    @Test
    void matchingPrincipalStillCannotAuthorizeAnyGcAction() {
        assertEquals(Finding.UNVERIFIED, PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 3, BROKER_THREE), heartbeat(), BOUND
        ));
    }

    @Test
    void boundBrokerFencingShutdownOrOfflineDirsCannotOfferWalEvidence() {
        assertEquals(Finding.BROKER_NOT_ELIGIBLE,
            inspectBound(heartbeat().setWantFence(true)));
        assertEquals(Finding.BROKER_NOT_ELIGIBLE,
            inspectBound(heartbeat().setWantShutDown(true)));
        assertEquals(Finding.BROKER_NOT_ELIGIBLE,
            inspectBound(heartbeat().setOfflineLogDirs(java.util.List.of(new Uuid(15L, 16L)))));
        assertEquals(Finding.UNVERIFIED, inspectBound(heartbeat()));
    }

    @Test
    void decodingPreservesBrokerEpochNonceAndExactSourceWindow() {
        var decoded = PartitionRetirementHeartbeatReportGate.fromWire(heartbeat()).orElseThrow();
        assertEquals(new Uuid(1L, 2L), decoded.request().nonce());
        assertEquals(new Uuid(3L, 4L), decoded.request().topicId());
        assertEquals(new Uuid(5L, 6L), decoded.request().brokerIncarnationId());
        assertEquals(3, decoded.request().brokerId());
        assertEquals(54L, decoded.request().brokerEpoch());
        assertEquals(8, decoded.request().sourceLeaderEpoch());
        assertEquals(-1L, decoded.request().expectedPreviousAuthorityOffset());
        assertEquals(100L, decoded.request().nextAuthorityOffset());
        assertEquals(0L, decoded.nativeLogStartOffset());
        assertEquals(10L, decoded.nativeHighWatermark());
        assertEquals(10L, decoded.nativeLogEndOffset());
        assertEquals(12L, decoded.observedWalMutationRevision());
        assertEquals(13L, decoded.observedWalAdmissionRevision());
    }

    @Test
    void oldHeartbeatProducesNoReportObject() {
        assertTrue(PartitionRetirementHeartbeatReportGate.fromWire(
            new BrokerHeartbeatRequestData().setBrokerId(3).setBrokerEpoch(54L)
        ).isEmpty());
    }

    @Test
    void missingNonceAndWrongIncarnationAreMalformed() {
        var missingNonce = heartbeat();
        missingNonce.sourceWalProbeReport().setNonce(Uuid.ZERO_UUID);
        assertEquals(Finding.MALFORMED, inspectBound(missingNonce));
        var wrongIncarnation = heartbeat();
        wrongIncarnation.sourceWalProbeReport().setBrokerIncarnationId(Uuid.ZERO_UUID);
        assertEquals(Finding.MALFORMED, inspectBound(wrongIncarnation));
    }

    @Test
    void nullWireIdentityIsRejectedWithoutCatchingNullPointerException() {
        var topicMissing = heartbeat();
        topicMissing.sourceWalProbeReport().setTopicId(null);
        assertEquals(Finding.MALFORMED, inspectBound(topicMissing));
        var clusterMissing = heartbeat();
        clusterMissing.sourceWalProbeReport().setClusterId(null);
        assertEquals(Finding.MALFORMED, inspectBound(clusterMissing));
        var incarnationMissing = heartbeat();
        incarnationMissing.sourceWalProbeReport().setBrokerIncarnationId(null);
        assertEquals(Finding.MALFORMED, inspectBound(incarnationMissing));
        var nonceMissing = heartbeat();
        nonceMissing.sourceWalProbeReport().setNonce(null);
        assertEquals(Finding.MALFORMED, inspectBound(nonceMissing));
    }

    @Test
    void missingOrEmptyNativeSourceWindowIsMalformed() {
        var missing = heartbeat();
        missing.sourceWalProbeReport().setNativeHighWatermark(0L);
        assertEquals(Finding.MALFORMED, inspectBound(missing));
        var inverted = heartbeat();
        inverted.sourceWalProbeReport().setNativeLogEndOffset(9L);
        assertEquals(Finding.MALFORMED, inspectBound(inverted));
    }

    @Test
    void negativeLocalWalGenerationAndRevisionAreMalformed() {
        var generation = heartbeat();
        generation.sourceWalProbeReport().setLocalLeadershipGeneration(0L);
        assertEquals(Finding.MALFORMED, inspectBound(generation));
        var mutation = heartbeat();
        mutation.sourceWalProbeReport().setWalMutationRevision(-1L);
        assertEquals(Finding.MALFORMED, inspectBound(mutation));
    }

    @Test
    void changedOuterBrokerEpochCannotBeAcceptedAsOriginalProof() {
        var changed = heartbeat().setBrokerEpoch(55L);
        var decoded = PartitionRetirementHeartbeatReportGate.fromWire(changed).orElseThrow();
        assertEquals(55L, decoded.request().brokerEpoch());
        assertFalse(decoded.request().brokerEpoch() == 54L);
        assertEquals(Finding.UNVERIFIED, inspectBound(changed));
    }

    private static Finding inspectBound(BrokerHeartbeatRequestData heartbeat) {
        return PartitionRetirementHeartbeatReportGate.inspect(
            context((short) 3, BROKER_THREE), heartbeat, BOUND
        );
    }

    private static ControllerRequestContext context(short apiVersion, KafkaPrincipal principal) {
        return new ControllerRequestContext(
            new RequestHeaderData().setRequestApiKey((short) 63).setRequestApiVersion(apiVersion),
            principal, OptionalLong.empty()
        );
    }

    private static BrokerHeartbeatRequestData heartbeat() {
        return new BrokerHeartbeatRequestData()
            .setBrokerId(3)
            .setBrokerEpoch(54L)
            .setCurrentMetadataOffset(100L)
            .setSourceWalProbeReport(new SourceWalProbeReport()
                .setNonce(new Uuid(1L, 2L))
                .setClusterId("cluster-a")
                .setTopicId(new Uuid(3L, 4L))
                .setPartitionId(0)
                .setBrokerIncarnationId(new Uuid(5L, 6L))
                .setSourceLeaderEpoch(8)
                .setExpectedPreviousAuthorityOffset(-1L)
                .setNextAuthorityOffset(100L)
                .setLocalLeadershipGeneration(2L)
                .setNativeLogStartOffset(0L)
                .setNativeHighWatermark(10L)
                .setNativeLogEndOffset(10L)
                .setWalMutationRevision(12L)
                .setWalAdmissionRevision(13L));
    }
}
