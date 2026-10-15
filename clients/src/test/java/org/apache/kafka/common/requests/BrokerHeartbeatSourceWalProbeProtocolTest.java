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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiMessageType.ListenerType;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.SourceWalProbeReport;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData.SourceWalProbeChallenge;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The optional v3 source/WAL fields are diagnostic only. Production listeners
 * must continue to advertise v0-v2; no v3 payload authorizes KRaft or GC.
 */
public class BrokerHeartbeatSourceWalProbeProtocolTest {
    @Test
    public void productionControllerHeartbeatRemainsAtVersionTwo() {
        assertEquals(2, ApiKeys.BROKER_HEARTBEAT.latestVersion(false));
        assertEquals(3, ApiKeys.BROKER_HEARTBEAT.latestVersion(true));
        assertFalse(ApiKeys.BROKER_HEARTBEAT.isVersionEnabled((short) 3, false));
        assertTrue(ApiKeys.BROKER_HEARTBEAT.isVersionEnabled((short) 3, true));
        assertEquals(
            2, ApiKeys.BROKER_HEARTBEAT.toApiVersionForApiResponse(false, ListenerType.CONTROLLER)
                .orElseThrow().maxVersion()
        );
        assertEquals(
            3, ApiKeys.BROKER_HEARTBEAT.toApiVersionForApiResponse(true, ListenerType.CONTROLLER)
                .orElseThrow().maxVersion()
        );
    }

    @Test
    public void defaultBuilderRejectsUnstableReportBeforeNetworkDispatch() {
        assertEquals(2, new BrokerHeartbeatRequest.Builder(baseRequest()).latestAllowedVersion());
        assertThrows(IllegalArgumentException.class, () ->
            new BrokerHeartbeatRequest.Builder(baseRequest().setSourceWalProbeReport(report())));
    }

    @Test
    public void explicitDiagnosticBuilderPinsExactVersionThree() {
        var data = baseRequest().setSourceWalProbeReport(report());
        var builder = new BrokerHeartbeatRequest.Builder(data, (short) 3);
        assertEquals(3, builder.oldestAllowedVersion());
        assertEquals(3, builder.latestAllowedVersion());
        assertEquals(3, builder.build().version());
        assertEquals(data, builder.build().data());
        assertThrows(IllegalArgumentException.class,
            () -> new BrokerHeartbeatRequest.Builder(data, (short) 2));
        assertThrows(IllegalArgumentException.class,
            () -> new BrokerHeartbeatRequest.Builder(baseRequest(), (short) 3));
    }

    @Test
    public void legacyRequestVersionsNeverCarrySourceWalReport() {
        for (short version = 0; version <= 2; version++) {
            BrokerHeartbeatRequestData request = baseRequest();
            BrokerHeartbeatRequestData parsed = new BrokerHeartbeatRequestData(
                MessageUtil.toByteBufferAccessor(request, version), version
            );
            assertEquals(request.brokerId(), parsed.brokerId());
            assertEquals(request.brokerEpoch(), parsed.brokerEpoch());
            assertNull(parsed.sourceWalProbeReport());
        }
    }

    @Test
    public void unstableReportRoundTripsWithExactBrokerSourceCoordinates() {
        short version = 3;
        BrokerHeartbeatRequestData data = baseRequest().setSourceWalProbeReport(report());
        BrokerHeartbeatRequestData parsed = new BrokerHeartbeatRequestData(
            MessageUtil.toByteBufferAccessor(data, version), version
        );
        assertEquals(data.sourceWalProbeReport(), parsed.sourceWalProbeReport());
        assertEquals(new Uuid(3L, 4L), parsed.sourceWalProbeReport().topicId());
        assertEquals(10L, parsed.sourceWalProbeReport().nativeHighWatermark());
        assertEquals(12L, parsed.sourceWalProbeReport().walMutationRevision());
    }

    @Test
    public void unstableControllerChallengeRoundTripsAndLegacyReplyStaysEmpty() {
        short version = 3;
        BrokerHeartbeatResponseData response = new BrokerHeartbeatResponseData()
            .setSourceWalProbeChallenge(challenge());
        BrokerHeartbeatResponseData parsed = new BrokerHeartbeatResponseData(
            MessageUtil.toByteBufferAccessor(response, version), version
        );
        assertEquals(response.sourceWalProbeChallenge(), parsed.sourceWalProbeChallenge());
        assertEquals(54L, parsed.sourceWalProbeChallenge().brokerEpoch());
        for (short legacy = 0; legacy <= 2; legacy++) {
            BrokerHeartbeatResponseData old = new BrokerHeartbeatResponseData(
                MessageUtil.toByteBufferAccessor(new BrokerHeartbeatResponseData(), legacy), legacy
            );
            assertNull(old.sourceWalProbeChallenge());
        }
    }

    @Test
    public void legacyRequestCannotSilentlyDowngradeNonemptyReport() {
        BrokerHeartbeatRequestData request = baseRequest().setSourceWalProbeReport(report());
        assertThrows(UnsupportedVersionException.class, () ->
            MessageUtil.toByteBufferAccessor(request, (short) 2));
    }

    @Test
    public void legacyResponseCannotSilentlyDowngradeNonemptyChallenge() {
        BrokerHeartbeatResponseData response = new BrokerHeartbeatResponseData()
            .setSourceWalProbeChallenge(challenge());
        assertThrows(UnsupportedVersionException.class, () ->
            MessageUtil.toByteBufferAccessor(response, (short) 2));
    }

    private static BrokerHeartbeatRequestData baseRequest() {
        return new BrokerHeartbeatRequestData()
            .setBrokerId(3)
            .setBrokerEpoch(54L)
            .setCurrentMetadataOffset(100L)
            .setWantFence(false)
            .setWantShutDown(false);
    }

    private static SourceWalProbeReport report() {
        return new SourceWalProbeReport()
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
            .setWalAdmissionRevision(13L);
    }

    private static SourceWalProbeChallenge challenge() {
        return new SourceWalProbeChallenge()
            .setNonce(new Uuid(1L, 2L))
            .setClusterId("cluster-a")
            .setTopicId(new Uuid(3L, 4L))
            .setPartitionId(0)
            .setBrokerId(3)
            .setBrokerEpoch(54L)
            .setBrokerIncarnationId(new Uuid(5L, 6L))
            .setSourceLeaderEpoch(8)
            .setExpectedPreviousAuthorityOffset(-1L)
            .setNextAuthorityOffset(100L)
            .setClaimedLogStartOffset(0L);
    }
}
