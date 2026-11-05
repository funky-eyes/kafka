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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.BrokerLifecycleManager.SourceWalProbeCaptureState;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

/**
 * Dormant, non-emitting Source/WAL report preparation for heartbeat v3.
 *
 * <p>Only an active Controller transport that independently attests the
 * exact BrokerRegistration and returns the matching authenticated connection
 * versions can satisfy the send gate. Kafka's CLUSTER_ACTION permission,
 * an API version lookup, or a claimed BrokerId is never sufficient. The
 * generic Controller channel currently returns no such attestation; this
 * adapter is not invoked by ordinary BrokerLifecycleManager heartbeats.</p>
 *
 * <p>A locally prepared report is still untrusted by the Controller and
 * cannot authorize KRaft writes or COMMITTED object deletion.</p>
 */
final class BrokerHeartbeatSourceWalReportDispatch {
    private static final short REPORT_VERSION = 3;
    private static final Logger LOG = LoggerFactory.getLogger(BrokerHeartbeatSourceWalReportDispatch.class);

    /**
     * Dormant, package-private transport adapter; ordinary Broker heartbeats
     * never call this path. A future production caller must demonstrate a
     * unique per-Broker authenticated channel from the real TLS/SASL socket.
     * Unlike the preparation-only helper, this transport adapter CANNOT
     * accept a caller-supplied Boolean for that prerequisite.
     *
     * <p>The report is consumed locally before enqueue. A failed enqueue
     * cannot reuse the same report. Controller errors/timeouts are handled
     * by a dedicated callback which never changes Broker lifecycle state.
     * The underlying channel may internally retry a queued RPC; Controller
     * replay protection therefore remains mandatory.</p>
     */
    static boolean sendDiagnosticOnce(
        NodeToControllerChannelManager channel,
        SourceWalProbeCaptureState captures,
        BrokerHeartbeatRequestData ordinaryHeartbeat,
        long nowNs,
        Uuid brokerIncarnationId,
        BrokerState state,
        boolean registered
    ) {
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(ordinaryHeartbeat, "ordinaryHeartbeat");
        // This ONE connection-bound query must attest identity AND versions.
        // Never combine generic controllerApiVersions() with a claimed BrokerId
        // or a caller-controlled "verified" flag across an active-controller
        // failover. The current network manager defaults to Optional.empty().
        Optional<NodeApiVersions> authenticatedVersions = Objects.requireNonNull(
            channel.authenticatedBrokerDiagnosticVersions(
                ordinaryHeartbeat.brokerId(), ordinaryHeartbeat.brokerEpoch(), brokerIncarnationId
            ),
            "authenticatedBrokerDiagnosticVersions"
        );
        Optional<BrokerHeartbeatRequest.Builder> prepared = prepareOnce(
            captures, ordinaryHeartbeat, authenticatedVersions,
            authenticatedVersions.isPresent(), nowNs, brokerIncarnationId,
            state, registered
        );
        if (prepared.isEmpty()) {
            return false;
        }
        // Never send through the ordinary BrokerHeartbeatResponseHandler.
        channel.sendRequest(prepared.get(), new DiagnosticResponseHandler());
        return true;
    }

    /** No diagnostic completion can change broker liveness or grant GC. */
    private static final class DiagnosticResponseHandler implements ControllerRequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse response) {
            if (response == null || response.responseBody() == null) {
                LOG.debug("Controller Source/WAL diagnostic yielded no response.");
            } else {
                LOG.debug("Controller Source/WAL diagnostic response remains non-authorizing.");
            }
        }

        @Override
        public void onTimeout() {
            LOG.debug("Controller Source/WAL diagnostic timed out without affecting normal heartbeats.");
        }
    }

    private BrokerHeartbeatSourceWalReportDispatch() {
    }

    static Optional<BrokerHeartbeatRequest.Builder> prepareOnce(
        SourceWalProbeCaptureState captures,
        BrokerHeartbeatRequestData ordinaryHeartbeat,
        Optional<NodeApiVersions> controllerVersions,
        boolean uniqueBrokerChannelIdentityVerified,
        long nowNs,
        Uuid brokerIncarnationId,
        BrokerState state,
        boolean registered
    ) {
        Objects.requireNonNull(captures, "captures");
        Objects.requireNonNull(ordinaryHeartbeat, "ordinaryHeartbeat");
        Objects.requireNonNull(controllerVersions, "controllerVersions");
        if (!uniqueBrokerChannelIdentityVerified || !registered || state != BrokerState.RUNNING) {
            return Optional.empty();
        }
        if (ordinaryHeartbeat.wantFence() || ordinaryHeartbeat.wantShutDown()
            || !ordinaryHeartbeat.offlineLogDirs().isEmpty()) {
            return Optional.empty();
        }
        if (ordinaryHeartbeat.sourceWalProbeReport() != null
            || ordinaryHeartbeat.brokerId() < 0 || ordinaryHeartbeat.brokerEpoch() < 0L) {
            return Optional.empty();
        }
        if (controllerVersions.isEmpty()) {
            return Optional.empty();
        }
        try {
            controllerVersions.get().latestUsableVersion(
                ApiKeys.BROKER_HEARTBEAT, REPORT_VERSION, REPORT_VERSION
            );
        } catch (UnsupportedVersionException unavailable) {
            return Optional.empty();
        }
        // Only after all independent eligibility checks may the same
        // one-time mailbox as the typed diagnostic view be drained.
        return captures.takeWireOnce(
            nowNs, ordinaryHeartbeat.brokerId(), ordinaryHeartbeat.brokerEpoch(),
            brokerIncarnationId, state, registered
        ).map(report -> {
            BrokerHeartbeatRequestData diagnostic = ordinaryHeartbeat.duplicate();
            diagnostic.setSourceWalProbeReport(report);
            return new BrokerHeartbeatRequest.Builder(diagnostic, REPORT_VERSION);
        });
    }
}
