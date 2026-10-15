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
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.BrokerLifecycleManager.SourceWalProbeCaptureState;

import java.util.Objects;
import java.util.Optional;

/**
 * Dormant, non-emitting Source/WAL report preparation for heartbeat v3.
 *
 * <p>A caller must independently verify a unique per-Broker authenticated
 * Controller channel before satisfying the identity precondition. Neither
 * Kafka's CLUSTER_ACTION permission nor a claimed BrokerId does that. This
 * helper never authenticates a caller, never sends a request, and is not
 * invoked by ordinary BrokerLifecycleManager heartbeats.</p>
 *
 * <p>A locally prepared report is still untrusted by the Controller and
 * cannot authorize KRaft writes or COMMITTED object deletion.</p>
 */
final class BrokerHeartbeatSourceWalReportDispatch {
    private static final short REPORT_VERSION = 3;

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
