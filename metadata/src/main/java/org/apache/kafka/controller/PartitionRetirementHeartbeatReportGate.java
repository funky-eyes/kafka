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

import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.SourceWalProbeReport;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;

import java.util.Objects;
import java.util.Optional;

/**
 * Non-authorizing ingress gate for the unstable BrokerHeartbeat v3 Source/WAL
 * diagnostic. The normal v0-v2 heartbeat path is unchanged.
 *
 * <p>Network CLUSTER_ACTION is not a unique broker identity. A wire report
 * is never accepted as KRaft authority or as proof of object durability.
 * Before a trusted authentication/channel binding and Controller-issued
 * challenge are connected, every non-null report MUST be rejected.</p>
 */
final class PartitionRetirementHeartbeatReportGate {
    enum Finding {
        ABSENT,
        VERSION_NOT_NEGOTIATED,
        PRINCIPAL_UNBOUND,
        MALFORMED,
        BROKER_NOT_ELIGIBLE,
        UNVERIFIED
    }

    private PartitionRetirementHeartbeatReportGate() {
    }

    static Finding inspect(
        ControllerRequestContext context,
        BrokerHeartbeatRequestData heartbeat,
        PartitionRetirementBrokerPrincipalPolicy principalPolicy
    ) {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(heartbeat, "heartbeat");
        Objects.requireNonNull(principalPolicy, "principalPolicy");

        if (heartbeat.sourceWalProbeReport() == null) {
            return Finding.ABSENT;
        }
        // Future protocol versions are not implicitly trusted. Review any
        // new tagged-field semantics before accepting a different version.
        if (!isVersionThreeHeartbeat(context)) {
            return Finding.VERSION_NOT_NEGOTIATED;
        }
        if (!principalPolicy.matches(context.principal(), heartbeat.brokerId())) {
            return Finding.PRINCIPAL_UNBOUND;
        }
        // Failed directories, broker fencing, and controlled shutdown
        // invalidate any claim of an available native Source/WAL window.
        if (heartbeat.wantFence() || heartbeat.wantShutDown()
            || !heartbeat.offlineLogDirs().isEmpty()) {
            return Finding.BROKER_NOT_ELIGIBLE;
        }
        try {
            fromWire(heartbeat);
        } catch (IllegalArgumentException invalid) {
            return Finding.MALFORMED;
        }
        // A syntactically valid self-report is still untrusted. Deliberately
        // do not call an authority writer or consume a Controller probe here.
        return Finding.UNVERIFIED;
    }

    /**
     * Exact request-header binding for BOTH diagnostic ingress and challenge
     * egress. Matching a version number on another API is never sufficient
     * to opt into unstable BrokerHeartbeat tagged fields.
     */
    static boolean isVersionThreeHeartbeat(ControllerRequestContext context) {
        Objects.requireNonNull(context, "context");
        return context.requestHeader() != null
            && context.requestHeader().requestApiKey() == ApiKeys.BROKER_HEARTBEAT.id
            && context.requestHeader().requestApiVersion() == 3;
    }

    static Optional<PartitionRetirementSourceProbe.Report> fromWire(
        BrokerHeartbeatRequestData heartbeat
    ) {
        Objects.requireNonNull(heartbeat, "heartbeat");
        SourceWalProbeReport wire = heartbeat.sourceWalProbeReport();
        if (wire == null) {
            return Optional.empty();
        }
        // Explicit validation, not catching NullPointerException. This is an
        // untrusted network payload; malformed identities must fail closed.
        if (wire.nonce() == null || wire.clusterId() == null
            || wire.topicId() == null || wire.brokerIncarnationId() == null) {
            throw new IllegalArgumentException("Missing required Source/WAL report identity");
        }
        PartitionRetirementSourceProbe.Request request =
            new PartitionRetirementSourceProbe.Request(
                wire.nonce(), wire.clusterId(), wire.topicId(), wire.partitionId(),
                heartbeat.brokerId(), heartbeat.brokerEpoch(), wire.brokerIncarnationId(),
                wire.sourceLeaderEpoch(), wire.expectedPreviousAuthorityOffset(),
                wire.nextAuthorityOffset(), wire.nativeLogStartOffset()
            );
        return Optional.of(new PartitionRetirementSourceProbe.Report(
            request, wire.localLeadershipGeneration(), wire.nativeLogStartOffset(),
            wire.nativeHighWatermark(), wire.nativeLogEndOffset(),
            wire.walMutationRevision(), wire.walAdmissionRevision()
        ));
    }
}
