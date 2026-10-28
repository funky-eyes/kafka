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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.shared.StartupWalReplayEvidence;

import java.util.Objects;

/**
 * A process-local diagnostic pairing Kafka's broker-ready registration
 * snapshot with the WAL records this extension actually replayed at startup.
 *
 * <p>The snapshot is not a Controller registration lease, is not refreshed
 * on broker re-registration and does not attest WAL replication, fsync of
 * later writes, Kafka source LogStart coverage or deletion eligibility.
 * It must never be submitted as KRaft CAS or COMMITTED GC authorization.</p>
 */
public final class BrokerStartupReplayObservation {
    private final String clusterId;
    private final int brokerId;
    private final long brokerEpoch;
    private final Uuid brokerIncarnationId;
    private final StartupWalReplayEvidence replay;

    // Not constructible by arbitrary external callers. Still only a local
    // pairing of two snapshots, never a cryptographic/authenticated proof.
    BrokerStartupReplayObservation(
        StorageExtensionBrokerContext context,
        StartupWalReplayEvidence replay
    ) {
        Objects.requireNonNull(context, "context");
        this.replay = Objects.requireNonNull(replay, "replay");
        if (!context.hasBrokerIdentitySnapshot()) {
            throw new IllegalArgumentException("A complete broker-ready registration snapshot is required");
        }
        clusterId = context.clusterId();
        brokerId = context.brokerId();
        brokerEpoch = context.brokerEpoch();
        brokerIncarnationId = context.brokerIncarnationId();
    }

    public String clusterId() {
        return clusterId;
    }

    public int brokerId() {
        return brokerId;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public Uuid brokerIncarnationId() {
        return brokerIncarnationId;
    }

    public StartupWalReplayEvidence replay() {
        return replay;
    }
}
