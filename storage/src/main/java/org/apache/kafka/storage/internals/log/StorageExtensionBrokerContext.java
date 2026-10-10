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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Stable broker-network context delivered to a storage extension after Kafka has enabled request processing.
 *
 * <p>The extension startup phase intentionally runs before listeners are ready so physical WAL recovery can happen
 * before any partition is loaded. This second context is delivered only after Kafka has resolved the real listener
 * endpoints and started the SocketServer acceptors, which allows an extension to bootstrap ordinary Kafka clients
 * without depending on BrokerServer, ReplicaManager or controller internals.</p>
 *
 * <p>The registered broker epoch and process-incarnation UUID are snapshots
 * of BrokerLifecycleManager state at callback time. They are not a durable
 * KRaft registration certificate, a lease, or retirement/GC authorization.
 * Consumers must revalidate current registration against the controller.</p>
 */
public record StorageExtensionBrokerContext(
    String clusterId,
    int brokerId,
    List<Endpoint> listeners,
    Map<String, ?> originals,
    Time time,
    long brokerEpoch,
    Uuid brokerIncarnationId
) {
    /**
     * Source-compatible constructor for older storage extensions. Missing
     * registration evidence is explicitly unbound, never fabricated.
     */
    public StorageExtensionBrokerContext(
        String clusterId,
        int brokerId,
        List<Endpoint> listeners,
        Map<String, ?> originals,
        Time time
    ) {
        this(clusterId, brokerId, listeners, originals, time, -1L, Uuid.ZERO_UUID);
    }

    public StorageExtensionBrokerContext {
        if (clusterId == null || clusterId.isBlank()) {
            throw new IllegalArgumentException("clusterId must not be blank");
        }
        Objects.requireNonNull(listeners, "listeners");
        Objects.requireNonNull(originals, "originals");
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(brokerIncarnationId, "brokerIncarnationId");
        if (brokerEpoch < -1L) {
            throw new IllegalArgumentException("brokerEpoch must be >= -1");
        }
        boolean hasEpoch = brokerEpoch >= 0L;
        boolean hasIncarnation = !Uuid.ZERO_UUID.equals(brokerIncarnationId);
        if (hasEpoch != hasIncarnation) {
            throw new IllegalArgumentException("Broker epoch and incarnation must be bound together");
        }
        if (listeners.isEmpty()) {
            throw new IllegalArgumentException("listeners must not be empty");
        }
        clusterId = clusterId.trim();
        listeners = List.copyOf(listeners);
        // KafkaConfig.originals can legally contain null values. Preserve them while making a stable snapshot.
        originals = Collections.unmodifiableMap(new LinkedHashMap<>(originals));
    }

    /** True only for a complete process-local broker registration snapshot. */
    public boolean hasBrokerIdentitySnapshot() {
        return brokerEpoch >= 0L;
    }
}
