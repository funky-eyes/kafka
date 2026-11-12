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

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Explicit, one-principal-per-broker trust policy for a future authenticated
 * broker-to-controller Source/WAL observation transport.
 *
 * <p>Kafka CLUSTER_ACTION authorization and a self-reported BrokerId do not
 * establish a unique broker-process identity. Reusing the same inter-broker
 * principal for multiple brokers is deliberately rejected. Mappings must
 * originate from trusted controller configuration, never a claim payload.
 * The production default is deny-all; there is no RPC or GC grant here.</p>
 */
final class PartitionRetirementBrokerPrincipalPolicy {
    static final PartitionRetirementBrokerPrincipalPolicy DENY_ALL =
        new PartitionRetirementBrokerPrincipalPolicy(Map.of());

    private final Map<Integer, KafkaPrincipal> expectedByBroker;

    PartitionRetirementBrokerPrincipalPolicy(Map<Integer, KafkaPrincipal> expectedByBroker) {
        Objects.requireNonNull(expectedByBroker, "expectedByBroker");
        Set<KafkaPrincipal> unique = new HashSet<>();
        for (Map.Entry<Integer, KafkaPrincipal> entry : expectedByBroker.entrySet()) {
            Integer brokerId = Objects.requireNonNull(entry.getKey(), "brokerId");
            KafkaPrincipal principal = Objects.requireNonNull(entry.getValue(), "brokerPrincipal");
            if (brokerId < 0 || KafkaPrincipal.ANONYMOUS.equals(principal)
                || principal.getName().isBlank() || "*".equals(principal.getName())
                || principal.getPrincipalType().isBlank()) {
                throw new IllegalArgumentException("A broker needs its own authenticated, nonwildcard principal");
            }
            if (!unique.add(principal)) {
                throw new IllegalArgumentException("A shared authenticated principal cannot identify one broker");
            }
        }
        this.expectedByBroker = Map.copyOf(expectedByBroker);
    }

    boolean matches(KafkaPrincipal authenticatedPrincipal, int brokerId) {
        return authenticatedPrincipal != null && expectedByBroker.containsKey(brokerId)
            && expectedByBroker.get(brokerId).equals(authenticatedPrincipal);
    }
}
