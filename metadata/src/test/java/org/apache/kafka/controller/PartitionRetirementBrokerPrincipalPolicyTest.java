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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementBrokerPrincipalPolicyTest {
    private static final KafkaPrincipal BROKER_THREE = broker("broker-three");
    private static final KafkaPrincipal BROKER_FOUR = broker("broker-four");

    @Test
    void productionDefaultDeniesEveryBrokerEvenWhenPrincipalLooksUnique() {
        assertFalse(PartitionRetirementBrokerPrincipalPolicy.DENY_ALL.matches(BROKER_THREE, 3));
        assertFalse(PartitionRetirementBrokerPrincipalPolicy.DENY_ALL.matches(null, 3));
    }

    @Test
    void distinctAuthenticatedPrincipalsBindToTheirOwnBrokerOnly() {
        var policy = new PartitionRetirementBrokerPrincipalPolicy(
            Map.of(3, BROKER_THREE, 4, BROKER_FOUR)
        );
        assertTrue(policy.matches(BROKER_THREE, 3));
        assertTrue(policy.matches(BROKER_FOUR, 4));
        assertFalse(policy.matches(BROKER_THREE, 4));
        assertFalse(policy.matches(BROKER_FOUR, 3));
        assertFalse(policy.matches(BROKER_THREE, 5));
    }

    @Test
    void sharedInterbrokerPrincipalCannotClaimTwoBrokerIds() {
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(Map.of(3, BROKER_THREE, 4, BROKER_THREE)));
    }

    @Test
    void anonymousPrincipalCannotBindEvenWithClusterActionPrivileges() {
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(Map.of(3, KafkaPrincipal.ANONYMOUS)));
    }

    @Test
    void wildcardPrincipalCannotBeUsedAsAUniqueBrokerIdentity() {
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(Map.of(3, broker("*"))));
    }

    @Test
    void blankPrincipalNameOrTypeCannotBeBound() {
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(Map.of(3, broker(""))));
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(
                Map.of(3, new KafkaPrincipal("", "broker-three"))));
    }

    @Test
    void brokerIdsMustBeNonnegative() {
        assertThrows(IllegalArgumentException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(Map.of(-1, BROKER_THREE)));
    }

    @Test
    void trustedMappingIsDefensivelyCopiedBeforeUse() {
        Map<Integer, KafkaPrincipal> mutable = new HashMap<>();
        mutable.put(3, BROKER_THREE);
        var policy = new PartitionRetirementBrokerPrincipalPolicy(mutable);
        mutable.put(3, BROKER_FOUR);
        assertTrue(policy.matches(BROKER_THREE, 3));
        assertFalse(policy.matches(BROKER_FOUR, 3));
    }

    @Test
    void missingPolicyMapIsRejected() {
        assertThrows(NullPointerException.class, () ->
            new PartitionRetirementBrokerPrincipalPolicy(null));
    }

    private static KafkaPrincipal broker(String name) {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name);
    }
}
