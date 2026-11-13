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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificate.hasIndependentCompleteReceipts;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WalReplicaDurabilityCertificateTest {
    private static final WalReplicaDurabilityCertificate.Coordinate COORDINATE =
        new WalReplicaDurabilityCertificate.Coordinate(Uuid.randomUuid(), 0, 7, 11, 100, 200, 3, 1000);

    private static WalReplicaDurabilityCertificate.Receipt receipt(int broker, long end) {
        return new WalReplicaDurabilityCertificate.Receipt(
            broker, 100 + broker, Uuid.randomUuid(), 1, end, true, true);
    }

    @Test
    void distinctAuthenticatedCurrentIsrReceiptsCanPassNecessaryPrecheck() {
        assertTrue(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(2, 210)), Set.of(1, 2, 3), 2));
    }

    @Test
    void duplicateBrokerCannotCountAsIndependentReplica() {
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(1, 200)), Set.of(1, 2), 2));
    }

    @Test
    void missingDurableCoverageFailsClosed() {
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(2, 199)), Set.of(1, 2), 2));
    }

    @Test
    void unauthenticatedAndUnverifiedReceiptsFailClosed() {
        var untrusted = new WalReplicaDurabilityCertificate.Receipt(
            2, 102, Uuid.randomUuid(), 1, 200, false, true);
        var unchecked = new WalReplicaDurabilityCertificate.Receipt(
            2, 102, Uuid.randomUuid(), 1, 200, true, false);
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), untrusted), Set.of(1, 2), 2));
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), unchecked), Set.of(1, 2), 2));
    }

    @Test
    void foreignIsrAndInsufficientQuorumFailClosed() {
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(3, 200)), Set.of(1, 2), 2));
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(2, 200)), Set.of(1, 2), 3));
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(2, 200)), Set.of(1, 2), 1));
    }

    @Test
    void invalidOffsetsAndIdentityAreRejectedAtConstruction() {
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.Coordinate(Uuid.randomUuid(), 0, 0, 0, 200, 200, 0, 0));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.Receipt(1, -1, Uuid.randomUuid(), 1, 200, true, true));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.Coordinate(Uuid.ZERO_UUID, 0, 0, 0, 0, 1, 0, 0));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.Receipt(1, 1, Uuid.ZERO_UUID, 1, 200, true, true));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.Receipt(1, 1, Uuid.randomUuid(), 0, 200, true, true));
    }
}
