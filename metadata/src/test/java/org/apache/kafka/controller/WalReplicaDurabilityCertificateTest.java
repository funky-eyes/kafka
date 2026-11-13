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
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificate.hasCurrentRegisteredCompleteReceipts;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificate.hasIndependentCompleteReceipts;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificate.hasKRaftReplayPreconditions;
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

    private static WalReplicaDurabilityCertificate.CurrentRegistration current(
        WalReplicaDurabilityCertificate.Receipt receipt
    ) {
        return new WalReplicaDurabilityCertificate.CurrentRegistration(
            receipt.brokerEpoch(), receipt.incarnationId(), receipt.storageGeneration(), false, false);
    }

    private static WalReplicaDurabilityCertificate.KRaftReplayContext replay(
        long controllerEpoch,
        long committedNextOffset,
        long logRecordOffset,
        long snapshotNextOffset
    ) {
        return new WalReplicaDurabilityCertificate.KRaftReplayContext(
            COORDINATE.topicId(), COORDINATE.partition(),
            COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(),
            controllerEpoch, committedNextOffset, logRecordOffset, snapshotNextOffset);
    }

    @Test
    void distinctAuthenticatedCurrentIsrReceiptsCanPassNecessaryPrecheck() {
        var first = receipt(1, 200);
        var second = receipt(2, 210);
        assertTrue(hasIndependentCompleteReceipts(COORDINATE,
            List.of(first, second), Set.of(1, 2, 3), 2));
        assertTrue(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, second), Set.of(1, 2, 3),
            Map.of(1, current(first), 2, current(second)), 2));
        Map<Integer, WalReplicaDurabilityCertificate.CurrentRegistration> registrations =
            Map.of(1, current(first), 2, current(second));
        assertTrue(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2, 3), registrations, replay(3, 1001, 1000, -1), 2));
        assertTrue(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2, 3), registrations, replay(3, 1001, -1, 1001), 2));
    }

    @Test
    void duplicateBrokerCannotCountAsIndependentReplica() {
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(1, 200)), Set.of(1, 2), 2));

        // Distinct Broker IDs are not independent identities if the process
        // incarnation was duplicated or replayed across registrations.
        var first = receipt(1, 200);
        var duplicateIncarnation = new WalReplicaDurabilityCertificate.Receipt(
            2, 102, first.incarnationId(), 1, 200, true, true);
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, duplicateIncarnation), Set.of(1, 2),
            Map.of(1, current(first), 2, current(duplicateIncarnation)), 2));
    }

    @Test
    void missingDurableCoverageFailsClosed() {
        var first = receipt(1, 200);
        var incomplete = receipt(2, 199);
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(first, incomplete), Set.of(1, 2), 2));
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, incomplete), Set.of(1, 2),
            Map.of(1, current(first), 2, current(incomplete)), 2));
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, incomplete),
            Set.of(1, 2), Map.of(1, current(first), 2, current(incomplete)),
            replay(3, 1001, 1000, -1), 2));
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

        var first = receipt(1, 200);
        var second = receipt(2, 210);
        Map<Integer, WalReplicaDurabilityCertificate.CurrentRegistration> current =
            Map.of(1, current(first), 2, current(second));
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, untrusted), Set.of(1, 2), current, 2));
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, unchecked), Set.of(1, 2), current, 2));

        // Restart/reregistration or disk replacement must revoke old receipts
        // even if the Broker ID, ISR and durable end offset did not change.
        var staleEpoch = new WalReplicaDurabilityCertificate.Receipt(
            2, second.brokerEpoch() - 1, second.incarnationId(),
            second.storageGeneration(), 210, true, true);
        var staleIncarnation = new WalReplicaDurabilityCertificate.Receipt(
            2, second.brokerEpoch(), Uuid.randomUuid(),
            second.storageGeneration(), 210, true, true);
        var staleDisk = new WalReplicaDurabilityCertificate.Receipt(
            2, second.brokerEpoch(), second.incarnationId(),
            second.storageGeneration() + 1, 210, true, true);
        for (var stale : List.of(staleEpoch, staleIncarnation, staleDisk)) {
            assertTrue(hasIndependentCompleteReceipts(COORDINATE,
                List.of(first, stale), Set.of(1, 2), 2),
                "identity-only stale receipts pass the old necessary count precheck");
            assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
                List.of(first, stale), Set.of(1, 2), current, 2));
            assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, stale),
                Set.of(1, 2), current, replay(3, 1001, 1000, -1), 2));
        }
    }

    @Test
    void foreignIsrAndInsufficientQuorumFailClosed() {
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(3, 200)), Set.of(1, 2), 2));
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(2, 200)), Set.of(1, 2), 3));
        assertFalse(hasIndependentCompleteReceipts(COORDINATE,
            List.of(receipt(1, 200), receipt(2, 200)), Set.of(1, 2), 1));

        var first = receipt(1, 200);
        var second = receipt(2, 200);
        var active = current(second);
        var fenced = new WalReplicaDurabilityCertificate.CurrentRegistration(
            active.brokerEpoch(), active.incarnationId(), active.storageGeneration(), true, false);
        var shuttingDown = new WalReplicaDurabilityCertificate.CurrentRegistration(
            active.brokerEpoch(), active.incarnationId(), active.storageGeneration(), false, true);
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, second), Set.of(1, 2), null, 2));
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, second), Set.of(1, 2), Map.of(1, current(first)), 2));
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, second), Set.of(1, 2),
            Map.of(1, current(first), 2, fenced), 2));
        assertFalse(hasCurrentRegisteredCompleteReceipts(COORDINATE,
            List.of(first, second), Set.of(1, 2),
            Map.of(1, current(first), 2, shuttingDown), 2));

        Map<Integer, WalReplicaDurabilityCertificate.CurrentRegistration> registrations =
            Map.of(1, current(first), 2, current(second));
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2), registrations, null, 2));
        // An old controller epoch may never silently authorize previously
        // replayed receipts on a newly elected controller.
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2), registrations, replay(4, 1001, 1000, -1), 2));
        // Exact KRaft log-source offset and exclusive snapshot horizon matter.
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2), registrations, replay(3, 1001, 999, -1), 2));
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2), registrations, replay(3, 1001, -1, 1000), 2));

        var wrongTopic = new WalReplicaDurabilityCertificate.KRaftReplayContext(
            Uuid.randomUuid(), COORDINATE.partition(), COORDINATE.leaderEpoch(),
            COORDINATE.partitionEpoch(), 3, 1001, 1000, -1);
        var wrongPartitionEpoch = new WalReplicaDurabilityCertificate.KRaftReplayContext(
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(),
            COORDINATE.partitionEpoch() + 1, 3, 1001, 1000, -1);
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2), registrations, wrongTopic, 2));
        assertFalse(hasKRaftReplayPreconditions(COORDINATE, List.of(first, second),
            Set.of(1, 2), registrations, wrongPartitionEpoch, 2));
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
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.CurrentRegistration(-1, Uuid.randomUuid(), 1, false, false));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.CurrentRegistration(1, Uuid.ZERO_UUID, 1, false, false));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityCertificate.CurrentRegistration(1, Uuid.randomUuid(), 0, false, false));

        // Neither a missing source nor two competing sources can supply
        // trusted KRaft replay position.
        assertThrows(IllegalArgumentException.class, () -> replay(3, 1001, -1, -1));
        assertThrows(IllegalArgumentException.class, () -> replay(3, 1001, 1000, 1001));
        assertThrows(IllegalArgumentException.class, () -> replay(3, 1000, 1000, -1));
        assertThrows(IllegalArgumentException.class, () -> replay(3, 1001, -1, 1002));
        assertThrows(IllegalArgumentException.class, () -> replay(3, 1001, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> replay(-1, 1001, 1000, -1));
    }
}
