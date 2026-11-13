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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.controller.WalReplicaDurabilityRangePrecheck.hasCompleteIndependentRanges;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Untrusted structural evidence is necessary, never sufficient, for an ACK. */
class WalReplicaDurabilityRangePrecheckTest {
    private static final WalReplicaDurabilityCertificate.Coordinate COORDINATE =
        new WalReplicaDurabilityCertificate.Coordinate(Uuid.randomUuid(), 0, 7, 11, 100, 200, 3, 1000);
    private static final WalReplicaDurabilityCertificate.Receipt FIRST = receipt(1);
    private static final WalReplicaDurabilityCertificate.Receipt SECOND = receipt(2);
    private static final List<WalReplicaDurabilityCertificate.Receipt> RECEIPTS = List.of(FIRST, SECOND);
    private static final Set<Integer> ISR = Set.of(1, 2);
    private static final Map<Integer, WalReplicaDurabilityCertificate.CurrentRegistration> REGISTRATIONS =
        Map.of(1, current(FIRST), 2, current(SECOND));

    private static WalReplicaDurabilityCertificate.Receipt receipt(int brokerId) {
        return new WalReplicaDurabilityCertificate.Receipt(
            brokerId, brokerId + 100, Uuid.randomUuid(), 1L, 200L, true, true);
    }

    private static WalReplicaDurabilityCertificate.CurrentRegistration current(
        WalReplicaDurabilityCertificate.Receipt receipt
    ) {
        return new WalReplicaDurabilityCertificate.CurrentRegistration(
            receipt.brokerEpoch(), receipt.incarnationId(), receipt.storageGeneration(), false, false);
    }

    private static WalReplicaDurabilityRangePrecheck.Span span(
        WalReplicaDurabilityCertificate.Receipt receipt, long start, long end
    ) {
        return new WalReplicaDurabilityRangePrecheck.Span(
            receipt.brokerId(), receipt.brokerEpoch(), receipt.incarnationId(), receipt.storageGeneration(),
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), start, end, true);
    }

    private static Map<Integer, List<WalReplicaDurabilityRangePrecheck.Span>> validSpans() {
        return Map.of(1, List.of(span(FIRST, 100, 140), span(FIRST, 140, 200)),
            2, List.of(span(SECOND, 100, 200)));
    }

    private static boolean verify(Map<Integer, List<WalReplicaDurabilityRangePrecheck.Span>> spans) {
        return hasCompleteIndependentRanges(COORDINATE, RECEIPTS, ISR, REGISTRATIONS, spans, 2);
    }

    @Test
    void authenticatedCurrentReplicasNeedContiguousWalSpans() {
        assertTrue(verify(validSpans()));
        assertTrue(verify(Map.of(1, List.of(span(FIRST, 100, 200)),
            2, List.of(span(SECOND, 100, 160), span(SECOND, 160, 200)))));
    }

    @Test
    void gapOverlapAndReorderedSpanFailClosed() {
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 100, 140), span(FIRST, 141, 200)),
            2, List.of(span(SECOND, 100, 200)))));
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 100, 140), span(FIRST, 139, 200)),
            2, List.of(span(SECOND, 100, 200)))));
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 140, 200), span(FIRST, 100, 140)),
            2, List.of(span(SECOND, 100, 200)))));
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 100, 199)),
            2, List.of(span(SECOND, 100, 200)))));
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 100, 201)),
            2, List.of(span(SECOND, 100, 200)))));
    }

    @Test
    void foreignPartitionOrReplacedDiskCannotSupplySpan() {
        var foreignTopic = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1,
            Uuid.randomUuid(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, true);
        var wrongPartition = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1,
            COORDINATE.topicId(), COORDINATE.partition() + 1, COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, true);
        var staleLeader = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch() - 1, COORDINATE.partitionEpoch(), 100, 200, true);
        var stalePartitionEpoch = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(),
            COORDINATE.partitionEpoch() - 1, 100, 200, true);
        var replacedDisk = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 2,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, true);
        var replayedEpoch = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch() - 1, FIRST.incarnationId(), 1,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, true);
        var wrongIncarnation = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), Uuid.randomUuid(), 1,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, true);
        var borrowedBroker = new WalReplicaDurabilityRangePrecheck.Span(
            SECOND.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, true);
        for (var invalid : List.of(foreignTopic, wrongPartition, staleLeader, stalePartitionEpoch,
            replacedDisk, replayedEpoch, wrongIncarnation, borrowedBroker)) {
            assertFalse(verify(Map.of(1, List.of(invalid), 2, List.of(span(SECOND, 100, 200)))));
        }
    }

    @Test
    void missingOrUnverifiedReplicaSpanFailsClosed() {
        assertFalse(verify(null));
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 100, 200)))));
        assertFalse(verify(Map.of(1, List.of(span(FIRST, 100, 200)),
            2, List.of(span(SECOND, 100, 200)), 3, List.of(span(SECOND, 100, 200)))));
        var unverified = new WalReplicaDurabilityRangePrecheck.Span(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1,
            COORDINATE.topicId(), COORDINATE.partition(), COORDINATE.leaderEpoch(), COORDINATE.partitionEpoch(), 100, 200, false);
        assertFalse(verify(Map.of(1, List.of(unverified), 2, List.of(span(SECOND, 100, 200)))));
        var unauthenticated = new WalReplicaDurabilityCertificate.Receipt(
            FIRST.brokerId(), FIRST.brokerEpoch(), FIRST.incarnationId(), 1, 200, false, true);
        assertFalse(hasCompleteIndependentRanges(COORDINATE, List.of(unauthenticated, SECOND),
            ISR, REGISTRATIONS, validSpans(), 2));
        assertFalse(hasCompleteIndependentRanges(COORDINATE, RECEIPTS,
            ISR, REGISTRATIONS, validSpans(), 3));
    }

    @Test
    void invalidOrExcessiveSpanInventoryFailsClosed() {
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityRangePrecheck.Span(1, 101, FIRST.incarnationId(), 1,
                COORDINATE.topicId(), 0, 7, COORDINATE.partitionEpoch(), 100, 100, true));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityRangePrecheck.Span(1, 101, Uuid.ZERO_UUID, 1,
                COORDINATE.topicId(), 0, 7, COORDINATE.partitionEpoch(), 100, 200, true));
        assertThrows(IllegalArgumentException.class, () ->
            new WalReplicaDurabilityRangePrecheck.Span(1, 101, FIRST.incarnationId(), 0,
                COORDINATE.topicId(), 0, 7, COORDINATE.partitionEpoch(), 100, 200, true));
        List<WalReplicaDurabilityRangePrecheck.Span> tooMany = Collections.nCopies(
            WalReplicaDurabilityRangePrecheck.MAX_SPANS_PER_REPLICA + 1, span(FIRST, 100, 200));
        assertFalse(verify(Map.of(1, tooMany, 2, List.of(span(SECOND, 100, 200)))));
    }
}
