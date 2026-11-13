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
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.controller.WalReplicaDurabilityCertificateRecordPrecheck.hasCommittedReplayOrigin;
import static org.apache.kafka.controller.WalReplicaDurabilityCertificateRecordPrecheck.requireWellFormedUntrustedRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The experimental record format is NEVER an ACK or physical deletion permit. */
class WalReplicaDurabilityCertificateRecordPrecheckTest {
    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(int brokerId) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(brokerId)
            .setBrokerEpoch(100 + brokerId)
            .setIncarnationId(Uuid.randomUuid())
            .setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int broker, long start, long end) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(broker)
            .setStartOffset(start)
            .setEndOffset(end)
            .setCrc32c(123456L);
    }

    private static WalReplicaDurabilityCertificateRecord valid() {
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(Uuid.randomUuid())
            .setPartitionId(0)
            .setLeaderEpoch(7)
            .setPartitionEpoch(11)
            .setStartOffset(100)
            .setEndOffset(200)
            .setControllerEpoch(3)
            .setKRaftOffset(1000)
            .setReplicaReceipts(List.of(receipt(1), receipt(2)))
            .setWalSpans(List.of(span(1, 100, 150), span(1, 150, 200), span(2, 100, 200)));
    }

    @Test
    void experimentalMetadataRecordV0RetainsReplicaAndSpanFieldsOnBinaryRoundTrip() {
        WalReplicaDurabilityCertificateRecord record = valid();
        assertEquals(30, record.apiKey());
        assertEquals(MetadataRecordType.WAL_REPLICA_DURABILITY_CERTIFICATE_RECORD,
            MetadataRecordType.fromId(record.apiKey()));
        requireWellFormedUntrustedRecord(record);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer encoded = ByteBuffer.allocate(record.size(cache, (short) 0));
        record.write(new ByteBufferAccessor(encoded), cache, (short) 0);
        encoded.flip();
        WalReplicaDurabilityCertificateRecord decoded = new WalReplicaDurabilityCertificateRecord();
        decoded.read(new ByteBufferAccessor(encoded), (short) 0);
        assertEquals(record, decoded);
        requireWellFormedUntrustedRecord(decoded);
    }

    @Test
    void reservedTopicAndReplayedBrokerIncarnationsAreRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            requireWellFormedUntrustedRecord(valid().setTopicId(Uuid.ZERO_UUID)));
        assertThrows(IllegalArgumentException.class, () ->
            requireWellFormedUntrustedRecord(valid().setPartitionId(-1)));
        assertThrows(IllegalArgumentException.class, () ->
            requireWellFormedUntrustedRecord(valid().setLeaderEpoch(-1)));
        assertThrows(IllegalArgumentException.class, () ->
            requireWellFormedUntrustedRecord(valid().setEndOffset(100)));
        WalReplicaDurabilityCertificateRecord forged = valid();
        var first = forged.replicaReceipts().get(0);
        var second = forged.replicaReceipts().get(1);
        second.setIncarnationId(first.incarnationId());
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(forged));

        WalReplicaDurabilityCertificateRecord duplicate = valid();
        duplicate.setReplicaReceipts(List.of(receipt(1), receipt(1)));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(duplicate));
    }

    @Test
    void holesOverlapsAndForeignBrokerSpansCannotBecomeCertificates() {
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(1, 100, 149), span(1, 150, 200),
                span(2, 100, 200)))));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(1, 100, 151), span(1, 150, 200),
                span(2, 100, 200)))));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(1, 100, 200), span(3, 100, 200)))));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(2, 100, 200), span(1, 100, 200)))));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(1, 100, 200)))));
    }

    @Test
    void experimentalFeatureAndCommittedKRaftReplayOriginAreNecessary() {
        WalReplicaDurabilityCertificateRecord record = valid();
        assertFalse(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.LATEST_PRODUCTION, true,
            Optional.empty(), 1000, 1001));
        assertFalse(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, false,
            Optional.empty(), 1000, 1001));
        assertTrue(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.empty(), 1000, 1001));
        assertFalse(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.empty(), 999, 1001));
        assertFalse(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.empty(), 1000, 1000));
        assertTrue(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.of(new OffsetAndEpoch(1001, 9)), 3000, 1001));
        assertFalse(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.of(new OffsetAndEpoch(1000, 9)), 3000, 1001));
        assertFalse(hasCommittedReplayOrigin(record, (short) 0, MetadataVersion.IBP_4_4_IV0, true,
            Optional.of(new OffsetAndEpoch(1002, 9)), 3000, 1001));
        // A V1 record does not self-claim its append position. Never parse
        // that wire version through the older V0 replay-origin check.
        assertFalse(hasCommittedReplayOrigin(record, (short) 1, MetadataVersion.IBP_4_4_IV0, true,
            Optional.empty(), 1000, 1001));
    }

    @Test
    void invalidChecksumsDurableEndsAndOversizedListsFailClosed() {
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(1, 100, 200).setCrc32c(-1),
                span(2, 100, 200)))));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(List.of(span(1, 100, 200).setCrc32c(0x1_0000_0000L),
                span(2, 100, 200)))));
        WalReplicaDurabilityCertificateRecord lowEnd = valid();
        lowEnd.replicaReceipts().get(0).setDurableEndOffset(199);
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(lowEnd));
        List<WalReplicaDurabilityCertificateRecord.ReplicaReceipt> excessive = new ArrayList<>();
        for (int broker = 1; broker <= 17; broker++) {
            excessive.add(receipt(broker));
        }
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setReplicaReceipts(excessive)));
        assertThrows(IllegalArgumentException.class, () -> requireWellFormedUntrustedRecord(
            valid().setWalSpans(Collections.nCopies(513, span(1, 100, 200)))));
    }
}
