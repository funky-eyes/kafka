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
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.server.common.MetadataVersion;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.kafka.controller.WalReplicaDurabilityV1PositionPrecheck.CommitBatch;
import static org.apache.kafka.controller.WalReplicaDurabilityV1PositionPrecheck.hasAssignedCommittedPosition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Non-emitting V1 position checks are necessary conditions, never an ACK authorization. */
class WalReplicaDurabilityV1PositionPrecheckTest {
    private static WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt(int broker) {
        return new WalReplicaDurabilityCertificateRecord.ReplicaReceipt()
            .setBrokerId(broker).setBrokerEpoch(100L + broker)
            .setIncarnationId(Uuid.randomUuid()).setStorageGeneration(1)
            .setDurableEndOffset(200);
    }

    private static WalReplicaDurabilityCertificateRecord.WalSpan span(int broker) {
        return new WalReplicaDurabilityCertificateRecord.WalSpan()
            .setBrokerId(broker).setStartOffset(100).setEndOffset(200).setCrc32c(1);
    }

    private static WalReplicaDurabilityCertificateRecord v1(long previousOffset) {
        return new WalReplicaDurabilityCertificateRecord()
            .setTopicId(Uuid.randomUuid()).setPartitionId(0)
            .setLeaderEpoch(7).setPartitionEpoch(11)
            .setStartOffset(100).setEndOffset(200)
            .setControllerEpoch(9)
            .setExpectedPreviousKRaftOffset(previousOffset)
            .setReplicaReceipts(List.of(receipt(1), receipt(2)))
            .setWalSpans(List.of(span(1), span(2)));
    }

    private static CommitBatch committed() {
        return new CommitBatch(1000, 1003, 9, 1004);
    }

    private static boolean accepts(
        WalReplicaDurabilityCertificateRecord record, long observed, long previous
    ) {
        return hasAssignedCommittedPosition(
            record, (short) 1, MetadataVersion.IBP_4_4_IV0,
            true, committed(), observed, previous);
    }

    @Test
    void binaryV1RoundTripUsesPreviousCasNotFutureOwnAppendOffset() {
        var record = v1(-1);
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(record.size(cache, (short) 1));
        record.write(new ByteBufferAccessor(buffer), cache, (short) 1);
        buffer.flip();
        var readBack = new WalReplicaDurabilityCertificateRecord();
        readBack.read(new ByteBufferAccessor(buffer), (short) 1);
        assertEquals(record, readBack);
        assertEquals(-1L, readBack.expectedPreviousKRaftOffset());
        assertEquals(0L, readBack.kRaftOffset());
        assertTrue(accepts(readBack, 1000, -1));
    }

    @Test
    void oldV0WireEncodingRetainsLegacyFieldWithoutAuthorizingV1Cas() {
        var record = v1(-1).setKRaftOffset(1000);
        // V0 has its own claimed position, preserved solely for compatibility.
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer buffer = ByteBuffer.allocate(record.size(cache, (short) 0));
        record.write(new ByteBufferAccessor(buffer), cache, (short) 0);
        buffer.flip();
        var readBack = new WalReplicaDurabilityCertificateRecord();
        readBack.read(new ByteBufferAccessor(buffer), (short) 0);
        assertEquals(1000L, readBack.kRaftOffset());
        assertEquals(-1L, readBack.expectedPreviousKRaftOffset());
        assertFalse(hasAssignedCommittedPosition(readBack, (short) 0,
            MetadataVersion.IBP_4_4_IV0, true, committed(), 1000, -1));
        // V0 at offset zero is especially dangerous: the V0 field's value
        // alone cannot distinguish a V1 default without KRaft's wire version.
        readBack.setKRaftOffset(0);
        assertFalse(hasAssignedCommittedPosition(readBack, (short) 0,
            MetadataVersion.IBP_4_4_IV0, true, committed(), 1000, -1));
    }

    @Test
    void exactAssignedPositionAndPreviousCertificateCasMustAgree() {
        assertTrue(accepts(v1(-1), 1000, -1));
        assertTrue(accepts(v1(999), 1002, 999));
        assertFalse(accepts(v1(998), 1002, 999));
        assertFalse(accepts(v1(1002), 1002, 1002));
        assertFalse(accepts(v1(1004), 1002, 1004));
        assertFalse(accepts(v1(-2), 1002, -2));
        assertFalse(accepts(v1(999), 999, 999));
        assertFalse(accepts(v1(-1).setControllerEpoch(8), 1000, -1));
    }

    @Test
    void speculativeOrUncommittedKRaftPositionMustFailClosed() {
        assertFalse(accepts(v1(-1), 999, -1));
        assertFalse(accepts(v1(-1), 1004, -1));
        assertFalse(hasAssignedCommittedPosition(v1(-1), (short) 1, MetadataVersion.IBP_4_4_IV0,
            true, new CommitBatch(1000, 1003, 9, 1004), 2000, -1));
        assertThrows(IllegalArgumentException.class, () -> new CommitBatch(1000, 1003, 9, 1003));
        assertThrows(IllegalArgumentException.class, () -> new CommitBatch(1003, 1000, 9, 1004));
        assertThrows(IllegalArgumentException.class, () -> new CommitBatch(-1, 1000, 9, 1004));
        assertThrows(IllegalArgumentException.class, () -> new CommitBatch(0, 1, -1, 2));
    }

    @Test
    void malformedFeatureOrForgedBrokerClaimsCannotAuthorizeV1Position() {
        var record = v1(-1);
        assertFalse(hasAssignedCommittedPosition(record, (short) 1, MetadataVersion.LATEST_PRODUCTION,
            true, committed(), 1000, -1));
        assertFalse(hasAssignedCommittedPosition(record, (short) 1, MetadataVersion.IBP_4_4_IV0,
            false, committed(), 1000, -1));
        assertFalse(hasAssignedCommittedPosition(record, (short) 1, MetadataVersion.IBP_4_4_IV0,
            true, null, 1000, -1));
        assertFalse(hasAssignedCommittedPosition(null, (short) 1, MetadataVersion.IBP_4_4_IV0,
            true, committed(), 1000, -1));
        record.replicaReceipts().get(1).setIncarnationId(
            record.replicaReceipts().get(0).incarnationId());
        assertFalse(accepts(record, 1000, -1));
    }
}
