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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.MetadataRecordType;
import org.apache.kafka.common.metadata.PartitionRetirementAuthorityRecord;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionRetirementAuthorityStateTest {
    private static final Key PARTITION = new Key(Uuid.randomUuid(), 0);

    @Test
    void generatedKRaftRecordIdIsReservedAndVersionZero() {
        PartitionRetirementAuthorityRecord record = PartitionRetirementAuthorityState.record(
            PARTITION, active(4L, 10, 1, 0L)
        );
        assertEquals(29, record.apiKey());
        assertEquals(
            MetadataRecordType.PARTITION_RETIREMENT_AUTHORITY_RECORD,
            MetadataRecordType.fromId(record.apiKey())
        );
        assertEquals(PARTITION, PartitionRetirementAuthorityState.key(record));
        assertEquals(active(4L, 10, 1, 0L), PartitionRetirementAuthorityState.value(record));
    }

    @Test
    void taggedPartitionEpochSurvivesKRaftMetadataVersionZeroBinaryRoundTrip() {
        Uuid incarnation = Uuid.randomUuid();
        Value original = new Value(90L, 7, 12, 1, 50L, false, 300L, incarnation);
        PartitionRetirementAuthorityRecord record = PartitionRetirementAuthorityState.record(PARTITION, original);
        assertEquals(12, record.sourcePartitionEpoch());
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer encoded = ByteBuffer.allocate(record.size(cache, (short) 0));
        record.write(new ByteBufferAccessor(encoded), cache, (short) 0);
        encoded.flip();
        PartitionRetirementAuthorityRecord decoded = new PartitionRetirementAuthorityRecord();
        decoded.read(new ByteBufferAccessor(encoded), (short) 0);
        assertEquals(original, PartitionRetirementAuthorityState.value(decoded));
        assertEquals(PARTITION, PartitionRetirementAuthorityState.key(decoded));
        assertEquals(true, original.hasPartitionTopologyProof());
    }

    @Test
    void legacyKRaftRecordWithoutTaggedPartitionEpochRemainsUnverified() {
        Value legacy = new Value(90L, 7, 1, 50L, false, 300L, Uuid.randomUuid());
        PartitionRetirementAuthorityRecord record = PartitionRetirementAuthorityState.record(PARTITION, legacy);
        assertEquals(-1, record.sourcePartitionEpoch());
        assertEquals(legacy, PartitionRetirementAuthorityState.value(record));
        assertEquals(false, legacy.hasPartitionTopologyProof());
    }

    @Test
    void replayRejectsRegressedOrErasedPartitionEpoch() {
        Uuid incarnation = Uuid.randomUuid();
        Value original = new Value(90L, 7, 12, 1, 50L, false, 300L, incarnation);
        Value regressed = new Value(91L, 7, 11, 1, 50L, false, 300L, incarnation);
        Value erased = new Value(92L, 7, 1, 50L, false, 300L, incarnation);
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(original, regressed));
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(original, erased));
        PartitionRetirementAuthorityState.validateAdvance(erased, original);
    }

    @Test
    void freshLeaderEpochAlsoRequiresAdvancedPartitionEpoch() {
        Uuid incarnation = Uuid.randomUuid();
        Value previous = new Value(90L, 7, 12, 1, 50L, false, 300L, incarnation);
        Value invalid = new Value(91L, 8, 12, 1, 50L, false, 300L, incarnation);
        Value valid = new Value(91L, 8, 13, 1, 50L, false, 300L, incarnation);
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(previous, invalid));
        PartitionRetirementAuthorityState.validateAdvance(previous, valid);
    }

    @Test
    void sameLeaderEpochAllowsAdvancingIsrGenerationButNeverRegressingIt() {
        Uuid incarnation = Uuid.randomUuid();
        Value previous = new Value(90L, 7, 12, 1, 50L, false, 300L, incarnation);
        Value next = new Value(91L, 7, 13, 1, 50L, false, 300L, incarnation);
        PartitionRetirementAuthorityState.validateAdvance(previous, next);
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(next, previous));
    }

    @Test
    void distinctTopicIncarnationNeverSharesAnAuthorityKey() {
        Key recreated = new Key(Uuid.randomUuid(), 0);
        assertNotEquals(PARTITION, recreated);
        assertNotEquals(PARTITION, new Key(PARTITION.topicId(), 1));
    }

    @Test
    void reservedZeroTopicIdCannotBecomeAnAuthorityKey() {
        assertThrows(IllegalArgumentException.class, () -> new Key(Uuid.ZERO_UUID, 0));
    }

    @Test
    void malformedKRaftRecordWithZeroTopicIdCannotBeDecodedAsAuthorityKey() {
        PartitionRetirementAuthorityRecord forged = PartitionRetirementAuthorityState.record(
            PARTITION, active(30L, 7, 1, 5L)
        ).setTopicId(Uuid.ZERO_UUID);
        assertThrows(IllegalArgumentException.class, () -> PartitionRetirementAuthorityState.key(forged));
    }

    @Test
    void malformedPartitionOrUnknownInitialStateFailsClosed() {
        assertThrows(IllegalArgumentException.class, () -> new Key(PARTITION.topicId(), -1));
        assertThrows(IllegalArgumentException.class, () -> new Value(-1L, 0, 1, 0L, false));
        assertThrows(IllegalArgumentException.class, () -> new Value(0L, -1, 1, -1L, true));
        assertThrows(IllegalArgumentException.class, () -> new Value(0L, -1, -1, -1L, false));
        assertThrows(IllegalArgumentException.class, () -> new Value(0L, 0, 1, -2L, false));
    }

    @Test
    void oldAuthorityOffsetCannotOverwriteCurrentState() {
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(
                active(10L, 5, 1, 20L), active(10L, 6, 2, 21L)
            )
        );
    }

    @Test
    void replayRejectsRegressedSourceLeaderEpoch() {
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(
                active(10L, 6, 1, 0L), active(11L, 5, 2, 0L)
            )
        );
    }

    @Test
    void replayRejectsRegressedMonotonicWatermark() {
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(
                active(10L, 6, 1, 40L), active(11L, 7, 2, 20L)
            )
        );
    }

    @Test
    void equalEpochDemotionAllowedButRepromotionBlocked() {
        Value elected = active(10L, 6, 1, 40L);
        Value demoted = active(11L, 6, -1, 40L);
        PartitionRetirementAuthorityState.validateAdvance(elected, demoted);
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(demoted, active(12L, 6, 1, 40L))
        );
        PartitionRetirementAuthorityState.validateAdvance(demoted, active(12L, 7, 2, 40L));
    }

    @Test
    void equalEpochCannotAssignDifferentBroker() {
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(
                active(10L, 6, 1, 40L), active(11L, 6, 2, 40L)
            )
        );
    }

    @Test
    void terminalTopicIdIsIrrevocable() {
        Value deleted = new Value(11L, 6, -1, 40L, true);
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(deleted, active(12L, 7, 2, 40L))
        );
        assertThrows(IllegalArgumentException.class, () -> new Value(12L, 7, 2, 40L, true));
        PartitionRetirementAuthorityState.validateAdvance(
            active(10L, 6, 1, 40L), deleted
        );
    }

    @Test
    void aTopicMayBeDeletedBeforeFirstLeaderElection() {
        Value deleted = new Value(0L, -1, -1, -1L, true);
        assertEquals(deleted, PartitionRetirementAuthorityState.value(
            PartitionRetirementAuthorityState.record(PARTITION, deleted)
        ));
    }

    @Test
    void authenticatedBrokerRegistrationRoundTripsThroughGeneratedMetadataRecord() {
        Uuid incarnation = Uuid.randomUuid();
        Value authenticated = new Value(90L, 7, 1, 50L, false, 300L, incarnation);
        PartitionRetirementAuthorityRecord record = PartitionRetirementAuthorityState.record(PARTITION, authenticated);
        assertEquals(300L, record.brokerEpoch());
        assertEquals(incarnation, record.brokerIncarnationId());
        assertEquals(authenticated, PartitionRetirementAuthorityState.value(record));
        assertEquals(true, authenticated.hasBrokerIdentityProof());
    }

    @Test
    void anUnboundLegacyBrokerValueNeverSuppliesRegistrationProof() {
        Value legacy = active(90L, 7, 1, 50L);
        assertEquals(-1L, legacy.brokerEpoch());
        assertEquals(Uuid.ZERO_UUID, legacy.brokerIncarnationId());
        assertEquals(false, legacy.hasBrokerIdentityProof());
    }

    @Test
    void halfPopulatedRegistrationIdentityIsRejected() {
        Uuid incarnation = Uuid.randomUuid();
        assertThrows(IllegalArgumentException.class, () ->
            new Value(90L, 7, 1, 50L, false, 300L, Uuid.ZERO_UUID));
        assertThrows(IllegalArgumentException.class, () ->
            new Value(90L, 7, 1, 50L, false, -1L, incarnation));
        assertThrows(IllegalArgumentException.class, () ->
            new Value(90L, 7, 1, 50L, false, -2L, incarnation));
    }

    @Test
    void noLeaderOrDeletedRecordCannotRetainRegistrationIdentity() {
        Uuid incarnation = Uuid.randomUuid();
        assertThrows(IllegalArgumentException.class, () ->
            new Value(91L, 7, -1, 50L, false, 300L, incarnation));
        assertThrows(IllegalArgumentException.class, () ->
            new Value(91L, 7, -1, 50L, true, 300L, incarnation));
    }

    @Test
    void equalLeaderEpochCannotReplaceAnAuthenticatedBrokerIncarnation() {
        Value previous = new Value(90L, 7, 1, 50L, false, 300L, Uuid.randomUuid());
        Value replacement = new Value(91L, 7, 1, 50L, false, 301L, Uuid.randomUuid());
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(previous, replacement));
    }

    @Test
    void authenticatedLeaderCannotDowngradeToUnboundRecordAfterRestart() {
        Value previous = new Value(90L, 7, 1, 50L, false, 300L, Uuid.randomUuid());
        assertThrows(IllegalStateException.class, () ->
            PartitionRetirementAuthorityState.validateAdvance(previous, active(91L, 8, 1, 50L)));
    }

    @Test
    void higherSourceLeaderEpochMayCarryNewAuthenticatedBrokerIncarnation() {
        Value previous = new Value(90L, 7, 1, 50L, false, 300L, Uuid.randomUuid());
        Value elected = new Value(91L, 8, 1, 50L, false, 301L, Uuid.randomUuid());
        PartitionRetirementAuthorityState.validateAdvance(previous, elected);
        assertEquals(true, elected.hasBrokerIdentityProof());
    }

    private static Value active(long offset, int epoch, int broker, long watermark) {
        return new Value(offset, epoch, broker, watermark, false);
    }
}
