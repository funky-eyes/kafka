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
package org.apache.kafka.storage.internals.shared.metadata;

import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Decision;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Outcome;
import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Snapshot;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.OptionalLong;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fail-closed, non-emitting v1 envelope proof. No test commits KRaft metadata or deletes objects. */
class PartitionRetirementAuthoritySnapshotCodecTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId RECREATED = new SharedPartitionId(1L, 3L, 0);
    private static final SharedPartitionId OTHER_PARTITION = new SharedPartitionId(1L, 2L, 1);

    @Test
    void fixedLengthAndBigEndianHeaderAreStable() {
        byte[] encoded = encode(active(50L));
        ByteBuffer buffer = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        assertEquals(PartitionRetirementAuthoritySnapshotCodec.ENCODED_LENGTH, encoded.length);
        assertEquals(0x52544155, buffer.getInt(0));
        assertEquals(1, buffer.getShort(4));
        assertEquals(1, buffer.getShort(6));
        assertEquals(1L, buffer.getLong(8));
        assertEquals(2L, buffer.getLong(16));
        assertEquals(0, buffer.getInt(24));
        assertEquals(3L, buffer.getLong(28));
        assertEquals(50L, buffer.getLong(44));
    }

    @Test
    void explicitZeroRoundTripsWithoutBeingConfusedWithMissing() {
        Snapshot source = active(0L);
        Snapshot restored = decode(encode(source), PARTITION, 3L);
        assertEquals(source, restored);
        assertEquals(OptionalLong.of(0L), restored.explicitLogStart());
    }

    @Test
    void missingWatermarkRoundTripsWithCanonicalZeroPayload() {
        Snapshot source = new Snapshot(PARTITION, 0L, 10, 1, OptionalLong.empty(), false);
        byte[] encoded = encode(source);
        assertEquals(0, ByteBuffer.wrap(encoded).getShort(6));
        assertEquals(0L, ByteBuffer.wrap(encoded).getLong(44));
        assertEquals(source, decode(encoded, PARTITION, 0L));
        assertTrue(decode(encoded, PARTITION, 0L).explicitLogStart().isEmpty());
    }

    @Test
    void positiveWatermarkSurvivesLeaderHandoverCheckpoint() {
        Snapshot source = new Snapshot(PARTITION, 12L, 15, 2, OptionalLong.of(90L), false);
        Snapshot restored = decode(encode(source), PARTITION, 10L);
        assertEquals(12L, restored.authorityOffset());
        assertEquals(15, restored.maxSourceLeaderEpoch());
        assertEquals(2, restored.activeBrokerId());
        assertEquals(OptionalLong.of(90L), restored.explicitLogStart());
    }

    @Test
    void terminalAfterElectionRetainsHighestWatermarkAcrossReplay() {
        Snapshot deleted = new Snapshot(PARTITION, 15L, 15, -1, OptionalLong.of(90L), true);
        Snapshot restored = decode(encode(deleted), PARTITION, 15L);
        assertTrue(restored.terminallyDeleted());
        assertEquals(OptionalLong.of(90L), restored.explicitLogStart());
        Decision delayed = PartitionRetirementAuthorityModel.observeLeader(
            restored, PARTITION, 3, 16, 15L, 16L
        );
        assertEquals(Outcome.TERMINALLY_DELETED, delayed.outcome());
    }

    @Test
    void terminalBeforeFirstElectionRestoresWithoutForgingAnEpoch() {
        Snapshot deleted = new Snapshot(PARTITION, 0L, -1, -1, OptionalLong.empty(), true);
        Snapshot restored = decode(encode(deleted), PARTITION, 0L);
        assertTrue(restored.terminallyDeleted());
        assertEquals(-1, restored.maxSourceLeaderEpoch());
        assertEquals(-1, restored.activeBrokerId());
    }

    @Test
    void uncommittedInitialStateCannotBeEncodedAsDurableAuthority() {
        assertThrows(IllegalArgumentException.class, () ->
            encode(PartitionRetirementAuthorityModel.initial(PARTITION))
        );
    }

    @Test
    void wrongTopicIncarnationFailsEvenWithAValidChecksum() {
        byte[] encoded = encode(active(50L));
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, RECREATED, 3L));
    }

    @Test
    void wrongPartitionNumberFailsEvenWithAValidChecksum() {
        byte[] encoded = encode(active(50L));
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, OTHER_PARTITION, 3L));
    }

    @Test
    void callerCannotOmitTheProvenNonnegativeAuthorityHorizon() {
        byte[] encoded = encode(active(50L));
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, -1L));
    }

    @Test
    void olderSnapshotThanRequiredCommittedHorizonFailsClosed() {
        byte[] encoded = encode(active(50L));
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 4L));
    }

    @Test
    void truncatedAndExtendedEnvelopesFailClosed() {
        byte[] encoded = encode(active(50L));
        assertThrows(IllegalArgumentException.class, () ->
            decode(Arrays.copyOf(encoded, encoded.length - 1), PARTITION, 3L)
        );
        assertThrows(IllegalArgumentException.class, () ->
            decode(Arrays.copyOf(encoded, encoded.length + 1), PARTITION, 3L)
        );
    }

    @Test
    void mutatedPayloadWithOriginalChecksumCannotReplay() {
        byte[] encoded = encode(active(50L));
        encoded[27] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void unsupportedVersionFailsDespiteARecomputedChecksum() {
        byte[] encoded = encode(active(50L));
        view(encoded).putShort(4, (short) 2);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void mismatchedMagicFailsDespiteARecomputedChecksum() {
        byte[] encoded = encode(active(50L));
        view(encoded).putInt(0, 0x52544156);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void unknownFlagBitsFailDespiteARecomputedChecksum() {
        byte[] encoded = encode(active(50L));
        view(encoded).putShort(6, (short) 0x41);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void noncanonicalAbsentWatermarkFailsDespiteARecomputedChecksum() {
        byte[] encoded = encode(new Snapshot(PARTITION, 3L, 10, 1, OptionalLong.empty(), false));
        view(encoded).putLong(44, 50L);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void invalidNegativeWatermarkFailsDespiteARecomputedChecksum() {
        byte[] encoded = encode(active(50L));
        view(encoded).putLong(44, -1L);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void terminalEnvelopeCannotResurrectAnActiveBroker() {
        byte[] encoded = encode(active(50L));
        view(encoded).putShort(6, (short) 3);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void recoveredHigherWatermarkStillRejectsStaleProposal() {
        Snapshot state = decode(encode(active(50L)), PARTITION, 3L);
        Decision stale = PartitionRetirementAuthorityModel.advanceLogStart(
            state, PARTITION, 1, 10, 40L, 50L, 3L, 4L
        );
        assertEquals(Outcome.REGRESSED_WATERMARK, stale.outcome());
        assertEquals(OptionalLong.of(50L), stale.snapshot().explicitLogStart());
        assertFalse(stale.snapshot().terminallyDeleted());
        assertArrayEquals(encode(state), encode(decode(encode(state), PARTITION, 3L)));
    }

    @Test
    void recomputedChecksumCannotSwapImmutableTopicId() {
        byte[] encoded = encode(active(50L));
        view(encoded).putLong(16, 3L);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void negativeEncodedPartitionIsRejectedAfterChecksumRepair() {
        byte[] encoded = encode(active(50L));
        view(encoded).putInt(24, -1);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void invalidBrokerIdIsRejectedAfterChecksumRepair() {
        byte[] encoded = encode(active(50L));
        view(encoded).putInt(40, -2);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void nonterminalUnknownSourceEpochIsRejectedAfterChecksumRepair() {
        byte[] encoded = encode(active(50L));
        view(encoded).putInt(36, -1);
        fixChecksum(encoded);
        assertThrows(IllegalArgumentException.class, () -> decode(encoded, PARTITION, 3L));
    }

    @Test
    void forgedHigherAuthorityOffsetProvesCrcIsNotAuthentication() {
        byte[] encoded = encode(active(50L));
        view(encoded).putLong(28, 400L);
        fixChecksum(encoded);

        // Deliberately documents the codec's limitation. An attacker can recalculate
        // CRC and forge an apparently newer offset; only committed controller state
        // outside this codec can authenticate or reject the snapshot.
        Snapshot untrusted = decode(encoded, PARTITION, 4L);
        assertEquals(400L, untrusted.authorityOffset());
    }

    @Test
    void forgedTerminalFlagDemonstratesNeedForControllerProvenance() {
        Snapshot deleted = new Snapshot(PARTITION, 15L, 10, -1, OptionalLong.of(50L), true);
        byte[] encoded = encode(deleted);
        view(encoded).putShort(6, (short) 1);
        fixChecksum(encoded);

        // A fresh CRC cannot prove a RemoveTopicRecord was never committed.
        // Never use decode() output as a source of write or delete authorization.
        assertFalse(decode(encoded, PARTITION, 15L).terminallyDeleted());
    }

    private static Snapshot active(long watermark) {
        return new Snapshot(PARTITION, 3L, 10, 1, OptionalLong.of(watermark), false);
    }

    private static byte[] encode(Snapshot snapshot) {
        return PartitionRetirementAuthoritySnapshotCodec.encode(snapshot);
    }

    private static Snapshot decode(byte[] encoded, SharedPartitionId partition, long minimumOffset) {
        return PartitionRetirementAuthoritySnapshotCodec.decode(encoded, partition, minimumOffset);
    }

    private static ByteBuffer view(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
    }

    private static void fixChecksum(byte[] encoded) {
        CRC32C crc = new CRC32C();
        crc.update(encoded, 0, 52);
        view(encoded).putInt(52, (int) crc.getValue());
    }
}
