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
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe.Request;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe.Report;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementSourceProbeValueTest {
    private static final Uuid NONCE = new Uuid(1L, 2L);
    private static final Uuid TOPIC_ID = new Uuid(3L, 4L);
    private static final Uuid INCARNATION = new Uuid(5L, 6L);

    @Test
    void requestValuesRemainEqualAcrossReconstructedJava11Instances() {
        Request left = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        Request right = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        assertEquals(left, left);
        assertEquals(left, right);
        assertEquals(right, left);
        assertEquals(left.hashCode(), right.hashCode());
        assertFalse(left.equals(null));
        assertFalse(left.equals("not a request"));
    }

    @Test
    void changedNonceOrTopicCannotShareRequestEquality() {
        Request current = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        assertNotEquals(current, request(new Uuid(9L, 10L), TOPIC_ID,
            3, 54L, INCARNATION, 8, 20L, 0L));
        assertNotEquals(current, request(NONCE, new Uuid(11L, 12L),
            3, 54L, INCARNATION, 8, 20L, 0L));
    }

    @Test
    void changedBrokerEpochIncarnationAndLeaderNeverShareRequestEquality() {
        Request current = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        assertNotEquals(current, request(NONCE, TOPIC_ID, 4, 54L, INCARNATION, 8, 20L, 0L));
        assertNotEquals(current, request(NONCE, TOPIC_ID, 3, 55L, INCARNATION, 8, 20L, 0L));
        assertNotEquals(current, request(NONCE, TOPIC_ID,
            3, 54L, new Uuid(5L, 7L), 8, 20L, 0L));
        assertNotEquals(current, request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 9, 20L, 0L));
    }

    @Test
    void changedKRaftWritePositionOrKafkaLogStartIsNotEqual() {
        Request current = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        assertNotEquals(current, request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 21L, 0L));
        assertNotEquals(current, request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 1L));
    }

    @Test
    void reportEqualityTracksSourceAndWalRevisionCoordinates() {
        Request first = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        Request second = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        Report original = new Report(first, 1L, 0L, 10L, 10L, 2L, 3L);
        Report reconstructed = new Report(second, 1L, 0L, 10L, 10L, 2L, 3L);
        assertEquals(original, original);
        assertEquals(original, reconstructed);
        assertEquals(reconstructed, original);
        assertEquals(original.hashCode(), reconstructed.hashCode());
        assertNotEquals(original, new Report(second, 2L, 0L, 10L, 10L, 2L, 3L));
        assertNotEquals(original, new Report(second, 1L, 0L, 11L, 11L, 2L, 3L));
        assertNotEquals(original, new Report(second, 1L, 0L, 10L, 11L, 2L, 3L));
        assertNotEquals(original, new Report(second, 1L, 0L, 10L, 10L, 3L, 3L));
        assertNotEquals(original, new Report(second, 1L, 0L, 10L, 10L, 2L, 4L));
        assertFalse(original.equals(null));
        assertFalse(original.equals("not a report"));
    }

    @Test
    void requestRejectsZeroIdentitiesBlankClusterAndInvalidCasBounds() {
        assertThrows(IllegalArgumentException.class, () ->
            request(Uuid.ZERO_UUID, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            request(NONCE, Uuid.ZERO_UUID, 3, 54L, INCARNATION, 8, 20L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            request(NONCE, TOPIC_ID, 3, 54L, Uuid.ZERO_UUID, 8, 20L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new Request(NONCE, "", TOPIC_ID, 0, 3, 54L,
                INCARNATION, 8, -1L, 20L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 0L, 1L));
    }

    @Test
    void requestRejectsNegativeBrokerOrUnknownSourceLeader() {
        assertThrows(IllegalArgumentException.class, () ->
            request(NONCE, TOPIC_ID, -1, 54L, INCARNATION, 8, 20L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            request(NONCE, TOPIC_ID, 3, -1L, INCARNATION, 8, 20L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, -1, 20L, 0L));
        assertThrows(NullPointerException.class, () ->
            new Request(null, "cluster-a", TOPIC_ID, 0, 3, 54L,
                INCARNATION, 8, -1L, 20L, 0L));
    }

    @Test
    void reportRejectsEmptySourceMissingWalAndInconsistentLogEnd() {
        Request source = request(NONCE, TOPIC_ID, 3, 54L, INCARNATION, 8, 20L, 0L);
        assertThrows(IllegalArgumentException.class, () ->
            new Report(source, 1L, 0L, 0L, 0L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new Report(source, 1L, 1L, 10L, 10L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new Report(source, 1L, 0L, 10L, 9L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new Report(source, 0L, 0L, 10L, 10L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () ->
            new Report(source, 1L, 0L, 10L, 10L, -1L, 0L));
        assertThrows(NullPointerException.class, () ->
            new Report(null, 1L, 0L, 10L, 10L, 0L, 0L));
        assertTrue(new Report(source, 1L, 0L, 10L, 10L, 0L, 0L).nativeHighWatermark() > 0L);
    }

    private static Request request(
        Uuid nonce,
        Uuid topicId,
        int brokerId,
        long brokerEpoch,
        Uuid incarnation,
        int leaderEpoch,
        long nextWriteOffset,
        long logStartOffset
    ) {
        return new Request(
            nonce, "cluster-a", topicId, 0, brokerId, brokerEpoch, incarnation,
            leaderEpoch, -1L, nextWriteOffset, logStartOffset
        );
    }
}
