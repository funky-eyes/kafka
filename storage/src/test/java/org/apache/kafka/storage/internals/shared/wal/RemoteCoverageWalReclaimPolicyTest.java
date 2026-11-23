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
package org.apache.kafka.storage.internals.shared.wal;

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RemoteCoverageWalReclaimPolicyTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11, 22, 3);
    private static final WalAppendResult LOCATION = new WalAppendResult(4, 100, 200);

    @Test
    void dataMustRemainUntilTheExactRangeIsAuthoritativelyCovered() {
        RemoteObjectIndex remoteIndex = new RemoteObjectIndex();
        RemoteCoverageWalReclaimPolicy policy = new RemoteCoverageWalReclaimPolicy(remoteIndex);
        WalRecord record = WalRecord.data(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(),
            7, 100, 109, new byte[]{1}
        );

        assertFalse(policy.canReclaim(record, LOCATION));

        remoteIndex.add(new SharedObjectMetadata(
            1,
            1,
            123,
            List.of(new SharedObjectRange(
                PARTITION,
                new OffsetRange(100, 110),
                7,
                0,
                1,
                checksum(new byte[] {1})
            ))
        ));

        assertTrue(policy.canReclaim(record, LOCATION));
        assertFalse(policy.canReclaim(WalRecord.data(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(),
            7, 100, 109, new byte[] {2}), LOCATION), "different bytes cannot be reclaimed");
        assertFalse(policy.canReclaim(WalRecord.data(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(),
            8, 100, 109, new byte[] {1}), LOCATION), "different epochs cannot be reclaimed");
        assertFalse(policy.canReclaim(WalRecord.data(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(),
            7, 101, 109, new byte[] {1}), LOCATION), "partial offset coverage is not the same batch");
        assertFalse(policy.canReclaim(WalRecord.data(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(),
            7, 100, 109, new byte[] {1, 2}), LOCATION), "different lengths cannot be reclaimed");
    }

    @Test
    void verifiedBlockSliceUsesOnlyPayloadAndDoesNotChangeItsBounds() {
        byte[] data = new byte[] {9, 1, 2, 8};
        ByteBuffer payload = ByteBuffer.wrap(data);
        payload.position(1).limit(3);
        WalRecord record = WalRecord.data(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(), 7, 100, 109, new byte[] {1, 2});
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.add(new SharedObjectMetadata(1, 2, 0, List.of(new SharedObjectRange(
            PARTITION, new OffsetRange(100, 110), 7, 0, 2, checksum(new byte[] {1, 2})))));
        var policy = new RemoteCoverageWalReclaimPolicy(index);
        assertTrue(policy.matchesRecord(record, payload));
        assertTrue(policy.matches(RemoteCoverageWalReclaimPolicy.identity(record, payload)));
        assertEquals(1, payload.position());
        assertEquals(3, payload.limit());
        assertFalse(policy.matchesRecord(record, ByteBuffer.wrap(new byte[] {2, 1})));
        assertThrows(IllegalArgumentException.class, () -> policy.matchesRecord(record, ByteBuffer.wrap(new byte[] {1})));
        assertThrows(IllegalArgumentException.class, () -> policy.matchesRecord(
            WalRecord.truncate(PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(), 7, 100),
            ByteBuffer.allocate(0)));
    }

    @Test
    void truncateCanLeaveWithAnAlreadySafePhysicalPrefix() {
        RemoteCoverageWalReclaimPolicy policy = new RemoteCoverageWalReclaimPolicy(new RemoteObjectIndex());
        WalRecord truncate = WalRecord.truncate(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(), 8, 120
        );
        assertTrue(policy.canReclaim(truncate, LOCATION));
    }

    private static long checksum(byte[] value) {
        CRC32C crc = new CRC32C();
        crc.update(value, 0, value.length);
        return crc.getValue();
    }
}
