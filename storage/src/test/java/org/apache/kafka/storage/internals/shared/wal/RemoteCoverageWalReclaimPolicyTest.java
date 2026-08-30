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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
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
            10,
            123,
            List.of(new SharedObjectRange(
                PARTITION,
                new OffsetRange(100, 110),
                7,
                0,
                10,
                456
            ))
        ));

        assertTrue(policy.canReclaim(record, LOCATION));
    }

    @Test
    void truncateCanLeaveWithAnAlreadySafePhysicalPrefix() {
        RemoteCoverageWalReclaimPolicy policy = new RemoteCoverageWalReclaimPolicy(new RemoteObjectIndex());
        WalRecord truncate = WalRecord.truncate(
            PARTITION.topicIdHigh(), PARTITION.topicIdLow(), PARTITION.partition(), 8, 120
        );
        assertTrue(policy.canReclaim(truncate, LOCATION));
    }
}
