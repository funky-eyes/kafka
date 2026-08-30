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
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.util.Objects;

/**
 * Reclaims WAL DATA only after authoritative remote metadata covers the exact logical Kafka range.
 *
 * <p>TRUNCATE is a local recovery control record. Because physical reclamation is prefix-only, reaching a TRUNCATE
 * means every older group was already judged reclaimable; deleting the control record together with that prefix cannot
 * resurrect an older physical record. GROUP_COMMIT is framing owned by the WAL and must never be passed to this policy.</p>
 */
public final class RemoteCoverageWalReclaimPolicy implements WalReclaimPolicy {
    private final RemoteObjectIndex remoteIndex;

    public RemoteCoverageWalReclaimPolicy(RemoteObjectIndex remoteIndex) {
        this.remoteIndex = Objects.requireNonNull(remoteIndex, "remoteIndex");
    }

    @Override
    public boolean canReclaim(WalRecord record, WalAppendResult appendResult) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(appendResult, "appendResult");
        return switch (record.type()) {
            case DATA -> remoteIndex.coverage(partition(record)).covers(new OffsetRange(
                record.firstOffset(),
                Math.addExact(record.lastOffset(), 1L)
            ));
            case TRUNCATE -> true;
            case GROUP_COMMIT -> throw new IllegalArgumentException(
                "GROUP_COMMIT is internal WAL framing and is not reclaim-policy input");
        };
    }

    private static SharedPartitionId partition(WalRecord record) {
        return new SharedPartitionId(
            record.topicIdHigh(),
            record.topicIdLow(),
            record.partition()
        );
    }
}
