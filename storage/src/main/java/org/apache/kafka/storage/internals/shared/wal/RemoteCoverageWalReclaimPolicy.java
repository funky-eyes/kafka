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

import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Reclaims WAL DATA only when authoritative remote metadata matches its offset range,
 * leader epoch, payload length and CRC32C. CRC32C detects accidental mismatches;
 * it is not a cryptographic identity proof.
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
            case DATA -> matchesRecordUnchecked(record, record.payload());
            case TRUNCATE -> true;
            case GROUP_COMMIT -> throw new IllegalArgumentException(
                "GROUP_COMMIT is internal WAL framing and is not reclaim-policy input");
        };
    }

    record DataIdentity(SharedPartitionId partition, long firstOffset, long lastOffset, int leaderEpoch,
                        int payloadLength, long checksum) { }

    /** The payload must be the verified body of this record from the currently pinned block. */
    boolean matchesRecord(WalRecord record, ByteBuffer payload) {
        requireRecordPayload(record, payload);
        return matchesRecordUnchecked(record, payload.duplicate());
    }

    private boolean matchesRecordUnchecked(WalRecord record, ByteBuffer payload) {
        var reference = remoteIndex.find(partition(record), record.firstOffset());
        if (reference.isEmpty()) return false;
        SharedObjectRange range = reference.get().range();
        if (!sameBatchShape(range, record.firstOffset(), record.lastOffset(), record.leaderEpoch(), payload.remaining()))
            return false;
        CRC32C checksum = new CRC32C();
        checksum.update(payload);
        return range.checksum() == checksum.getValue();
    }

    static DataIdentity identity(WalRecord record, ByteBuffer payload) {
        requireRecordPayload(record, payload);
        int length = payload.remaining();
        CRC32C checksum = new CRC32C();
        checksum.update(payload.duplicate());
        return new DataIdentity(partition(record), record.firstOffset(), record.lastOffset(), record.leaderEpoch(),
            length, checksum.getValue());
    }

    private static void requireRecordPayload(WalRecord record, ByteBuffer payload) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(payload, "payload");
        if (record.type() != WalRecordType.DATA || payload.remaining() != record.payloadUnsafe().remaining())
            throw new IllegalArgumentException("Expected the complete DATA payload from a validated WAL record");
    }

    boolean matches(DataIdentity expected) {
        var reference = remoteIndex.find(expected.partition(), expected.firstOffset());
        if (reference.isEmpty()) return false;
        SharedObjectRange range = reference.get().range();
        return sameBatchShape(range, expected.firstOffset(), expected.lastOffset(), expected.leaderEpoch(),
            expected.payloadLength()) &&
            range.checksum() == expected.checksum();
    }

    private static boolean sameBatchShape(SharedObjectRange range, long firstOffset, long lastOffset,
                                          int leaderEpoch, int payloadLength) {
        return range.offsets().startOffset() == firstOffset && range.offsets().endOffset() - 1 == lastOffset &&
            range.leaderEpoch() == leaderEpoch && range.objectLength() == payloadLength;
    }

    private static SharedPartitionId partition(WalRecord record) {
        return new SharedPartitionId(
            record.topicIdHigh(),
            record.topicIdLow(),
            record.partition()
        );
    }
}
