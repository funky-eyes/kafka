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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public final class WalRecord {
    private static final ByteBuffer EMPTY_PAYLOAD = ByteBuffer.allocate(0).asReadOnlyBuffer();

    private final WalRecordType type;
    private final long topicIdHigh;
    private final long topicIdLow;
    private final int partition;
    private final int leaderEpoch;
    private final long firstOffset;
    private final long lastOffset;
    private final ByteBuffer payload;

    private WalRecord(
        WalRecordType type,
        long topicIdHigh,
        long topicIdLow,
        int partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer payload
    ) {
        this.type = Objects.requireNonNull(type, "type");
        this.topicIdHigh = topicIdHigh;
        this.topicIdLow = topicIdLow;
        this.partition = partition;
        this.leaderEpoch = leaderEpoch;
        this.firstOffset = firstOffset;
        this.lastOffset = lastOffset;
        this.payload = Objects.requireNonNull(payload, "payload").slice().asReadOnlyBuffer();
    }

    public static WalRecord data(
        long topicIdHigh,
        long topicIdLow,
        int partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        byte[] payload
    ) {
        Objects.requireNonNull(payload, "payload");
        return dataOwned(
            topicIdHigh,
            topicIdLow,
            partition,
            leaderEpoch,
            firstOffset,
            lastOffset,
            ByteBuffer.wrap(Arrays.copyOf(payload, payload.length))
        );
    }

    /** Creates a DATA record by copying the supplied buffer. */
    public static WalRecord data(
        long topicIdHigh,
        long topicIdLow,
        int partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer payload
    ) {
        Objects.requireNonNull(payload, "payload");
        ByteBuffer source = payload.duplicate();
        ByteBuffer owned = ByteBuffer.allocate(source.remaining());
        owned.put(source).flip();
        return dataOwned(topicIdHigh, topicIdLow, partition, leaderEpoch, firstOffset, lastOffset, owned);
    }

    /**
     * Creates a DATA record without copying payload bytes.
     *
     * <p>The caller transfers ownership of the bytes reachable through {@code payload}. They must not be modified
     * after this call. This API exists for the Kafka adapter, which serializes each RecordBatch into a dedicated
     * owned buffer before handing it to the WAL.</p>
     */
    public static WalRecord dataOwned(
        long topicIdHigh,
        long topicIdLow,
        int partition,
        int leaderEpoch,
        long firstOffset,
        long lastOffset,
        ByteBuffer payload
    ) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        if (firstOffset < 0 || lastOffset < firstOffset) {
            throw new IllegalArgumentException("invalid offset range [" + firstOffset + ", " + lastOffset + "]");
        }
        if (payload == null || !payload.hasRemaining()) {
            throw new IllegalArgumentException("DATA record payload must not be empty");
        }
        return new WalRecord(
            WalRecordType.DATA,
            topicIdHigh,
            topicIdLow,
            partition,
            leaderEpoch,
            firstOffset,
            lastOffset,
            payload
        );
    }

    public static WalRecord truncate(
        long topicIdHigh,
        long topicIdLow,
        int partition,
        int leaderEpoch,
        long truncateOffset
    ) {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative");
        }
        if (truncateOffset < 0) {
            throw new IllegalArgumentException("truncateOffset must be non-negative");
        }
        return new WalRecord(
            WalRecordType.TRUNCATE,
            topicIdHigh,
            topicIdLow,
            partition,
            leaderEpoch,
            truncateOffset,
            truncateOffset,
            EMPTY_PAYLOAD
        );
    }

    /**
     * Commit marker for one atomic append group.
     *
     * <p>GROUP_COMMIT is an internal WAL control record. Its group id is encoded in topicIdHigh and its record count
     * in partition; the remaining fields are zero. Data/control records preceding this marker become replay-visible
     * only after the marker itself is durable.</p>
     */
    public static WalRecord groupCommit(long groupId, int recordCount) {
        if (groupId < 0) {
            throw new IllegalArgumentException("groupId must be non-negative");
        }
        if (recordCount <= 0) {
            throw new IllegalArgumentException("recordCount must be positive");
        }
        return new WalRecord(
            WalRecordType.GROUP_COMMIT,
            groupId,
            0L,
            recordCount,
            0,
            0L,
            0L,
            EMPTY_PAYLOAD
        );
    }

    public WalRecordType type() {
        return type;
    }

    public long topicIdHigh() {
        return topicIdHigh;
    }

    public long topicIdLow() {
        return topicIdLow;
    }

    public int partition() {
        return partition;
    }

    public int leaderEpoch() {
        return leaderEpoch;
    }

    public long firstOffset() {
        return firstOffset;
    }

    public long lastOffset() {
        return lastOffset;
    }

    public long truncateOffset() {
        if (type != WalRecordType.TRUNCATE) {
            throw new IllegalStateException("Not a TRUNCATE record");
        }
        return firstOffset;
    }

    public long groupId() {
        if (type != WalRecordType.GROUP_COMMIT) {
            throw new IllegalStateException("Not a GROUP_COMMIT record");
        }
        return topicIdHigh;
    }

    public int groupRecordCount() {
        if (type != WalRecordType.GROUP_COMMIT) {
            throw new IllegalStateException("Not a GROUP_COMMIT record");
        }
        return partition;
    }

    public ByteBuffer payload() {
        return payload.asReadOnlyBuffer();
    }

    ByteBuffer payloadUnsafe() {
        return payload.duplicate();
    }
}
