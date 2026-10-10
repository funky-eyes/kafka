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

import org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModel.Snapshot;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.zip.CRC32C;

/**
 * Offline, versioned binary envelope for a hypothetical KRaft authority snapshot.
 *
 * <p>The CRC catches accidental corruption only; it is NOT authentication or proof of a
 * committed controller image. The source leader, generation, log-start and authority offset
 * are only values supplied to this codec. No production Kafka metadata writer, controller
 * snapshot hook, deletion or authorization path references this class.</p>
 *
 * <p>The fixed v1 encoding is big-endian, 56 bytes:
 * magic(4), version(2), flags(2), topicHigh(8), topicLow(8), partition(4),
 * authorityOffset(8), sourceEpoch(4), brokerId(4), watermark(8), CRC32C(4).
 * Flag 1 means an explicit watermark, flag 2 means permanently deleted. Unknown
 * versions, reserved flags, noncanonical absent values and trailing bytes fail closed.</p>
 */
public final class PartitionRetirementAuthoritySnapshotCodec {
    private static final int MAGIC = 0x52544155;
    private static final short VERSION = 1;
    private static final int FLAG_HAS_WATERMARK = 1;
    private static final int FLAG_TERMINAL = 2;
    private static final int FLAG_MASK = FLAG_HAS_WATERMARK | FLAG_TERMINAL;
    private static final int CONTENT_LENGTH = 52;
    public static final int ENCODED_LENGTH = 56;

    private PartitionRetirementAuthoritySnapshotCodec() {
    }

    /** Serializes only a state with a hypothetical accepted authority transition. */
    public static byte[] encode(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot");
        if (snapshot.authorityOffset() < 0L) {
            throw new IllegalArgumentException("Uncommitted initial state cannot be snapshotted as authority");
        }
        OptionalLong watermark = snapshot.explicitLogStart();
        int flags = (watermark.isPresent() ? FLAG_HAS_WATERMARK : 0)
            | (snapshot.terminallyDeleted() ? FLAG_TERMINAL : 0);
        SharedPartitionId partition = snapshot.partition();
        ByteBuffer out = ByteBuffer.allocate(ENCODED_LENGTH).order(ByteOrder.BIG_ENDIAN);
        out.putInt(MAGIC);
        out.putShort(VERSION);
        out.putShort((short) flags);
        out.putLong(partition.topicIdHigh());
        out.putLong(partition.topicIdLow());
        out.putInt(partition.partition());
        out.putLong(snapshot.authorityOffset());
        out.putInt(snapshot.maxSourceLeaderEpoch());
        out.putInt(snapshot.activeBrokerId());
        out.putLong(watermark.orElse(0L));
        out.putInt(crc(out.array()));
        return out.array();
    }

    /**
     * Validates an encoded checkpoint against a caller-supplied immutable Topic ID and
     * externally established minimum authority horizon. The horizon is untrusted input;
     * callers must obtain it from a truly committed controller state at recovery time.
     * A decoded snapshot never grants the caller permission to produce or delete data.
     */
    public static Snapshot decode(
        byte[] encoded,
        SharedPartitionId expectedPartition,
        long minimumAuthorityOffset
    ) {
        requireDecodableEnvelope(encoded, expectedPartition, minimumAuthorityOffset);
        ByteBuffer in = ByteBuffer.wrap(encoded).order(ByteOrder.BIG_ENDIAN);
        validateHeaderAndChecksum(in, encoded);
        int flags = readFlags(in);
        SharedPartitionId partition = readPartition(in, expectedPartition);
        long authorityOffset = readAuthorityOffset(in, minimumAuthorityOffset);
        int sourceLeaderEpoch = in.getInt();
        int brokerId = in.getInt();
        OptionalLong watermark = readWatermark(in, flags);
        return new Snapshot(
            partition, authorityOffset, sourceLeaderEpoch, brokerId,
            watermark, (flags & FLAG_TERMINAL) != 0
        );
    }

    private static void requireDecodableEnvelope(
        byte[] encoded,
        SharedPartitionId expectedPartition,
        long minimumAuthorityOffset
    ) {
        Objects.requireNonNull(encoded, "encoded");
        Objects.requireNonNull(expectedPartition, "expectedPartition");
        if (minimumAuthorityOffset < 0L) {
            throw new IllegalArgumentException("A proven non-negative authority horizon is required");
        }
        if (encoded.length != ENCODED_LENGTH) {
            throw new IllegalArgumentException("Invalid authority snapshot envelope length");
        }
    }

    private static void validateHeaderAndChecksum(ByteBuffer in, byte[] encoded) {
        if (in.getInt(CONTENT_LENGTH) != crc(encoded)) {
            throw new IllegalArgumentException("Authority snapshot CRC32C mismatch");
        }
        if (in.getInt() != MAGIC) {
            throw new IllegalArgumentException("Unknown authority snapshot magic");
        }
        if (in.getShort() != VERSION) {
            throw new IllegalArgumentException("Unsupported authority snapshot version");
        }
    }

    private static int readFlags(ByteBuffer in) {
        int flags = Short.toUnsignedInt(in.getShort());
        if ((flags & ~FLAG_MASK) != 0) {
            throw new IllegalArgumentException("Unknown authority snapshot flags");
        }
        return flags;
    }

    private static SharedPartitionId readPartition(ByteBuffer in, SharedPartitionId expected) {
        SharedPartitionId partition = new SharedPartitionId(in.getLong(), in.getLong(), in.getInt());
        if (!expected.equals(partition)) {
            throw new IllegalArgumentException("Authority snapshot belongs to another Topic ID or partition");
        }
        return partition;
    }

    private static long readAuthorityOffset(ByteBuffer in, long minimumAuthorityOffset) {
        long authorityOffset = in.getLong();
        if (authorityOffset < minimumAuthorityOffset) {
            throw new IllegalArgumentException("Authority snapshot is behind required committed horizon");
        }
        return authorityOffset;
    }

    private static OptionalLong readWatermark(ByteBuffer in, int flags) {
        long watermarkValue = in.getLong();
        boolean hasWatermark = (flags & FLAG_HAS_WATERMARK) != 0;
        if (!hasWatermark && watermarkValue != 0L) {
            throw new IllegalArgumentException("Absent watermark must use its canonical zero encoding");
        }
        return hasWatermark ? OptionalLong.of(watermarkValue) : OptionalLong.empty();
    }

    private static int crc(byte[] bytes) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, CONTENT_LENGTH);
        return (int) checksum.getValue();
    }
}
