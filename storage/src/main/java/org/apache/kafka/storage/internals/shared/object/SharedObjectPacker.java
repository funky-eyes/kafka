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
package org.apache.kafka.storage.internals.shared.object;

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Packs Kafka RecordBatch WAL entries from different topics and partitions into one immutable object.
 * Object boundaries are independent from Kafka LogSegment and WAL-segment boundaries.
 */
public final class SharedObjectPacker {
    static final int MAGIC = 0x4b534f31; // KSO1
    static final short VERSION = 1;
    static final int OBJECT_HEADER_BYTES = 16;
    static final int BLOCK_HEADER_BYTES = 48;

    public PackedObject pack(
        long objectId,
        List<SharedStorageEngine.UploadCandidate> candidates,
        SharedStorageEngine engine
    ) throws IOException {
        if (objectId < 0) {
            throw new IllegalArgumentException("objectId must be non-negative");
        }
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(engine, "engine");
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("candidates must not be empty");
        }

        List<WalRecord> records = engine.readUploadCandidates(candidates);
        if (records.size() != candidates.size()) {
            throw new IOException("WAL batch read returned an unexpected record count");
        }

        List<Block> blocks = new ArrayList<>(candidates.size());
        long totalSize = OBJECT_HEADER_BYTES;
        for (int i = 0; i < candidates.size(); i++) {
            SharedStorageEngine.UploadCandidate candidate = candidates.get(i);
            WalRecord record = records.get(i);
            if (record.firstOffset() != candidate.offsets().startOffset() ||
                Math.addExact(record.lastOffset(), 1) != candidate.offsets().endOffset()) {
                throw new IOException("WAL candidate logical range changed before packing: " + candidate);
            }
            ByteBuffer payload = record.payload();
            long checksum = crc32c(payload);
            blocks.add(new Block(candidate, record.leaderEpoch(), payload, checksum));
            totalSize = Math.addExact(totalSize, BLOCK_HEADER_BYTES + (long) payload.remaining());
        }
        if (totalSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Packed object exceeds Java ByteBuffer limit: " + totalSize);
        }

        ByteBuffer object = ByteBuffer.allocate((int) totalSize).order(ByteOrder.BIG_ENDIAN);
        object.putInt(MAGIC);
        object.putShort(VERSION);
        object.putShort((short) 0);
        object.putLong(objectId);

        List<SharedObjectRange> ranges = new ArrayList<>(blocks.size());
        for (Block block : blocks) {
            SharedStorageEngine.UploadCandidate candidate = block.candidate;
            ByteBuffer payload = block.payload.duplicate();
            object.putLong(candidate.partition().topicIdHigh());
            object.putLong(candidate.partition().topicIdLow());
            object.putInt(candidate.partition().partition());
            object.putInt(block.leaderEpoch);
            object.putLong(candidate.offsets().startOffset());
            object.putLong(candidate.offsets().endOffset());
            object.putInt(payload.remaining());
            object.putInt((int) block.checksum);
            long payloadPosition = object.position();
            int payloadLength = payload.remaining();
            object.put(payload);
            ranges.add(new SharedObjectRange(
                candidate.partition(),
                candidate.offsets(),
                block.leaderEpoch,
                payloadPosition,
                payloadLength,
                block.checksum
            ));
        }
        object.flip();
        long objectChecksum = crc32c(object);
        SharedObjectMetadata metadata = new SharedObjectMetadata(objectId, totalSize, objectChecksum, ranges);
        return new PackedObject(object, metadata);
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }

    private record Block(
        SharedStorageEngine.UploadCandidate candidate,
        int leaderEpoch,
        ByteBuffer payload,
        long checksum
    ) {
    }
}
