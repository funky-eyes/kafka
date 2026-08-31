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

/**
 * Logical WAL address plus Kafka batch metadata used by the in-memory partition index.
 *
 * <p>The public identity is {@code walOffset}; no caller outside the WAL backend needs a physical file/position pair.
 * Package-private physical adapters remain only while the current file backend is being replaced.</p>
 */
public record WalLocation(
    long walOffset,
    int length,
    int payloadLength,
    int leaderEpoch,
    long firstOffset,
    long lastOffset
) {
    public WalLocation {
        if (walOffset < 0 || length < WalRecordCodec.HEADER_BYTES) {
            throw new IllegalArgumentException("invalid logical WAL location");
        }
        if (payloadLength < 0 || payloadLength != length - WalRecordCodec.HEADER_BYTES) {
            throw new IllegalArgumentException("invalid WAL payload length");
        }
        if (firstOffset < 0 || lastOffset < firstOffset) {
            throw new IllegalArgumentException("invalid logical Kafka range");
        }
    }

    public WalLocation(long walOffset, int length, int leaderEpoch, long firstOffset, long lastOffset) {
        this(walOffset, length, length - WalRecordCodec.HEADER_BYTES, leaderEpoch, firstOffset, lastOffset);
    }

    /** Backend-only adapter for the current rotating-file implementation. */
    WalLocation(
        long extentId,
        long position,
        int length,
        int payloadLength,
        int leaderEpoch,
        long firstOffset,
        long lastOffset
    ) {
        this(WalAppendResult.encodePhysical(extentId, position), length, payloadLength, leaderEpoch, firstOffset, lastOffset);
    }

    /** Backend-only adapter for the current rotating-file implementation. */
    WalLocation(long extentId, long position, int length, int leaderEpoch, long firstOffset, long lastOffset) {
        this(WalAppendResult.encodePhysical(extentId, position), length, leaderEpoch, firstOffset, lastOffset);
    }

    /** Backend-only bridge; physical layout must not escape this package. */
    long segmentId() {
        return WalAppendResult.extentId(walOffset);
    }

    /** Backend-only bridge; physical layout must not escape this package. */
    long position() {
        return WalAppendResult.extentPosition(walOffset);
    }

    public boolean contains(long offset) {
        return firstOffset <= offset && offset <= lastOffset;
    }
}
