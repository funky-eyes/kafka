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
 * Stable logical address returned after a WAL record crosses the durability barrier.
 *
 * <p>The public contract is a monotonically increasing logical offset plus encoded length. Physical extent ids,
 * file positions, ring slots and generations are backend implementation details. Package-private physical helpers
 * exist only to bridge the current file backend while it is being replaced.</p>
 */
public record WalAppendResult(long offset, int length) {
    private static final int PHYSICAL_POSITION_BITS = 32;
    private static final long PHYSICAL_POSITION_MASK = (1L << PHYSICAL_POSITION_BITS) - 1L;
    private static final long MAX_PHYSICAL_EXTENT_ID = Integer.MAX_VALUE;

    public WalAppendResult {
        if (offset < 0 || length < WalRecordCodec.HEADER_BYTES) {
            throw new IllegalArgumentException("invalid logical WAL append result");
        }
    }

    /** Internal adapter used by the current rotating-file backend. */
    WalAppendResult(long extentId, long position, int length) {
        this(encodePhysical(extentId, position), length);
    }

    /** Backend-only bridge; physical layout must not escape this package. */
    long segmentId() {
        return extentId(offset);
    }

    /** Backend-only bridge; physical layout must not escape this package. */
    long position() {
        return extentPosition(offset);
    }

    static long encodePhysical(long extentId, long position) {
        if (extentId < 0 || extentId > MAX_PHYSICAL_EXTENT_ID) {
            throw new IllegalArgumentException("invalid WAL extent id: " + extentId);
        }
        if (position < 0 || position > PHYSICAL_POSITION_MASK) {
            throw new IllegalArgumentException("invalid WAL extent position: " + position);
        }
        return (extentId << PHYSICAL_POSITION_BITS) | position;
    }

    static long extentId(long offset) {
        return offset >>> PHYSICAL_POSITION_BITS;
    }

    static long extentPosition(long offset) {
        return offset & PHYSICAL_POSITION_MASK;
    }

    static long firstOffsetAfterExtent(long extentId) {
        return encodePhysical(Math.addExact(extentId, 1L), 0L);
    }
}
