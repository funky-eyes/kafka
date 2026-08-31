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
 * Physical layout math for a fixed-capacity circular WAL.
 *
 * <p>Logical offsets are monotonically increasing and never wrap. Only the physical data region wraps. A record is
 * always physically contiguous: when the remaining bytes at the end of the ring cannot hold the full encoded record,
 * the allocator consumes that tail as padding and starts the record at the beginning of the next generation. Padding
 * remains part of the retained logical window until the head advances past it, so reuse can never overwrite an unsafe
 * record merely because there is fragmented physical space.</p>
 */
final class RingWalLayout {
    static final long DEFAULT_TOTAL_CAPACITY_BYTES = 2L * 1024 * 1024 * 1024;
    static final int SUPERBLOCK_BYTES = 4 * 1024;
    static final int SUPERBLOCK_COPIES = 2;
    static final long DATA_START = (long) SUPERBLOCK_BYTES * SUPERBLOCK_COPIES;

    private final long totalCapacityBytes;
    private final long dataCapacityBytes;

    RingWalLayout(long totalCapacityBytes) {
        long minimumCapacity = Math.addExact(DATA_START, WalRecordCodec.MIN_RECORD_BYTES);
        if (totalCapacityBytes < minimumCapacity) {
            throw new IllegalArgumentException(
                "ring WAL capacity is too small: capacity=" + totalCapacityBytes + ", minimum=" + minimumCapacity);
        }
        this.totalCapacityBytes = totalCapacityBytes;
        this.dataCapacityBytes = totalCapacityBytes - DATA_START;
    }

    long totalCapacityBytes() {
        return totalCapacityBytes;
    }

    long dataCapacityBytes() {
        return dataCapacityBytes;
    }

    long retainedBytes(long headOffset, long tailOffset) {
        validateWindow(headOffset, tailOffset);
        return tailOffset - headOffset;
    }

    long availableBytes(long headOffset, long tailOffset) {
        return dataCapacityBytes - retainedBytes(headOffset, tailOffset);
    }

    PhysicalAddress address(long logicalOffset) {
        if (logicalOffset < 0) {
            throw new IllegalArgumentException("logicalOffset must be non-negative");
        }
        long generation = logicalOffset / dataCapacityBytes;
        long positionInRing = logicalOffset % dataCapacityBytes;
        return new PhysicalAddress(generation, Math.addExact(DATA_START, positionInRing));
    }

    Allocation allocate(long headOffset, long tailOffset, int encodedLength) {
        validateWindow(headOffset, tailOffset);
        if (encodedLength <= 0 || encodedLength > dataCapacityBytes) {
            throw new IllegalArgumentException(
                "encodedLength must be in [1, " + dataCapacityBytes + "]: " + encodedLength);
        }

        long positionInRing = tailOffset % dataCapacityBytes;
        long remainingAtEnd = dataCapacityBytes - positionInRing;
        long paddingBytes = encodedLength > remainingAtEnd ? remainingAtEnd : 0L;
        long recordOffset = Math.addExact(tailOffset, paddingBytes);
        long nextTailOffset = Math.addExact(recordOffset, encodedLength);
        long retainedAfterAppend = nextTailOffset - headOffset;
        if (retainedAfterAppend > dataCapacityBytes) {
            throw new WalCapacityExceededException(
                "Ring WAL capacity exceeded: head=" + headOffset +
                    ", tail=" + tailOffset +
                    ", padding=" + paddingBytes +
                    ", recordBytes=" + encodedLength +
                    ", retainedAfterAppend=" + retainedAfterAppend +
                    ", dataCapacity=" + dataCapacityBytes);
        }

        PhysicalAddress physical = address(recordOffset);
        return new Allocation(
            recordOffset,
            physical.generation(),
            physical.position(),
            encodedLength,
            paddingBytes,
            nextTailOffset
        );
    }

    private void validateWindow(long headOffset, long tailOffset) {
        if (headOffset < 0 || tailOffset < headOffset) {
            throw new IllegalArgumentException(
                "invalid logical WAL window: head=" + headOffset + ", tail=" + tailOffset);
        }
        long retained = tailOffset - headOffset;
        if (retained > dataCapacityBytes) {
            throw new IllegalArgumentException(
                "logical WAL window exceeds ring capacity: retained=" + retained +
                    ", dataCapacity=" + dataCapacityBytes);
        }
    }

    record PhysicalAddress(long generation, long position) {
        PhysicalAddress {
            if (generation < 0 || position < DATA_START) {
                throw new IllegalArgumentException("invalid ring WAL physical address");
            }
        }
    }

    record Allocation(
        long walOffset,
        long generation,
        long physicalPosition,
        int encodedLength,
        long paddingBytes,
        long nextTailOffset
    ) {
        Allocation {
            if (walOffset < 0 || generation < 0 || physicalPosition < DATA_START || encodedLength <= 0 ||
                paddingBytes < 0 || nextTailOffset <= walOffset) {
                throw new IllegalArgumentException("invalid ring WAL allocation");
            }
        }

        long reservedBytes() {
            return Math.addExact(paddingBytes, encodedLength);
        }
    }
}
