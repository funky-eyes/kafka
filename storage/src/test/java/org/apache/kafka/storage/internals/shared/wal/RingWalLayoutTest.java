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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingWalLayoutTest {
    @Test
    void defaultCapacityReservesOnlyTwoSuperblocksOutsideDataRing() {
        RingWalLayout layout = new RingWalLayout(RingWalLayout.DEFAULT_TOTAL_CAPACITY_BYTES);

        assertEquals(2L * 1024 * 1024 * 1024, layout.totalCapacityBytes());
        assertEquals(
            RingWalLayout.DEFAULT_TOTAL_CAPACITY_BYTES - 2L * RingWalLayout.SUPERBLOCK_BYTES,
            layout.dataCapacityBytes()
        );
    }

    @Test
    void mapsMonotonicLogicalOffsetsAcrossPhysicalGenerations() {
        RingWalLayout layout = layout(128);

        assertEquals(
            new RingWalLayout.PhysicalAddress(0, RingWalLayout.DATA_START + 17),
            layout.address(17)
        );
        assertEquals(
            new RingWalLayout.PhysicalAddress(1, RingWalLayout.DATA_START + 17),
            layout.address(145)
        );
        assertEquals(
            new RingWalLayout.PhysicalAddress(3, RingWalLayout.DATA_START),
            layout.address(384)
        );
    }

    @Test
    void consumesTailPaddingBeforeStartingContiguousRecordInNextGeneration() {
        RingWalLayout layout = layout(128);

        RingWalLayout.Allocation first = layout.allocate(0, 0, 100);
        assertEquals(0, first.walOffset());
        assertEquals(RingWalLayout.DATA_START, first.physicalPosition());
        assertEquals(100, first.nextTailOffset());
        assertEquals(0, first.paddingBytes());

        RingWalLayout.Allocation wrapped = layout.allocate(100, 100, 40);
        assertEquals(128, wrapped.walOffset());
        assertEquals(1, wrapped.generation());
        assertEquals(RingWalLayout.DATA_START, wrapped.physicalPosition());
        assertEquals(28, wrapped.paddingBytes());
        assertEquals(68, wrapped.reservedBytes());
        assertEquals(168, wrapped.nextTailOffset());
    }

    @Test
    void refusesWrapWhenPaddingWouldOverwriteUnreclaimedHead() {
        RingWalLayout layout = layout(128);
        RingWalLayout.Allocation first = layout.allocate(0, 0, 100);

        WalCapacityExceededException full = assertThrows(
            WalCapacityExceededException.class,
            () -> layout.allocate(0, first.nextTailOffset(), 40)
        );
        assertEquals(28, 128 - (first.nextTailOffset() % 128));
        assertEquals(28, layout.availableBytes(0, first.nextTailOffset()));
        org.junit.jupiter.api.Assertions.assertTrue(full.getMessage().contains("padding=28"));
    }

    @Test
    void reclaimedHeadAllowsPhysicalSlotReuseAtHigherGeneration() {
        RingWalLayout layout = layout(128);
        RingWalLayout.Allocation first = layout.allocate(0, 0, 100);

        RingWalLayout.Allocation reused = layout.allocate(100, first.nextTailOffset(), 40);
        assertEquals(128, reused.walOffset());
        assertEquals(1, reused.generation());
        assertEquals(RingWalLayout.DATA_START, reused.physicalPosition());
        assertEquals(68, layout.retainedBytes(100, reused.nextTailOffset()));
    }

    @Test
    void rejectsRecoveredWindowLargerThanPhysicalRing() {
        RingWalLayout layout = layout(128);

        assertThrows(IllegalArgumentException.class, () -> layout.retainedBytes(0, 129));
        assertThrows(IllegalArgumentException.class, () -> layout.allocate(10, 9, 10));
        assertThrows(IllegalArgumentException.class, () -> layout.allocate(0, 0, 129));
    }

    private static RingWalLayout layout(long dataCapacityBytes) {
        return new RingWalLayout(Math.addExact(RingWalLayout.DATA_START, dataCapacityBytes));
    }
}
