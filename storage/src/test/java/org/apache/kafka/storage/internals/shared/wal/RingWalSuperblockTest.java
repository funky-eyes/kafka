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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingWalSuperblockTest {
    @Test
    void roundTripsDurableLogicalWindow() throws Exception {
        RingWalSuperblock.State state = new RingWalSuperblock.State(7, 128, 900, 1024);

        assertEquals(state, RingWalSuperblock.decode(RingWalSuperblock.encode(state)));
        assertEquals(1, RingWalSuperblock.copyIndex(state.sequence()));
        assertEquals(0, RingWalSuperblock.copyIndex(state.next(200, 950).sequence()));
    }

    @Test
    void rejectsLegacyRingFormatVersion() {
        RingWalSuperblock.State state = new RingWalSuperblock.State(7, 128, 900, 1024);
        ByteBuffer legacy = mutableCopy(RingWalSuperblock.encode(state)).order(ByteOrder.BIG_ENDIAN);
        legacy.putShort(4, (short) 1);
        legacy.position(0);

        assertThrows(WalCorruptionException.class, () -> RingWalSuperblock.decode(legacy));
    }

    @Test
    void selectsHighestValidSequence() throws Exception {
        RingWalSuperblock.State older = new RingWalSuperblock.State(10, 100, 700, 1024);
        RingWalSuperblock.State newer = new RingWalSuperblock.State(11, 200, 900, 1024);

        assertEquals(
            newer,
            RingWalSuperblock.selectNewest(
                RingWalSuperblock.encode(older),
                RingWalSuperblock.encode(newer),
                1024
            )
        );
    }

    @Test
    void fallsBackToPreviousCopyWhenLatestCheckpointIsTorn() throws Exception {
        RingWalSuperblock.State older = new RingWalSuperblock.State(20, 256, 800, 1024);
        RingWalSuperblock.State newer = new RingWalSuperblock.State(21, 300, 1000, 1024);
        ByteBuffer torn = mutableCopy(RingWalSuperblock.encode(newer));
        torn.putLong(24, 999L);
        torn.position(0);

        assertEquals(
            older,
            RingWalSuperblock.selectNewest(
                RingWalSuperblock.encode(older),
                torn,
                1024
            )
        );
    }

    @Test
    void rejectsCopiesFromDifferentConfiguredRingCapacity() {
        RingWalSuperblock.State first = new RingWalSuperblock.State(1, 0, 10, 1024);
        RingWalSuperblock.State second = new RingWalSuperblock.State(2, 0, 10, 2048);

        assertThrows(
            WalCorruptionException.class,
            () -> RingWalSuperblock.selectNewest(
                RingWalSuperblock.encode(first),
                RingWalSuperblock.encode(second),
                4096
            )
        );
    }

    @Test
    void detectsConflictingCopiesWithSameSequence() {
        RingWalSuperblock.State first = new RingWalSuperblock.State(30, 100, 700, 1024);
        RingWalSuperblock.State second = new RingWalSuperblock.State(30, 200, 700, 1024);

        assertThrows(
            WalCorruptionException.class,
            () -> RingWalSuperblock.selectNewest(
                RingWalSuperblock.encode(first),
                RingWalSuperblock.encode(second),
                1024
            )
        );
    }

    @Test
    void rejectsWhenBothCopiesFailChecksumValidation() {
        ByteBuffer first = mutableCopy(RingWalSuperblock.encode(new RingWalSuperblock.State(1, 0, 10, 1024)));
        ByteBuffer second = mutableCopy(RingWalSuperblock.encode(new RingWalSuperblock.State(2, 0, 20, 1024)));
        first.put(8, (byte) (first.get(8) ^ 0x01));
        second.put(16, (byte) (second.get(16) ^ 0x01));
        first.position(0);
        second.position(0);

        assertThrows(
            WalCorruptionException.class,
            () -> RingWalSuperblock.selectNewest(first, second, 1024)
        );
    }

    private static ByteBuffer mutableCopy(ByteBuffer source) {
        ByteBuffer duplicate = source.duplicate();
        byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return ByteBuffer.wrap(bytes);
    }
}
