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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingPaddingMarkerTest {

    @Test
    void validatesExactLogicalBoundary() {
        ByteBuffer marker = RingPaddingMarker.encode(421L, 512L);
        assertDoesNotThrow(() -> RingPaddingMarker.validate(marker, 421L, 512L));
        assertThrows(WalCorruptionException.class, () -> RingPaddingMarker.validate(marker, 420L, 512L));
        assertThrows(WalCorruptionException.class, () -> RingPaddingMarker.validate(marker, 421L, 1024L));
    }

    @Test
    void detectsMarkerCorruption() {
        ByteBuffer encoded = RingPaddingMarker.encode(421L, 512L);
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);
        bytes[20] ^= 0x01;
        assertThrows(
            WalCorruptionException.class,
            () -> RingPaddingMarker.validate(ByteBuffer.wrap(bytes), 421L, 512L)
        );
    }

    @Test
    void refusesMarkerForSuffixThatCannotContainWalRecord() {
        assertThrows(
            IllegalArgumentException.class,
            () -> RingPaddingMarker.encode(512L - WalRecordCodec.MIN_RECORD_BYTES + 1L, 512L)
        );
    }
}
