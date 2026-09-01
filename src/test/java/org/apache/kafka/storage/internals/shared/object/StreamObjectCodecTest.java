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

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamObjectCodecTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(1, 2, 0);
    private static final SharedPartitionId P1 = new SharedPartitionId(3, 4, 1);

    @Test
    void shouldEncodeCrossPartitionBlocksWithSelfContainedIndexAndFooter() {
        ByteBuffer encoded = StreamObjectCodec.encode(42L, List.of(
            block(P0, 3, 10, 20, 1, 2, 3),
            block(P1, 4, 30, 40, 4, 5)
        ));

        StreamObjectCodec.DecodedObject decoded = StreamObjectCodec.decode(encoded);
        assertEquals(42L, decoded.objectId());
        assertEquals(2, decoded.indexes().size());
        assertEquals(encoded.remaining() - StreamObjectCodec.FOOTER_BYTES, decoded.footer().footerPosition());
        assertEquals(decoded.footer().indexStart(), decoded.footer().dataLength());

        StreamObjectCodec.BlockIndex first = decoded.indexes().get(0);
        StreamObjectCodec.BlockIndex second = decoded.indexes().get(1);
        assertEquals(P0, first.partition());
        assertEquals(10L, first.startOffset());
        assertEquals(20L, first.endOffset());
        assertEquals(P1, second.partition());
        assertEquals(30L, second.startOffset());
        assertEquals(40L, second.endOffset());
        assertTrue(second.blockPosition() > first.blockPosition());

        assertArrayEquals(new byte[]{1, 2, 3}, bytes(StreamObjectCodec.readBlock(encoded, first)));
        assertArrayEquals(new byte[]{4, 5}, bytes(StreamObjectCodec.readBlock(encoded, second)));
    }

    @Test
    void shouldRejectPayloadCorruptionBeforePublishingBlock() {
        ByteBuffer encoded = StreamObjectCodec.encode(7L, List.of(
            block(P0, 2, 100, 101, 9, 8, 7)
        ));
        StreamObjectCodec.DecodedObject decoded = StreamObjectCodec.decode(encoded);
        StreamObjectCodec.BlockIndex index = decoded.indexes().get(0);

        ByteBuffer corrupted = writableCopy(encoded);
        int payloadPosition = Math.toIntExact(index.blockPosition()) + StreamObjectCodec.DATA_BLOCK_HEADER_BYTES;
        corrupted.put(payloadPosition, (byte) 0x55);

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> StreamObjectCodec.decode(corrupted)
        );
        assertTrue(error.getMessage().contains("checksum"));
    }

    @Test
    void shouldRejectFooterOrIndexTruncation() {
        ByteBuffer encoded = StreamObjectCodec.encode(9L, List.of(
            block(P0, 1, 0, 1, 1)
        ));
        ByteBuffer truncated = encoded.duplicate();
        truncated.limit(encoded.limit() - 8);

        assertThrows(IllegalArgumentException.class, () -> StreamObjectCodec.decode(truncated));
    }

    private static StreamObjectCodec.DataBlock block(
        SharedPartitionId partition,
        int leaderEpoch,
        long startOffset,
        long endOffset,
        int... payload
    ) {
        byte[] bytes = new byte[payload.length];
        for (int i = 0; i < payload.length; i++) {
            bytes[i] = (byte) payload[i];
        }
        return new StreamObjectCodec.DataBlock(
            partition,
            leaderEpoch,
            startOffset,
            endOffset,
            ByteBuffer.wrap(bytes)
        );
    }

    private static ByteBuffer writableCopy(ByteBuffer source) {
        ByteBuffer copy = ByteBuffer.allocate(source.remaining());
        copy.put(source.duplicate());
        copy.flip();
        return copy;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
