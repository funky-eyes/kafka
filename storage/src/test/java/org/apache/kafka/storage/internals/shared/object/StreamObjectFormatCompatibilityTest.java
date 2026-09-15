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

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Golden-byte compatibility contract for the GA KSO2 v2 persistent object format.
 *
 * <p>Changing any expected byte here requires an explicit persistent-format version change and a compatible
 * migration/read strategy for objects already stored remotely.</p>
 */
class StreamObjectFormatCompatibilityTest {
    private static final long OBJECT_ID = 0x0102030405060708L;
    private static final long CHECKSUM = 0x11223344L;

    @Test
    void preservesGaV2BinaryLayout() throws Exception {
        ByteBuffer objectHeader = ByteBuffer.allocate(StreamObjectFormat.OBJECT_HEADER_BYTES);
        StreamObjectFormat.writeObjectHeader(objectHeader, OBJECT_ID);
        assertEncoding(
            "4b534f32000200000102030405060708",
            objectHeader
        );
        assertEquals(OBJECT_ID, StreamObjectFormat.readObjectId(bytes("4b534f32000200000102030405060708")));

        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 3);
        OffsetRange offsets = new OffsetRange(10L, 20L);

        ByteBuffer dataBlockHeader = ByteBuffer.allocate(StreamObjectFormat.DATA_BLOCK_HEADER_BYTES);
        StreamObjectFormat.writeDataBlockHeader(
            dataBlockHeader,
            partition,
            4,
            offsets,
            1,
            5,
            CHECKSUM
        );
        assertEncoding(
            "000000000000000100000000000000020000000300000004000000000000000a" +
                "000000000000001400000001000000051122334400000000",
            dataBlockHeader
        );

        ByteBuffer batchEntryHeader = ByteBuffer.allocate(StreamObjectFormat.BATCH_ENTRY_HEADER_BYTES);
        StreamObjectFormat.writeBatchEntryHeader(batchEntryHeader, offsets, 5, CHECKSUM);
        assertEncoding(
            "000000000000000a00000000000000140000000511223344",
            batchEntryHeader
        );

        StreamObjectFormat.DataBlockIndexEntry indexEntry = new StreamObjectFormat.DataBlockIndexEntry(
            partition,
            4,
            offsets,
            StreamObjectFormat.OBJECT_HEADER_BYTES,
            StreamObjectFormat.DATA_BLOCK_HEADER_BYTES,
            1,
            CHECKSUM
        );
        ByteBuffer indexBlock = ByteBuffer.allocate(
            StreamObjectFormat.INDEX_HEADER_BYTES + StreamObjectFormat.INDEX_ENTRY_BYTES
        );
        StreamObjectFormat.writeIndexBlock(indexBlock, List.of(indexEntry));
        assertEncoding(
            "4b53493200020000000000010000004000000000000000010000000000000002" +
                "0000000300000004000000000000000a00000000000000140000000000000010" +
                "00000038000000011122334400000000",
            indexBlock
        );

        ByteBuffer footer = ByteBuffer.allocate(StreamObjectFormat.FOOTER_BYTES);
        StreamObjectFormat.writeFooter(
            footer,
            StreamObjectFormat.OBJECT_HEADER_BYTES,
            StreamObjectFormat.INDEX_HEADER_BYTES + StreamObjectFormat.INDEX_ENTRY_BYTES,
            1,
            CHECKSUM,
            0x55667788L
        );
        assertEncoding(
            "4b53463200020000000000000000001000000050000000011122334455667788",
            footer
        );
    }

    private static void assertEncoding(String expectedHex, ByteBuffer encoded) {
        ByteBuffer actualBuffer = encoded.duplicate();
        actualBuffer.flip();
        byte[] actual = new byte[actualBuffer.remaining()];
        actualBuffer.get(actual);
        assertArrayEquals(HexFormat.of().parseHex(expectedHex), actual);
    }

    private static ByteBuffer bytes(String hex) {
        return ByteBuffer.wrap(HexFormat.of().parseHex(hex));
    }
}
