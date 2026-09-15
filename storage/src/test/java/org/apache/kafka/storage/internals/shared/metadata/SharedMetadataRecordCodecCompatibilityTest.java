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

import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Golden-byte compatibility contract for the GA v1 records in {@code __shared_storage_metadata}.
 *
 * <p>Compacted key bytes are permanent identities. Changing any expected value byte requires an explicit value-version
 * evolution strategy that keeps records written by already released brokers readable.</p>
 */
class SharedMetadataRecordCodecCompatibilityTest {
    private static final long OBJECT_ID = 0x0102030405060708L;
    private static final long CREATED_TIME_MS = 0x0102030405060708L;

    @Test
    void preservesGaV1PermanentKeysAndValueEncoding() {
        assertEncoding("010102030405060708", SharedMetadataRecordCodec.objectKey(OBJECT_ID));
        assertEncoding("030102030405060708", SharedMetadataRecordCodec.objectCleanupKey(OBJECT_ID));
        assertEncoding("0200000007", SharedMetadataRecordCodec.brokerSequenceKey(7));

        assertEncoding(
            "0001010102030405060708",
            SharedMetadataRecordCodec.preparedObjectValue(CREATED_TIME_MS)
        );
        assertEncoding(
            "0001040102030405060708",
            SharedMetadataRecordCodec.cleanupClaimedValue(CREATED_TIME_MS)
        );
        assertEncoding(
            "0001050102030405060708",
            SharedMetadataRecordCodec.cleanupDeletedValue(CREATED_TIME_MS)
        );
        assertEncoding("000103000000000000002a", SharedMetadataRecordCodec.brokerSequenceValue(42L));

        SharedObjectMetadata committed = new SharedObjectMetadata(
            OBJECT_ID,
            200L,
            999L,
            List.of(new SharedObjectRange(
                new SharedPartitionId(1L, 2L, 3),
                new OffsetRange(10L, 20L),
                4,
                30L,
                40,
                50L
            ))
        );
        assertEncoding(
            "00010200000000000000c800000000000003e7000000010000000000000001" +
                "00000000000000020000000300000004000000000000000a0000000000000014" +
                "000000000000001e000000280000000000000032",
            SharedMetadataRecordCodec.committedObjectValue(committed)
        );
    }

    @Test
    void readsGaV1GoldenRecords() {
        SharedMetadataRecordCodec.MetadataKey objectKey = SharedMetadataRecordCodec.decodeKey(
            bytes("010102030405060708")
        );
        assertEquals(SharedMetadataRecordCodec.KeyType.OBJECT, objectKey.type());
        assertEquals(OBJECT_ID, objectKey.id());

        SharedMetadataRecordCodec.PreparedObjectValue prepared = assertInstanceOf(
            SharedMetadataRecordCodec.PreparedObjectValue.class,
            SharedMetadataRecordCodec.decodeValue(objectKey, bytes("0001010102030405060708"))
        );
        assertEquals(CREATED_TIME_MS, prepared.createdTimeMs());

        SharedMetadataRecordCodec.CommittedObjectValue committed = assertInstanceOf(
            SharedMetadataRecordCodec.CommittedObjectValue.class,
            SharedMetadataRecordCodec.decodeValue(
                objectKey,
                bytes(
                    "00010200000000000000c800000000000003e7000000010000000000000001" +
                        "00000000000000020000000300000004000000000000000a0000000000000014" +
                        "000000000000001e000000280000000000000032"
                )
            )
        );
        assertEquals(OBJECT_ID, committed.metadata().objectId());
        assertEquals(200L, committed.metadata().objectSize());
        assertEquals(999L, committed.metadata().objectChecksum());
        assertEquals(1, committed.metadata().ranges().size());
        assertEquals(new SharedPartitionId(1L, 2L, 3), committed.metadata().ranges().get(0).partition());
        assertEquals(new OffsetRange(10L, 20L), committed.metadata().ranges().get(0).offsets());
    }

    private static void assertEncoding(String expectedHex, byte[] actual) {
        assertArrayEquals(bytes(expectedHex), actual);
    }

    private static byte[] bytes(String hex) {
        return HexFormat.of().parseHex(hex);
    }
}
