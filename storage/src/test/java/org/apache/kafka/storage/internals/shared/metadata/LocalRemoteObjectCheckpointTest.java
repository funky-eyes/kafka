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
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalRemoteObjectCheckpointTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11, 22, 3);
    private static final int LEGACY_HEADER_BYTES = Integer.BYTES + Short.BYTES + Short.BYTES + Integer.BYTES;
    private static final int LEGACY_ENTRY_BYTES =
        Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES +
            Long.BYTES + Long.BYTES + Long.BYTES + Integer.BYTES + Long.BYTES;

    @TempDir
    Path tempDir;

    @Test
    void shouldPersistAndReloadAuthoritativeRemoteRanges() throws Exception {
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        SharedObjectMetadata first = object(100, 100, 110, 444);
        SharedObjectMetadata second = object(101, 110, 120, 555);
        checkpoint.add(first);
        checkpoint.add(second);

        RemoteObjectIndex.RangeReference firstReference = checkpoint.find(PARTITION, 105).orElseThrow();
        assertEquals(100L, firstReference.objectId());
        assertTrue(firstReference.hasObjectDescriptor());
        assertEquals(first.objectSize(), firstReference.objectSize());
        assertEquals(first.objectChecksum(), firstReference.objectChecksum());
        assertEquals(101L, checkpoint.find(PARTITION, 119).orElseThrow().objectId());
        assertTrue(checkpoint.find(PARTITION, 120).isEmpty());

        LocalRemoteObjectCheckpoint reopened = new LocalRemoteObjectCheckpoint(tempDir);
        assertEquals(checkpoint.references(), reopened.references());
        assertEquals(2, reopened.ranges(PARTITION, 105, 120).size());
        assertFalse(Files.exists(tempDir.resolve(LocalRemoteObjectCheckpoint.FILE_NAME + ".tmp")));
    }

    @Test
    void shouldReadLegacyVersionOneAndUpgradeDescriptorOnAuthoritativeReplay() throws Exception {
        SharedObjectMetadata legacyObject = object(100, 100, 110, 444);
        writeLegacyCheckpoint(tempDir, legacyObject);

        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        RemoteObjectIndex.RangeReference legacy = checkpoint.find(PARTITION, 105).orElseThrow();
        assertEquals(legacyObject.objectId(), legacy.objectId());
        assertFalse(legacy.hasObjectDescriptor());

        checkpoint.add(legacyObject);
        RemoteObjectIndex.RangeReference upgraded = checkpoint.find(PARTITION, 105).orElseThrow();
        assertTrue(upgraded.hasObjectDescriptor());
        assertEquals(legacyObject.objectSize(), upgraded.objectSize());
        assertEquals(legacyObject.objectChecksum(), upgraded.objectChecksum());

        byte[] rewritten = Files.readAllBytes(tempDir.resolve(LocalRemoteObjectCheckpoint.FILE_NAME));
        ByteBuffer header = ByteBuffer.wrap(rewritten).order(ByteOrder.BIG_ENDIAN);
        assertEquals(LocalRemoteObjectCheckpoint.VERSION, header.getShort(Integer.BYTES));

        LocalRemoteObjectCheckpoint reopened = new LocalRemoteObjectCheckpoint(tempDir);
        RemoteObjectIndex.RangeReference durable = reopened.find(PARTITION, 105).orElseThrow();
        assertTrue(durable.hasObjectDescriptor());
        assertEquals(upgraded, durable);
    }

    @Test
    void duplicateLogicalRangeWithSameContentMayKeepOriginalPhysicalReference() throws Exception {
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        checkpoint.add(object(100, 100, 110, 444));
        checkpoint.add(object(999, 100, 110, 444));

        assertEquals(1, checkpoint.references().size());
        assertEquals(100L, checkpoint.references().get(0).objectId());
        assertEquals(checkpoint.references(), new LocalRemoteObjectCheckpoint(tempDir).references());
    }

    @Test
    void conflictingLogicalRangeMustNotReplaceLastDurableSnapshot() throws Exception {
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        SharedObjectMetadata first = object(100, 100, 110, 444);
        checkpoint.add(first);

        SharedObjectMetadata conflict = object(101, 100, 111, 555);
        assertThrows(IOException.class, () -> checkpoint.add(conflict));

        assertEquals(1, checkpoint.references().size());
        assertEquals(100L, checkpoint.references().get(0).objectId());
        assertEquals(checkpoint.references(), new LocalRemoteObjectCheckpoint(tempDir).references());
    }

    @Test
    void corruptedSnapshotMustFailClosedBecauseWalMayAlreadyBeGone() throws Exception {
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        checkpoint.add(object(100, 100, 110, 444));

        Path file = tempDir.resolve(LocalRemoteObjectCheckpoint.FILE_NAME);
        byte[] bytes = Files.readAllBytes(file);
        bytes[bytes.length / 2] ^= 0x3f;
        Files.write(file, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

        IOException failure = assertThrows(IOException.class, () -> new LocalRemoteObjectCheckpoint(tempDir));
        assertTrue(failure.getMessage().contains("checksum"));
    }

    private static void writeLegacyCheckpoint(Path directory, SharedObjectMetadata object) throws IOException {
        SharedObjectRange range = object.ranges().get(0);
        ByteBuffer bytes = ByteBuffer.allocate(LEGACY_HEADER_BYTES + LEGACY_ENTRY_BYTES + Integer.BYTES)
            .order(ByteOrder.BIG_ENDIAN);
        bytes.putInt(LocalRemoteObjectCheckpoint.MAGIC)
            .putShort(LocalRemoteObjectCheckpoint.LEGACY_VERSION)
            .putShort((short) 0)
            .putInt(1)
            .putLong(object.objectId())
            .putLong(range.partition().topicIdHigh())
            .putLong(range.partition().topicIdLow())
            .putInt(range.partition().partition())
            .putInt(range.leaderEpoch())
            .putLong(range.offsets().startOffset())
            .putLong(range.offsets().endOffset())
            .putLong(range.objectPosition())
            .putInt(range.objectLength())
            .putLong(range.checksum());
        ByteBuffer checksummed = bytes.duplicate();
        checksummed.flip();
        CRC32C crc = new CRC32C();
        crc.update(checksummed);
        bytes.putInt((int) crc.getValue());
        Files.write(directory.resolve(LocalRemoteObjectCheckpoint.FILE_NAME), bytes.array());
    }

    private static SharedObjectMetadata object(
        long objectId,
        long startOffset,
        long endOffset,
        long checksum
    ) {
        int payloadLength = Math.toIntExact(endOffset - startOffset + 64);
        SharedObjectRange range = new SharedObjectRange(
            PARTITION,
            new OffsetRange(startOffset, endOffset),
            7,
            objectId * 10,
            payloadLength,
            checksum
        );
        return new SharedObjectMetadata(
            objectId,
            range.objectPosition() + range.objectLength(),
            checksum + 1,
            List.of(range)
        );
    }
}
