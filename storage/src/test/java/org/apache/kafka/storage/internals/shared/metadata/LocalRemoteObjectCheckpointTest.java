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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
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
    void targetedCheckpointLookupResolvesEachPackedPhysicalObjectOnlyOnce() throws Exception {
        SharedObjectRange first = object(140L, 100L, 110L, 444L).ranges().get(0);
        SharedObjectRange second = new SharedObjectRange(
            PARTITION, new OffsetRange(110L, 120L), 7, 2000L, 74, 555L
        );
        SharedObjectMetadata packed = new SharedObjectMetadata(
            140L, 2074L, 777L, List.of(first, second)
        );
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        checkpoint.add(packed);
        AtomicInteger lookups = new AtomicInteger();

        checkpoint.validateAgainstCommittedLookup(id -> {
            lookups.incrementAndGet();
            assertEquals(packed.objectId(), id);
            return Optional.of(packed);
        });
        assertEquals(1, lookups.get(), "One packed object needs one authoritative lookup");
        assertEquals(2, checkpoint.references().size());
        assertEquals(checkpoint.references(), new LocalRemoteObjectCheckpoint(tempDir).references());
    }

    @Test
    void targetedCheckpointLookupRejectsWrongObjectRangeAndDescriptorButAcceptsLegacy() throws Exception {
        SharedObjectMetadata expected = object(141L, 100L, 110L, 444L);
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        checkpoint.add(expected);
        checkpoint.validateAgainstCommittedLookup(id -> Optional.of(expected));

        IOException absent = assertThrows(
            IOException.class, () -> checkpoint.validateAgainstCommittedLookup(id -> Optional.empty())
        );
        assertTrue(absent.getMessage().contains("not COMMITTED"));
        IOException wrongId = assertThrows(
            IOException.class,
            () -> checkpoint.validateAgainstCommittedLookup(id -> Optional.of(object(142L, 100L, 110L, 444L)))
        );
        assertTrue(wrongId.getMessage().contains("incorrect physical object"));
        IOException wrongRange = assertThrows(
            IOException.class,
            () -> checkpoint.validateAgainstCommittedLookup(id -> Optional.of(object(141L, 100L, 111L, 444L)))
        );
        assertTrue(wrongRange.getMessage().contains("range differs"));

        SharedObjectMetadata modified = new SharedObjectMetadata(
            expected.objectId(), expected.objectSize() + 1L,
            expected.objectChecksum() + 1L, expected.ranges()
        );
        IOException wrongDescriptor = assertThrows(
            IOException.class, () -> checkpoint.validateAgainstCommittedLookup(id -> Optional.of(modified))
        );
        assertTrue(wrongDescriptor.getMessage().contains("descriptor differs"));
        assertEquals(1, new LocalRemoteObjectCheckpoint(tempDir).references().size());

        Path legacyDir = tempDir.resolve("legacy-targeted-lookup");
        Files.createDirectories(legacyDir);
        writeLegacyCheckpoint(legacyDir, expected);
        LocalRemoteObjectCheckpoint legacy = new LocalRemoteObjectCheckpoint(legacyDir);
        legacy.validateAgainstCommittedLookup(id -> Optional.of(expected));
        assertFalse(legacy.find(PARTITION, 105L).orElseThrow().hasObjectDescriptor());
    }

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
    void physicalDuplicateCheckpointConvergesAcrossOppositeReplicaReplayOrder() throws Exception {
        LocalRemoteObjectCheckpoint first = new LocalRemoteObjectCheckpoint(tempDir.resolve("broker-one"));
        LocalRemoteObjectCheckpoint second = new LocalRemoteObjectCheckpoint(tempDir.resolve("broker-two"));
        SharedObjectMetadata low = object(100L, 100L, 110L, 444L);
        SharedObjectMetadata high = object(999L, 100L, 110L, 444L);

        first.add(high);
        assertEquals(999L, first.find(PARTITION, 105L).orElseThrow().objectId());
        first.add(low);
        second.add(low);
        second.add(high);

        assertEquals(first.references(), second.references());
        assertEquals(100L, first.find(PARTITION, 105L).orElseThrow().objectId());
        assertEquals(
            first.references(),
            new LocalRemoteObjectCheckpoint(tempDir.resolve("broker-one")).references()
        );
        assertEquals(
            second.references(),
            new LocalRemoteObjectCheckpoint(tempDir.resolve("broker-two")).references()
        );

        RemoteObjectIndex recovered = new RemoteObjectIndex();
        recovered.restore(first.references());
        recovered.add(high);
        assertEquals(100L, recovered.find(PARTITION, 105L).orElseThrow().objectId());
        assertEquals(first.find(PARTITION, 105L), recovered.find(PARTITION, 105L));
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
    void sameCrcDifferentKafkaByteLengthMustNotReplaceDurableCheckpoint() throws Exception {
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        SharedObjectMetadata committed = object(100L, 100L, 110L, 444L);
        checkpoint.add(committed);
        List<RemoteObjectIndex.RangeReference> durable = checkpoint.references();

        SharedObjectRange original = committed.ranges().get(0);
        SharedObjectRange incompatible = new SharedObjectRange(
            PARTITION, original.offsets(), original.leaderEpoch(), 2000L,
            original.objectLength() + 1, original.checksum()
        );
        SharedObjectMetadata conflicting = new SharedObjectMetadata(
            90L, incompatible.objectPosition() + incompatible.objectLength(), 445L,
            List.of(incompatible)
        );
        assertThrows(IOException.class, () -> checkpoint.add(conflicting));
        assertEquals(durable, checkpoint.references());
        assertEquals(durable, new LocalRemoteObjectCheckpoint(tempDir).references());
    }

    @Test
    void historicalObjectTombstoneRejectsPreviouslyCheckpointedPhysicalObject() throws Exception {
        SharedObjectMetadata oldCommit = object(130L, 100L, 110L, 444L);
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        checkpoint.add(oldCommit);

        SharedMetadataImage image = new SharedMetadataImage();
        byte[] key = SharedMetadataRecordCodec.objectKey(oldCommit.objectId());
        image.apply(key, SharedMetadataRecordCodec.committedObjectValue(oldCommit));
        image.apply(key, null);
        image.markReady();

        assertTrue(image.committedObjects().isEmpty());
        IOException mismatch = assertThrows(
            IOException.class,
            () -> checkpoint.validateAgainstCommittedInventory(image.committedObjects())
        );
        assertTrue(mismatch.getMessage().contains("not COMMITTED"));
        assertEquals(1, new LocalRemoteObjectCheckpoint(tempDir).references().size(),
            "Mismatch must not silently discard a durable WAL-recovery checkpoint");
    }

    @Test
    void legacyAndCurrentCheckpointsRequireExactCommittedPhysicalIdentity() throws Exception {
        SharedObjectMetadata committed = object(131L, 100L, 110L, 444L);
        LocalRemoteObjectCheckpoint current = new LocalRemoteObjectCheckpoint(tempDir.resolve("current"));
        current.add(committed);
        current.validateAgainstCommittedInventory(List.of(committed));

        Path legacyDir = tempDir.resolve("legacy");
        Files.createDirectories(legacyDir);
        writeLegacyCheckpoint(legacyDir, committed);
        LocalRemoteObjectCheckpoint legacy = new LocalRemoteObjectCheckpoint(legacyDir);
        assertFalse(legacy.find(PARTITION, 105L).orElseThrow().hasObjectDescriptor());
        legacy.validateAgainstCommittedInventory(List.of(committed));
        assertEquals(1, legacy.references().size());
    }

    @Test
    void checkpointCannotBorrowEquivalentObjectOrMutatedPackedDescriptor() throws Exception {
        SharedObjectMetadata committed = object(132L, 100L, 110L, 444L);
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        checkpoint.add(committed);

        SharedObjectMetadata differentObject = object(133L, 100L, 110L, 444L);
        IOException absent = assertThrows(
            IOException.class,
            () -> checkpoint.validateAgainstCommittedInventory(List.of(differentObject))
        );
        assertTrue(absent.getMessage().contains("not COMMITTED"));

        SharedObjectMetadata changedRange = object(132L, 100L, 111L, 444L);
        IOException changed = assertThrows(
            IOException.class,
            () -> checkpoint.validateAgainstCommittedInventory(List.of(changedRange))
        );
        assertTrue(changed.getMessage().contains("range differs"));

        SharedObjectMetadata changedDescriptor = new SharedObjectMetadata(
            committed.objectId(), committed.objectSize() + 1L,
            committed.objectChecksum() + 1L, committed.ranges()
        );
        IOException descriptor = assertThrows(
            IOException.class,
            () -> checkpoint.validateAgainstCommittedInventory(List.of(changedDescriptor))
        );
        assertTrue(descriptor.getMessage().contains("descriptor differs"));
        assertEquals(1, checkpoint.references().size());
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
