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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalRemoteObjectCheckpointTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11, 22, 3);

    @TempDir
    Path tempDir;

    @Test
    void shouldPersistAndReloadAuthoritativeRemoteRanges() throws Exception {
        LocalRemoteObjectCheckpoint checkpoint = new LocalRemoteObjectCheckpoint(tempDir);
        SharedObjectMetadata first = object(100, 100, 110, 444);
        SharedObjectMetadata second = object(101, 110, 120, 555);
        checkpoint.add(first);
        checkpoint.add(second);

        assertEquals(100L, checkpoint.find(PARTITION, 105).orElseThrow().objectId());
        assertEquals(101L, checkpoint.find(PARTITION, 119).orElseThrow().objectId());
        assertTrue(checkpoint.find(PARTITION, 120).isEmpty());

        LocalRemoteObjectCheckpoint reopened = new LocalRemoteObjectCheckpoint(tempDir);
        assertEquals(checkpoint.references(), reopened.references());
        assertEquals(2, reopened.ranges(PARTITION, 105, 120).size());
        assertFalse(Files.exists(tempDir.resolve(LocalRemoteObjectCheckpoint.FILE_NAME + ".tmp")));
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
