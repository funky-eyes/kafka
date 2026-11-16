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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.checkpoint.PartitionMetadataFile;
import org.apache.kafka.storage.internals.log.IncompleteLogInitializationException;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SharedStorageModeMarkerTest {
    @TempDir
    Path tempDir;

    @Test
    void sharedIdentitySurvivesRestartAndRejectsNativeFallbackOrAnotherTopicId() throws Exception {
        Uuid id = Uuid.randomUuid();
        Path dir = tempDir.resolve("events-0");
        assertEquals(Optional.of(id), SharedStorageModeMarker.prepareShared(context(dir, Optional.of(id), true), false));
        assertEquals(Optional.of(id), SharedStorageModeMarker.prepareShared(context(dir, Optional.empty(), true), false));
        assertThrows(IOException.class, () -> SharedStorageModeMarker.validateClassic(context(dir, Optional.of(id), false)));
        assertThrows(InconsistentTopicIdException.class, () ->
            SharedStorageModeMarker.prepareShared(context(dir, Optional.of(Uuid.randomUuid()), true), false));
    }

    @Test
    void nativeFactoryRejectsEnabledTopicEvenWithoutMarkerOrExtension() {
        assertThrows(IOException.class, () -> SharedStorageModeMarker.validateClassic(
            context(tempDir.resolve("events-0"), Optional.of(Uuid.randomUuid()), true)));
    }

    @Test
    void newClassicTopicUnderLegacyDefaultAllRetainsItsModeAndTopicId() throws Exception {
        Path dir = tempDir.resolve("classic-0");
        Uuid id = Uuid.randomUuid();
        SharedStorageModeMarker.prepareClassic(context(dir, Optional.of(id), false), true);
        Files.writeString(dir.resolve("00000000000000000000.log"), "native payload");
        SharedStorageModeMarker.prepareClassic(context(dir, Optional.empty(), false), true);
        assertEquals(Optional.of(id), SharedStorageModeMarker.validateClassic(context(dir, Optional.empty(), false)));
        assertThrows(IOException.class, () ->
            SharedStorageModeMarker.prepareShared(context(dir, Optional.of(id), true), true));
    }

    @Test
    void unmarkedLegacyPlaceholderCannotBeReinterpretedAsClassicOrAdoptedImplicitly() throws Exception {
        Path dir = Files.createDirectory(tempDir.resolve("legacy-0"));
        Path placeholder = Files.createFile(dir.resolve("00000000000000000000.log"));
        Uuid id = Uuid.randomUuid();
        assertThrows(IOException.class, () ->
            SharedStorageModeMarker.prepareClassic(context(dir, Optional.of(id), false), true));
        assertThrows(IOException.class, () ->
            SharedStorageModeMarker.prepareShared(context(dir, Optional.of(id), true), false));
        assertFalse(Files.exists(dir.resolve(SharedStorageModeMarker.FILE_NAME)));
        assertEquals(0, Files.size(placeholder));
        assertEquals(Optional.of(id), SharedStorageModeMarker.prepareShared(context(dir, Optional.of(id), true), true));
    }

    @Test
    void legacyAdoptionNeverAcceptsNativePayloads() throws Exception {
        Path dir = Files.createDirectory(tempDir.resolve("native-0"));
        Path data = dir.resolve("00000000000000000000.log");
        Files.writeString(data, "native payload");
        assertThrows(IOException.class, () -> SharedStorageModeMarker.prepareShared(
            context(dir, Optional.of(Uuid.randomUuid()), true), true));
        assertFalse(Files.exists(dir.resolve(SharedStorageModeMarker.FILE_NAME)));
        assertEquals("native payload", Files.readString(data));
    }

    @Test
    void malformedModeMarkerCannotSelectEitherStorageMode() throws Exception {
        Path dir = Files.createDirectory(tempDir.resolve("broken-0"));
        Files.writeString(dir.resolve(SharedStorageModeMarker.FILE_NAME), "partial");
        assertThrows(IOException.class, () ->
            SharedStorageModeMarker.validateClassic(context(dir, Optional.empty(), false)));
        assertThrows(IOException.class, () ->
            SharedStorageModeMarker.prepareShared(context(dir, Optional.empty(), true), true));
    }

    @Test
    void classicUnmarkedTopicWithoutExtensionKeepsNativeBehavior() throws Exception {
        Path dir = Files.createDirectory(tempDir.resolve("classic-0"));
        Files.writeString(dir.resolve("00000000000000000000.log"), "native payload");
        assertEquals(Optional.empty(), SharedStorageModeMarker.validateClassic(context(dir, Optional.empty(), false)));
        assertFalse(Files.exists(dir.resolve(SharedStorageModeMarker.FILE_NAME)));
    }

    @Test
    void existingWalRequiresExplicitLegacyModeAndCannotDropItAfterPartialLogRecovery() throws Exception {
        Path walDir = Files.createDirectory(tempDir.resolve("wal"));
        Path data = walDir.resolve("wal-00000000000000000000.log");
        Files.writeString(data, "wal payload");
        assertThrows(IOException.class, () -> SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, false)));
        assertFalse(Files.exists(walDir.resolve(SharedStorageModeMarker.WAL_FORMAT_FILE_NAME)));
        SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, true));
        SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, true));
        assertThrows(IOException.class, () -> SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, false)));
        assertEquals("wal payload", Files.readString(data));
    }

    @Test
    void freshWalMarkerIsIdempotentAndCannotBeDowngradedToLegacy() throws Exception {
        Path walDir = tempDir.resolve("wal");
        SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, false));
        Files.writeString(walDir.resolve("ring.wal"), "data");
        SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, false));
        assertThrows(IOException.class, () -> SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, true)));
        SharedStorageModeMarker.initializeWalDirectory(configuration(walDir, false));
        assertTrue(Files.readString(walDir.resolve(SharedStorageModeMarker.WAL_FORMAT_FILE_NAME)).contains("topic"));
    }

    @Test
    void temporaryModeAndPartitionMetadataFilesPreserveIncompleteInitializationRecovery() throws Exception {
        for (List<String> names : List.of(List.of(SharedStorageModeMarker.FILE_NAME + ".tmp"),
            List.of("partition.metadata.tmp"), List.of(SharedStorageModeMarker.FILE_NAME + ".tmp", "partition.metadata.tmp"))) {
            Path dir = Files.createTempDirectory(tempDir, "incomplete-");
            for (String name : names) {
                Files.writeString(dir.resolve(name), "partial");
            }
            UnifiedLogCreationContext context = context(dir, Optional.empty(), true);
            Optional<Uuid> id = SharedStorageModeMarker.prepareShared(context, false);
            assertTrue(id.isEmpty());
            assertThrows(IncompleteLogInitializationException.class, () -> SharedUnifiedLogFactory.resolveAndPersistTopicId(
                dir.toFile(), new TopicPartition("events", 0), id, context.logDirFailureChannel()));
            assertFalse(Files.exists(dir));
        }
    }

    @Test
    void temporaryModeFileNeverAuthorizesDeletingAnyExistingLogArtifact() throws Exception {
        Path dir = Files.createDirectory(tempDir.resolve("existing-0"));
        Path data = Files.createFile(dir.resolve("00000000000000000000.log"));
        Files.writeString(dir.resolve(SharedStorageModeMarker.FILE_NAME + ".tmp"), "partial");
        Files.writeString(Path.of(PartitionMetadataFile.newFile(dir.toFile()) + ".tmp"), "partial");
        UnifiedLogCreationContext context = context(dir, Optional.empty(), true);
        assertThrows(IOException.class, () -> SharedStorageModeMarker.prepareShared(context, false));
        assertThrows(IOException.class, () -> SharedUnifiedLogFactory.resolveAndPersistTopicId(
            dir.toFile(), new TopicPartition("events", 0), Optional.empty(), context.logDirFailureChannel()));
        assertTrue(Files.exists(data));
        assertTrue(Files.exists(dir.resolve(SharedStorageModeMarker.FILE_NAME + ".tmp")));
    }

    private SharedStorageConfiguration configuration(Path walDir, boolean legacy) {
        return SharedStorageConfiguration.from(new StorageExtensionContext(Map.of(
            SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString(),
            SharedStorageConfiguration.LEGACY_SELECTORS_ENABLED_CONFIG, legacy
        ), List.of(tempDir.resolve("kafka-log").toFile()), 1, Time.SYSTEM));
    }

    private static UnifiedLogCreationContext context(Path dir, Optional<Uuid> topicId, boolean shared) {
        UnifiedLogCreationContext context = mock(UnifiedLogCreationContext.class);
        when(context.dir()).thenReturn(dir.toFile());
        when(context.topicId()).thenReturn(topicId);
        when(context.config()).thenReturn(new LogConfig(Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, shared)));
        when(context.logDirFailureChannel()).thenReturn(new LogDirFailureChannel(1));
        return context;
    }
}
