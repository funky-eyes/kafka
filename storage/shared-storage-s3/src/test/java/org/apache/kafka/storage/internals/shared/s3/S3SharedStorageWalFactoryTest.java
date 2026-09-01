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
package org.apache.kafka.storage.internals.shared.s3;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.shared.kafka.SharedStorageConfiguration;
import org.apache.kafka.storage.internals.shared.wal.RingSharedWal;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SharedStorageWalFactoryTest {
    private static final long RING_CAPACITY = 64L * 1024L;

    @TempDir
    Path tempDir;

    @Test
    void createsFixedLengthRingWalByDefault() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log"));
        Path walDir = tempDir.resolve("shared-wal");
        SharedStorageConfiguration configuration = configuration(logDir, walDir, Map.of());

        try (SharedWal wal = S3SharedStorageExtension.createWal(configuration)) {
            assertInstanceOf(RingSharedWal.class, wal);
            assertEquals(RING_CAPACITY - 8L * 1024L, wal.capacityBytes());
        }

        assertEquals(RING_CAPACITY, Files.size(walDir.resolve("shared-ring.wal")));
    }

    @Test
    void refusesRingStartupWhenLegacyRotatingSegmentExists() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-legacy"));
        Path walDir = Files.createDirectories(tempDir.resolve("legacy-wal"));
        Path legacySegment = Files.createFile(walDir.resolve("wal-00000000000000000000.log"));
        SharedStorageConfiguration configuration = configuration(logDir, walDir, Map.of());

        IOException error = assertThrows(
            IOException.class,
            () -> S3SharedStorageExtension.createWal(configuration)
        );
        assertTrue(error.getMessage().contains(legacySegment.toString()));
        assertTrue(error.getMessage().contains(
            SharedStorageConfiguration.WAL_ENGINE_CONFIG + "=rotating-file"));
        assertTrue(Files.notExists(walDir.resolve("shared-ring.wal")),
            "migration guard must fail before creating the new ring file");
    }

    @Test
    void refusesRotatingStartupWhenRingWalFileExists() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-ring"));
        Path walDir = tempDir.resolve("ring-wal");
        SharedStorageConfiguration ringConfiguration = configuration(logDir, walDir, Map.of());

        try (SharedWal ignored = S3SharedStorageExtension.createWal(ringConfiguration)) {
            // Establish a valid fixed Ring WAL on disk before attempting the unsafe backend switch.
        }
        Path ringWal = walDir.resolve("shared-ring.wal");
        assertTrue(Files.isRegularFile(ringWal));

        SharedStorageConfiguration rotatingConfiguration = configuration(
            logDir,
            walDir,
            Map.of(
                SharedStorageConfiguration.WAL_ENGINE_CONFIG, "rotating-file",
                SharedStorageConfiguration.WAL_SEGMENT_BYTES_CONFIG, 16L * 1024L
            )
        );
        IOException error = assertThrows(
            IOException.class,
            () -> S3SharedStorageExtension.createWal(rotatingConfiguration)
        );
        assertTrue(error.getMessage().contains(ringWal.toString()));
        assertTrue(error.getMessage().contains(SharedStorageConfiguration.WAL_ENGINE_CONFIG + "=ring"));
        try (var entries = Files.list(walDir)) {
            assertFalse(
                entries.anyMatch(path -> path.getFileName().toString().endsWith(".log")),
                "migration guard must fail before creating a rotating WAL segment"
            );
        }
    }

    @Test
    void ignoresUnrelatedFilesWhenOpeningRingWal() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-unrelated"));
        Path walDir = Files.createDirectories(tempDir.resolve("wal-with-checkpoint"));
        Files.writeString(walDir.resolve("remote-object.checkpoint"), "checkpoint");
        SharedStorageConfiguration configuration = configuration(logDir, walDir, Map.of());

        try (SharedWal wal = S3SharedStorageExtension.createWal(configuration)) {
            assertInstanceOf(RingSharedWal.class, wal);
        }
    }

    private static SharedStorageConfiguration configuration(
        Path logDir,
        Path walDir,
        Map<String, ?> extra
    ) {
        java.util.HashMap<String, Object> originals = new java.util.HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString());
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, RING_CAPACITY);
        originals.putAll(extra);
        StorageExtensionContext context = new StorageExtensionContext(
            originals,
            List.of(new File(logDir.toString())),
            3,
            Time.SYSTEM
        );
        return SharedStorageConfiguration.from(context);
    }
}
