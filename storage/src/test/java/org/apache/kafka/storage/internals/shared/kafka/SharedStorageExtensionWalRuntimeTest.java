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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageExtensionWalRuntimeTest {
    private static final long WAL_CAPACITY = 64L * 1024L;

    @TempDir
    Path tempDir;

    @Test
    void defaultRuntimeActuallyStartsRingWal() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-ring"));
        Path walDir = tempDir.resolve("shared-wal-ring");
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, walDir, Map.of()));

            Path ringWal = SharedStorageWalFactory.ringWalPath(walDir);
            assertTrue(Files.isRegularFile(ringWal));
            assertEquals(WAL_CAPACITY, Files.size(ringWal));
        } finally {
            extension.close();
        }
    }

    @Test
    void explicitRollbackRuntimeActuallyStartsRotatingWal() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-rotating"));
        Path walDir = tempDir.resolve("shared-wal-rotating");
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(
                logDir,
                walDir,
                Map.of(
                    SharedStorageConfiguration.WAL_ENGINE_CONFIG, "rotating-file",
                    SharedStorageConfiguration.WAL_SEGMENT_BYTES_CONFIG, 16L * 1024L
                )
            ));
            extension.storage().appendData(
                new SharedPartitionId(1L, 2L, 0),
                3,
                0L,
                0L,
                ByteBuffer.wrap(new byte[] {1, 2, 3})
            ).join();

            assertFalse(Files.exists(SharedStorageWalFactory.ringWalPath(walDir)));
            try (var entries = Files.list(walDir)) {
                assertTrue(entries.anyMatch(path ->
                    path.getFileName().toString().matches("wal-\\d{20}\\.log")));
            }
        } finally {
            extension.close();
        }
    }

    private static StorageExtensionContext context(
        Path logDir,
        Path walDir,
        Map<String, ?> extra
    ) {
        HashMap<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString());
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, WAL_CAPACITY);
        originals.putAll(extra);
        return new StorageExtensionContext(
            originals,
            List.of(logDir.toFile()),
            3,
            Time.SYSTEM
        );
    }
}
