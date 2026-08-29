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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageConfigurationTest {
    @Test
    void defaultsToAllNonInternalTopicsAndBrokerWideWalDefaults() {
        File logDir = TestUtils.tempDirectory();
        SharedStorageConfiguration config = SharedStorageConfiguration.from(context(Map.of(), logDir));
        Path kafkaLogDir = logDir.toPath().toAbsolutePath().normalize();
        Path expectedWalDir = kafkaLogDir
            .resolveSibling(kafkaLogDir.getFileName() + ".shared-storage")
            .resolve("broker-3")
            .resolve("wal")
            .normalize();

        assertEquals(SharedStorageConfiguration.DEFAULT_WAL_CAPACITY_BYTES, config.walCapacityBytes());
        assertEquals(SharedStorageConfiguration.DEFAULT_WAL_SEGMENT_BYTES, config.walSegmentBytes());
        assertEquals(SharedStorageConfiguration.DEFAULT_OBJECT_TARGET_BYTES, config.objectTargetBytes());
        assertEquals(SharedStorageConfiguration.DEFAULT_UPLOAD_INTERVAL_MS, config.uploadIntervalMs());
        assertEquals(expectedWalDir, config.walDir());
        assertFalse(config.walDir().startsWith(kafkaLogDir));
        assertEquals(kafkaLogDir.getParent(), config.walDir().getParent().getParent().getParent());
        assertTrue(config.useSharedStorage("events"));
        assertFalse(config.useSharedStorage("__consumer_offsets"));
        assertFalse(config.useSharedStorage("__transaction_state"));
        assertFalse(config.useSharedStorage("__shared_storage_metadata"));
    }

    @Test
    void explicitWalDirectoryMustNotBeNestedInsideAnyKafkaLogRoot() {
        File firstLogDir = TestUtils.tempDirectory();
        File secondLogDir = TestUtils.tempDirectory();
        Path nestedWalDir = secondLogDir.toPath().resolve(".shared-wal");
        Map<String, Object> originals = Map.of(
            SharedStorageConfiguration.WAL_DIR_CONFIG,
            nestedWalDir.toString()
        );
        StorageExtensionContext context = new StorageExtensionContext(
            originals,
            List.of(firstLogDir, secondLogDir),
            3,
            Time.SYSTEM
        );

        IllegalArgumentException error = assertThrows(
            IllegalArgumentException.class,
            () -> SharedStorageConfiguration.from(context)
        );
        assertTrue(error.getMessage().contains("must be outside every Kafka log directory"));
    }

    @Test
    void explicitSiblingWalDirectoryIsAccepted() {
        File logDir = TestUtils.tempDirectory();
        Path kafkaLogDir = logDir.toPath().toAbsolutePath().normalize();
        Path siblingWalDir = kafkaLogDir.resolveSibling("dedicated-shared-wal");
        SharedStorageConfiguration config = SharedStorageConfiguration.from(context(
            Map.of(SharedStorageConfiguration.WAL_DIR_CONFIG, siblingWalDir.toString()),
            logDir
        ));

        assertEquals(siblingWalDir, config.walDir());
    }

    @Test
    void combinesExactAndPatternSelectors() {
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.TOPICS_CONFIG, "orders, payments");
        originals.put(SharedStorageConfiguration.TOPIC_PATTERN_CONFIG, "cdc-.*");
        SharedStorageConfiguration config = SharedStorageConfiguration.from(
            context(originals, TestUtils.tempDirectory())
        );

        assertTrue(config.useSharedStorage("orders"));
        assertTrue(config.useSharedStorage("payments"));
        assertTrue(config.useSharedStorage("cdc-users"));
        assertFalse(config.useSharedStorage("audit"));
        assertFalse(config.useSharedStorage("__internal"));
    }

    @Test
    void parsesExplicitUploadTuning() {
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.OBJECT_TARGET_BYTES_CONFIG, "67108864");
        originals.put(SharedStorageConfiguration.UPLOAD_INTERVAL_MS_CONFIG, 250L);

        SharedStorageConfiguration config = SharedStorageConfiguration.from(
            context(originals, TestUtils.tempDirectory())
        );

        assertEquals(64L * 1024 * 1024, config.objectTargetBytes());
        assertEquals(250L, config.uploadIntervalMs());
    }

    @Test
    void validatesWalCapacityAndSegmentSize() {
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, 1024L);
        originals.put(SharedStorageConfiguration.WAL_SEGMENT_BYTES_CONFIG, 2048L);

        assertThrows(
            IllegalArgumentException.class,
            () -> SharedStorageConfiguration.from(context(originals, TestUtils.tempDirectory()))
        );
    }

    @Test
    void rejectsNonPositiveUploadTuning() {
        File logDir = TestUtils.tempDirectory();
        Map<String, Object> targetBytes = new HashMap<>();
        targetBytes.put(SharedStorageConfiguration.OBJECT_TARGET_BYTES_CONFIG, 0L);
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedStorageConfiguration.from(context(targetBytes, logDir))
        );

        Map<String, Object> interval = new HashMap<>();
        interval.put(SharedStorageConfiguration.UPLOAD_INTERVAL_MS_CONFIG, -1L);
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedStorageConfiguration.from(context(interval, logDir))
        );
    }

    private static StorageExtensionContext context(Map<String, ?> originals, File logDir) {
        return new StorageExtensionContext(originals, List.of(logDir), 3, Time.SYSTEM);
    }
}
