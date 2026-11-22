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
import org.apache.kafka.storage.internals.shared.wal.CoordinatedRingWalAdapter;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalCapacityExceededException;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ParallelBlockWalFactoryTest {
    private static final long CAPACITY = 1024 * 1024;
    @TempDir Path temp;

    @ParameterizedTest
    @ValueSource(strings = {"nio", "aio"})
    void selectedFactoryBackendReopensAcrossIoModesAndAcceptsLargeRecord(String mode) throws Exception {
        Path log = Files.createDirectory(temp.resolve("log-" + mode));
        Path walDir = temp.resolve("wal-" + mode);
        var config = configuration(log, walDir, Map.of(
            SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block",
            SharedStorageConfiguration.WAL_IO_BACKEND_CONFIG, mode));
        try (SharedWal wal = SharedStorageWalFactory.create(config)) {
            assertInstanceOf(CoordinatedRingWalAdapter.class, wal);
            assertEquals(CAPACITY - CoordinatedRingWalAdapter.METADATA_BYTES, wal.capacityBytes());
            wal.append(WalRecord.data(1, 2, 0, 3, 0, 0, new byte[128 * 1024])).join();
        }
        assertEquals(CAPACITY, Files.size(SharedStorageWalFactory.ringWalPath(walDir)));
        var opposite = configuration(log, walDir, Map.of(
            SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block",
            SharedStorageConfiguration.WAL_IO_BACKEND_CONFIG, mode.equals("nio") ? "aio" : "nio"));
        try (SharedWal wal = SharedStorageWalFactory.create(opposite)) {
            List<Integer> sizes = new ArrayList<>();
            wal.replay((record, location) -> sizes.add(record.payload().remaining()));
            assertEquals(List.of(128 * 1024), sizes);
        }
    }

    @Test
    void configuredAdmissionRejectsOversizeWithoutFencing() throws Exception {
        Path log = Files.createDirectory(temp.resolve("limits-log"));
        Path walDir = temp.resolve("limits-wal");
        var config = configuration(log, walDir, Map.of(
            SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block",
            SharedStorageConfiguration.WAL_ADMISSION_MAX_GROUPS_CONFIG, 1,
            SharedStorageConfiguration.WAL_ADMISSION_MAX_BYTES_CONFIG, 130));
        try (SharedWal wal = SharedStorageWalFactory.create(config)) {
            var error = assertThrows(CompletionException.class,
                () -> wal.append(WalRecord.data(1, 2, 0, 3, 0, 0, new byte[32])).join());
            assertInstanceOf(WalCapacityExceededException.class, error.getCause());
            wal.append(WalRecord.data(1, 2, 0, 3, 1, 1, new byte[1])).join();
        }
    }

    @Test
    void refusesImplicitSwitchInBothDirections() throws Exception {
        Path log = Files.createDirectory(temp.resolve("switch-log"));
        Path oldDir = temp.resolve("old-wal");
        var old = configuration(log, oldDir, Map.of());
        var block = configuration(log, oldDir, Map.of(SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block"));
        try (SharedWal wal = SharedStorageWalFactory.create(old)) {
            wal.append(WalRecord.data(1, 2, 0, 3, 0, 0, new byte[]{1})).join();
        }
        assertThrows(IOException.class, () -> SharedStorageWalFactory.create(block));
        try (SharedWal wal = SharedStorageWalFactory.create(old)) {
            List<Long> offsets = new ArrayList<>();
            wal.replay((record, location) -> offsets.add(record.firstOffset()));
            assertEquals(List.of(0L), offsets);
        }
        Path newDir = temp.resolve("new-wal");
        var newConfig = configuration(log, newDir, Map.of(SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block"));
        var oldConfig = configuration(log, newDir, Map.of());
        try (SharedWal wal = SharedStorageWalFactory.create(newConfig)) {
            wal.append(WalRecord.data(1, 2, 0, 3, 1, 1, new byte[]{1})).join();
        }
        assertThrows(IOException.class, () -> SharedStorageWalFactory.create(oldConfig));
        try (SharedWal wal = SharedStorageWalFactory.create(newConfig)) {
            List<Long> offsets = new ArrayList<>();
            wal.replay((record, location) -> offsets.add(record.firstOffset()));
            assertEquals(List.of(1L), offsets);
        }
    }

    @Test
    void rejectsUnalignedTotalCapacityBeforeCreatingFile() throws Exception {
        Path log = Files.createDirectory(temp.resolve("alignment-log"));
        Path walDir = temp.resolve("alignment-wal");
        var config = configuration(log, walDir, Map.of(
            SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block",
            SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, CAPACITY + 1));
        assertThrows(IllegalArgumentException.class, () -> SharedStorageWalFactory.create(config));
        assertEquals(false, Files.exists(SharedStorageWalFactory.ringWalPath(walDir)));
    }

    private static SharedStorageConfiguration configuration(Path logDir, Path walDir, Map<String, ?> extra) {
        HashMap<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString());
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, CAPACITY);
        originals.putAll(extra);
        return SharedStorageConfiguration.from(new StorageExtensionContext(originals, List.of(new File(logDir.toString())),
            3, Time.SYSTEM));
    }
}
