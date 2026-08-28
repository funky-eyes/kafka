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

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogFileUtils;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class SharedLogSegmentRecoveryTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);

    @TempDir
    Path tempDir;

    @Test
    void shouldRecoverLogicalSegmentsFromBrokerWal() throws Exception {
        Path walDir = tempDir.resolve("wal");
        File logDir = tempDir.resolve("topic-0").toFile();
        Files.createDirectories(logDir.toPath());
        LogConfig config = new LogConfig(new Properties());
        MockTime time = new MockTime();

        MemoryRecords firstRecords = records(0L, 7, 1000L, "a", "b");
        MemoryRecords secondRecords = records(2L, 8, 2000L, "c", "d");
        int firstSize = firstRecords.sizeInBytes();
        int secondSize = secondRecords.sizeInBytes();

        try (SharedStorageEngine engine = engine(walDir)) {
            SharedLogSegment first = SharedLogSegment.open(
                logDir, 0L, config, time, engine, PARTITION, false, "");
            first.append(1L, firstRecords);
            first.onBecomeInactiveSegment();

            SharedLogSegment second = SharedLogSegment.open(
                logDir, 2L, config, time, engine, PARTITION, false, "");
            second.append(3L, secondRecords);

            assertEquals(firstSize, first.size());
            assertEquals(secondSize, second.size());
            first.close();
            second.close();
        }

        assertEquals(0L, Files.size(LogFileUtils.logFile(logDir, 0L).toPath()));
        assertEquals(0L, Files.size(LogFileUtils.logFile(logDir, 2L).toPath()));

        try (SharedStorageEngine recoveredEngine = engine(walDir)) {
            SharedLogSegment first = SharedLogSegment.open(
                logDir, 0L, config, time, recoveredEngine, PARTITION, true, "");
            SharedLogSegment second = SharedLogSegment.open(
                logDir, 2L, config, time, recoveredEngine, PARTITION, true, "");

            assertEquals(firstSize, first.size());
            assertEquals(secondSize, second.size());
            assertEquals(2L, first.readNextOffset());
            assertEquals(4L, second.readNextOffset());

            var firstOffset = first.translateOffset(1L);
            assertNotNull(firstOffset);
            assertEquals(0L, firstOffset.offset);
            assertEquals(0, firstOffset.position);
            assertEquals(firstSize, firstOffset.size);

            var secondOffset = second.translateOffset(3L);
            assertNotNull(secondOffset);
            assertEquals(2L, secondOffset.offset);
            assertEquals(0, secondOffset.position);
            assertEquals(secondSize, secondOffset.size);

            FetchDataInfo firstFetch = first.read(0L, Integer.MAX_VALUE, Optional.of((long) first.size()), false);
            FetchDataInfo secondFetch = second.read(2L, Integer.MAX_VALUE, Optional.of((long) second.size()), false);
            assertNotNull(firstFetch);
            assertNotNull(secondFetch);
            assertEquals(firstSize, firstFetch.records.sizeInBytes());
            assertEquals(secondSize, secondFetch.records.sizeInBytes());
            assertNull(first.read(2L, Integer.MAX_VALUE, Optional.of((long) first.size()), false));

            first.close();
            second.close();
        }
    }

    private SharedStorageEngine engine(Path walDir) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(walDir, 1024 * 1024, 4096));
    }

    private static MemoryRecords records(
        long initialOffset,
        int leaderEpoch,
        long timestamp,
        String first,
        String second
    ) {
        return MemoryRecords.withRecords(
            initialOffset,
            Compression.NONE,
            leaderEpoch,
            new SimpleRecord(timestamp, first.getBytes(StandardCharsets.UTF_8)),
            new SimpleRecord(timestamp + 1, second.getBytes(StandardCharsets.UTF_8))
        );
    }
}
