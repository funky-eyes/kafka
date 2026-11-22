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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.storage.internals.shared.kafka.SharedStorageConfiguration;
import org.apache.kafka.storage.internals.shared.kafka.SharedUnifiedLog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class S3SharedStoragePipelineWiringTest {
    @TempDir
    Path tempDir;

    @Test
    void s3FactoryPropagatesAsyncPartitionSetting() throws Exception {
        for (boolean enabled : List.of(false, true)) {
            Path root = Files.createDirectory(tempDir.resolve("logs-" + enabled));
            Files.writeString(root.resolve("meta.properties"), "cluster.id=test-cluster\n");
            Path walDir = tempDir.resolve("wal-" + enabled);
            Time time = Time.SYSTEM;
            KafkaScheduler scheduler = new KafkaScheduler(1);
            scheduler.startup();
            BrokerTopicStats stats = new BrokerTopicStats(false);
            Map<String, Object> originals = Map.of(
                SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString(),
                SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, 65536L,
                SharedStorageConfiguration.WAL_ENGINE_CONFIG, "parallel-block",
                SharedStorageConfiguration.WAL_ASYNC_PARTITION_APPEND_CONFIG, enabled,
                S3ObjectStoreConfig.BUCKET_CONFIG, "test-bucket");
            try (var extension = new S3SharedStorageExtension()) {
                extension.start(new StorageExtensionContext(originals, List.of(root.toFile()), 3, time));
                var context = new UnifiedLogCreationContext(root.resolve("events-0").toFile(),
                    new LogConfig(Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, true)), 0L, 0L,
                    scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                    600000, new LogDirFailureChannel(10), true, Optional.of(Uuid.randomUuid()),
                    new ConcurrentHashMap<>(), false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
                var log = assertInstanceOf(SharedUnifiedLog.class, extension.unifiedLogFactory().create(context));
                try {
                    MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, 3,
                        new SimpleRecord(1L, new byte[24]));
                    assertEquals(enabled, log.allowsExperimentalAsyncAppend(records));
                } finally {
                    log.close();
                }
            } finally {
                stats.close();
                scheduler.shutdown();
            }
        }
    }
}
