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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.InMemoryObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.InMemoryObjectStore;
import org.apache.kafka.storage.internals.shared.object.SharedObjectPacker;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUploadRoleIdentityTest {
    private static final SharedPartitionId DATA_PARTITION = new SharedPartitionId(10L, 20L, 0);
    private static final SharedPartitionId DIFFERENT_TOPIC_SAME_PARTITION = new SharedPartitionId(11L, 21L, 0);

    @TempDir
    Path tempDir;

    @Test
    void onlyExactTopicIdAndPartitionLeadershipUnlocksUpload() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("wal"), 1024 * 1024, 4096))) {
            engine.appendData(
                DATA_PARTITION,
                3,
                0L,
                9L,
                ByteBuffer.wrap(new byte[] {1, 2, 3})
            ).get(10, TimeUnit.SECONDS);

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(DATA_PARTITION, 0L);
            progress.onHighWatermarkUpdated(DATA_PARTITION, 10L);
            progress.onLeader(DIFFERENT_TOPIC_SAME_PARTITION);

            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine
            );
            try (SharedUploadScheduler scheduler = new SharedUploadScheduler(
                engine,
                progress,
                uploader,
                () -> 100L,
                () -> 1_000L,
                1024L
            )) {
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
                assertFalse(engine.remoteIndex().coverage(DATA_PARTITION).covers(new OffsetRange(0L, 10L)));

                progress.onLeader(DATA_PARTITION);
                assertTrue(scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS).isPresent());
                assertTrue(engine.remoteIndex().coverage(DATA_PARTITION).covers(new OffsetRange(0L, 10L)));
            }
        }
    }
}
