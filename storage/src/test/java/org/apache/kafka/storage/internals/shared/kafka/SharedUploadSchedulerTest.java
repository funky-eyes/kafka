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
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.InMemoryObjectStore;
import org.apache.kafka.storage.internals.shared.object.SharedObjectPacker;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploader;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUploadSchedulerTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId P1 = new SharedPartitionId(3L, 4L, 1);

    @TempDir
    Path tempDir;

    @Test
    void neverUploadsWalBatchAtOrBeyondKafkaHighWatermark() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("hw-boundary")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, P0, 10L, 19L, new byte[] {4, 5});

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onHighWatermarkUpdated(P0, 10L);
            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 1024L)) {
                Optional<SharedObjectMetadata> result = scheduler.tryUploadOnce().get(10, TimeUnit.SECONDS);

                assertTrue(result.isPresent());
                assertEquals(1, result.get().ranges().size());
                assertEquals(new OffsetRange(0L, 10L), result.get().ranges().get(0).offsets());
                assertTrue(engine.remoteIndex().coverage(P0).covers(new OffsetRange(0L, 10L)));
                assertFalse(engine.remoteIndex().coverage(P0).covers(new OffsetRange(10L, 20L)));
                assertEquals(1, engine.uploadCandidates(P0, 0L, 20L).size());
            }
        }
    }

    @Test
    void packsCommittedBatchesAcrossPartitionsInPhysicalWalOrder() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("cross-partition")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3});
            append(engine, P1, 20L, 29L, new byte[] {4, 5, 6, 7});

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onLogLoaded(P1, 20L);
            progress.onHighWatermarkUpdated(P0, 10L);
            progress.onHighWatermarkUpdated(P1, 30L);
            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 1024L)) {
                SharedObjectMetadata metadata = scheduler.tryUploadOnce()
                    .get(10, TimeUnit.SECONDS)
                    .orElseThrow();

                assertEquals(2, metadata.ranges().size());
                assertEquals(P0, metadata.ranges().get(0).partition());
                assertEquals(P1, metadata.ranges().get(1).partition());
                assertTrue(engine.remoteIndex().coverage(P0).covers(new OffsetRange(0L, 10L)));
                assertTrue(engine.remoteIndex().coverage(P1).covers(new OffsetRange(20L, 30L)));
                assertTrue(metadataStore.isCommitted(metadata.objectId()));
                assertTrue(objectStore.contains(metadata.objectId()));
            }
        }
    }

    @Test
    void permitsSingleOversizedBatchSoUploadCannotStallForever() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("oversized")) {
            append(engine, P0, 0L, 9L, new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

            SharedCommitProgress progress = new SharedCommitProgress();
            progress.onLogLoaded(P0, 0L);
            progress.onHighWatermarkUpdated(P0, 10L);
            try (SharedUploadScheduler scheduler = scheduler(engine, progress, objectStore, metadataStore, 4L)) {
                SharedObjectMetadata metadata = scheduler.tryUploadOnce()
                    .get(10, TimeUnit.SECONDS)
                    .orElseThrow();

                assertEquals(1, metadata.ranges().size());
                assertEquals(new OffsetRange(0L, 10L), metadata.ranges().get(0).offsets());
            }
        }
    }

    private SharedStorageEngine engine(String name) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), 1024 * 1024, 4096));
    }

    private static SharedUploadScheduler scheduler(
        SharedStorageEngine engine,
        SharedCommitProgress progress,
        InMemoryObjectStore objectStore,
        InMemoryObjectMetadataStore metadataStore,
        long targetObjectBytes
    ) {
        AtomicLong objectIds = new AtomicLong(100L);
        SharedObjectUploader uploader = new SharedObjectUploader(
            objectStore,
            metadataStore,
            new SharedObjectPacker(),
            engine
        );
        return new SharedUploadScheduler(
            engine,
            progress,
            uploader,
            objectIds::getAndIncrement,
            () -> 1_000L,
            targetObjectBytes
        );
    }

    private static void append(
        SharedStorageEngine engine,
        SharedPartitionId partition,
        long firstOffset,
        long lastOffset,
        byte[] payload
    ) throws Exception {
        engine.appendData(
            partition,
            3,
            firstOffset,
            lastOffset,
            ByteBuffer.wrap(payload)
        ).get(10, TimeUnit.SECONDS);
    }
}
