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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUploadTriggerPolicyTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11L, 12L, 0);

    @TempDir
    Path tempDir;

    @Test
    void scheduledUploadWaitsWhenNoTriggerIsSatisfied() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        try (SharedStorageEngine engine = engine("wait", 1024 * 1024L);
             SharedUploadScheduler scheduler = scheduler(engine, clock, 1024L, 10_000L, 100)) {
            append(engine, new byte[] {1, 2, 3});

            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
        }
    }

    @Test
    void scheduledUploadStartsImmediatelyAtTargetSize() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        try (SharedStorageEngine engine = engine("size", 1024 * 1024L);
             SharedUploadScheduler scheduler = scheduler(engine, clock, 3L, 10_000L, 100)) {
            append(engine, new byte[] {1, 2, 3});

            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isPresent());
        }
    }

    @Test
    void scheduledUploadStartsAfterOldestCandidateReachesMaxLinger() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        try (SharedStorageEngine engine = engine("linger", 1024 * 1024L);
             SharedUploadScheduler scheduler = scheduler(engine, clock, 1024L, 500L, 100)) {
            append(engine, new byte[] {1, 2, 3});

            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
            clock.addAndGet(499L);
            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isEmpty());
            clock.incrementAndGet();
            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isPresent());
        }
    }

    @Test
    void scheduledUploadStartsUnderWalPressureEvenBelowSizeAndLinger() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        try (SharedStorageEngine engine = engine("pressure", 1024L);
             SharedUploadScheduler scheduler = scheduler(engine, clock, 1024L, 60_000L, 1)) {
            append(engine, new byte[] {1, 2, 3});

            assertTrue(scheduler.tryScheduledUploadOnce().get(10, TimeUnit.SECONDS).isPresent());
        }
    }

    private SharedStorageEngine engine(String name, long capacityBytes) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), capacityBytes, 512L));
    }

    private static SharedUploadScheduler scheduler(
        SharedStorageEngine engine,
        AtomicLong clock,
        long targetObjectBytes,
        long maxLingerMs,
        int pressurePercent
    ) {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        SharedObjectUploader uploader = new SharedObjectUploader(
            objectStore,
            metadataStore,
            new SharedObjectPacker(),
            engine
        );
        AtomicLong objectIds = new AtomicLong(1L);
        return new SharedUploadScheduler(
            engine,
            leaderProgress(),
            uploader,
            objectIds::getAndIncrement,
            clock::get,
            targetObjectBytes,
            maxLingerMs,
            pressurePercent
        );
    }

    private static SharedCommitProgress leaderProgress() {
        SharedCommitProgress progress = new SharedCommitProgress();
        progress.onLogLoaded(PARTITION, 0L);
        progress.onHighWatermarkUpdated(PARTITION, 10L);
        progress.onLeader(PARTITION);
        return progress;
    }

    private static void append(SharedStorageEngine engine, byte[] payload) throws Exception {
        engine.appendData(
            PARTITION,
            3,
            0L,
            9L,
            ByteBuffer.wrap(payload)
        ).get(10, TimeUnit.SECONDS);
    }
}
