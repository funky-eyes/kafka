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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;
import org.apache.kafka.storage.internals.log.VerificationGuard;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.CoordinatedRingWalAdapter;
import org.apache.kafka.storage.internals.shared.wal.ParallelWalTestInspector;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUnifiedLogTrackedAppendTest {
    @TempDir
    Path tempDir;

    @Test
    void asynchronousLeaderAppendReturnsBeforeForceButCompletesAfterDurability() throws Exception {
        var backend = new TrackedWalTestBackend(false, 0);
        Uuid topicId = Uuid.randomUuid();
        var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
        MockTime time = new MockTime();
        BrokerTopicStats stats = new BrokerTopicStats(false);
        var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("callback-wal"), backend, 65536L, 4096,
            1_000_000L, 8, 8192L);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            Path dir = tempDir.resolve("callback-0");
            var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
            SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
            log.enableExperimentalAsyncAppend(partition, 8, 8192L);
            try {
                backend.arm();
                MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, 3,
                    new SimpleRecord(1L, new byte[24]));
                var pending = log.appendAsLeaderAsync(records, 3, AppendOrigin.CLIENT, RequestLocal.noCaching(),
                    VerificationGuard.SENTINEL, (short) 0);
                assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                assertFalse(pending.isDone());
                assertEquals(0L, log.logEndOffset());
                var requestBuffer = records.buffer().duplicate();
                byte[] expected = new byte[requestBuffer.remaining()];
                requestBuffer.duplicate().get(expected);
                for (int i = requestBuffer.position(); i < requestBuffer.limit(); i++) requestBuffer.put(i, (byte) 0);
                backend.forceRelease.countDown();
                assertEquals(0L, pending.get(10, TimeUnit.SECONDS).firstOffset());
                assertEquals(1L, log.logEndOffset());
                var stored = engine.readLocal(partition, 0).orElseThrow().payload();
                byte[] actual = new byte[stored.remaining()];
                stored.get(actual);
                assertArrayEquals(expected, actual, "WAL must own its data after the request buffer is released");
            } finally {
                backend.forceRelease.countDown();
                log.close();
            }
        } finally {
            stats.close();
        }
    }

    @Test
    void synchronousIdempotentAppendDrainsTheExperimentalPipeline() throws Exception {
        var backend = new TrackedWalTestBackend(false, 0);
        Uuid topicId = Uuid.randomUuid();
        var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
        MockTime time = new MockTime();
        BrokerTopicStats stats = new BrokerTopicStats(false);
        var executor = Executors.newFixedThreadPool(2);
        var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("drain-wal"), backend, 65536L, 4096, 500_000_000L, 8, 8192L);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            Path dir = tempDir.resolve("drain-0");
            var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
            SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
            log.enableExperimentalAsyncAppend(partition, 8, 8192L);
            MemoryRecords regular = MemoryRecords.withRecords(0L, Compression.NONE, 3, new SimpleRecord(1L, new byte[24]));
            assertTrue(log.allowsExperimentalAsyncAppend(regular));
            assertFalse(log.allowsExperimentalAsyncAppend(idempotentRecords()));
            try {
                backend.arm();
                var first = executor.submit(() -> log.appendAsLeaderWithDurableWait(regular, 3, AppendOrigin.CLIENT,
                    RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                var second = executor.submit(() -> log.appendAsLeader(idempotentRecords(), 3, AppendOrigin.CLIENT,
                    RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                assertThrows(java.util.concurrent.TimeoutException.class, () -> second.get(200, TimeUnit.MILLISECONDS));
                assertEquals(0L, log.logEndOffset());
                backend.forceRelease.countDown();
                assertEquals(0L, first.get(10, TimeUnit.SECONDS).firstOffset());
                assertEquals(1L, second.get(10, TimeUnit.SECONDS).firstOffset());
                assertEquals(2L, log.logEndOffset());
            } finally {
                backend.forceRelease.countDown();
                executor.shutdownNow();
                log.close();
            }
        } finally {
            backend.forceRelease.countDown();
            executor.shutdownNow();
            stats.close();
        }
    }

    @Test
    void asynchronousFailureFencesFallbackAndAvoidsSpeculativeCloseSnapshot() throws Exception {
        for (boolean aio : java.util.List.of(false, true)) {
            var backend = new TrackedWalTestBackend(aio, 1);
            Uuid topicId = Uuid.randomUuid();
            var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
            MockTime time = new MockTime();
            BrokerTopicStats stats = new BrokerTopicStats(false);
            var executor = Executors.newSingleThreadExecutor();
            var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("fence-wal-" + aio), backend,
                65536L, 4096, 500_000_000L, 8, 8192L);
            try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
                Path dir = tempDir.resolve("fence-" + aio + "-0");
                var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                    time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                    600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                    false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
                SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
                log.enableExperimentalAsyncAppend(partition, 8, 8192L);
                MemoryRecords regular = MemoryRecords.withRecords(0L, Compression.NONE, 3, new SimpleRecord(1L, new byte[24]));
                try {
                    backend.arm();
                    var first = executor.submit(() -> log.appendAsLeaderWithDurableWait(regular, 3, AppendOrigin.CLIENT,
                        RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                    assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                    backend.forceRelease.countDown();
                    assertThrows(ExecutionException.class, () -> first.get(10, TimeUnit.SECONDS));
                    assertEquals(0L, log.logEndOffset());
                    assertThrows(KafkaStorageException.class, () -> log.appendAsLeader(idempotentRecords(), 3,
                        AppendOrigin.CLIENT, RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                    assertThrows(KafkaStorageException.class, log::close);
                    assertTrue(log.latestProducerSnapshotOffset().orElse(0L) <= 0L);
                } finally {
                    backend.forceRelease.countDown();
                    executor.shutdownNow();
                }
            } finally {
                backend.forceRelease.countDown();
                executor.shutdownNow();
                stats.close();
            }
        }
    }

    @Test
    void truncationDrainsTrackedWritesBeforeChangingOffsets() throws Exception {
        for (boolean aio : java.util.List.of(false, true)) {
            var backend = new TrackedWalTestBackend(aio, 0);
            Uuid topicId = Uuid.randomUuid();
            var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
            MockTime time = new MockTime();
            BrokerTopicStats stats = new BrokerTopicStats(false);
            var executor = Executors.newFixedThreadPool(2);
            var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("truncate-wal-" + aio), backend,
                65536L, 4096, 500_000_000L, 8, 8192L);
            try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
                Path dir = tempDir.resolve("truncate-" + aio + "-0");
                var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                    time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                    600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                    false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
                SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
                log.enableExperimentalAsyncAppend(partition, 8, 8192L);
                MemoryRecords regular = MemoryRecords.withRecords(0L, Compression.NONE, 3, new SimpleRecord(1L, new byte[24]));
                try {
                    backend.arm();
                    var first = executor.submit(() -> log.appendAsLeaderWithDurableWait(regular, 3, AppendOrigin.CLIENT,
                        RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                    assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                    var truncation = executor.submit(() -> log.truncateTo(0L));
                    assertThrows(java.util.concurrent.TimeoutException.class, () -> truncation.get(200, TimeUnit.MILLISECONDS));
                    assertEquals(0L, log.logEndOffset());
                    backend.forceRelease.countDown();
                    first.get(10, TimeUnit.SECONDS);
                    assertTrue(truncation.get(10, TimeUnit.SECONDS));
                    assertEquals(0L, log.logEndOffset());
                    var next = log.appendAsLeaderWithDurableWait(regular, 3, AppendOrigin.CLIENT,
                        RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0);
                    assertEquals(0L, next.firstOffset());
                } finally {
                    backend.forceRelease.countDown();
                    executor.shutdownNow();
                    log.close();
                }
            } finally {
                backend.forceRelease.countDown();
                executor.shutdownNow();
                stats.close();
            }
        }
    }

    @Test
    void segmentRollWaitsForPriorDurabilityBeforeSnapshot() throws Exception {
        var backend = new TrackedWalTestBackend(false, 0);
        Uuid topicId = Uuid.randomUuid();
        var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
        MockTime time = new MockTime();
        BrokerTopicStats stats = new BrokerTopicStats(false);
        var executor = Executors.newFixedThreadPool(2);
        var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("roll-wal"), backend, 4L * 1024 * 1024, 1024 * 1024, 500_000_000L, 8, 2L * 1024 * 1024);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            Properties properties = new Properties();
            properties.put("segment.bytes", "1048576");
            Path dir = tempDir.resolve("roll-0");
            var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(properties), 0L, 0L,
                time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
            SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
            log.enableExperimentalAsyncAppend(partition, 8, 2L * 1024 * 1024);
            MemoryRecords regular = MemoryRecords.withRecords(0L, Compression.NONE, 3, new SimpleRecord(1L, new byte[600_000]));
            try {
                backend.arm();
                var first = executor.submit(() -> log.appendAsLeaderWithDurableWait(regular, 3, AppendOrigin.CLIENT,
                    RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                var second = executor.submit(() -> log.appendAsLeaderWithDurableWait(regular, 3, AppendOrigin.CLIENT,
                    RequestLocal.noCaching(), VerificationGuard.SENTINEL, (short) 0));
                assertThrows(java.util.concurrent.TimeoutException.class, () -> second.get(200, TimeUnit.MILLISECONDS));
                assertEquals(1L, log.experimentalReservedEndOffset());
                assertEquals(0L, log.logEndOffset());
                backend.forceRelease.countDown();
                assertEquals(0L, first.get(10, TimeUnit.SECONDS).firstOffset());
                assertEquals(1L, second.get(10, TimeUnit.SECONDS).firstOffset());
                assertEquals(2L, log.logEndOffset());
            } finally {
                backend.forceRelease.countDown();
                executor.shutdownNow();
                log.close();
            }
        } finally {
            backend.forceRelease.countDown();
            executor.shutdownNow();
            stats.close();
        }
    }

    @Test
    void kafkaLogCanReserveSeveralAppendsButNoneAreConfirmedBeforeWal() throws Exception {
        for (boolean aio : java.util.List.of(false, true)) {
            var backend = new TrackedWalTestBackend(aio, 0);
            Uuid topicId = Uuid.randomUuid();
            var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
            MockTime time = new MockTime();
            BrokerTopicStats stats = new BrokerTopicStats(false);
            var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("unified-wal-" + aio), backend, 65536L, 4096, 1_000_000_000L, 8, 8192L);
            try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
                Path dir = tempDir.resolve("tracked-" + aio + "-0");
                var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                    time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                    600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                    false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
                SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
                var tracker = new PartitionDurableOffsetTracker(1L, log.logEndOffset(), Runnable::run, 8, 8192L);
                try {
                    backend.arm();
                    var first = append(log, tracker, partition, 0L);
                    assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                    var second = append(log, tracker, partition, 1L);
                    var third = append(log, tracker, partition, 2L);
                    assertEquals(3L, log.experimentalReservedEndOffset());
                    assertEquals(0L, log.logEndOffset());
                    assertEquals(0L, log.logEndOffsetMetadata().messageOffset);
                    assertEquals(0L, log.fetchOffsetSnapshot().logEndOffset().messageOffset);
                    assertEquals(0L, log.endOffsetForEpoch(3).orElseThrow().offset());
                    assertEquals(0L, log.highWatermark());
                    assertEquals(0L, log.updateHighWatermark(3L));
                    assertEquals(0L, log.highWatermark());
                    assertEquals(0, log.read(0L, 65536, FetchIsolation.LOG_END, false).records.sizeInBytes());
                    assertEquals(0L, tracker.confirmedOffset());
                    assertFalse(first.confirmation().isDone());
                    assertFalse(second.confirmation().isDone());
                    assertFalse(third.confirmation().isDone());
                    backend.forceRelease.countDown();
                    first.confirmation().get(10, TimeUnit.SECONDS);
                    second.confirmation().get(10, TimeUnit.SECONDS);
                    third.confirmation().get(10, TimeUnit.SECONDS);
                    assertEquals(3L, tracker.confirmedOffset());
                    assertEquals(3L, log.logEndOffset());
                    assertEquals(3L, log.logEndOffsetMetadata().messageOffset);
                    assertEquals(3L, log.endOffsetForEpoch(3).orElseThrow().offset());
                    log.updateHighWatermark(3L);
                    assertTrue(log.read(0L, 65536, FetchIsolation.LOG_END, false).records.sizeInBytes() > 0);
                    assertEquals(2L, ParallelWalTestInspector.blocks(wal));
                } finally {
                    backend.forceRelease.countDown();
                    log.close();
                }
            } finally {
                backend.forceRelease.countDown();
                stats.close();
            }
        }
    }

    @Test
    void duplicateIdempotentRetryWaitsForTheOriginalDurability() throws Exception {
        var backend = new TrackedWalTestBackend(false, 0);
        Uuid topicId = Uuid.randomUuid();
        var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
        MockTime time = new MockTime();
        BrokerTopicStats stats = new BrokerTopicStats(false);
        var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("duplicate-wal"), backend, 65536L, 4096, 1_000_000_000L, 8, 8192L);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            Path dir = tempDir.resolve("duplicate-0");
            var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
            SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
            var tracker = new PartitionDurableOffsetTracker(1L, log.logEndOffset(), Runnable::run, 8, 8192L);
            try {
                backend.arm();
                var first = log.appendAsLeaderTracked(idempotentRecords(), 3, AppendOrigin.CLIENT, RequestLocal.noCaching(),
                    VerificationGuard.SENTINEL, (short) 0, partition, tracker, 1L);
                assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                var duplicate = log.appendAsLeaderTracked(idempotentRecords(), 3, AppendOrigin.CLIENT, RequestLocal.noCaching(),
                    VerificationGuard.SENTINEL, (short) 0, partition, tracker, 1L);
                assertEquals(0L, first.info().firstOffset());
                assertEquals(0L, duplicate.info().firstOffset());
                assertEquals(1L, log.experimentalReservedEndOffset());
                assertEquals(0L, log.logEndOffset());
                assertFalse(first.confirmation().isDone());
                assertFalse(duplicate.confirmation().isDone());
                backend.forceRelease.countDown();
                first.confirmation().get(10, TimeUnit.SECONDS);
                duplicate.confirmation().get(10, TimeUnit.SECONDS);
                assertEquals(1L, wal.durabilityStats().durableAppendGroupCount());
            } finally {
                backend.forceRelease.countDown();
                log.close();
            }
        } finally {
            backend.forceRelease.countDown();
            stats.close();
        }
    }

    @Test
    void kafkaAppendsAreFencedWhenEitherWalBarrierFails() throws Exception {
        for (boolean aio : java.util.List.of(false, true)) {
            for (int failingForce : java.util.List.of(1, 2)) {
                var backend = new TrackedWalTestBackend(aio, failingForce);
                Uuid topicId = Uuid.randomUuid();
                var partition = new SharedPartitionId(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits(), 0);
                MockTime time = new MockTime();
                BrokerTopicStats stats = new BrokerTopicStats(false);
                var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("failed-wal-" + aio + "-" + failingForce),
                    backend, 65536L, 4096, 1_000_000_000L, 8, 8192L);
                try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
                    Path dir = tempDir.resolve("failed-log-" + aio + "-" + failingForce + "-0");
                    var context = new UnifiedLogCreationContext(dir.toFile(), new LogConfig(new Properties()), 0L, 0L,
                        time.scheduler, stats, time, 3600000, new ProducerStateManagerConfig(3600000, false),
                        600000, new LogDirFailureChannel(10), true, Optional.of(topicId), new ConcurrentHashMap<>(),
                        false, LogOffsetsListener.NO_OP_OFFSETS_LISTENER);
                    SharedUnifiedLog log = (SharedUnifiedLog) new SharedUnifiedLogFactory(engine).create(context);
                    var tracker = new PartitionDurableOffsetTracker(1L, log.logEndOffset(), Runnable::run, 8, 8192L);
                    try {
                        backend.arm();
                        var first = append(log, tracker, partition, 0L);
                        assertTrue(backend.forceEntered.await(10, TimeUnit.SECONDS));
                        var second = append(log, tracker, partition, 1L);
                        backend.forceRelease.countDown();
                        assertThrows(ExecutionException.class, () -> first.confirmation().get(10, TimeUnit.SECONDS));
                        assertThrows(ExecutionException.class, () -> second.confirmation().get(10, TimeUnit.SECONDS));
                        assertEquals(0L, tracker.confirmedOffset());
                        assertTrue(tracker.awaitSettled(10, TimeUnit.SECONDS));
                        assertThrows(IllegalStateException.class, () -> append(log, tracker, partition, 2L));
                    } finally {
                        backend.forceRelease.countDown();
                        log.close();
                    }
                } finally {
                    backend.forceRelease.countDown();
                    stats.close();
                }
            }
        }
    }

    private static MemoryRecords idempotentRecords() {
        return MemoryRecords.withIdempotentRecords(0L, Compression.NONE, 42L, (short) 0, 0, 3,
            new SimpleRecord(1L, new byte[24]));
    }

    private static SharedUnifiedLog.TrackedAppend append(SharedUnifiedLog log, PartitionDurableOffsetTracker tracker,
                                                         SharedPartitionId partition, long offset) {
        MemoryRecords records = MemoryRecords.withRecords(0L, Compression.NONE, 3,
            new SimpleRecord(1L, new byte[24]));
        var appended = log.appendAsLeaderTracked(records, 3, AppendOrigin.CLIENT, RequestLocal.noCaching(),
            VerificationGuard.SENTINEL, (short) 0, partition, tracker, 1L);
        assertEquals(offset, appended.info().firstOffset());
        return appended;
    }
}
