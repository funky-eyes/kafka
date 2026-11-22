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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.ControlRecordType;
import org.apache.kafka.common.record.internal.EndTransactionMarker;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.AbortedTxn;
import org.apache.kafka.storage.internals.log.BatchMetadata;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.AsynchronousFileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.FileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.RingSharedWal;
import org.apache.kafka.storage.internals.shared.wal.CoordinatedRingWalAdapter;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CoordinatedRingWalTransactionTest {
    @TempDir
    Path dir;
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final long CAPACITY = 8192L + 65536L;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void realProducerAndAbortStateSurviveParallelBlockRecovery(boolean aio) throws Exception {
        Path original = dir.resolve("original.wal");
        Path copied = dir.resolve("copied.wal");
        Path originalLog = Files.createDirectory(dir.resolve("original-log"));
        Path copiedLog = Files.createDirectory(dir.resolve("copied-log"));
        MockTime time = new MockTime();
        LogConfig config = new LogConfig(new Properties());
        MemoryRecords data = MemoryRecords.withTransactionalRecords(0L, Compression.NONE, 123L,
            (short) 1, 0, 3, new SimpleRecord(new byte[] {1}));
        MemoryRecords abort = MemoryRecords.withEndTransactionMarker(1L, 1000L, 3, 123L, (short) 1,
            new EndTransactionMarker(ControlRecordType.ABORT, 1));
        MemoryRecords ongoing = MemoryRecords.withTransactionalRecords(2L, Compression.NONE, 456L,
            (short) 2, 0, 3, new SimpleRecord(new byte[] {2}));
        MemoryRecords committed = MemoryRecords.withTransactionalRecords(3L, Compression.NONE, 789L,
            (short) 3, 0, 3, new SimpleRecord(new byte[] {3}));
        MemoryRecords commit = MemoryRecords.withEndTransactionMarker(4L, 1001L, 3, 789L, (short) 3,
            new EndTransactionMarker(ControlRecordType.COMMIT, 1));
        MemoryRecords idempotent = MemoryRecords.withIdempotentRecords(5L, Compression.NONE, 321L,
            (short) 4, 7, 3, new SimpleRecord(new byte[] {4}));
        try (WalIoBackend backend = backend(aio); RingSharedWal wal = new RingSharedWal(original, CAPACITY, backend)) {
            wal.appendBatch(List.of(record(0, data), record(1, abort), record(2, ongoing),
                record(3, committed), record(4, commit), record(5, idempotent))).join();
        }
        List<WalRecord> replayed = new ArrayList<>();
        try (WalIoBackend backend = backend(aio); RingSharedWal wal = new RingSharedWal(original, CAPACITY, backend)) {
            wal.replay((record, location) -> replayed.add(record));
        }
        try (WalIoBackend backend = backend(aio); SharedWal wal = CoordinatedRingWalAdapter.create(copied, backend, 65536, 16384, 1_000_000)) {
            wal.appendBatch(replayed).join();
        }
        Snapshot before = recover(original, originalLog, aio, config, time, false);
        Snapshot after = recover(copied, copiedLog, aio, config, time, true);
        assertEquals(before, after);
        assertEquals(6L, after.endOffset());
        assertEquals(List.of(new AbortedTxn(123L, 0L, 1L, 2L)), after.aborted());
        assertEquals(2L, after.openTransaction());
        assertEquals(1, after.firstEpoch());
        assertEquals(2, after.secondEpoch());
    }

    private Snapshot recover(Path walPath, Path logDir, boolean aio, LogConfig config, MockTime time, boolean block) throws Exception {
        try (WalIoBackend backend = backend(aio);
             SharedWal wal = block ? CoordinatedRingWalAdapter.reopen(walPath, backend, 65536, 16384, 1_000_000) :
                 new RingSharedWal(walPath, CAPACITY, backend);
             SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            SharedLogSegment segment = SharedLogSegment.open(logDir.toFile(), 0L, config, time,
                engine, PARTITION, true, "");
            try {
                ProducerStateManager producer = new ProducerStateManager(new TopicPartition("test", 0), logDir.toFile(),
                    60000, new ProducerStateManagerConfig(60000, true), time);
                LeaderEpochFileCache epochs = Mockito.mock(LeaderEpochFileCache.class);
                Mockito.when(epochs.latestEpoch()).thenReturn(Optional.empty());
                segment.recover(producer, epochs);
                assertTrue(producer.lastEntry(123L).isPresent());
                assertTrue(producer.lastEntry(456L).isPresent());
                assertTrue(producer.lastEntry(789L).orElseThrow().currentTxnFirstOffset().isEmpty());
                assertEquals(7, producer.lastEntry(321L).orElseThrow().batchMetadata().iterator().next().lastSeq());
                return new Snapshot(producer.mapEndOffset(), segment.txnIndex().allAbortedTxns(),
                    producer.lastEntry(456L).orElseThrow().currentTxnFirstOffset().orElseThrow(),
                    producer.lastEntry(123L).orElseThrow().producerEpoch(),
                    producer.lastEntry(456L).orElseThrow().producerEpoch(),
                    List.copyOf(producer.lastEntry(321L).orElseThrow().batchMetadata()));
            } finally {
                segment.close();
            }
        }
    }

    private static WalIoBackend backend(boolean aio) {
        return aio ? new AsynchronousFileChannelWalIoBackend() : new FileChannelWalIoBackend();
    }

    private static WalRecord record(long offset, MemoryRecords records) {
        ByteBuffer buffer = records.buffer().duplicate();
        byte[] payload = new byte[buffer.remaining()];
        buffer.get(payload);
        return WalRecord.data(1L, 2L, 0, 3, offset, offset, payload);
    }

    private record Snapshot(long endOffset, List<AbortedTxn> aborted, long openTransaction, short firstEpoch,
                            short secondEpoch, List<BatchMetadata> idempotentBatches) { }
}
