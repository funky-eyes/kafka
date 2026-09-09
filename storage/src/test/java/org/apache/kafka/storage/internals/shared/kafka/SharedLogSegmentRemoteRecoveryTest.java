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
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.ObjectStore;
import org.apache.kafka.storage.internals.shared.object.SharedObjectReader;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedLogSegmentRemoteRecoveryTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(11L, 12L, 0);

    @TempDir
    Path tempDir;

    @Test
    void shouldRebuildLogicalStateFromExactRemoteBatchWithoutConsumingWal() throws Exception {
        MemoryRecords records = records(0L, 9, 1000L, "remote-a", "remote-b");
        KafkaRecordBatchAdapter.SerializedBatch batch = onlyBatch(records);
        long objectId = 1001L;
        RemoteObjectIndex.RangeReference reference = remoteReference(
            objectId,
            batch,
            batch.leaderEpoch(),
            batch.bytes().remaining(),
            crc32c(batch.bytes())
        );

        Path walDir = tempDir.resolve("exact-remote-wal");
        File logDir = tempDir.resolve("exact-remote-topic-0").toFile();
        Files.createDirectories(logDir.toPath());

        try (SharedStorageEngine engine = engine(walDir)) {
            engine.remoteIndex().restore(List.of(reference));
            engine.installRemoteReader(new SharedObjectReader(
                new SingleObjectStore(objectId, batch.bytes()),
                engine.remoteIndex()
            ));

            long walBytesBefore = engine.walUsedBytes();
            SharedLogSegment segment = SharedLogSegment.open(
                logDir,
                batch.firstOffset(),
                new LogConfig(new Properties()),
                new MockTime(),
                engine,
                PARTITION,
                false,
                ""
            );

            segment.append(batch.lastOffset(), records);

            assertEquals(walBytesBefore, engine.walUsedBytes());
            assertTrue(engine.localLocations(PARTITION, batch.firstOffset(), batch.lastOffset() + 1).isEmpty());
            assertEquals(records.sizeInBytes(), segment.size());
            assertEquals(batch.lastOffset() + 1, segment.readNextOffset());

            FetchDataInfo fetch = segment.read(
                batch.firstOffset(),
                Integer.MAX_VALUE,
                Optional.of((long) segment.size()),
                false
            );
            assertNotNull(fetch);
            assertEquals(records.sizeInBytes(), fetch.records.sizeInBytes());
            segment.close();
        }
    }

    @Test
    void shouldMaterializeNewRemotePrefixWhenWalTailAlreadyRaisesLeo() throws Exception {
        MemoryRecords remoteRecords = records(0L, 3, 1500L, "remote-prefix-a", "remote-prefix-b");
        MemoryRecords tailRecords = records(2L, 4, 1600L, "wal-tail-a", "wal-tail-b");
        KafkaRecordBatchAdapter.SerializedBatch remoteBatch = onlyBatch(remoteRecords);
        KafkaRecordBatchAdapter.SerializedBatch tailBatch = onlyBatch(tailRecords);
        long objectId = 1501L;
        RemoteObjectIndex.RangeReference reference = remoteReference(
            objectId,
            remoteBatch,
            remoteBatch.leaderEpoch(),
            remoteBatch.bytes().remaining(),
            crc32c(remoteBatch.bytes())
        );

        Path walDir = tempDir.resolve("remote-prefix-wal-tail-wal");
        File logDir = tempDir.resolve("remote-prefix-wal-tail-topic-0").toFile();
        Files.createDirectories(logDir.toPath());

        try (SharedStorageEngine engine = engine(walDir)) {
            SharedLogSegment initial = SharedLogSegment.open(
                logDir,
                0L,
                new LogConfig(new Properties()),
                new MockTime(),
                engine,
                PARTITION,
                false,
                ""
            );
            initial.append(tailBatch.lastOffset(), tailRecords);
            initial.close();

            SharedLogSegment beforeMetadataReplay = SharedLogSegment.open(
                logDir,
                0L,
                new LogConfig(new Properties()),
                new MockTime(),
                engine,
                PARTITION,
                true,
                ""
            );
            assertEquals(tailBatch.lastOffset() + 1, beforeMetadataReplay.readNextOffset());
            FetchDataInfo beforeReplayFetch = beforeMetadataReplay.read(
                remoteBatch.firstOffset(),
                Integer.MAX_VALUE,
                Optional.of((long) beforeMetadataReplay.size()),
                false
            );
            assertNotNull(beforeReplayFetch);
            assertEquals(tailRecords.sizeInBytes(), beforeReplayFetch.records.sizeInBytes(),
                "Before metadata replay the logical fetch view must contain only the surviving WAL tail");

            long revisionBeforeReplay = engine.remoteIndex().revision(PARTITION);
            engine.remoteIndex().restore(List.of(reference));
            engine.installRemoteReader(new SharedObjectReader(
                new SingleObjectStore(objectId, remoteBatch.bytes()),
                engine.remoteIndex()
            ));
            long revisionAfterReplay = engine.remoteIndex().revision(PARTITION);
            assertTrue(revisionAfterReplay > revisionBeforeReplay);
            FetchDataInfo staleFetch = beforeMetadataReplay.read(
                remoteBatch.firstOffset(),
                Integer.MAX_VALUE,
                Optional.of((long) beforeMetadataReplay.size()),
                false
            );
            assertNotNull(staleFetch);
            assertEquals(tailRecords.sizeInBytes(), staleFetch.records.sizeInBytes(),
                "A segment loaded before metadata replay must retain its stale WAL-only fetch view until reopened");
            beforeMetadataReplay.close();

            SharedLogSegment afterMetadataReplay = SharedLogSegment.open(
                logDir,
                0L,
                new LogConfig(new Properties()),
                new MockTime(),
                engine,
                PARTITION,
                true,
                ""
            );
            assertNotNull(afterMetadataReplay.translateOffset(remoteBatch.firstOffset()));
            assertNotNull(afterMetadataReplay.translateOffset(tailBatch.firstOffset()));
            assertEquals(tailBatch.lastOffset() + 1, afterMetadataReplay.readNextOffset());

            FetchDataInfo fetch = afterMetadataReplay.read(
                remoteBatch.firstOffset(),
                Integer.MAX_VALUE,
                Optional.of((long) afterMetadataReplay.size()),
                false
            );
            assertNotNull(fetch);
            assertEquals(remoteRecords.sizeInBytes() + tailRecords.sizeInBytes(), fetch.records.sizeInBytes(),
                "Reopening after metadata replay must materialize the remote prefix ahead of the WAL tail");

            engine.remoteIndex().restore(List.of(reference));
            assertEquals(revisionAfterReplay, engine.remoteIndex().revision(PARTITION),
                "Replaying identical authoritative metadata must not force another rematerialization");
            afterMetadataReplay.close();
        }
    }

    @Test
    void shouldReWalWhenRemoteIdentityDoesNotExactlyMatch() throws Exception {
        MemoryRecords records = records(0L, 13, 2000L, "local-a", "local-b");
        KafkaRecordBatchAdapter.SerializedBatch batch = onlyBatch(records);
        long checksum = crc32c(batch.bytes());

        assertReWalForMismatch(
            "checksum",
            2001L,
            records,
            batch,
            batch.leaderEpoch(),
            batch.bytes().remaining(),
            checksum ^ 1L
        );
        assertReWalForMismatch(
            "leader-epoch",
            2002L,
            records,
            batch,
            batch.leaderEpoch() + 1,
            batch.bytes().remaining(),
            checksum
        );
        assertReWalForMismatch(
            "length",
            2003L,
            records,
            batch,
            batch.leaderEpoch(),
            batch.bytes().remaining() + 1,
            checksum
        );
    }

    private void assertReWalForMismatch(
        String name,
        long objectId,
        MemoryRecords records,
        KafkaRecordBatchAdapter.SerializedBatch batch,
        int remoteLeaderEpoch,
        int remoteLength,
        long remoteChecksum
    ) throws Exception {
        Path walDir = tempDir.resolve(name + "-wal");
        File logDir = tempDir.resolve(name + "-topic-0").toFile();
        Files.createDirectories(logDir.toPath());

        try (SharedStorageEngine engine = engine(walDir)) {
            engine.remoteIndex().restore(List.of(remoteReference(
                objectId,
                batch,
                remoteLeaderEpoch,
                remoteLength,
                remoteChecksum
            )));

            long walBytesBefore = engine.walUsedBytes();
            SharedLogSegment segment = SharedLogSegment.open(
                logDir,
                batch.firstOffset(),
                new LogConfig(new Properties()),
                new MockTime(),
                engine,
                PARTITION,
                false,
                ""
            );

            segment.append(batch.lastOffset(), records);

            assertTrue(engine.walUsedBytes() > walBytesBefore, name + " mismatch must append to WAL");
            assertEquals(
                1,
                engine.localLocations(PARTITION, batch.firstOffset(), batch.lastOffset() + 1).size(),
                name + " mismatch must retain a local WAL batch"
            );
            segment.close();
        }
    }

    private SharedStorageEngine engine(Path walDir) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(walDir, 1024 * 1024, 4096));
    }

    private static RemoteObjectIndex.RangeReference remoteReference(
        long objectId,
        KafkaRecordBatchAdapter.SerializedBatch batch,
        int leaderEpoch,
        int objectLength,
        long checksum
    ) {
        SharedObjectRange range = new SharedObjectRange(
            PARTITION,
            new OffsetRange(batch.firstOffset(), batch.lastOffset() + 1),
            leaderEpoch,
            0L,
            objectLength,
            checksum
        );
        return new RemoteObjectIndex.RangeReference(objectId, range);
    }

    private static KafkaRecordBatchAdapter.SerializedBatch onlyBatch(MemoryRecords records) {
        List<KafkaRecordBatchAdapter.SerializedBatch> batches = KafkaRecordBatchAdapter.serializeBatches(records);
        assertEquals(1, batches.size());
        return batches.get(0);
    }

    private static long crc32c(ByteBuffer bytes) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes.duplicate());
        return checksum.getValue();
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

    private static final class SingleObjectStore implements ObjectStore {
        private final long objectId;
        private final ByteBuffer object;

        private SingleObjectStore(long objectId, ByteBuffer object) {
            this.objectId = objectId;
            ByteBuffer copy = ByteBuffer.allocate(object.remaining());
            copy.put(object.duplicate());
            copy.flip();
            this.object = copy.asReadOnlyBuffer();
        }

        @Override
        public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("put not used"));
        }

        @Override
        public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
            if (objectId != this.objectId) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("unknown object " + objectId));
            }
            if (position < 0 || length < 0 || position > object.limit() - (long) length) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("invalid range"));
            }
            ByteBuffer range = object.duplicate();
            range.position(Math.toIntExact(position));
            range.limit(Math.toIntExact(position + length));
            return CompletableFuture.completedFuture(range.slice().asReadOnlyBuffer());
        }

        @Override
        public CompletableFuture<Void> delete(long objectId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("delete not used"));
        }
    }
}
