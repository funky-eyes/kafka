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
package org.apache.kafka.storage.internals.shared;

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageEngineTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1, 2, 0);

    @TempDir
    Path tempDir;

    @Test
    void newLeaderShouldUploadOnlyCommittedRangesMissingFromRemoteStorage() throws Exception {
        SharedObjectMetadata firstRemoteObject;

        try (SharedStorageEngine oldLeader = engine("leader-a")) {
            append(oldLeader, 100, 109, 1);
            append(oldLeader, 110, 119, 2);
            append(oldLeader, 120, 129, 3); // leader-only tail, above HW

            List<SharedStorageEngine.UploadCandidate> beforeCrash = oldLeader.uploadCandidates(PARTITION, 100, 120);
            assertEquals(List.of(new OffsetRange(100, 110), new OffsetRange(110, 120)),
                beforeCrash.stream().map(SharedStorageEngine.UploadCandidate::offsets).toList());

            firstRemoteObject = object(10, 100, 110, 111);
            oldLeader.commitRemoteObject(firstRemoteObject);
            assertEquals(List.of(new OffsetRange(110, 120)),
                oldLeader.uploadCandidates(PARTITION, 100, 120).stream()
                    .map(SharedStorageEngine.UploadCandidate::offsets).toList());
        }

        // This broker had been a follower. The two batches below represent data durably replicated before the old leader died.
        try (SharedStorageEngine newLeader = engine("leader-b")) {
            append(newLeader, 100, 109, 1);
            append(newLeader, 110, 119, 2);
            newLeader.commitRemoteObject(firstRemoteObject); // metadata replay after failover

            List<SharedStorageEngine.UploadCandidate> recovery = newLeader.uploadCandidates(PARTITION, 100, 120);
            assertEquals(1, recovery.size());
            assertEquals(new OffsetRange(110, 120), recovery.get(0).offsets());
        }
    }

    @Test
    void shouldNeverRecoverLeaderOnlyAcksOneTailFromAnotherBroker() throws Exception {
        try (SharedStorageEngine followerThatBecomesLeader = engine("acks-one-follower")) {
            append(followerThatBecomesLeader, 100, 109, 1);

            // Old leader may have ACKed 110..129 with acks=1, but this replica never received them.
            // The new Kafka HW can only expose data this safe leader actually owns.
            List<SharedStorageEngine.UploadCandidate> candidates =
                followerThatBecomesLeader.uploadCandidates(PARTITION, 100, 110);

            assertEquals(1, candidates.size());
            assertEquals(new OffsetRange(100, 110), candidates.get(0).offsets());
            assertTrue(followerThatBecomesLeader.readLocal(PARTITION, 110).isEmpty());
        }
    }

    @Test
    void truncatedOldEpochTailMustNotBecomeUploadable() throws Exception {
        try (SharedStorageEngine engine = engine("truncate")) {
            append(engine, 100, 109, 1);
            append(engine, 110, 119, 2);
            engine.truncate(PARTITION, 4, 110).get(10, TimeUnit.SECONDS);

            assertEquals(List.of(new OffsetRange(100, 110)),
                engine.uploadCandidates(PARTITION, 100, 120).stream()
                    .map(SharedStorageEngine.UploadCandidate::offsets).toList());
            assertTrue(engine.readLocal(PARTITION, 110).isEmpty());
        }
    }

    private SharedStorageEngine engine(String name) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), 1024 * 1024, 4096));
    }

    private static void append(SharedStorageEngine engine, long firstOffset, long lastOffset, int marker) throws Exception {
        engine.appendData(
            PARTITION,
            3,
            firstOffset,
            lastOffset,
            ByteBuffer.wrap(new byte[]{(byte) marker})
        ).get(10, TimeUnit.SECONDS);
    }

    private static SharedObjectMetadata object(long objectId, long start, long end, long checksum) {
        int length = Math.toIntExact(end - start);
        return new SharedObjectMetadata(
            objectId,
            length,
            checksum,
            List.of(new SharedObjectRange(PARTITION, new OffsetRange(start, end), 3, 0, length, checksum))
        );
    }
}
