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

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedCommitProgressTest {
    @Test
    void followsKafkaHighWatermarkExactlyRatherThanTakingMaximum() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 3);

        progress.onHighWatermarkUpdated(partition, 100L);
        assertEquals(100L, progress.highWatermark(partition).orElseThrow());

        // Recovery or leadership changes can restore a lower HW. Kafka remains the authority.
        progress.onHighWatermarkUpdated(partition, 80L);
        assertEquals(80L, progress.highWatermark(partition).orElseThrow());
    }

    @Test
    void snapshotsKafkaLogStartHighWatermarkAndRoleAsOneCommitWindow() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 6);

        progress.onLogLoaded(partition, 20L);
        SharedCommitProgress.PartitionProgress loaded = progress.partitionProgress(partition).orElseThrow();
        assertEquals(20L, loaded.logStartOffset());
        assertEquals(20L, loaded.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.UNKNOWN, loaded.role());

        progress.onHighWatermarkUpdated(partition, 50L);
        progress.onLeader(partition);
        SharedCommitProgress.PartitionProgress snapshot = progress.snapshot().get(partition);
        assertEquals(20L, snapshot.logStartOffset());
        assertEquals(50L, snapshot.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.LEADER, snapshot.role());
    }

    @Test
    void roleTransitionsDoNotChangeKafkaOffsets() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 8);

        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 40L);
        progress.onLeader(partition);
        progress.onFollower(partition);

        SharedCommitProgress.PartitionProgress snapshot = progress.partitionProgress(partition).orElseThrow();
        assertEquals(10L, snapshot.logStartOffset());
        assertEquals(40L, snapshot.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.FOLLOWER, snapshot.role());
    }

    @Test
    void logReloadCanReplaceLogStartWithoutInventingAHighWatermarkOrRole() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 7);

        progress.onLogLoaded(partition, 10L);
        progress.onHighWatermarkUpdated(partition, 40L);
        progress.onLeader(partition);
        progress.onLogLoaded(partition, 25L);

        SharedCommitProgress.PartitionProgress snapshot = progress.partitionProgress(partition).orElseThrow();
        assertEquals(25L, snapshot.logStartOffset());
        assertEquals(40L, snapshot.highWatermark());
        assertEquals(SharedCommitProgress.ReplicaRole.LEADER, snapshot.role());
    }

    @Test
    void removesPartitionProgress() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 4);
        progress.onHighWatermarkUpdated(partition, 10L);

        progress.remove(partition);

        assertFalse(progress.highWatermark(partition).isPresent());
    }

    @Test
    void rejectsNegativeOffsets() {
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 5);

        assertThrows(
            IllegalArgumentException.class,
            () -> progress.onHighWatermarkUpdated(partition, -1L)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> progress.onLogLoaded(partition, -1L)
        );
    }
}
