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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SharedPartitionRoleListenerTest {
    @TempDir
    Path tempDir;

    @Test
    void routesOnlySelectedUserTopicsAndTracksLeaderDemotion() {
        SharedStorageConfiguration configuration = SharedStorageConfiguration.from(new StorageExtensionContext(
            Map.of(SharedStorageConfiguration.TOPICS_CONFIG, "shared-topic"),
            List.of(tempDir.toFile()),
            1,
            new MockTime()
        ));
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionRoleListener listener = new SharedPartitionRoleListener(configuration, progress);

        Uuid sharedTopicId = Uuid.randomUuid();
        TopicIdPartition shared = new TopicIdPartition(
            sharedTopicId,
            new TopicPartition("shared-topic", 0)
        );
        TopicIdPartition classic = new TopicIdPartition(
            Uuid.randomUuid(),
            new TopicPartition("classic-topic", 0)
        );
        TopicIdPartition internal = new TopicIdPartition(
            Uuid.randomUuid(),
            new TopicPartition("__consumer_offsets", 0)
        );

        listener.onLeadershipChange(List.of(shared, classic, internal), List.of());

        SharedPartitionId sharedId = new SharedPartitionId(
            sharedTopicId.getMostSignificantBits(),
            sharedTopicId.getLeastSignificantBits(),
            0
        );
        assertEquals(
            SharedCommitProgress.ReplicaRole.LEADER,
            progress.partitionProgress(sharedId).orElseThrow().role()
        );
        assertEquals(1, progress.snapshot().size());

        listener.onLeadershipChange(List.of(), List.of(shared));
        assertEquals(
            SharedCommitProgress.ReplicaRole.FOLLOWER,
            progress.partitionProgress(sharedId).orElseThrow().role()
        );
        assertFalse(progress.partitionProgress(sharedId).orElseThrow().isLeader());
    }
}
