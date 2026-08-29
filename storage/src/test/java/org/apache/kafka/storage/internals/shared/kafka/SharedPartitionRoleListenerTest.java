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
        SharedStorageConfiguration configuration = configuration(Map.of(
            SharedStorageConfiguration.TOPICS_CONFIG,
            "shared-topic"
        ));
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionRoleListener listener = new SharedPartitionRoleListener(configuration, progress);

        Uuid sharedTopicId = Uuid.randomUuid();
        TopicIdPartition shared = topicPartition(sharedTopicId, "shared-topic", 0);
        TopicIdPartition classic = topicPartition(Uuid.randomUuid(), "classic-topic", 0);
        TopicIdPartition internal = topicPartition(Uuid.randomUuid(), "__consumer_offsets", 0);

        listener.onLeadershipChange(List.of(shared, classic, internal), List.of());

        SharedPartitionId sharedId = sharedPartitionId(sharedTopicId, 0);
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

    @Test
    void removedReplicaClearsCommitWindowAndUploadOwnership() {
        SharedStorageConfiguration configuration = configuration(Map.of());
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionRoleListener listener = new SharedPartitionRoleListener(configuration, progress);

        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition partition = topicPartition(topicId, "shared-topic", 2);
        SharedPartitionId sharedId = sharedPartitionId(topicId, 2);
        progress.onLogLoaded(sharedId, 10L);
        progress.onHighWatermarkUpdated(sharedId, 50L);
        listener.onLeadershipChange(List.of(partition), List.of());

        listener.onPartitionsRemoved(List.of(partition));

        assertFalse(progress.partitionProgress(sharedId).isPresent());
    }

    @Test
    void removedClassicOrInternalReplicaDoesNotTouchSharedTracking() {
        SharedStorageConfiguration configuration = configuration(Map.of(
            SharedStorageConfiguration.TOPICS_CONFIG,
            "shared-topic"
        ));
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionRoleListener listener = new SharedPartitionRoleListener(configuration, progress);

        Uuid sharedTopicId = Uuid.randomUuid();
        TopicIdPartition shared = topicPartition(sharedTopicId, "shared-topic", 0);
        SharedPartitionId sharedId = sharedPartitionId(sharedTopicId, 0);
        listener.onLeadershipChange(List.of(shared), List.of());

        listener.onPartitionsRemoved(List.of(
            topicPartition(Uuid.randomUuid(), "classic-topic", 0),
            topicPartition(Uuid.randomUuid(), "__consumer_offsets", 0)
        ));

        assertEquals(1, progress.snapshot().size());
        assertEquals(
            SharedCommitProgress.ReplicaRole.LEADER,
            progress.partitionProgress(sharedId).orElseThrow().role()
        );
    }

    @Test
    void defaultAllUserTopicRoutingStillNeverTracksInternalTopics() {
        SharedStorageConfiguration configuration = configuration(Map.of());
        SharedCommitProgress progress = new SharedCommitProgress();
        SharedPartitionRoleListener listener = new SharedPartitionRoleListener(configuration, progress);

        Uuid userTopicId = Uuid.randomUuid();
        listener.onLeadershipChange(
            List.of(
                topicPartition(userTopicId, "user-topic", 1),
                topicPartition(Uuid.randomUuid(), "__transaction_state", 1)
            ),
            List.of()
        );

        assertEquals(1, progress.snapshot().size());
        assertEquals(
            SharedCommitProgress.ReplicaRole.LEADER,
            progress.partitionProgress(sharedPartitionId(userTopicId, 1)).orElseThrow().role()
        );
    }

    private SharedStorageConfiguration configuration(Map<String, ?> originals) {
        return SharedStorageConfiguration.from(new StorageExtensionContext(
            originals,
            List.of(tempDir.toFile()),
            1,
            new MockTime()
        ));
    }

    private static TopicIdPartition topicPartition(Uuid topicId, String topic, int partition) {
        return new TopicIdPartition(topicId, new TopicPartition(topic, partition));
    }

    private static SharedPartitionId sharedPartitionId(Uuid topicId, int partition) {
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition
        );
    }
}
