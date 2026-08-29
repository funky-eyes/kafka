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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.internals.log.StoragePartitionRoleListener;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.util.Collection;
import java.util.Objects;

/**
 * Routes Kafka replica-role notifications into the shared-storage commit tracker.
 *
 * <p>Only topics selected for shared storage are tracked. The callback performs bounded in-memory map updates only;
 * it never starts object-store or metadata-store I/O on Kafka's metadata application thread.</p>
 */
public final class SharedPartitionRoleListener implements StoragePartitionRoleListener {
    private final SharedStorageConfiguration configuration;
    private final SharedCommitProgress commitProgress;

    public SharedPartitionRoleListener(
        SharedStorageConfiguration configuration,
        SharedCommitProgress commitProgress
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.commitProgress = Objects.requireNonNull(commitProgress, "commitProgress");
    }

    @Override
    public void onLeadershipChange(
        Collection<TopicIdPartition> leaders,
        Collection<TopicIdPartition> followers
    ) {
        Objects.requireNonNull(leaders, "leaders");
        Objects.requireNonNull(followers, "followers");
        leaders.forEach(partition -> updateRole(partition, true));
        followers.forEach(partition -> updateRole(partition, false));
    }

    private void updateRole(TopicIdPartition partition, boolean leader) {
        Objects.requireNonNull(partition, "partition");
        if (!configuration.useSharedStorage(partition.topic())) {
            return;
        }
        SharedPartitionId sharedPartition = sharedPartitionId(partition);
        if (leader) {
            commitProgress.onLeader(sharedPartition);
        } else {
            commitProgress.onFollower(sharedPartition);
        }
    }

    private static SharedPartitionId sharedPartitionId(TopicIdPartition partition) {
        Uuid topicId = partition.topicId();
        return new SharedPartitionId(
            topicId.getMostSignificantBits(),
            topicId.getLeastSignificantBits(),
            partition.partition()
        );
    }
}
