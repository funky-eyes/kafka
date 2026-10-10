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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Routes Kafka replica-role and assignment notifications into the shared-storage commit tracker.
 *
 * <p>Only topics selected for shared storage are tracked. The callbacks perform bounded in-memory map updates only;
 * they never start object-store or metadata-store I/O on Kafka's metadata application thread.</p>
 */
public final class SharedPartitionRoleListener implements StoragePartitionRoleListener {
    private static final Logger LOG = LoggerFactory.getLogger(SharedPartitionRoleListener.class);
    private final SharedStorageConfiguration configuration;
    private final SharedCommitProgress commitProgress;
    private final LocalRetirementLeadershipFence retirementFence;
    // Keep remove/reassign and upload-role publication ordered per listener,
    // without I/O or coordination with Kafka's native log monitor.
    private final Object roleCallbackLock = new Object();

    public SharedPartitionRoleListener(
        SharedStorageConfiguration configuration,
        SharedCommitProgress commitProgress
    ) {
        this(configuration, commitProgress, new LocalRetirementLeadershipFence());
    }

    SharedPartitionRoleListener(
        SharedStorageConfiguration configuration,
        SharedCommitProgress commitProgress,
        LocalRetirementLeadershipFence retirementFence
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.commitProgress = Objects.requireNonNull(commitProgress, "commitProgress");
        this.retirementFence = Objects.requireNonNull(retirementFence, "retirementFence");
    }

    @Override
    public void onLeadershipChange(
        Collection<TopicIdPartition> leaders,
        Collection<TopicIdPartition> followers
    ) {
        Objects.requireNonNull(leaders, "leaders");
        Objects.requireNonNull(followers, "followers");
        leaders.forEach(partition -> updateRole(partition, true, -1));
        followers.forEach(partition -> updateRole(partition, false, -1));
    }

    @Override
    public void onLeadershipChangeWithEpochs(
        Map<TopicIdPartition, Integer> leaders,
        Map<TopicIdPartition, Integer> followers
    ) {
        Objects.requireNonNull(leaders, "leaders");
        Objects.requireNonNull(followers, "followers");
        leaders.forEach((partition, epoch) -> updateRole(partition, true, Objects.requireNonNull(epoch, "epoch")));
        followers.forEach((partition, epoch) -> updateRole(partition, false, Objects.requireNonNull(epoch, "epoch")));
    }

    @Override
    public void onPartitionsRemoved(Collection<TopicIdPartition> partitions) {
        Objects.requireNonNull(partitions, "partitions");
        synchronized (roleCallbackLock) {
            partitions.forEach(partition -> {
                Objects.requireNonNull(partition, "partition");
                if (configuration.useSharedStorage(partition.topic())) {
                    SharedPartitionId id = sharedPartitionId(partition);
                    retirementFence.onRemoved(id);
                    commitProgress.remove(id);
                }
            });
            disableUploadsIfQuarantined();
        }
    }

    private void updateRole(TopicIdPartition partition, boolean leader, int leaderEpoch) {
        Objects.requireNonNull(partition, "partition");
        if (!configuration.useSharedStorage(partition.topic())) {
            return;
        }
        SharedPartitionId sharedPartition = sharedPartitionId(partition);
        synchronized (roleCallbackLock) {
            applyRoleCallback(sharedPartition, leader, leaderEpoch);
        }
    }

    private void applyRoleCallback(SharedPartitionId partition, boolean leader, int epoch) {
        if (leader) {
            if (epoch >= 0) {
                retirementFence.onLeader(partition, epoch);
            } else {
                retirementFence.onLeader(partition);
            }
        } else if (epoch >= 0) {
            retirementFence.onFollower(partition, epoch);
        } else {
            retirementFence.onFollower(partition);
        }

        disableUploadsIfQuarantined();
        // The tombstone remains until an explicit newer epoch reassigns the
        // partition. A stale leader/follower callback after removal must NOT
        // recreate an upload-progress entry with a fresh default offset 0.
        if (retirementFence.isRemoved(partition)) {
            commitProgress.remove(partition);
            return;
        }
        if (retirementFence.captureLeader(partition).isPresent()) {
            commitProgress.onLeader(partition);
        } else {
            commitProgress.onFollower(partition);
        }
    }

    private void disableUploadsIfQuarantined() {
        // The fence clears its identity map when full, which revokes all local
        // tickets. The upload scheduler must also see an empty commit-window
        // map, including after any late Kafka HW/log-load callback.
        if (retirementFence.isQuarantined()) {
            if (!commitProgress.isDisabledForRetirementQuarantine()) {
                LOG.error(
                    "Shared storage role identity budget exhausted; disabling local upload eligibility "
                        + "until broker restart to preserve removed-topic epoch fences"
                );
            }
            commitProgress.disableForRetirementQuarantine();
        }
    }

    /** Read-only local role ticket; distributed generation fencing must still be verified. */
    public Optional<LocalRetirementLeadershipFence.LeaderTicket> captureRetirementLeader(
        SharedPartitionId partition
    ) {
        return retirementFence.captureLeader(partition);
    }

    /** Requires a real KRaft epoch rather than a legacy/unversioned role callback. */
    public Optional<LocalRetirementLeadershipFence.LeaderTicket> captureEpochRetirementLeader(
        SharedPartitionId partition
    ) {
        return retirementFence.captureEpochLeader(partition);
    }

    /** False after any demotion, reassignment, duplicate leadership callback, or removal. */
    public boolean stillRetirementLeader(LocalRetirementLeadershipFence.LeaderTicket ticket) {
        return retirementFence.stillLeader(ticket);
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
