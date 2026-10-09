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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local, non-blocking retirement-writer role invalidation for Kafka partition callbacks.
 *
 * <p>Each leadership notification replaces the local generation even when the role is unchanged.
 * A stale ticket cannot become current again after leader-follower-leader, partition removal,
 * or reassignment. A ticket is <strong>not</strong> a Kafka leader epoch, distributed lease,
 * transactional-producer fence, or authorization to emit watermark metadata.</p>
 */
public final class LocalRetirementLeadershipFence {
    private final ConcurrentMap<SharedPartitionId, Role> roles = new ConcurrentHashMap<>();
    private final AtomicLong nextGeneration = new AtomicLong();

    public void onLeader(SharedPartitionId partition) {
        setRole(partition, true);
    }

    public void onFollower(SharedPartitionId partition) {
        setRole(partition, false);
    }

    public void onRemoved(SharedPartitionId partition) {
        roles.remove(Objects.requireNonNull(partition, "partition"));
    }

    private void setRole(SharedPartitionId partition, boolean leader) {
        Objects.requireNonNull(partition, "partition");
        roles.compute(partition, (ignored, old) -> {
            long generation = nextGeneration.updateAndGet(current -> Math.addExact(current, 1L));
            return new Role(generation, leader);
        });
    }

    public Optional<LeaderTicket> captureLeader(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        Role current = roles.get(partition);
        if (current == null || !current.leader()) {
            return Optional.empty();
        }
        return Optional.of(new LeaderTicket(this, partition, current.generation()));
    }

    public boolean stillLeader(LeaderTicket ticket) {
        Objects.requireNonNull(ticket, "ticket");
        if (ticket.owner != this) {
            return false;
        }
        Role current = roles.get(ticket.partition());
        return current != null && current.leader() && current.generation() == ticket.localGeneration();
    }

    public static final class LeaderTicket {
        private final LocalRetirementLeadershipFence owner;
        private final SharedPartitionId partition;
        private final long localGeneration;

        private LeaderTicket(
            LocalRetirementLeadershipFence owner,
            SharedPartitionId partition,
            long localGeneration
        ) {
            this.owner = owner;
            this.partition = partition;
            this.localGeneration = localGeneration;
        }

        public SharedPartitionId partition() {
            return partition;
        }

        public long localGeneration() {
            return localGeneration;
        }
    }

    private record Role(long generation, boolean leader) {
    }
}
