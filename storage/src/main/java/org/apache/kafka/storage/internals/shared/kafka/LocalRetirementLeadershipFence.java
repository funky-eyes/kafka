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
    private static final int UNKNOWN_LEADER_EPOCH = -1;

    /**
     * Compatibility path for callbacks without an epoch. This can never issue
     * an epoch-aware leader ticket even if the caller reports LEADER.
     */
    public void onLeader(SharedPartitionId partition) {
        setRole(partition, true, UNKNOWN_LEADER_EPOCH);
    }

    public void onLeader(SharedPartitionId partition, int leaderEpoch) {
        requireEpoch(leaderEpoch);
        setRole(partition, true, leaderEpoch);
    }

    public void onFollower(SharedPartitionId partition) {
        setRole(partition, false, UNKNOWN_LEADER_EPOCH);
    }

    public void onFollower(SharedPartitionId partition, int leaderEpoch) {
        requireEpoch(leaderEpoch);
        setRole(partition, false, leaderEpoch);
    }

    /**
     * Keep the highest observed KRaft source epoch after unassignment.
     * Forgetting it allows a delayed LEADER(E) to look like a first
     * assignment and reopen uploads after the partition was removed.
     * A subsequent assignment requires a strictly newer explicit epoch.
     */
    public void onRemoved(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        roles.compute(partition, (ignored, old) -> new Role(
            nextGeneration.updateAndGet(value -> Math.addExact(value, 1L)),
            false,
            old == null ? UNKNOWN_LEADER_EPOCH : old.leaderEpoch(),
            true
        ));
    }

    private static void requireEpoch(int leaderEpoch) {
        if (leaderEpoch < 0) {
            throw new IllegalArgumentException("Kafka leaderEpoch must be non-negative");
        }
    }

    private void setRole(SharedPartitionId partition, boolean leader, int leaderEpoch) {
        Objects.requireNonNull(partition, "partition");
        roles.compute(partition, (ignored, old) -> {
            long generation = nextGeneration.updateAndGet(current -> Math.addExact(current, 1L));
            int observedMaxEpoch = old == null ? UNKNOWN_LEADER_EPOCH : old.leaderEpoch();
            // A stale lower epoch or an unversioned callback after a known epoch
            // must never re-promote the local retirement writer.
            //
            // Even an EQUAL epoch is stale once we have observed FOLLOWER for that
            // epoch: a delayed old LEADER callback cannot undo demotion. Repeated
            // LEADER notifications at the current epoch remain valid and refresh
            // the local generation, preserving Kafka's duplicate-callback behavior.
            boolean removed = old != null && old.removed();
            boolean freshEpoch = leaderEpoch >= 0 && leaderEpoch > observedMaxEpoch;
            boolean epochCanPromote = old == null || freshEpoch
                || (!removed && (old.leader() || observedMaxEpoch == UNKNOWN_LEADER_EPOCH));
            boolean newLeader = leader && leaderEpoch >= observedMaxEpoch && epochCanPromote;
            return new Role(
                generation, newLeader, Math.max(observedMaxEpoch, leaderEpoch),
                removed && !newLeader
            );
        });
    }

    /**
     * Fail-closed marker retained after KRaft unassignment. Callers must not
     * recreate upload progress from callbacks rejected by this tombstone.
     */
    public boolean isRemoved(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        Role current = roles.get(partition);
        return current != null && current.removed();
    }

    public Optional<LeaderTicket> captureLeader(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        Role current = roles.get(partition);
        if (current == null || !current.leader()) {
            return Optional.empty();
        }
        return Optional.of(new LeaderTicket(
            this, partition, current.generation(), current.leaderEpoch()
        ));
    }

    /**
     * Reports only leaders whose authoritative KRaft leader epoch was
     * carried through the epoch-aware broker callback.
     */
    public Optional<LeaderTicket> captureEpochLeader(SharedPartitionId partition) {
        return captureLeader(partition).filter(ticket -> ticket.leaderEpoch() >= 0);
    }

    public boolean stillLeader(LeaderTicket ticket) {
        Objects.requireNonNull(ticket, "ticket");
        if (ticket.owner != this) {
            return false;
        }
        Role current = roles.get(ticket.partition());
        return current != null && current.leader()
            && current.generation() == ticket.localGeneration()
            && current.leaderEpoch() == ticket.leaderEpoch();
    }

    public static final class LeaderTicket {
        private final LocalRetirementLeadershipFence owner;
        private final SharedPartitionId partition;
        private final long localGeneration;
        private final int leaderEpoch;

        private LeaderTicket(
            LocalRetirementLeadershipFence owner,
            SharedPartitionId partition,
            long localGeneration,
            int leaderEpoch
        ) {
            this.owner = owner;
            this.partition = partition;
            this.localGeneration = localGeneration;
            this.leaderEpoch = leaderEpoch;
        }

        public SharedPartitionId partition() {
            return partition;
        }

        public long localGeneration() {
            return localGeneration;
        }

        /** Kafka source epoch or -1 when the legacy notification lacked one. */
        public int leaderEpoch() {
            return leaderEpoch;
        }
    }

    private record Role(long generation, boolean leader, int leaderEpoch, boolean removed) {
    }
}
