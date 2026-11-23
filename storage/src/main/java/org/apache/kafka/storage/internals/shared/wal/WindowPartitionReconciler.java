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
package org.apache.kafka.storage.internals.shared.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Classifies a validated physical window against caller-supplied partition boundaries.
 * Neither a boundary nor a valid candidate proves durability, Kafka leadership, or replica authority.
 * Results must not be used after the owning recovery lease ends, nor to publish or reclaim data.
 */
final class WindowPartitionReconciler {
    private static final int MAX_BOUNDARIES = 65536;

    /** Exclusive offsets and the only epoch accepted by this deliberately restricted first pass. */
    record Boundary(long startOffset, long requiredEndOffset, int leaderEpoch) {
        Boundary {
            if (startOffset < 0 || requiredEndOffset < startOffset || leaderEpoch < 0)
                throw new IllegalArgumentException("Invalid partition recovery boundary");
        }
    }

    enum Problem { MISSING_BOUNDARY, UNSUPPORTED_ATOMIC_GROUP, CONTROL_RECORD, EPOCH_MISMATCH,
        GAP_OR_OVERLAP, REQUIRED_OFFSET_MISSING }

    record CandidateGroup(long blockStart, int blockLength, BlockRingRecovery.WindowGroup group) { }

    record PartitionResult(Boundary boundary, long nextOffset, List<CandidateGroup> contiguous,
                           List<CandidateGroup> quarantined, Set<Problem> problems) {
        PartitionResult {
            contiguous = List.copyOf(contiguous);
            quarantined = List.copyOf(quarantined);
            problems = Set.copyOf(problems);
        }
    }

    record Analysis(long identityHigh, long identityLow, long checkpointGeneration,
                    long capacity, long head, long tail, long ceiling, long lastCommittedGroup,
                    List<BlockRingRecovery.WindowUnclassified> unclassified,
                    Map<WalPartitionKey, PartitionResult> partitions) {
        Analysis {
            unclassified = List.copyOf(unclassified);
            partitions = Map.copyOf(partitions);
        }
    }

    private static final class State {
        final Boundary boundary;
        final List<CandidateGroup> contiguous = new ArrayList<>();
        final List<CandidateGroup> quarantined = new ArrayList<>();
        final EnumSet<Problem> problems = EnumSet.noneOf(Problem.class);
        long next;
        boolean blocked;

        State(Boundary boundary) {
            this.boundary = boundary;
            this.next = boundary == null ? -1 : boundary.startOffset();
            if (boundary == null) {
                problems.add(Problem.MISSING_BOUNDARY);
                blocked = true;
            }
        }

        void quarantine(CandidateGroup group, Problem problem) {
            quarantined.add(group);
            problems.add(problem);
            blocked = true;
        }
    }

    private WindowPartitionReconciler() { }

    static Analysis analyze(BlockRingRecovery.WindowSnapshot snapshot, Map<WalPartitionKey, Boundary> boundaries)
        throws IOException {
        Objects.requireNonNull(snapshot, "snapshot");
        Objects.requireNonNull(boundaries, "boundaries");
        if (boundaries.size() > MAX_BOUNDARIES) throw new IOException("Too many recovery boundaries");
        Map<WalPartitionKey, State> states = new HashMap<>();
        for (var entry : boundaries.entrySet()) {
            if (states.size() >= MAX_BOUNDARIES) throw new IOException("Too many recovery boundaries");
            states.put(Objects.requireNonNull(entry.getKey(), "partition"),
                new State(Objects.requireNonNull(entry.getValue(), "boundary")));
        }
        List<BlockRingRecovery.WindowUnclassified> unclassified = new ArrayList<>();
        long lastGroup = snapshot.lastCommittedGroup();
        for (var item : snapshot.items()) {
            if (item instanceof BlockRingRecovery.WindowUnclassified hole) {
                unclassified.add(hole);
            } else if (item instanceof BlockRingRecovery.WindowCandidate block) {
                for (var group : block.groups()) {
                    if (group.id() <= lastGroup) throw new IOException("WAL recovery group order is invalid");
                    lastGroup = group.id();
                    analyzeGroup(new CandidateGroup(block.start(), block.length(), group), states);
                }
            }
        }
        Map<WalPartitionKey, PartitionResult> results = new HashMap<>();
        states.forEach((partition, state) -> {
            if (state.boundary != null && state.next < state.boundary.requiredEndOffset())
                state.problems.add(Problem.REQUIRED_OFFSET_MISSING);
            results.put(partition, new PartitionResult(state.boundary, state.next, state.contiguous,
                state.quarantined, state.problems));
        });
        return new Analysis(snapshot.identityHigh(), snapshot.identityLow(), snapshot.checkpointGeneration(),
            snapshot.capacity(), snapshot.head(), snapshot.tail(), snapshot.ceiling(), snapshot.lastCommittedGroup(),
            unclassified, results);
    }

    private static void analyzeGroup(CandidateGroup candidate, Map<WalPartitionKey, State> states) throws IOException {
        BlockRingRecovery.WindowGroup group = candidate.group();
        if (group.entries().isEmpty()) throw new IOException("Empty WAL recovery group");
        Set<WalPartitionKey> partitions = new LinkedHashSet<>();
        for (var entry : group.entries()) partitions.add(entry.partition());
        if (partitions.size() != 1) {
            for (var partition : partitions) {
                states.computeIfAbsent(partition, ignored -> new State(null))
                    .quarantine(candidate, Problem.UNSUPPORTED_ATOMIC_GROUP);
            }
            return;
        }
        WalPartitionKey partition = partitions.iterator().next();
        State state = states.computeIfAbsent(partition, ignored -> new State(null));
        if (state.blocked) {
            state.quarantined.add(candidate);
            return;
        }
        long next = state.next;
        for (var entry : group.entries()) {
            Problem problem = validateEntry(entry, state.boundary, next);
            if (problem != null) {
                state.quarantine(candidate, problem);
                return;
            }
            try {
                next = Math.addExact(entry.lastOffset(), 1);
            } catch (ArithmeticException invalid) {
                throw new IOException("WAL recovery offset overflow", invalid);
            }
        }
        state.next = next;
        state.contiguous.add(candidate);
    }
    private static Problem validateEntry(BlockRingRecovery.WindowEntry entry, Boundary boundary, long next) {
        if (entry.type() != WalRecordType.DATA) return Problem.CONTROL_RECORD;
        if (entry.leaderEpoch() != boundary.leaderEpoch()) return Problem.EPOCH_MISMATCH;
        if (entry.firstOffset() != next) return Problem.GAP_OR_OVERLAP;
        return null;
    }

}
