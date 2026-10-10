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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Non-blocking bridge from Kafka's native log offsets and replica role into the shared-storage upload plane.
 *
 * <p>This class performs no I/O and no waiting because callbacks may execute while Kafka log or ReplicaManager locks
 * are held. Upload workers consume immutable snapshots asynchronously. Kafka remains the sole source of truth for the
 * log-start offset, exclusive high-watermark boundary and current local replica role.</p>
 */
public final class SharedCommitProgress {
    private final ConcurrentMap<SharedPartitionId, PartitionProgress> partitions = new ConcurrentHashMap<>();
    // Role mutations and upload admission share a single CAS state. An odd
    // role revision means that a Kafka leadership/removal callback is active.
    // Previously admitted PUTs remain owned until their completion callback.
    private final AtomicReference<UploadAdmissionState> uploadAdmissions =
        new AtomicReference<>(new UploadAdmissionState(0L, 0L, false));
    private final Object roleMutationMonitor = new Object();
    private volatile boolean disabledForQuarantine;

    /**
     * Permanent process-local fail-closed latch. Only a fresh broker restart
     * can reset the role/epoch tombstone capacity quarantine.
     */
    void disableForRetirementQuarantine() {
        // Close admissions atomically, before clearing cached Kafka windows.
        uploadAdmissions.getAndUpdate(state ->
            new UploadAdmissionState(state.inFlight(), state.roleRevision(), true)
        );
        disabledForQuarantine = true;
        partitions.clear();
    }

    boolean isDisabledForRetirementQuarantine() {
        return uploadAdmissions.get().closed();
    }

    /**
     * Captures the role revision before selecting WAL candidates. Admission
     * rejects odd or changed revisions, including leader-follower-leader ABA.
     */
    long uploadRoleRevision() {
        return uploadAdmissions.get().roleRevision();
    }

    boolean tryAcquireUploadAdmission() {
        return tryAcquireUploadAdmission(uploadRoleRevision());
    }

    /**
     * CAS-linearized local admission against both irreversible quarantine and
     * Kafka role changes. A permit acquired before either mutation may drain.
     */
    boolean tryAcquireUploadAdmission(long expectedRoleRevision) {
        while (true) {
            UploadAdmissionState current = uploadAdmissions.get();
            if (current.closed() || (current.roleRevision() & 1L) != 0L
                || current.roleRevision() != expectedRoleRevision) {
                return false;
            }
            if (current.inFlight() == Long.MAX_VALUE) {
                throw new IllegalStateException("Shared upload admission counter exhausted");
            }
            UploadAdmissionState next =
                new UploadAdmissionState(current.inFlight() + 1L, current.roleRevision(), false);
            if (uploadAdmissions.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    void releaseUploadAdmission() {
        while (true) {
            UploadAdmissionState current = uploadAdmissions.get();
            if (current.inFlight() == 0L) {
                throw new IllegalStateException("Shared upload admission counter underflow");
            }
            UploadAdmissionState next = new UploadAdmissionState(
                current.inFlight() - 1L, current.roleRevision(), current.closed()
            );
            if (uploadAdmissions.compareAndSet(current, next)) {
                return;
            }
        }
    }

    long activeUploadAdmissions() {
        return uploadAdmissions.get().inFlight();
    }

    public void onLogLoaded(SharedPartitionId partition, long logStartOffset) {
        Objects.requireNonNull(partition, "partition");
        if (logStartOffset < 0) {
            throw new IllegalArgumentException("logStartOffset must be non-negative");
        }
        if (disabledForQuarantine) {
            return;
        }
        partitions.compute(partition, (ignored, current) -> disabledForQuarantine ? null : new PartitionProgress(
            logStartOffset,
            current == null ? logStartOffset : current.highWatermark(),
            current == null ? ReplicaRole.UNKNOWN : current.role()
        ));
    }

    /**
     * Observes a successful live Kafka DeleteRecords/retention advance, not a
     * broker's proposed retirement watermark. Out-of-order notifications from
     * concurrent advances must not roll the observed source log start backward.
     * Unknown or removed partitions must never be recreated by a late callback.
     */
    public void onLogStartOffsetAdvanced(SharedPartitionId partition, long logStartOffset) {
        Objects.requireNonNull(partition, "partition");
        if (logStartOffset < 0L) {
            throw new IllegalArgumentException("logStartOffset must be non-negative");
        }
        if (disabledForQuarantine) {
            return;
        }
        partitions.computeIfPresent(partition, (ignored, current) -> disabledForQuarantine ? null :
            new PartitionProgress(
                Math.max(current.logStartOffset(), logStartOffset),
                current.highWatermark(),
                current.role()
            ));
    }

    /**
     * Kafka log truncation/recovery can legally LOWER the source log start.
     * This must replace the cached value rather than using a monotonic max.
     * Existing role and high-watermark callbacks retain separate ownership;
     * no state may be fabricated after topic removal.
     */
    public void onLogRebased(SharedPartitionId partition, long logStartOffset) {
        Objects.requireNonNull(partition, "partition");
        if (logStartOffset < 0L) {
            throw new IllegalArgumentException("logStartOffset must be non-negative");
        }
        if (disabledForQuarantine) {
            return;
        }
        partitions.computeIfPresent(partition, (ignored, current) -> disabledForQuarantine ? null :
            new PartitionProgress(
                logStartOffset,
                current.highWatermark(),
                current.role()
            ));
    }

    public void onHighWatermarkUpdated(SharedPartitionId partition, long highWatermark) {
        Objects.requireNonNull(partition, "partition");
        if (highWatermark < 0) {
            throw new IllegalArgumentException("highWatermark must be non-negative");
        }
        // Use assignment rather than max(): recovery or a leadership change may legitimately restore a lower HW.
        if (disabledForQuarantine) {
            return;
        }
        partitions.compute(partition, (ignored, current) -> disabledForQuarantine ? null : new PartitionProgress(
            current == null ? 0L : current.logStartOffset(),
            highWatermark,
            current == null ? ReplicaRole.UNKNOWN : current.role()
        ));
    }

    public void onLeader(SharedPartitionId partition) {
        updateRole(partition, ReplicaRole.LEADER);
    }

    public void onFollower(SharedPartitionId partition) {
        updateRole(partition, ReplicaRole.FOLLOWER);
    }

    private void updateRole(SharedPartitionId partition, ReplicaRole role) {
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(role, "role");
        if (disabledForQuarantine) {
            return;
        }
        synchronized (roleMutationMonitor) {
            if (!beginRoleMutation()) {
                return;
            }
            try {
                partitions.compute(partition, (ignored, current) -> disabledForQuarantine ? null :
                    new PartitionProgress(
                        current == null ? 0L : current.logStartOffset(),
                        current == null ? 0L : current.highWatermark(),
                        role
                    ));
            } finally {
                endRoleMutation();
            }
        }
    }

    private boolean beginRoleMutation() {
        while (true) {
            UploadAdmissionState current = uploadAdmissions.get();
            if (current.closed()) {
                return false;
            }
            // Exhausting the version must never wrap and revive a stale token.
            if (current.roleRevision() >= Long.MAX_VALUE - 2L) {
                disableForRetirementQuarantine();
                return false;
            }
            if ((current.roleRevision() & 1L) != 0L) {
                throw new IllegalStateException("Shared role mutations must be serialized");
            }
            UploadAdmissionState next = new UploadAdmissionState(
                current.inFlight(), current.roleRevision() + 1L, false
            );
            if (uploadAdmissions.compareAndSet(current, next)) {
                return true;
            }
        }
    }

    private void endRoleMutation() {
        while (true) {
            UploadAdmissionState current = uploadAdmissions.get();
            if ((current.roleRevision() & 1L) == 0L) {
                throw new IllegalStateException("Shared role mutation was not in progress");
            }
            UploadAdmissionState next = new UploadAdmissionState(
                current.inFlight(), current.roleRevision() + 1L, current.closed()
            );
            if (uploadAdmissions.compareAndSet(current, next)) {
                return;
            }
        }
    }

    public OptionalLong highWatermark(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        if (disabledForQuarantine) {
            return OptionalLong.empty();
        }
        PartitionProgress progress = partitions.get(partition);
        return disabledForQuarantine || progress == null
            ? OptionalLong.empty() : OptionalLong.of(progress.highWatermark());
    }

    public Optional<PartitionProgress> partitionProgress(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        return disabledForQuarantine ? Optional.empty() : Optional.ofNullable(partitions.get(partition));
    }

    public Map<SharedPartitionId, PartitionProgress> snapshot() {
        return disabledForQuarantine ? Map.of() : Map.copyOf(partitions);
    }

    public void remove(SharedPartitionId partition) {
        Objects.requireNonNull(partition, "partition");
        synchronized (roleMutationMonitor) {
            if (!beginRoleMutation()) {
                return;
            }
            try {
                partitions.remove(partition);
            } finally {
                endRoleMutation();
            }
        }
    }

    private record UploadAdmissionState(long inFlight, long roleRevision, boolean closed) {
    }

    public enum ReplicaRole {
        UNKNOWN,
        LEADER,
        FOLLOWER
    }

    public record PartitionProgress(long logStartOffset, long highWatermark, ReplicaRole role) {
        public PartitionProgress {
            if (logStartOffset < 0) {
                throw new IllegalArgumentException("logStartOffset must be non-negative");
            }
            if (highWatermark < 0) {
                throw new IllegalArgumentException("highWatermark must be non-negative");
            }
            Objects.requireNonNull(role, "role");
        }

        public boolean isLeader() {
            return role == ReplicaRole.LEADER;
        }
    }
}
