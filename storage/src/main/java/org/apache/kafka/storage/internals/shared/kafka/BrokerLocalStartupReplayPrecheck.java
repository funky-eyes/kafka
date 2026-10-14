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

import org.apache.kafka.storage.internals.shared.CurrentLocalWalCoveragePrecheck;
import org.apache.kafka.storage.internals.shared.LocalWalDurabilityObservation;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck;
import org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck.Finding;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Read-only local join of (1) the real broker-ready incarnation pairing,
 * (2) two native Kafka source samples and (3) local WAL DATA batches.
 * Startup-replay-only and current durable-index observations share the same
 * broker incarnation, source and WAL mutation/admission fences.
 *
 * <p>No result is an authenticated cross-broker durability certificate.
 * Controller registration, metadata quorum, replica persistence, reader
 * quiescence and safe COMMITTED object deletion are NOT established.</p>
 */
public final class BrokerLocalStartupReplayPrecheck {
    private BrokerLocalStartupReplayPrecheck() {
    }

    public enum Status {
        BROKER_IDENTITY_UNAVAILABLE,
        NATIVE_SOURCE_UNAVAILABLE,
        NATIVE_SOURCE_CHANGED,
        LOCAL_WAL_IN_FLIGHT,
        LOCAL_WAL_CHANGED,
        BROKER_IDENTITY_REVOKED,
        REPLAY_WINDOW_CLASSIFIED,
        CURRENT_WINDOW_CLASSIFIED
    }

    public record Assessment(Status status, Optional<Finding> finding) {
        public Assessment {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(finding, "finding");
            if ((status == Status.REPLAY_WINDOW_CLASSIFIED) != finding.isPresent()) {
                throw new IllegalArgumentException("Recovered window finding requires stable local inputs");
            }
        }
    }

    /** Current indexed coverage is distinct from a successful startup replay. */
    public record CurrentAssessment(
        Status status,
        Optional<CurrentLocalWalCoveragePrecheck.Finding> finding
    ) {
        public CurrentAssessment {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(finding, "finding");
            if ((status == Status.CURRENT_WINDOW_CLASSIFIED) != finding.isPresent()) {
                throw new IllegalArgumentException("Current WAL coverage requires stable local inputs");
            }
        }
    }

    private record Observation<T>(Status status, Optional<T> finding) {
    }

    @FunctionalInterface
    private interface WindowClassifier<T> {
        T classify(
            BrokerStartupReplayObservation identity,
            SharedStorageEngine storage,
            SharedPartitionId partition,
            SharedUnifiedLog.NativeSourceWindow sourceWindow
        );
    }

    static Assessment assess(
        SharedStorageExtension extension,
        SharedPartitionRoleListener roles,
        SharedUnifiedLog sourceLog,
        SharedPartitionId partition
    ) throws IOException {
        Observation<Finding> result = evaluate(
            extension, roles, sourceLog, partition, Status.REPLAY_WINDOW_CLASSIFIED,
            (identity, storage, key, window) -> StartupWalReplayCoveragePrecheck.assess(
                identity.replay(), key,
                storage.localLocations(key, window.logStartOffset(), window.highWatermark()),
                window.logStartOffset(), window.highWatermark()
            )
        );
        return new Assessment(result.status(), result.finding());
    }

    /**
     * Read-only current durable-index coverage includes recovered and newly
     * successful WAL appends. It is NOT authenticated Controller evidence.
     */
    static CurrentAssessment assessCurrent(
        SharedStorageExtension extension,
        SharedPartitionRoleListener roles,
        SharedUnifiedLog sourceLog,
        SharedPartitionId partition
    ) throws IOException {
        Observation<CurrentLocalWalCoveragePrecheck.Finding> result = evaluate(
            extension, roles, sourceLog, partition, Status.CURRENT_WINDOW_CLASSIFIED,
            (identity, storage, key, window) -> storage.localDurableWindowCoverage(
                key, window.logStartOffset(), window.highWatermark()
            )
        );
        return new CurrentAssessment(result.status(), result.finding());
    }

    private static <T> Observation<T> evaluate(
        SharedStorageExtension extension,
        SharedPartitionRoleListener roles,
        SharedUnifiedLog sourceLog,
        SharedPartitionId partition,
        Status successStatus,
        WindowClassifier<T> classifier
    ) throws IOException {
        Objects.requireNonNull(extension, "extension");
        Objects.requireNonNull(roles, "roles");
        Objects.requireNonNull(sourceLog, "sourceLog");
        Objects.requireNonNull(partition, "partition");
        Objects.requireNonNull(classifier, "classifier");
        var paired = extension.brokerStartupReplayObservation();
        if (paired.isEmpty()) {
            return rejected(Status.BROKER_IDENTITY_UNAVAILABLE);
        }
        SharedStorageEngine storage = extension.storage();
        BrokerStartupReplayObservation identity = paired.get();
        if (identity.replay() != storage.startupWalReplayEvidence()) {
            return rejected(Status.BROKER_IDENTITY_REVOKED);
        }
        long mutationBefore = storage.walMutationRevision(partition);
        long admissionBefore = storage.walAdmissionRevision(partition);
        LocalWalDurabilityObservation.Snapshot completedBefore = storage.liveWalDurabilityObservation(partition);
        if (mutationBefore < 0L || admissionBefore < 0L) {
            return rejected(Status.LOCAL_WAL_IN_FLIGHT);
        }

        var first = SourceLogStartObservation.capture(roles, sourceLog, partition);
        if (first.isEmpty()) {
            return rejected(Status.NATIVE_SOURCE_UNAVAILABLE);
        }
        var source = first.get();
        var windows = source.offsets();
        T covered = classifier.classify(identity, storage, partition, windows);

        // A second observation must represent exactly the same Kafka source
        // window. An advancing HW/LEO is safe for other read paths but not
        // evidence that this older startup-only window is still current.
        var second = SourceLogStartObservation.capture(roles, sourceLog, partition);
        if (!sameSourceObservation(source, second)) {
            return rejected(Status.NATIVE_SOURCE_CHANGED);
        }
        if (!sameWalGeneration(storage, partition, admissionBefore, mutationBefore, completedBefore)) {
            return rejected(Status.LOCAL_WAL_CHANGED);
        }
        // Compare exact process-local identity: a revoke/ABA callback
        // invalidates the result even if Kafka epochs later repeat.
        if (extension.brokerStartupReplayObservation().orElse(null) != identity) {
            return rejected(Status.BROKER_IDENTITY_REVOKED);
        }
        return new Observation<>(successStatus, Optional.of(Objects.requireNonNull(covered, "window finding")));
    }

    private static boolean sameSourceObservation(
        SourceLogStartObservation.LocalObservation previous,
        Optional<SourceLogStartObservation.LocalObservation> next
    ) {
        return next.isPresent()
            && next.get().sourceLeaderEpoch() == previous.sourceLeaderEpoch()
            && next.get().localGeneration() == previous.localGeneration()
            && next.get().offsets().equals(previous.offsets());
    }

    private static boolean sameWalGeneration(
        SharedStorageEngine storage,
        SharedPartitionId partition,
        long admissionBefore,
        long mutationBefore,
        LocalWalDurabilityObservation.Snapshot completedBefore
    ) {
        long admissionAfter = storage.walAdmissionRevision(partition);
        long mutationAfter = storage.walMutationRevision(partition);
        // The immutable CAS-published completion observation must not be
        // replaced while Kafka source identity and offsets are sampled.
        return admissionAfter >= 0L && mutationAfter >= 0L
            && admissionAfter == admissionBefore && mutationAfter == mutationBefore
            && storage.liveWalDurabilityObservation(partition) == completedBefore;
    }

    private static <T> Observation<T> rejected(Status status) {
        return new Observation<>(status, Optional.empty());
    }
}
