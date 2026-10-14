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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck;
import org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck.Finding;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Read-only local join of (1) the real broker-ready incarnation pairing,
 * (2) two native Kafka source samples and (3) actual startup WAL DATA
 * ranges still visible in the local index.
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
        REPLAY_WINDOW_CLASSIFIED
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

    static Assessment assess(
        SharedStorageExtension extension,
        SharedPartitionRoleListener roles,
        SharedUnifiedLog sourceLog,
        SharedPartitionId partition
    ) throws IOException {
        Objects.requireNonNull(extension, "extension");
        Objects.requireNonNull(roles, "roles");
        Objects.requireNonNull(sourceLog, "sourceLog");
        Objects.requireNonNull(partition, "partition");
        var identity = extension.brokerStartupReplayObservation();
        if (identity.isEmpty()) {
            return rejected(Status.BROKER_IDENTITY_UNAVAILABLE);
        }

        SharedStorageEngine storage = extension.storage();
        if (identity.get().replay() != storage.startupWalReplayEvidence()) {
            return rejected(Status.BROKER_IDENTITY_REVOKED);
        }
        return assessLocal(extension, identity.get(), storage, roles, sourceLog, partition);
    }

    private static Assessment assessLocal(
        SharedStorageExtension extension,
        BrokerStartupReplayObservation identity,
        SharedStorageEngine storage,
        SharedPartitionRoleListener roles,
        SharedUnifiedLog sourceLog,
        SharedPartitionId partition
    ) throws IOException {
        long mutationBefore = storage.walMutationRevision(partition);
        long admissionBefore = storage.walAdmissionRevision(partition);
        if (mutationBefore < 0L || admissionBefore < 0L) {
            return rejected(Status.LOCAL_WAL_IN_FLIGHT);
        }

        var first = SourceLogStartObservation.capture(roles, sourceLog, partition);
        if (first.isEmpty()) {
            return rejected(Status.NATIVE_SOURCE_UNAVAILABLE);
        }
        var source = first.get();
        var windows = source.offsets();
        Finding recovered = StartupWalReplayCoveragePrecheck.assess(
            identity.replay(), partition,
            storage.localLocations(partition, windows.logStartOffset(), windows.highWatermark()),
            windows.logStartOffset(), windows.highWatermark()
        );

        // A second observation must represent exactly the same Kafka source
        // window. An advancing HW/LEO is safe for other read paths but not
        // evidence that this older startup-only window is still current.
        var second = SourceLogStartObservation.capture(roles, sourceLog, partition);
        if (!sameSourceObservation(source, second)) {
            return rejected(Status.NATIVE_SOURCE_CHANGED);
        }
        if (!sameWalGeneration(storage, partition, admissionBefore, mutationBefore)) {
            return rejected(Status.LOCAL_WAL_CHANGED);
        }
        // Compare exact process-local identity: a revoke/ABA callback
        // invalidates the result even if Kafka epochs later repeat.
        if (extension.brokerStartupReplayObservation().orElse(null) != identity) {
            return rejected(Status.BROKER_IDENTITY_REVOKED);
        }
        return new Assessment(Status.REPLAY_WINDOW_CLASSIFIED, Optional.of(recovered));
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
        long mutationBefore
    ) {
        long admissionAfter = storage.walAdmissionRevision(partition);
        long mutationAfter = storage.walMutationRevision(partition);
        return admissionAfter >= 0L && mutationAfter >= 0L
            && admissionAfter == admissionBefore && mutationAfter == mutationBefore;
    }

    private static Assessment rejected(Status status) {
        return new Assessment(status, Optional.empty());
    }
}
