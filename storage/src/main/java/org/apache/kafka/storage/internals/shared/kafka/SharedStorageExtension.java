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

import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;
import org.apache.kafka.storage.internals.log.KafkaStorageExtension;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.StoragePartitionRoleListener;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Broker-wide shared WAL extension.
 *
 * <p>Exactly one WAL/engine is created per broker extension instance. All selected topic-partitions on the broker share
 * that physical WAL; topic routing only controls which UnifiedLog implementation is created.</p>
 */
public final class SharedStorageExtension implements KafkaStorageExtension {
    private SharedStorageEngine storage;
    private SharedCommitProgress commitProgress;
    private UnifiedLogFactory unifiedLogFactory;
    private SharedPartitionRoleListener partitionRoleListener;
    private int startedBrokerId = -1;
    private boolean brokerReadyObserved;
    private BrokerStartupReplayObservation brokerStartupReplayObservation;

    @Override
    public synchronized void start(StorageExtensionContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        if (storage != null) {
            throw new IllegalStateException("Shared storage extension is already started");
        }

        SharedStorageConfiguration configuration = SharedStorageConfiguration.from(context);
        SharedWal wal = SharedStorageWalFactory.create(configuration);
        try {
            storage = new SharedStorageEngine(wal);
            commitProgress = new SharedCommitProgress();
            unifiedLogFactory = new RoutingUnifiedLogFactory(
                configuration,
                new SharedUnifiedLogFactory(storage, commitProgress)
            );
            partitionRoleListener = new SharedPartitionRoleListener(configuration, commitProgress);
            startedBrokerId = context.brokerId();
            brokerReadyObserved = false;
            brokerStartupReplayObservation = null;
        } catch (Throwable t) {
            storage = null;
            commitProgress = null;
            unifiedLogFactory = null;
            partitionRoleListener = null;
            startedBrokerId = -1;
            brokerReadyObserved = false;
            brokerStartupReplayObservation = null;
            try {
                wal.close();
            } catch (Throwable closeError) {
                t.addSuppressed(closeError);
            }
            if (t instanceof IOException ioException) {
                throw ioException;
            }
            if (t instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (t instanceof Error error) {
                throw error;
            }
            throw new IOException("Unable to initialize shared storage extension", t);
        }
    }

    /**
     * Pair the real post-registration broker-ready identity snapshot with the
     * already frozen backend replay inventory. Missing/mismatched identity
     * invalidates this optional diagnostic without breaking ordinary Kafka
     * startup. It is never a live Controller registration certificate.
     */
    @Override
    public synchronized CompletableFuture<Void> onBrokerReady(StorageExtensionBrokerContext context) {
        Objects.requireNonNull(context, "context");
        if (storage == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        if (brokerReadyObserved) {
            // A changed registration in the same extension lifetime cannot
            // rebind WAL recovered before the callback. Identical repeated
            // callbacks are idempotent; every other callback revokes the
            // pairing until a new extension startup and WAL replay.
            if (!sameBrokerReadyIdentity(context, brokerStartupReplayObservation)) {
                brokerStartupReplayObservation = null;
            }
            return CompletableFuture.completedFuture(null);
        }
        brokerReadyObserved = true;
        if (context.brokerId() == startedBrokerId && context.hasBrokerIdentitySnapshot()) {
            brokerStartupReplayObservation = new BrokerStartupReplayObservation(
                context, storage.startupWalReplayEvidence()
            );
        }
        return CompletableFuture.completedFuture(null);
    }

    private static boolean sameBrokerReadyIdentity(
        StorageExtensionBrokerContext context,
        BrokerStartupReplayObservation previous
    ) {
        return previous != null && context.hasBrokerIdentitySnapshot()
            && context.clusterId().equals(previous.clusterId())
            && context.brokerId() == previous.brokerId()
            && context.brokerEpoch() == previous.brokerEpoch()
            && context.brokerIncarnationId().equals(previous.brokerIncarnationId());
    }

    /** Local startup-only diagnostic; absent for legacy/unbound broker identity. */
    public synchronized Optional<BrokerStartupReplayObservation> brokerStartupReplayObservation() {
        return Optional.ofNullable(brokerStartupReplayObservation);
    }

    @Override
    public synchronized UnifiedLogFactory unifiedLogFactory() {
        if (unifiedLogFactory == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        return unifiedLogFactory;
    }

    @Override
    public synchronized StoragePartitionRoleListener partitionRoleListener() {
        if (partitionRoleListener == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        return partitionRoleListener;
    }

    public synchronized SharedStorageEngine storage() {
        if (storage == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        return storage;
    }

    public synchronized SharedCommitProgress commitProgress() {
        if (commitProgress == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        return commitProgress;
    }

    /**
     * Sample the native Kafka source and indexed, durable local WAL for a
     * Controller diagnostic. The selected log must be a real SharedUnifiedLog;
     * classic or unbound logs cannot manufacture a Source/WAL report.
     *
     * <p>Do not hold the extension monitor across Kafka/WAL reads. The
     * precheck independently fences role, broker-ready incarnation, source
     * offsets and WAL mutation/admission revisions before returning data.
     * This does not attest network origin or authorize physical deletion.</p>
     */
    public Optional<PartitionRetirementSourceProbe.Report> captureSourceWalProbe(
        PartitionRetirementSourceProbe.Request challenge,
        UnifiedLog log
    ) throws IOException {
        Objects.requireNonNull(challenge, "challenge");
        if (!(log instanceof SharedUnifiedLog nativeLog)) {
            return Optional.empty();
        }
        SharedPartitionRoleListener roles;
        synchronized (this) {
            if (storage == null || partitionRoleListener == null
                || brokerStartupReplayObservation == null
                || challenge.brokerId() != startedBrokerId) {
                return Optional.empty();
            }
            roles = partitionRoleListener;
        }
        SharedPartitionId partition = new SharedPartitionId(
            challenge.topicId().getMostSignificantBits(),
            challenge.topicId().getLeastSignificantBits(), challenge.partitionId()
        );
        return BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport(
            this, roles, nativeLog, partition, challenge
        );
    }

    /**
     * Adapt a live LogManager source lookup into an asynchronous, read-only
     * diagnostic sampler. The caller must provide a bounded executor; never
     * run this supplier on the BrokerHeartbeat event queue.
     *
     * <p>No sampler is registered during ordinary extension startup.
     * In particular, this factory does not send a Kafka request or
     * install any authenticated Broker-to-Controller report channel.</p>
     */
    public Function<PartitionRetirementSourceProbe.Request,
        CompletableFuture<Optional<PartitionRetirementSourceProbe.Report>>> readOnlySourceWalSampler(
        Function<PartitionRetirementSourceProbe.Request, Optional<UnifiedLog>> logLookup,
        Executor executor
    ) {
        Objects.requireNonNull(logLookup, "logLookup");
        Objects.requireNonNull(executor, "executor");
        return challenge -> {
            Objects.requireNonNull(challenge, "challenge");
            Thread submittingThread = Thread.currentThread();
            return CompletableFuture.supplyAsync(() -> {
                // supplyAsync does not guarantee a separate thread when the
                // caller injects an inline executor. Never run native WAL
                // inspection on the Broker heartbeat event thread.
                if (Thread.currentThread() == submittingThread) {
                    throw new IllegalStateException("Source/WAL sampler executor ran inline");
                }
                try {
                    Optional<UnifiedLog> first = Objects.requireNonNull(
                        logLookup.apply(challenge), "logLookup result"
                    );
                    if (first.isEmpty()) {
                        return Optional.empty();
                    }
                    // Preserve the exact Broker-ready pairing across both
                    // current-log lookups. Registration revocation during the
                    // second lookup must not resurrect a completed WAL scan.
                    BrokerStartupReplayObservation identity =
                        brokerStartupReplayObservation().orElse(null);
                    if (identity == null) {
                        return Optional.empty();
                    }
                    Optional<PartitionRetirementSourceProbe.Report> captured =
                        captureSourceWalProbe(challenge, first.get());
                    if (captured.isEmpty()) {
                        return Optional.empty();
                    }
                    // A native log can be unloaded, replaced, or re-created
                    // while Source/WAL revisions are sampled. Re-resolve the
                    // current TopicId and log after the local precheck, and
                    // require the exact same log object (not equals()).
                    Optional<UnifiedLog> second = Objects.requireNonNull(
                        logLookup.apply(challenge), "logLookup result"
                    );
                    if (second.isEmpty() || second.get() != first.get()) {
                        return Optional.empty();
                    }
                    if (brokerStartupReplayObservation().orElse(null) != identity) {
                        return Optional.empty();
                    }
                    return captured;
                } catch (IOException failure) {
                    throw new CompletionException(failure);
                }
            }, executor);
        };
    }

    @Override
    public synchronized void close() throws IOException {
        SharedStorageEngine currentStorage = storage;
        storage = null;
        commitProgress = null;
        unifiedLogFactory = null;
        partitionRoleListener = null;
        brokerStartupReplayObservation = null;
        brokerReadyObserved = false;
        startedBrokerId = -1;
        if (currentStorage != null) {
            currentStorage.close();
        }
    }
}
