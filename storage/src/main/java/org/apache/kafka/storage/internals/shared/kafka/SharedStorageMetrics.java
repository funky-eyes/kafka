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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Broker-scoped JMX metrics for the shared-storage data plane.
 *
 * <p>The metrics intentionally expose operational state without participating in any durability decision. Gauges read
 * already-authoritative in-memory state from the WAL engine and upload scheduler, so monitoring cannot change ACK,
 * checkpoint, upload, or reclaim ordering.</p>
 */
public final class SharedStorageMetrics implements AutoCloseable {
    static final String METRIC_GROUP = "kafka.server";
    static final String METRIC_TYPE = "SharedStorage";
    static final String BROKER_ID_TAG = "brokerId";

    private static final List<String> METRIC_NAMES = List.of(
        "WalUsedBytes",
        "WalCapacityBytes",
        "WalUtilizationPercent",
        "WalDurabilityBatchCount",
        "WalDurableAppendGroupCount",
        "WalDurableBytes",
        "WalDurabilityBarrierNanos",
        "WalDurabilityDataForceNanos",
        "WalDurabilityCheckpointForceNanos",
        "WalMaxDurabilityBarrierNanos",
        "WalMaxDurabilityDataForceNanos",
        "WalMaxDurabilityCheckpointForceNanos",
        "WalMaxGroupsPerDurabilityBatch",
        "WalSingletonCoalesceWaitCount",
        "WalSingletonCoalesceHitCount",
        "WalSingletonCoalesceWaitNanos",
        "WalAppendInterArrivalCount",
        "WalAppendInterArrivalNanos",
        "WalAppendInterArrivalLe100MicrosCount",
        "WalAppendInterArrivalLe250MicrosCount",
        "WalAppendInterArrivalLe500MicrosCount",
        "WalAppendInterArrivalLe1000MicrosCount",
        "PendingRemoteCheckpoints",
        "RemoteControlPlaneReady",
        "MetadataBootstrapFailureCount",
        "UploadsInProgress",
        "ReservedUploadCandidates",
        "UploadCandidateCount",
        "EligibleUploadBytes",
        "UploadFailurePresent",
        "MaintenanceFailurePresent"
    );

    private final SharedStorageEngine engine;
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(METRIC_GROUP, METRIC_TYPE);
    private final Map<String, String> tags;
    private final AtomicReference<SharedUploadScheduler> uploadScheduler = new AtomicReference<>();
    private final AtomicBoolean remoteControlPlaneReady = new AtomicBoolean();
    private final AtomicLong metadataBootstrapFailureCount = new AtomicLong();

    public SharedStorageMetrics(SharedStorageEngine engine, int brokerId) {
        this.engine = Objects.requireNonNull(engine, "engine");
        this.tags = Map.of(BROKER_ID_TAG, Integer.toString(brokerId));

        metricsGroup.newGauge("WalUsedBytes", engine::walUsedBytes, tags);
        metricsGroup.newGauge("WalCapacityBytes", engine::walCapacityBytes, tags);
        metricsGroup.newGauge("WalUtilizationPercent", this::walUtilizationPercent, tags);
        metricsGroup.newGauge(
            "WalDurabilityBatchCount",
            () -> engine.walDurabilityStats().durabilityBatchCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalDurableAppendGroupCount",
            () -> engine.walDurabilityStats().durableAppendGroupCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalDurableBytes",
            () -> engine.walDurabilityStats().durableBytes(),
            tags
        );
        metricsGroup.newGauge(
            "WalDurabilityBarrierNanos",
            () -> engine.walDurabilityStats().durabilityBarrierNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalDurabilityDataForceNanos",
            () -> engine.walDurabilityStats().durabilityDataForceNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalDurabilityCheckpointForceNanos",
            () -> engine.walDurabilityStats().durabilityCheckpointForceNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalMaxDurabilityBarrierNanos",
            () -> engine.walDurabilityStats().maxDurabilityBarrierNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalMaxDurabilityDataForceNanos",
            () -> engine.walDurabilityStats().maxDurabilityDataForceNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalMaxDurabilityCheckpointForceNanos",
            () -> engine.walDurabilityStats().maxDurabilityCheckpointForceNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalMaxGroupsPerDurabilityBatch",
            () -> engine.walDurabilityStats().maxGroupsPerDurabilityBatch(),
            tags
        );
        metricsGroup.newGauge(
            "WalSingletonCoalesceWaitCount",
            () -> engine.walDurabilityStats().singletonCoalesceWaitCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalSingletonCoalesceHitCount",
            () -> engine.walDurabilityStats().singletonCoalesceHitCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalSingletonCoalesceWaitNanos",
            () -> engine.walDurabilityStats().singletonCoalesceWaitNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalAppendInterArrivalCount",
            () -> engine.walDurabilityStats().appendInterArrivalCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalAppendInterArrivalNanos",
            () -> engine.walDurabilityStats().appendInterArrivalNanos(),
            tags
        );
        metricsGroup.newGauge(
            "WalAppendInterArrivalLe100MicrosCount",
            () -> engine.walDurabilityStats().appendInterArrivalLe100MicrosCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalAppendInterArrivalLe250MicrosCount",
            () -> engine.walDurabilityStats().appendInterArrivalLe250MicrosCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalAppendInterArrivalLe500MicrosCount",
            () -> engine.walDurabilityStats().appendInterArrivalLe500MicrosCount(),
            tags
        );
        metricsGroup.newGauge(
            "WalAppendInterArrivalLe1000MicrosCount",
            () -> engine.walDurabilityStats().appendInterArrivalLe1000MicrosCount(),
            tags
        );
        metricsGroup.newGauge("PendingRemoteCheckpoints", engine::pendingRemoteCheckpointCount, tags);
        metricsGroup.newGauge("RemoteControlPlaneReady", () -> remoteControlPlaneReady.get() ? 1 : 0, tags);
        metricsGroup.newGauge("MetadataBootstrapFailureCount", metadataBootstrapFailureCount::get, tags);
        metricsGroup.newGauge("UploadsInProgress", () -> schedulerInt(SharedUploadScheduler::uploadsInProgress), tags);
        metricsGroup.newGauge(
            "ReservedUploadCandidates",
            () -> schedulerInt(SharedUploadScheduler::reservedCandidateCount),
            tags
        );
        metricsGroup.newGauge(
            "UploadCandidateCount",
            () -> schedulerInt(SharedUploadScheduler::uploadCandidateCount),
            tags
        );
        metricsGroup.newGauge(
            "EligibleUploadBytes",
            () -> schedulerLong(SharedUploadScheduler::eligibleUploadBytes),
            tags
        );
        metricsGroup.newGauge(
            "UploadFailurePresent",
            () -> schedulerBoolean(SharedUploadScheduler::uploadFailurePresent),
            tags
        );
        metricsGroup.newGauge(
            "MaintenanceFailurePresent",
            () -> schedulerBoolean(SharedUploadScheduler::maintenanceFailurePresent),
            tags
        );
    }

    public void attachUploadScheduler(SharedUploadScheduler scheduler) {
        Objects.requireNonNull(scheduler, "scheduler");
        SharedUploadScheduler previous = uploadScheduler.getAndSet(scheduler);
        if (previous != null && previous != scheduler) {
            throw new IllegalStateException("Shared upload scheduler metrics are already attached");
        }
    }

    public void markRemoteControlPlaneReady() {
        remoteControlPlaneReady.set(true);
    }

    public void markRemoteControlPlaneUnavailable() {
        remoteControlPlaneReady.set(false);
    }

    public void recordMetadataBootstrapFailure() {
        metadataBootstrapFailureCount.incrementAndGet();
    }

    private double walUtilizationPercent() {
        long capacityBytes = engine.walCapacityBytes();
        if (capacityBytes <= 0L) {
            return 0.0d;
        }
        return Math.min(100.0d, engine.walUsedBytes() * 100.0d / capacityBytes);
    }

    private int schedulerInt(java.util.function.ToIntFunction<SharedUploadScheduler> value) {
        SharedUploadScheduler scheduler = uploadScheduler.get();
        return scheduler == null ? 0 : value.applyAsInt(scheduler);
    }

    private long schedulerLong(java.util.function.ToLongFunction<SharedUploadScheduler> value) {
        SharedUploadScheduler scheduler = uploadScheduler.get();
        return scheduler == null ? 0L : value.applyAsLong(scheduler);
    }

    private int schedulerBoolean(java.util.function.Predicate<SharedUploadScheduler> value) {
        SharedUploadScheduler scheduler = uploadScheduler.get();
        return scheduler != null && value.test(scheduler) ? 1 : 0;
    }

    @Override
    public void close() {
        remoteControlPlaneReady.set(false);
        uploadScheduler.set(null);
        METRIC_NAMES.forEach(name -> metricsGroup.removeMetric(name, tags));
    }
}
