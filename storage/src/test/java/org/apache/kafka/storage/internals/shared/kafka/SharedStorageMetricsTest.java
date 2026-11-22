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
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.CoordinatedRingWalAdapter;
import org.apache.kafka.storage.internals.shared.wal.FileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageMetricsTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);

    @TempDir
    Path tempDir;

    @Test
    void exposesCompletedBlockOccupancySeparatelyFromCheckpointCohorts() throws Exception {
        int brokerId = 37;
        MetricName blockName = metricName("WalBlockCount", brokerId);
        MetricName groupsName = metricName("WalBlockGroupCount", brokerId);
        MetricName bytesName = metricName("WalBlockRecordBytes", brokerId);
        MetricName maxName = metricName("WalMaxGroupsPerBlock", brokerId);
        MetricName trackedName = metricName("WalTrackedAppendCount", brokerId);
        MetricName nonBlockingName = metricName("WalNonBlockingRequestCount", brokerId);
        var wal = CoordinatedRingWalAdapter.create(tempDir.resolve("metric-block-wal"),
            new FileChannelWalIoBackend(), 65536L, 4096, 50_000_000L, 8, 8192L);
        try (SharedStorageEngine engine = new SharedStorageEngine(wal);
             SharedStorageMetrics metrics = new SharedStorageMetrics(engine, brokerId)) {
            assertEquals(0L, ((Number) gauge(trackedName).value()).longValue());
            engine.recordTrackedAppend();
            assertEquals(1L, ((Number) gauge(trackedName).value()).longValue());
            assertEquals(0L, ((Number) gauge(nonBlockingName).value()).longValue());
            engine.recordNonBlockingRequest();
            assertEquals(1L, ((Number) gauge(nonBlockingName).value()).longValue());
            var writes = java.util.stream.IntStream.range(0, 3).mapToObj(i ->
                wal.appendBatch(List.of(WalRecord.data(1, 2, 0, 1, i, i, new byte[] {(byte) i})))).toList();
            CompletableFuture.allOf(writes.toArray(CompletableFuture[]::new)).get(10, TimeUnit.SECONDS);
            long blocks = ((Number) gauge(blockName).value()).longValue();
            assertTrue(blocks >= 1 && blocks <= 3);
            assertEquals(3L, ((Number) gauge(groupsName).value()).longValue());
            assertTrue(((Number) gauge(bytesName).value()).longValue() > 0);
            assertTrue(((Number) gauge(maxName).value()).longValue() >= 1);
        }
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(blockName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(groupsName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(trackedName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(nonBlockingName));
    }

    @Test
    void exposesBrokerScopedWalAndControlPlaneMetrics() throws Exception {
        int brokerId = 17;
        MetricName usedBytesName = metricName("WalUsedBytes", brokerId);
        MetricName capacityName = metricName("WalCapacityBytes", brokerId);
        MetricName durabilityBatchCountName = metricName("WalDurabilityBatchCount", brokerId);
        MetricName durableAppendGroupCountName = metricName("WalDurableAppendGroupCount", brokerId);
        MetricName durabilityBarrierNanosName = metricName("WalDurabilityBarrierNanos", brokerId);
        MetricName dataForceNanosName = metricName("WalDurabilityDataForceNanos", brokerId);
        MetricName checkpointForceNanosName = metricName("WalDurabilityCheckpointForceNanos", brokerId);
        MetricName maxBarrierNanosName = metricName("WalMaxDurabilityBarrierNanos", brokerId);
        MetricName maxDataForceNanosName = metricName("WalMaxDurabilityDataForceNanos", brokerId);
        MetricName maxCheckpointForceNanosName =
            metricName("WalMaxDurabilityCheckpointForceNanos", brokerId);
        MetricName coalesceWaitCountName = metricName("WalSingletonCoalesceWaitCount", brokerId);
        MetricName coalesceHitCountName = metricName("WalSingletonCoalesceHitCount", brokerId);
        MetricName interArrivalCountName = metricName("WalAppendInterArrivalCount", brokerId);
        MetricName readyName = metricName("RemoteControlPlaneReady", brokerId);
        MetricName bootstrapFailuresName = metricName("MetadataBootstrapFailureCount", brokerId);
        MetricName cacheHitsName = metricName("WalLocalCacheHitCount", brokerId);
        MetricName cacheMissesName = metricName("WalLocalCacheMissCount", brokerId);
        MetricName cacheEvictionsName = metricName("WalLocalCacheEvictionCount", brokerId);
        MetricName cacheBytesName = metricName("WalLocalCacheBytes", brokerId);

        try (SharedStorageEngine engine = engine("metrics");
             SharedStorageMetrics metrics = new SharedStorageMetrics(engine, brokerId)) {
            Gauge<?> usedBytes = gauge(usedBytesName);
            Gauge<?> capacity = gauge(capacityName);
            Gauge<?> durabilityBatchCount = gauge(durabilityBatchCountName);
            Gauge<?> durableAppendGroupCount = gauge(durableAppendGroupCountName);
            Gauge<?> durabilityBarrierNanos = gauge(durabilityBarrierNanosName);
            Gauge<?> dataForceNanos = gauge(dataForceNanosName);
            Gauge<?> checkpointForceNanos = gauge(checkpointForceNanosName);
            Gauge<?> maxBarrierNanos = gauge(maxBarrierNanosName);
            Gauge<?> maxDataForceNanos = gauge(maxDataForceNanosName);
            Gauge<?> maxCheckpointForceNanos = gauge(maxCheckpointForceNanosName);
            Gauge<?> coalesceWaitCount = gauge(coalesceWaitCountName);
            Gauge<?> coalesceHitCount = gauge(coalesceHitCountName);
            Gauge<?> interArrivalCount = gauge(interArrivalCountName);
            Gauge<?> ready = gauge(readyName);
            Gauge<?> bootstrapFailures = gauge(bootstrapFailuresName);

            assertEquals(0L, ((Number) usedBytes.value()).longValue());
            assertEquals(engine.walCapacityBytes(), ((Number) capacity.value()).longValue());
            assertEquals(0L, ((Number) durabilityBatchCount.value()).longValue());
            assertEquals(0L, ((Number) durableAppendGroupCount.value()).longValue());
            assertEquals(0L, ((Number) durabilityBarrierNanos.value()).longValue());
            assertEquals(0L, ((Number) dataForceNanos.value()).longValue());
            assertEquals(0L, ((Number) checkpointForceNanos.value()).longValue());
            assertEquals(0L, ((Number) maxBarrierNanos.value()).longValue());
            assertEquals(0L, ((Number) maxDataForceNanos.value()).longValue());
            assertEquals(0L, ((Number) maxCheckpointForceNanos.value()).longValue());
            assertEquals(0L, ((Number) coalesceWaitCount.value()).longValue());
            assertEquals(0L, ((Number) coalesceHitCount.value()).longValue());
            assertEquals(0L, ((Number) interArrivalCount.value()).longValue());
            assertEquals(0, ((Number) ready.value()).intValue());
            assertEquals(0L, ((Number) bootstrapFailures.value()).longValue());

            engine.appendData(PARTITION, 1, 0L, 0L, ByteBuffer.wrap(new byte[] {1, 2, 3}))
                .get(10, TimeUnit.SECONDS);
            assertTrue(((Number) usedBytes.value()).longValue() > 0L);
            engine.readLocal(PARTITION, 0L).orElseThrow();
            engine.readLocal(PARTITION, 0L).orElseThrow();
            assertEquals(1L, ((Number) gauge(cacheHitsName).value()).longValue());
            assertEquals(1L, ((Number) gauge(cacheMissesName).value()).longValue());
            assertEquals(0L, ((Number) gauge(cacheEvictionsName).value()).longValue());
            assertTrue(((Number) gauge(cacheBytesName).value()).longValue() >= 3L);

            metrics.recordMetadataBootstrapFailure();
            metrics.markRemoteControlPlaneReady();
            assertEquals(1, ((Number) ready.value()).intValue());
            assertEquals(1L, ((Number) bootstrapFailures.value()).longValue());

            metrics.markRemoteControlPlaneUnavailable();
            assertEquals(0, ((Number) ready.value()).intValue());
        }

        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(usedBytesName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(capacityName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(durabilityBatchCountName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(durableAppendGroupCountName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(durabilityBarrierNanosName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(dataForceNanosName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(checkpointForceNanosName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(maxBarrierNanosName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(maxDataForceNanosName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(maxCheckpointForceNanosName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(coalesceWaitCountName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(coalesceHitCountName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(interArrivalCountName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(readyName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(bootstrapFailuresName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(cacheHitsName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(cacheMissesName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(cacheEvictionsName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(cacheBytesName));
    }

    @Test
    void allowsMultipleBrokerMetricScopesInOneJvm() throws Exception {
        MetricName brokerOneName = metricName("WalCapacityBytes", 21);
        MetricName brokerTwoName = metricName("WalCapacityBytes", 22);

        try (SharedStorageEngine firstEngine = engine("broker-one");
             SharedStorageEngine secondEngine = engine("broker-two");
             SharedStorageMetrics firstMetrics = new SharedStorageMetrics(firstEngine, 21);
             SharedStorageMetrics secondMetrics = new SharedStorageMetrics(secondEngine, 22)) {
            assertTrue(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(brokerOneName));
            assertTrue(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(brokerTwoName));
        }

        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(brokerOneName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(brokerTwoName));
    }

    private SharedStorageEngine engine(String name) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), 1024 * 1024, 4096));
    }

    private static MetricName metricName(String name, int brokerId) {
        return new KafkaMetricsGroup(SharedStorageMetrics.METRIC_GROUP, SharedStorageMetrics.METRIC_TYPE)
            .metricName(name, Map.of(SharedStorageMetrics.BROKER_ID_TAG, Integer.toString(brokerId)));
    }

    private static Gauge<?> gauge(MetricName name) {
        return assertInstanceOf(Gauge.class, KafkaYammerMetrics.defaultRegistry().allMetrics().get(name));
    }
}
