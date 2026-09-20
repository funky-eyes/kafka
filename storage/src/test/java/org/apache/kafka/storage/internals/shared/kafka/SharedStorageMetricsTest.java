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
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Map;
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
    void exposesBrokerScopedWalAndControlPlaneMetrics() throws Exception {
        int brokerId = 17;
        MetricName usedBytesName = metricName("WalUsedBytes", brokerId);
        MetricName capacityName = metricName("WalCapacityBytes", brokerId);
        MetricName readyName = metricName("RemoteControlPlaneReady", brokerId);
        MetricName bootstrapFailuresName = metricName("MetadataBootstrapFailureCount", brokerId);

        try (SharedStorageEngine engine = engine("metrics");
             SharedStorageMetrics metrics = new SharedStorageMetrics(engine, brokerId)) {
            Gauge<?> usedBytes = gauge(usedBytesName);
            Gauge<?> capacity = gauge(capacityName);
            Gauge<?> ready = gauge(readyName);
            Gauge<?> bootstrapFailures = gauge(bootstrapFailuresName);

            assertEquals(0L, ((Number) usedBytes.value()).longValue());
            assertEquals(engine.walCapacityBytes(), ((Number) capacity.value()).longValue());
            assertEquals(0, ((Number) ready.value()).intValue());
            assertEquals(0L, ((Number) bootstrapFailures.value()).longValue());

            engine.appendData(PARTITION, 1, 0L, 0L, ByteBuffer.wrap(new byte[] {1, 2, 3}))
                .get(10, TimeUnit.SECONDS);
            assertTrue(((Number) usedBytes.value()).longValue() > 0L);

            metrics.recordMetadataBootstrapFailure();
            metrics.markRemoteControlPlaneReady();
            assertEquals(1, ((Number) ready.value()).intValue());
            assertEquals(1L, ((Number) bootstrapFailures.value()).longValue());

            metrics.markRemoteControlPlaneUnavailable();
            assertEquals(0, ((Number) ready.value()).intValue());
        }

        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(usedBytesName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(capacityName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(readyName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(bootstrapFailuresName));
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
