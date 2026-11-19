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
package kafka.server;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DiskPressureGuardTest {
    private static Map<String, Object> threshold(double value) {
        return Map.of(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG, value);
    }

    @Test
    void exposesScopedMetricsAndRemovesThemOnClose() {
        int brokerId = 9981;
        AtomicReference<Double> usage = new AtomicReference<>(95.0);
        KafkaMetricsGroup group = new KafkaMetricsGroup("kafka.server", "DiskWriteProtection");
        Map<String, String> tags = Map.of("brokerId", Integer.toString(brokerId));
        MetricName blockedName = group.metricName("BlockedPaths", tags);
        MetricName rejectedName = group.metricName("RejectedAppends", tags);
        MetricName failureName = group.metricName("ReadFailures", tags);
        try (DiskPressureGuard guard = new DiskPressureGuard(90.0, path -> {
            if (usage.get() < 0) throw new IOException("unavailable");
            return usage.get();
        }, brokerId)) {
            Path path = Path.of("/tmp/disk-pressure-metrics");
            assertThrows(KafkaStorageException.class, () -> guard.check(path));
            assertEquals(1L, metricValue(blockedName).longValue());
            assertEquals(1L, metricValue(rejectedName).longValue());
            usage.set(-1.0);
            guard.refresh();
            assertEquals(1L, metricValue(failureName).longValue());
            guard.reconfigure(threshold(100));
            assertEquals(0L, metricValue(blockedName).longValue());
        }
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(blockedName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(rejectedName));
        assertFalse(KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(failureName));
    }

    private static Number metricValue(MetricName name) {
        Gauge<?> gauge = assertInstanceOf(Gauge.class, KafkaYammerMetrics.defaultRegistry().allMetrics().get(name));
        return (Number) gauge.value();
    }

    @Test
    void rejectsAtThresholdAndRecoversAfterRefresh() {
        AtomicReference<Double> usage = new AtomicReference<>(90.0);
        try (DiskPressureGuard guard = new DiskPressureGuard(90.0, path -> usage.get())) {
            Path path = Path.of("/tmp/disk-pressure-test");
            assertThrows(KafkaStorageException.class, () -> guard.check(path));
            usage.set(80.0);
            guard.refresh();
            assertDoesNotThrow(() -> guard.check(path));
        }
    }

    @Test
    void thresholdUpdatesAndDisableApplyWithoutRestart() {
        try (DiskPressureGuard guard = new DiskPressureGuard(100.0, path -> 80.0)) {
            Path path = Path.of("/tmp/disk-pressure-test");
            assertDoesNotThrow(() -> guard.check(path));
            guard.reconfigure(threshold(75.0));
            assertThrows(KafkaStorageException.class, () -> guard.check(path));
            guard.reconfigure(threshold(85.0));
            assertDoesNotThrow(() -> guard.check(path));
            guard.reconfigure(threshold(100.0));
            assertDoesNotThrow(() -> guard.check(path));
            assertThrows(ConfigException.class, () -> guard.validateReconfiguration(threshold(Double.NaN)));
        }
    }

    @Test
    void tracksIndependentFilesystems() {
        Path logPath = Path.of("/tmp/disk-pressure-logs");
        Path walPath = Path.of("/tmp/disk-pressure-wal");
        try (DiskPressureGuard guard = new DiskPressureGuard(90.0,
            path -> path.equals(walPath) ? 95.0 : 50.0)) {
            assertDoesNotThrow(() -> guard.check(logPath));
            assertThrows(KafkaStorageException.class, () -> guard.check(walPath));
        }
    }

    @Test
    void filesystemErrorsFailClosedUntilTheNextSuccessfulCheck() {
        AtomicReference<Boolean> fail = new AtomicReference<>(false);
        try (DiskPressureGuard guard = new DiskPressureGuard(90.0, path -> {
            if (fail.get()) throw new IOException("unavailable");
            return 50.0;
        })) {
            Path known = Path.of("/tmp/disk-pressure-known");
            assertDoesNotThrow(() -> guard.check(known));
            fail.set(true);
            guard.refresh();
            assertThrows(KafkaStorageException.class, () -> guard.check(known));
            assertThrows(KafkaStorageException.class, () -> guard.check(Path.of("/tmp/disk-pressure-unknown")));
            fail.set(false);
            guard.refresh();
            assertDoesNotThrow(() -> guard.check(known));
        }
    }
}
