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

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** Broker-scoped, dynamically configurable admission control for local disk pressure. */
public final class DiskPressureGuard implements Reconfigurable, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DiskPressureGuard.class);
    private static final long CHECK_INTERVAL_SECONDS = 5;
    private static final List<String> METRIC_NAMES = List.of(
        "Enabled", "ThresholdPercent", "ObservedPaths", "BlockedPaths", "ReadFailures", "RejectedAppends");
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup("kafka.server", "DiskWriteProtection");
    private final Map<String, String> metricTags;
    private final AtomicLong readFailures = new AtomicLong();
    private final AtomicLong rejectedAppends = new AtomicLong();
    private final ConcurrentHashMap<Path, Double> status = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "disk-pressure-check");
        thread.setDaemon(true);
        return thread;
    });
    @FunctionalInterface
    interface UsageReader {
        double read(Path path) throws IOException;
    }

    private final UsageReader usageReader;
    private volatile double maxUsagePercent;

    public DiskPressureGuard(double maxUsagePercent, int brokerId) {
        this(maxUsagePercent, DiskPressureGuard::usagePercent, brokerId);
    }

    DiskPressureGuard(double maxUsagePercent, UsageReader usageReader) {
        this(maxUsagePercent, usageReader, -1);
    }

    DiskPressureGuard(double maxUsagePercent, UsageReader usageReader, int brokerId) {
        validate(maxUsagePercent);
        this.usageReader = usageReader;
        this.maxUsagePercent = maxUsagePercent;
        this.metricTags = brokerId < 0 ? null : Map.of("brokerId", Integer.toString(brokerId));
        if (metricTags != null) {
            metricsGroup.newGauge("Enabled", () -> enabled() ? 1 : 0, metricTags);
            metricsGroup.newGauge("ThresholdPercent", () -> this.maxUsagePercent, metricTags);
            metricsGroup.newGauge("ObservedPaths", status::size, metricTags);
            metricsGroup.newGauge("BlockedPaths", this::blockedPaths, metricTags);
            metricsGroup.newGauge("ReadFailures", readFailures::get, metricTags);
            metricsGroup.newGauge("RejectedAppends", rejectedAppends::get, metricTags);
        }
        scheduler.scheduleWithFixedDelay(this::refresh, CHECK_INTERVAL_SECONDS, CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public boolean enabled() {
        return maxUsagePercent < 100;
    }

    public void check(Path directory) {
        if (maxUsagePercent >= 100) return;
        Path path = directory.toAbsolutePath().normalize();
        if (status.computeIfAbsent(path, this::readUsage) >= maxUsagePercent) {
            rejectedAppends.incrementAndGet();
            throw new KafkaStorageException("Disk write protection is active for " + path);
        }
    }

    double readUsage(Path path) {
        try {
            return usageReader.read(path);
        } catch (IOException | SecurityException e) {
            readFailures.incrementAndGet();
            LOG.warn("Cannot determine disk usage for {}; rejecting client writes until the next successful check", path, e);
            return Double.POSITIVE_INFINITY;
        }
    }

    private long blockedPaths() {
        double threshold = maxUsagePercent;
        return threshold >= 100 ? 0 : status.values().stream().filter(usage -> usage >= threshold).count();
    }

    private static double usagePercent(Path path) throws IOException {
        FileStore store = Files.getFileStore(path);
        long total = store.getTotalSpace();
        if (total <= 0) throw new IOException("Filesystem reports non-positive total space");
        return 100.0 * (total - Math.min(total, store.getUsableSpace())) / total;
    }

    void refresh() {
        try {
            status.replaceAll((path, previous) -> readUsage(path));
        } catch (RuntimeException e) {
            LOG.error("Failed to refresh disk pressure status", e);
        }
    }

    @Override
    public Set<String> reconfigurableConfigs() {
        return Set.of(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG);
    }

    private static void validate(double value) {
        if (!Double.isFinite(value) || value < 0.01 || value > 100)
            throw new ConfigException("Disk protection usage percentage must be in [0.01, 100]");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        reconfigure(configs);
    }

    @Override
    public void validateReconfiguration(Map<String, ?> configs) {
        validate(((Number) configs.get(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG)).doubleValue());
    }

    @Override
    public void reconfigure(Map<String, ?> configs) {
        double value = ((Number) configs.get(ServerLogConfigs.LOG_DISK_PROTECTION_MAX_USAGE_PERCENT_CONFIG)).doubleValue();
        validate(value);
        maxUsagePercent = value;
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        if (metricTags != null) {
            METRIC_NAMES.forEach(name -> metricsGroup.removeMetric(name, metricTags));
        }
    }
}
