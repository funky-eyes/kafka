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

import org.apache.kafka.storage.internals.log.StorageExtensionContext;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Parsed, Kafka-independent configuration for the shared WAL storage extension. */
public final class SharedStorageConfiguration {
    public static final String WAL_DIR_CONFIG = "shared.storage.wal.dir";
    public static final String WAL_CAPACITY_BYTES_CONFIG = "shared.storage.wal.capacity.bytes";
    public static final String WAL_SEGMENT_BYTES_CONFIG = "shared.storage.wal.segment.bytes";
    public static final String WAL_READ_CACHE_BYTES_CONFIG = "shared.storage.wal.read.cache.bytes";
    public static final String OBJECT_TARGET_BYTES_CONFIG = "shared.storage.object.target.bytes";
    public static final String UPLOAD_INTERVAL_MS_CONFIG = "shared.storage.upload.interval.ms";
    public static final String ORPHAN_CLEANUP_INTERVAL_MS_CONFIG = "shared.storage.orphan.cleanup.interval.ms";
    public static final String ORPHAN_GRACE_MS_CONFIG = "shared.storage.orphan.grace.ms";
    public static final String TOPICS_CONFIG = "shared.storage.topics";
    public static final String TOPIC_PATTERN_CONFIG = "shared.storage.topic.pattern";

    public static final long DEFAULT_WAL_CAPACITY_BYTES = 2L * 1024 * 1024 * 1024;
    public static final long DEFAULT_WAL_SEGMENT_BYTES = 64L * 1024 * 1024;
    public static final long DEFAULT_WAL_READ_CACHE_BYTES = 256L * 1024 * 1024;
    public static final long DEFAULT_OBJECT_TARGET_BYTES = 32L * 1024 * 1024;
    public static final long DEFAULT_UPLOAD_INTERVAL_MS = 100L;
    public static final long DEFAULT_ORPHAN_CLEANUP_INTERVAL_MS = 60_000L;
    public static final long DEFAULT_ORPHAN_GRACE_MS = 10L * 60 * 1_000;

    private final Path walDir;
    private final long walCapacityBytes;
    private final long walSegmentBytes;
    private final long walReadCacheBytes;
    private final long objectTargetBytes;
    private final long uploadIntervalMs;
    private final long orphanCleanupIntervalMs;
    private final long orphanGraceMs;
    private final Set<String> topics;
    private final Pattern topicPattern;

    private SharedStorageConfiguration(
        Path walDir,
        long walCapacityBytes,
        long walSegmentBytes,
        long walReadCacheBytes,
        long objectTargetBytes,
        long uploadIntervalMs,
        long orphanCleanupIntervalMs,
        long orphanGraceMs,
        Set<String> topics,
        Pattern topicPattern
    ) {
        this.walDir = walDir;
        this.walCapacityBytes = walCapacityBytes;
        this.walSegmentBytes = walSegmentBytes;
        this.walReadCacheBytes = walReadCacheBytes;
        this.objectTargetBytes = objectTargetBytes;
        this.uploadIntervalMs = uploadIntervalMs;
        this.orphanCleanupIntervalMs = orphanCleanupIntervalMs;
        this.orphanGraceMs = orphanGraceMs;
        this.topics = topics;
        this.topicPattern = topicPattern;
    }

    public static SharedStorageConfiguration from(StorageExtensionContext context) {
        Objects.requireNonNull(context, "context");
        Object configuredWalDir = context.originals().get(WAL_DIR_CONFIG);
        Path walDir = configuredWalDir == null || configuredWalDir.toString().isBlank()
            ? defaultWalDir(context.liveLogDirs().get(0), context.brokerId())
            : Path.of(configuredWalDir.toString().trim()).toAbsolutePath().normalize();
        validateWalDirOutsideKafkaLogRoots(walDir, context);

        long walCapacityBytes = positiveLong(
            context.originals().get(WAL_CAPACITY_BYTES_CONFIG),
            DEFAULT_WAL_CAPACITY_BYTES,
            WAL_CAPACITY_BYTES_CONFIG
        );
        long walSegmentBytes = positiveLong(
            context.originals().get(WAL_SEGMENT_BYTES_CONFIG),
            DEFAULT_WAL_SEGMENT_BYTES,
            WAL_SEGMENT_BYTES_CONFIG
        );
        if (walSegmentBytes > walCapacityBytes) {
            throw new IllegalArgumentException(
                WAL_SEGMENT_BYTES_CONFIG + " must not exceed " + WAL_CAPACITY_BYTES_CONFIG);
        }
        long walReadCacheBytes = nonNegativeLong(
            context.originals().get(WAL_READ_CACHE_BYTES_CONFIG),
            DEFAULT_WAL_READ_CACHE_BYTES,
            WAL_READ_CACHE_BYTES_CONFIG
        );
        long objectTargetBytes = positiveLong(
            context.originals().get(OBJECT_TARGET_BYTES_CONFIG),
            DEFAULT_OBJECT_TARGET_BYTES,
            OBJECT_TARGET_BYTES_CONFIG
        );
        long uploadIntervalMs = positiveLong(
            context.originals().get(UPLOAD_INTERVAL_MS_CONFIG),
            DEFAULT_UPLOAD_INTERVAL_MS,
            UPLOAD_INTERVAL_MS_CONFIG
        );
        long orphanCleanupIntervalMs = positiveLong(
            context.originals().get(ORPHAN_CLEANUP_INTERVAL_MS_CONFIG),
            DEFAULT_ORPHAN_CLEANUP_INTERVAL_MS,
            ORPHAN_CLEANUP_INTERVAL_MS_CONFIG
        );
        long orphanGraceMs = positiveLong(
            context.originals().get(ORPHAN_GRACE_MS_CONFIG),
            DEFAULT_ORPHAN_GRACE_MS,
            ORPHAN_GRACE_MS_CONFIG
        );

        Set<String> topics = parseTopics(context.originals().get(TOPICS_CONFIG));
        Pattern topicPattern = parsePattern(context.originals().get(TOPIC_PATTERN_CONFIG));
        return new SharedStorageConfiguration(
            walDir,
            walCapacityBytes,
            walSegmentBytes,
            walReadCacheBytes,
            objectTargetBytes,
            uploadIntervalMs,
            orphanCleanupIntervalMs,
            orphanGraceMs,
            topics,
            topicPattern
        );
    }

    private static Path defaultWalDir(File firstKafkaLogDir, int brokerId) {
        Path kafkaLogDir = firstKafkaLogDir.toPath().toAbsolutePath().normalize();
        Path fileName = kafkaLogDir.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException(
                "Kafka log directory cannot be the filesystem root when deriving " + WAL_DIR_CONFIG);
        }
        return kafkaLogDir
            .resolveSibling(fileName + ".shared-storage")
            .resolve("broker-" + brokerId)
            .resolve("wal")
            .normalize();
    }

    private static void validateWalDirOutsideKafkaLogRoots(Path walDir, StorageExtensionContext context) {
        for (File logDir : context.liveLogDirs()) {
            Path kafkaLogRoot = logDir.toPath().toAbsolutePath().normalize();
            if (walDir.startsWith(kafkaLogRoot)) {
                throw new IllegalArgumentException(
                    WAL_DIR_CONFIG + " must be outside every Kafka log directory because LogManager scans their " +
                        "children as topic-partition directories: walDir=" + walDir + ", kafkaLogDir=" + kafkaLogRoot);
            }
        }
    }

    public Path walDir() {
        return walDir;
    }

    public long walCapacityBytes() {
        return walCapacityBytes;
    }

    public long walSegmentBytes() {
        return walSegmentBytes;
    }

    public long walReadCacheBytes() {
        return walReadCacheBytes;
    }

    public long objectTargetBytes() {
        return objectTargetBytes;
    }

    public long uploadIntervalMs() {
        return uploadIntervalMs;
    }

    public long orphanCleanupIntervalMs() {
        return orphanCleanupIntervalMs;
    }

    public long orphanGraceMs() {
        return orphanGraceMs;
    }

    /**
     * Returns whether a user topic should use shared storage.
     *
     * <p>Kafka internal topics are always classic. If no selector is configured, all non-internal topics are shared.
     * When selectors are present, exact-name and regex selectors are combined as a union.</p>
     */
    public boolean useSharedStorage(String topic) {
        Objects.requireNonNull(topic, "topic");
        if (topic.startsWith("__")) {
            return false;
        }
        if (topics.isEmpty() && topicPattern == null) {
            return true;
        }
        return topics.contains(topic) || (topicPattern != null && topicPattern.matcher(topic).matches());
    }

    private static long positiveLong(Object value, long defaultValue, String name) {
        long parsed = nonNegativeLong(value, defaultValue, name);
        if (parsed == 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return parsed;
    }

    private static long nonNegativeLong(Object value, long defaultValue, String name) {
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        long parsed = value instanceof Number number
            ? number.longValue()
            : Long.parseLong(value.toString().trim());
        if (parsed < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
        return parsed;
    }

    private static Set<String> parseTopics(Object value) {
        if (value == null || value.toString().isBlank()) {
            return Set.of();
        }
        LinkedHashSet<String> result = new LinkedHashSet<>();
        Arrays.stream(value.toString().split(","))
            .map(String::trim)
            .filter(topic -> !topic.isEmpty())
            .forEach(result::add);
        return Collections.unmodifiableSet(result);
    }

    private static Pattern parsePattern(Object value) {
        if (value == null || value.toString().isBlank()) {
            return null;
        }
        try {
            return Pattern.compile(value.toString().trim());
        } catch (PatternSyntaxException e) {
            throw new IllegalArgumentException("Invalid " + TOPIC_PATTERN_CONFIG + ": " + value, e);
        }
    }
}
