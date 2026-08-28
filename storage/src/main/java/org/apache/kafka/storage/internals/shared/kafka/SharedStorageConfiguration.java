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
    public static final String TOPICS_CONFIG = "shared.storage.topics";
    public static final String TOPIC_PATTERN_CONFIG = "shared.storage.topic.pattern";

    public static final long DEFAULT_WAL_CAPACITY_BYTES = 2L * 1024 * 1024 * 1024;
    public static final long DEFAULT_WAL_SEGMENT_BYTES = 64L * 1024 * 1024;

    private final Path walDir;
    private final long walCapacityBytes;
    private final long walSegmentBytes;
    private final Set<String> topics;
    private final Pattern topicPattern;

    private SharedStorageConfiguration(
        Path walDir,
        long walCapacityBytes,
        long walSegmentBytes,
        Set<String> topics,
        Pattern topicPattern
    ) {
        this.walDir = walDir;
        this.walCapacityBytes = walCapacityBytes;
        this.walSegmentBytes = walSegmentBytes;
        this.topics = topics;
        this.topicPattern = topicPattern;
    }

    public static SharedStorageConfiguration from(StorageExtensionContext context) {
        Objects.requireNonNull(context, "context");
        Object configuredWalDir = context.originals().get(WAL_DIR_CONFIG);
        Path walDir = configuredWalDir == null || configuredWalDir.toString().isBlank()
            ? context.liveLogDirs().get(0).toPath()
                .resolve(".shared-storage")
                .resolve("broker-" + context.brokerId())
                .resolve("wal")
                .toAbsolutePath()
                .normalize()
            : Path.of(configuredWalDir.toString().trim()).toAbsolutePath().normalize();

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

        Set<String> topics = parseTopics(context.originals().get(TOPICS_CONFIG));
        Pattern topicPattern = parsePattern(context.originals().get(TOPIC_PATTERN_CONFIG));
        return new SharedStorageConfiguration(walDir, walCapacityBytes, walSegmentBytes, topics, topicPattern);
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
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        long parsed = value instanceof Number number
            ? number.longValue()
            : Long.parseLong(value.toString().trim());
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
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
