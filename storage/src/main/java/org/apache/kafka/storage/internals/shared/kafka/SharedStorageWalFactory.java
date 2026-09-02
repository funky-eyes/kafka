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

import org.apache.kafka.storage.internals.shared.wal.RingSharedWal;
import org.apache.kafka.storage.internals.shared.wal.RotatingFileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/** Creates the configured broker-wide shared WAL and prevents unsafe backend switches. */
public final class SharedStorageWalFactory {
    static final String RING_WAL_FILE = "shared-ring.wal";
    private static final Pattern LEGACY_ROTATING_WAL_SEGMENT = Pattern.compile("wal-\\d{20}\\.log");

    private SharedStorageWalFactory() {
    }

    public static SharedWal create(SharedStorageConfiguration configuration) throws IOException {
        Objects.requireNonNull(configuration, "configuration");
        return switch (configuration.walEngine()) {
            case RING -> {
                ensureNoLegacyRotatingWalSegments(configuration.walDir());
                yield new RingSharedWal(
                    ringWalPath(configuration.walDir()),
                    configuration.walCapacityBytes()
                );
            }
            case ROTATING_FILE -> {
                ensureNoRingWalFile(configuration.walDir());
                yield new RotatingFileSharedWal(
                    configuration.walDir(),
                    configuration.walCapacityBytes(),
                    configuration.walSegmentBytes()
                );
            }
        };
    }

    static Path ringWalPath(Path walDir) {
        return walDir.resolve(RING_WAL_FILE);
    }

    private static void ensureNoLegacyRotatingWalSegments(Path walDir) throws IOException {
        if (!Files.isDirectory(walDir)) {
            return;
        }
        try (Stream<Path> entries = Files.list(walDir)) {
            Path legacySegment = entries
                .filter(Files::isRegularFile)
                .filter(path -> LEGACY_ROTATING_WAL_SEGMENT.matcher(path.getFileName().toString()).matches())
                .findFirst()
                .orElse(null);
            if (legacySegment != null) {
                throw new IOException(
                    "Refusing to start ring WAL while legacy rotating WAL segment exists: " + legacySegment +
                        ". Start with " + SharedStorageConfiguration.WAL_ENGINE_CONFIG +
                        "=rotating-file until the legacy WAL is fully recovered and safely retired before switching to ring."
                );
            }
        }
    }

    private static void ensureNoRingWalFile(Path walDir) throws IOException {
        Path ringWal = ringWalPath(walDir);
        if (Files.exists(ringWal)) {
            throw new IOException(
                "Refusing to start rotating-file WAL while ring WAL file exists: " + ringWal +
                    ". Start with " + SharedStorageConfiguration.WAL_ENGINE_CONFIG +
                    "=ring until the ring WAL is fully recovered and safely retired before switching to rotating-file."
            );
        }
    }
}
