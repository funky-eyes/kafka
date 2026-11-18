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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.checkpoint.PartitionMetadataFile;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/** Durable local guard against reopening shared placeholders as ordinary Kafka payload files. */
public final class SharedStorageModeMarker {
    public static final String FILE_NAME = "shared-storage.mode";
    public static final String WAL_FORMAT_FILE_NAME = "topic-config.version";
    private static final String WAL_FORMAT = "1\ntopic\n";
    private static final String LEGACY_WAL_FORMAT = "1\nlegacy\n";

    private SharedStorageModeMarker() {
    }

    /** Runs before opening a WAL, so an old unmarked WAL cannot silently acquire the new default routing. */
    public static void initializeWalDirectory(SharedStorageConfiguration configuration) throws IOException {
        Path dir = configuration.walDir();
        Path marker = dir.resolve(WAL_FORMAT_FILE_NAME);
        Files.createDirectories(dir);
        if (Files.exists(marker)) {
            if (Files.size(marker) > 32) {
                throw new IOException("Invalid shared WAL topic configuration marker: " + marker);
            }
            String format = Files.readString(marker);
            if (!format.equals(WAL_FORMAT) && !format.equals(LEGACY_WAL_FORMAT)) {
                throw new IOException("Invalid shared WAL topic configuration marker: " + marker);
            }
            if (format.equals(LEGACY_WAL_FORMAT) && !configuration.legacySelectorsEnabled()) {
                throw new IOException("Legacy WAL routing cannot be changed in place; copy to a new Topic-config cluster: " + dir);
            }
            if (configuration.legacySelectorsEnabled() && !format.equals(LEGACY_WAL_FORMAT)) {
                throw new IOException("Topic-config WAL cannot be reopened with legacy selectors: " + dir);
            }
            return;
        }
        if (!configuration.legacySelectorsEnabled() && hasEntriesExcept(dir, WAL_FORMAT_FILE_NAME + ".tmp")) {
            throw new IOException("Existing unmarked shared WAL requires " +
                SharedStorageConfiguration.LEGACY_SELECTORS_ENABLED_CONFIG + "=true with unchanged legacy selectors: " + dir);
        }
        writeAtomically(marker, configuration.legacySelectorsEnabled() ? LEGACY_WAL_FORMAT : WAL_FORMAT);
    }

    /** Read the durable mode identity before startup applies a current, name-based Topic configuration. */
    public static Optional<Uuid> persistedTopicId(File dir, LogDirFailureChannel failureChannel) throws IOException {
        Optional<Marker> marker = read(dir.toPath());
        if (marker.isEmpty()) {
            return Optional.empty();
        }
        PartitionMetadataFile metadata = new PartitionMetadataFile(PartitionMetadataFile.newFile(dir), failureChannel);
        if (metadata.exists()) {
            validateIdentity(marker.get(), Optional.of(metadata.read().topicId()), dir.toPath());
        }
        return Optional.of(marker.get().topicId());
    }

    /** Called by Kafka's native factory, including when no storage extension is installed. */
    public static Optional<Uuid> validateClassic(UnifiedLogCreationContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        if (context.config().sharedStorageEnable()) {
            throw new IOException("Topic shared storage is enabled but no capable shared storage factory is installed");
        }
        Optional<Marker> marker = read(context.dir().toPath());
        if (marker.isEmpty()) {
            return context.topicId();
        }
        validateIdentity(marker.get(), effectiveTopicId(context), context.dir().toPath());
        if (marker.get().shared()) {
            throw new IOException("Refusing to open shared storage as classic Kafka: " + context.dir());
        }
        return Optional.of(marker.get().topicId());
    }

    /** Pin an explicit classic Topic before creating its files, including under a legacy default-all selector. */
    static void prepareClassic(UnifiedLogCreationContext context, boolean legacySelected) throws IOException {
        validateClassic(context);
        Path dir = context.dir().toPath();
        if (read(dir).isPresent()) {
            return;
        }
        if (legacySelected && hasEntriesExcept(dir, FILE_NAME + ".tmp")) {
            throw new IOException("Cannot reinterpret an existing unmarked legacy shared log as classic Kafka: " + dir);
        }
        Optional<Uuid> topicId = effectiveTopicId(context);
        if (topicId.isPresent()) {
            Files.createDirectories(dir);
            write(dir, new Marker(false, topicId.get()));
        }
    }

    /** Publish the storage mode and Topic ID before partition metadata, segments, or WAL payloads can be written. */
    static Optional<Uuid> prepareShared(UnifiedLogCreationContext context, boolean legacyAdoption) throws IOException {
        Path dir = context.dir().toPath();
        Optional<Marker> marker = read(dir);
        Optional<Uuid> topicId = effectiveTopicId(context);
        if (marker.isPresent()) {
            validateIdentity(marker.get(), topicId, dir);
            if (!marker.get().shared()) {
                throw new IOException("Cannot change an existing classic Kafka log to shared storage: " + dir);
            }
            return Optional.of(marker.get().topicId());
        }
        if (!legacyAdoption && hasEntriesExcept(dir, FILE_NAME + ".tmp",
            PartitionMetadataFile.newFile(context.dir()).getName() + ".tmp")) {
            throw new IOException("Existing unmarked log requires explicit legacy migration before shared storage: " + dir);
        }
        // Legacy logical .log files are empty placeholders. Non-empty native payloads are never migration input.
        if (legacyAdoption && Files.isDirectory(dir)) {
            try (Stream<Path> entries = Files.list(dir)) {
                for (Path entry : entries.toList()) {
                    Path fileName = entry.getFileName();
                    if (fileName == null) {
                        throw new IOException("Invalid entry in shared log directory: " + entry);
                    }
                    if (fileName.toString().endsWith(".log") && Files.size(entry) != 0) {
                        throw new IOException("Cannot adopt native Kafka payloads as shared storage: " + entry);
                    }
                }
            }
        }
        if (topicId.isPresent()) {
            Files.createDirectories(dir);
            write(dir, new Marker(true, topicId.get()));
        }
        // The caller removes an unbound directory only when it is empty or contains these known temporary files.
        return topicId;
    }

    private static Optional<Uuid> effectiveTopicId(UnifiedLogCreationContext context) {
        Optional<Uuid> requested = context.topicId();
        PartitionMetadataFile metadata = new PartitionMetadataFile(
            PartitionMetadataFile.newFile(context.dir()), context.logDirFailureChannel());
        if (metadata.exists()) {
            Uuid persisted = metadata.read().topicId();
            if (requested.isPresent() && !requested.get().equals(persisted)) {
                throw new InconsistentTopicIdException("Topic ID differs from partition.metadata in " + context.dir());
            }
            return Optional.of(persisted);
        }
        return requested;
    }

    private static void validateIdentity(Marker marker, Optional<Uuid> expected, Path dir) {
        if (expected.isPresent() && !expected.get().equals(marker.topicId())) {
            throw new InconsistentTopicIdException("Topic ID differs from shared storage mode marker in " + dir);
        }
    }

    private static boolean hasEntriesExcept(Path dir, String... ignored) throws IOException {
        if (!Files.exists(dir)) {
            return false;
        }
        Set<String> ignoredNames = Set.of(ignored);
        try (Stream<Path> entries = Files.list(dir)) {
            return entries.anyMatch(entry -> {
                Path fileName = entry.getFileName();
                return fileName == null || !ignoredNames.contains(fileName.toString());
            });
        }
    }

    private static Optional<Marker> read(Path dir) throws IOException {
        Path file = dir.resolve(FILE_NAME);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        if (Files.size(file) > 256) {
            throw new IOException("Invalid shared storage mode marker: " + file);
        }
        String[] lines = Files.readString(file, StandardCharsets.UTF_8).split("\n", -1);
        if (lines.length != 4 || !lines[0].equals("version=1") || !lines[3].isEmpty() ||
            (!lines[1].equals("mode=shared") && !lines[1].equals("mode=classic")) ||
            !lines[2].startsWith("topic.id=")) {
            throw new IOException("Invalid shared storage mode marker: " + file);
        }
        try {
            String encodedId = lines[2].substring("topic.id=".length());
            Uuid topicId = Uuid.fromString(encodedId);
            if (Uuid.ZERO_UUID.equals(topicId) || !topicId.toString().equals(encodedId)) {
                throw new IllegalArgumentException("Invalid Topic ID");
            }
            return Optional.of(new Marker(lines[1].equals("mode=shared"), topicId));
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid Topic ID in shared storage mode marker: " + file, e);
        }
    }

    private static void write(Path dir, Marker marker) throws IOException {
        if (Uuid.ZERO_UUID.equals(marker.topicId())) {
            throw new IOException("Shared storage mode requires a nonzero Topic ID");
        }
        writeAtomically(dir.resolve(FILE_NAME), "version=1\nmode=" + (marker.shared() ? "shared" : "classic") +
            "\ntopic.id=" + marker.topicId() + "\n");
    }

    private static void writeAtomically(Path target, String content) throws IOException {
        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileOutputStream output = new FileOutputStream(temporary.toFile())) {
            output.write(content.getBytes(StandardCharsets.UTF_8));
            output.getFD().sync();
        }
        Utils.atomicMoveWithFallback(temporary, target);
    }

    private record Marker(boolean shared, Uuid topicId) {
    }
}
