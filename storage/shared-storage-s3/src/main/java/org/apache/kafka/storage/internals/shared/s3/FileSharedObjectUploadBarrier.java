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
package org.apache.kafka.storage.internals.shared.s3;

import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.Phase;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.UploadContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/** File-backed, one-shot upload barrier used only by external-process correctness gates. */
final class FileSharedObjectUploadBarrier implements SharedObjectUploadHook {
    static final String PAUSE_AFTER_CONFIG = "shared.storage.test.upload.pause.after";
    static final String BARRIER_DIR_CONFIG = "shared.storage.test.upload.barrier.dir";

    private final Phase targetPhase;
    private final Path barrierDir;
    private final int brokerId;
    private final AtomicBoolean tripped = new AtomicBoolean();
    private final CompletableFuture<Void> pause = new CompletableFuture<>();

    static SharedObjectUploadHook from(Map<String, ?> originals, int brokerId) {
        Objects.requireNonNull(originals, "originals");
        Object configuredPhase = originals.get(PAUSE_AFTER_CONFIG);
        if (configuredPhase == null || configuredPhase.toString().isBlank()) {
            return SharedObjectUploadHook.NOOP;
        }

        final Phase phase;
        try {
            phase = Phase.valueOf(configuredPhase.toString().trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Invalid " + PAUSE_AFTER_CONFIG + ": " + configuredPhase,
                e
            );
        }

        Object configuredDir = originals.get(BARRIER_DIR_CONFIG);
        if (configuredDir == null || configuredDir.toString().isBlank()) {
            throw new IllegalArgumentException(
                BARRIER_DIR_CONFIG + " must be configured when " + PAUSE_AFTER_CONFIG + " is enabled");
        }
        return new FileSharedObjectUploadBarrier(
            phase,
            Path.of(configuredDir.toString().trim()).toAbsolutePath().normalize(),
            brokerId
        );
    }

    FileSharedObjectUploadBarrier(Phase targetPhase, Path barrierDir, int brokerId) {
        this.targetPhase = Objects.requireNonNull(targetPhase, "targetPhase");
        this.barrierDir = Objects.requireNonNull(barrierDir, "barrierDir").toAbsolutePath().normalize();
        if (brokerId < 0) {
            throw new IllegalArgumentException("brokerId must be non-negative");
        }
        this.brokerId = brokerId;
    }

    @Override
    public CompletableFuture<Void> onPhase(Phase phase, UploadContext context) {
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(context, "context");
        if (phase != targetPhase || tripped.get() || !Files.exists(armFile())) {
            return CompletableFuture.completedFuture(null);
        }
        if (!tripped.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            writeReachedMarker(context);
        } catch (IOException e) {
            pause.completeExceptionally(e);
        }
        return pause;
    }

    Path armFile() {
        return barrierDir.resolve("broker-" + brokerId + ".arm");
    }

    Path reachedFile() {
        return barrierDir.resolve("broker-" + brokerId + "." + targetPhase + ".reached");
    }

    private void writeReachedMarker(UploadContext context) throws IOException {
        Files.createDirectories(barrierDir);
        Path marker = reachedFile();
        Path temporary = barrierDir.resolve(
            "." + marker.getFileName() + "." + ProcessHandle.current().pid() + ".tmp");
        String value = String.join("\n",
            "phase=" + targetPhase,
            "brokerId=" + brokerId,
            "objectId=" + context.objectId(),
            "createdTimeMs=" + context.createdTimeMs(),
            "objectSize=" + context.objectSize(),
            ""
        );
        try {
            Files.writeString(
                temporary,
                value,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            );
            try {
                Files.move(
                    temporary,
                    marker,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
                );
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, marker, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
    }
}
