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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSharedObjectUploadBarrierTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldRemainDisabledWithoutExplicitPhase() {
        assertTrue(FileSharedObjectUploadBarrier.from(Map.of(), 1) == SharedObjectUploadHook.NOOP);
    }

    @Test
    void shouldRejectIncompleteOrInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () ->
            FileSharedObjectUploadBarrier.from(
                Map.of(FileSharedObjectUploadBarrier.PAUSE_AFTER_CONFIG, "AFTER_PUT"),
                1
            ));
        assertThrows(IllegalArgumentException.class, () ->
            FileSharedObjectUploadBarrier.from(
                Map.of(
                    FileSharedObjectUploadBarrier.PAUSE_AFTER_CONFIG, "not-a-phase",
                    FileSharedObjectUploadBarrier.BARRIER_DIR_CONFIG, tempDir.toString()
                ),
                1
            ));
    }

    @Test
    void shouldPauseArmedTargetPhaseExactlyOnceAndWriteAtomicEvidence() throws Exception {
        FileSharedObjectUploadBarrier barrier = new FileSharedObjectUploadBarrier(
            Phase.AFTER_PUT,
            tempDir,
            7
        );
        UploadContext context = UploadContext.planned(1234L, 5678L, 999L);

        assertTrue(barrier.onPhase(Phase.AFTER_PREPARE, context).isDone());
        assertTrue(barrier.onPhase(Phase.AFTER_PUT, context).isDone());

        Files.writeString(barrier.armFile(), "armed\n");
        CompletableFuture<Void> pause = barrier.onPhase(Phase.AFTER_PUT, context);
        assertFalse(pause.isDone());
        assertTrue(Files.isRegularFile(barrier.reachedFile()));
        String marker = Files.readString(barrier.reachedFile());
        assertTrue(marker.contains("phase=AFTER_PUT"));
        assertTrue(marker.contains("brokerId=7"));
        assertTrue(marker.contains("objectId=1234"));
        assertTrue(marker.contains("createdTimeMs=5678"));
        assertTrue(marker.contains("objectSize=999"));
        assertTrue(barrier.onPhase(Phase.AFTER_PUT, context).isDone());

        Files.writeString(barrier.releaseFile(), "release\n");
        pause.get(10, TimeUnit.SECONDS);
        assertTrue(pause.isDone());
    }

    @Test
    void shouldWritePlannedObjectSizeBeforeMetadataExists() throws Exception {
        FileSharedObjectUploadBarrier barrier = new FileSharedObjectUploadBarrier(
            Phase.AFTER_PREPARE,
            tempDir,
            8
        );
        Files.writeString(barrier.armFile(), "armed\n");

        CompletableFuture<Void> pause = barrier.onPhase(
            Phase.AFTER_PREPARE,
            UploadContext.planned(2222L, 3333L, 4444L)
        );
        String marker = Files.readString(barrier.reachedFile());
        assertTrue(marker.contains("objectSize=4444"));

        Files.writeString(barrier.releaseFile(), "release\n");
        pause.get(10, TimeUnit.SECONDS);
    }
}
