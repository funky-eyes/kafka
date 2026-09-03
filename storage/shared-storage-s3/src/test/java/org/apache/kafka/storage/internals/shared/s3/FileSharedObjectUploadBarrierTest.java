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

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.Phase;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.UploadContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSharedObjectUploadBarrierTest {
    @TempDir
    Path tempDir;

    @Test
    void shouldRemainDisabledWithoutExplicitPhase() {
        assertSame(SharedObjectUploadHook.NOOP, FileSharedObjectUploadBarrier.from(Map.of(), 1));
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
            2
        );
        SharedObjectMetadata metadata = metadata(200);
        UploadContext context = new UploadContext(200, 2_000, metadata.objectSize(), metadata);

        assertTrue(barrier.onPhase(Phase.AFTER_PREPARE, context).isDone());
        assertTrue(barrier.onPhase(Phase.AFTER_PUT, context).isDone());

        Files.writeString(barrier.armFile(), "armed\n");
        CompletableFuture<Void> paused = barrier.onPhase(Phase.AFTER_PUT, context);

        assertFalse(paused.isDone());
        assertTrue(Files.isRegularFile(barrier.reachedFile()));
        String evidence = Files.readString(barrier.reachedFile());
        assertTrue(evidence.contains("phase=AFTER_PUT"));
        assertTrue(evidence.contains("brokerId=2"));
        assertTrue(evidence.contains("objectId=200"));
        assertTrue(evidence.contains("createdTimeMs=2000"));
        assertTrue(evidence.contains("objectSize=1"));

        paused.complete(null);
        assertTrue(barrier.onPhase(Phase.AFTER_PUT, context).isDone());
    }

    @Test
    void shouldWritePlannedObjectSizeBeforeMetadataExists() throws Exception {
        FileSharedObjectUploadBarrier barrier = new FileSharedObjectUploadBarrier(
            Phase.AFTER_PREPARE,
            tempDir,
            3
        );
        Files.writeString(barrier.armFile(), "armed\n");

        CompletableFuture<Void> paused = barrier.onPhase(
            Phase.AFTER_PREPARE,
            UploadContext.planned(201, 2_001, 4_096)
        );

        assertFalse(paused.isDone());
        assertTrue(Files.readString(barrier.reachedFile()).contains("objectSize=4096"));
        paused.complete(null);
    }

    private static SharedObjectMetadata metadata(long objectId) {
        SharedPartitionId partition = new SharedPartitionId(1, 2, 0);
        SharedObjectRange range = new SharedObjectRange(
            partition,
            new OffsetRange(0, 1),
            3,
            0,
            1,
            17
        );
        return new SharedObjectMetadata(objectId, 1, 17, List.of(range));
    }
}
