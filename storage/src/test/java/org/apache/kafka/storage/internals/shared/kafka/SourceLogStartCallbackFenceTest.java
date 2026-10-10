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

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SourceLogStartCallbackFenceTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(10L, 11L, 0);

    @Test
    void nativeDeleteRecordsSuccessAdvancesTheTrackedSourceStart() {
        SharedCommitProgress progress = loaded(10L);
        SourceLogStartCallbackFence fence = new SourceLogStartCallbackFence(progress, PARTITION);
        long generation = fence.captureGeneration();

        fence.observeAdvance(generation, 30L);

        assertEquals(30L, logStart(progress));
    }

    @Test
    void callbackCapturedBeforeTruncationCannotRestoreDeletedPrefix() {
        SharedCommitProgress progress = loaded(10L);
        SourceLogStartCallbackFence fence = new SourceLogStartCallbackFence(progress, PARTITION);
        long staleGeneration = fence.captureGeneration();
        fence.invalidateForRebase();
        fence.observeRebase(5L);

        fence.observeAdvance(staleGeneration, 50L);

        assertEquals(5L, logStart(progress));
    }

    @Test
    void callbackCapturedDuringTruncationCannotOverrideFinalRebase() {
        SharedCommitProgress progress = loaded(10L);
        SourceLogStartCallbackFence fence = new SourceLogStartCallbackFence(progress, PARTITION);
        fence.invalidateForRebase();
        long interimGeneration = fence.captureGeneration();
        fence.observeRebase(4L);

        fence.observeAdvance(interimGeneration, 90L);

        assertEquals(4L, logStart(progress));
    }

    @Test
    void freshCallbackAfterTruncationUsesCurrentGeneration() {
        SharedCommitProgress progress = loaded(10L);
        SourceLogStartCallbackFence fence = new SourceLogStartCallbackFence(progress, PARTITION);
        fence.invalidateForRebase();
        fence.observeRebase(4L);

        fence.observeAdvance(fence.captureGeneration(), 17L);

        assertEquals(17L, logStart(progress));
    }

    @Test
    void nestedTruncationsCannotPublishEitherPriorCallback() {
        SharedCommitProgress progress = loaded(10L);
        SourceLogStartCallbackFence fence = new SourceLogStartCallbackFence(progress, PARTITION);
        long beforeOuter = fence.captureGeneration();
        fence.invalidateForRebase();
        long beforeNested = fence.captureGeneration();
        fence.invalidateForRebase();
        fence.observeRebase(3L);
        fence.observeRebase(3L);

        fence.observeAdvance(beforeOuter, 80L);
        fence.observeAdvance(beforeNested, 60L);

        assertEquals(3L, logStart(progress));
    }

    @Test
    void callbacksCannotRecreateAReplicaRemovedDuringTruncation() {
        SharedCommitProgress progress = loaded(10L);
        SourceLogStartCallbackFence fence = new SourceLogStartCallbackFence(progress, PARTITION);
        long generation = fence.captureGeneration();
        progress.remove(PARTITION);

        fence.invalidateForRebase();
        fence.observeRebase(2L);
        fence.observeAdvance(generation, 50L);
        fence.observeAdvance(fence.captureGeneration(), 60L);

        assertFalse(progress.partitionProgress(PARTITION).isPresent());
    }

    private static SharedCommitProgress loaded(long initialLogStart) {
        SharedCommitProgress progress = new SharedCommitProgress();
        progress.onLogLoaded(PARTITION, initialLogStart);
        return progress;
    }

    private static long logStart(SharedCommitProgress progress) {
        return progress.partitionProgress(PARTITION).orElseThrow().logStartOffset();
    }
}
