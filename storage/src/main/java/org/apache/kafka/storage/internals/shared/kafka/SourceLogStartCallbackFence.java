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

import java.util.Objects;

/**
 * Non-blocking, process-local generation fence for the Kafka native log-start
 * callback bridge. It must never acquire Kafka's log monitor or the remote
 * recovery fence: native DeleteRecords/retention may already hold the former
 * while metadata replay holds the latter and waits for it.
 *
 * <p>A generation is invalidated at both the start and the completion
 * of any native truncation. Notifications captured before or during the
 * truncation cannot overwrite its final lower log start, even if those
 * notifications run after the truncation completes. The tiny monitor only
 * encloses in-memory state and ConcurrentHashMap updates, never I/O.
 * This class does NOT authenticate LogStart for distributed KRaft writes.</p>
 */
final class SourceLogStartCallbackFence {
    private final SharedCommitProgress progress;
    private final SharedPartitionId partition;
    private long generation;

    SourceLogStartCallbackFence(SharedCommitProgress progress, SharedPartitionId partition) {
        this.progress = Objects.requireNonNull(progress, "progress");
        this.partition = Objects.requireNonNull(partition, "partition");
    }

    synchronized long captureGeneration() {
        return generation;
    }

    synchronized void invalidateForRebase() {
        generation = Math.addExact(generation, 1L);
    }

    synchronized void observeAdvance(long capturedGeneration, long logStartOffset) {
        if (generation == capturedGeneration) {
            progress.onLogStartOffsetAdvanced(partition, logStartOffset);
        }
    }

    synchronized void observeRebase(long logStartOffset) {
        generation = Math.addExact(generation, 1L);
        progress.onLogRebased(partition, logStartOffset);
    }
}
