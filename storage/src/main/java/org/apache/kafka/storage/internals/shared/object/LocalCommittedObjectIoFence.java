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
package org.apache.kafka.storage.internals.shared.object;

import java.util.Objects;

/**
 * A read-only, process-local combination of the existing reader and writer
 * admission/drain fences for one physical Object ID.
 *
 * <p>Both registries MUST be the actual instances shared by every local
 * reader and uploader being evaluated; the default constructors do not
 * guarantee such sharing. This helper never creates or authenticates a
 * cross-broker certificate, updates KRaft state, or deletes an object.</p>
 *
 * <p>Fencing is irreversible in both registries. The join is not sampled
 * until both admission gates have been fenced, so a concurrent admission
 * between the two fence calls is still counted until it actually drains.</p>
 */
final class LocalCommittedObjectIoFence {
    private final ActiveCommittedObjectReads reads;
    private final ActiveObjectUploads uploads;

    LocalCommittedObjectIoFence(
        ActiveCommittedObjectReads reads,
        ActiveObjectUploads uploads
    ) {
        this.reads = Objects.requireNonNull(reads, "reads");
        this.uploads = Objects.requireNonNull(uploads, "uploads");
    }

    synchronized void fence(long objectId) {
        reads.fence(objectId);
        uploads.fence(objectId);
    }

    /** An advisory local condition, never a physical COMMITTED DELETE grant. */
    synchronized boolean locallyDrainedAfterFence(long objectId) {
        return reads.locallyDrainedAfterFence(objectId)
            && uploads.locallyDrainedAfterFence(objectId);
    }
}
