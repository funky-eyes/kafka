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

import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;

import java.util.Objects;

/**
 * Broker-lifetime physical-object I/O admissions shared by the actual S3
 * reader and upload scheduler, including metadata-bootstrap retries.
 *
 * <p>Fencing an object is irreversible in this broker lifetime. A drained
 * local object is NOT an authenticated cross-Broker or KRaft DELETE_READY
 * grant. Controller admission and durable incarnation fences are still
 * required before any production physical GC can be enabled.</p>
 */
public final class SharedObjectIoFences {
    private final ActiveCommittedObjectReads reads = new ActiveCommittedObjectReads();
    private final ActiveObjectUploads uploads = new ActiveObjectUploads();
    private final LocalCommittedObjectIoFence local = new LocalCommittedObjectIoFence(reads, uploads);

    public SharedObjectReader newReader(
        ObjectStore objectStore,
        RemoteObjectIndex remoteIndex,
        int indexCacheEntries,
        long dataBlockCacheBytes
    ) {
        return new SharedObjectReader(
            Objects.requireNonNull(objectStore, "objectStore"),
            Objects.requireNonNull(remoteIndex, "remoteIndex"),
            indexCacheEntries, dataBlockCacheBytes, reads);
    }

    /** The exact upload registry used by the broker's upload scheduler. */
    public ActiveObjectUploads uploads() {
        return uploads;
    }

    /** Reject new reads and uploads for this object and retain active leases. */
    public void fence(long objectId) {
        local.fence(objectId);
    }

    /** Local only: never pass this boolean as a physical deletion permit. */
    public boolean locallyDrainedAfterFence(long objectId) {
        return local.locallyDrainedAfterFence(objectId);
    }
}
