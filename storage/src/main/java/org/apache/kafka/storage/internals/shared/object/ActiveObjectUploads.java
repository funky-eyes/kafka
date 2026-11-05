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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-local admission and drain fence for physical object uploads.
 *
 * <p>A lease remains active across PREPARE, PUT, COMMIT, and asynchronous
 * post-COMMIT publication. Fencing an object is irreversible in this registry:
 * new uploads fail, and an existing upload must finish before the local fence
 * can report drained. Stale or cancelled callers cannot release a later upload
 * of the same object ID through an earlier lease.</p>
 *
 * <p>This is neither a cross-broker upload certificate nor KRaft authority.
 * It cannot authorize COMMITTED physical object deletion. Legacy begin/end
 * callers retain their original API but cannot release leased uploads.</p>
 */
public final class ActiveObjectUploads {
    private final Map<Long, State> objects = new HashMap<>();

    /** Legacy admission API; new uploaders should retain the returned lease instead. */
    public synchronized void begin(long objectId) {
        State state = stateForAdmission(objectId);
        state.legacyActive = true;
    }

    /** Admit one upload, returning an identity-scoped, idempotent completion token. */
    public synchronized UploadLease beginLease(long objectId) {
        State state = stateForAdmission(objectId);
        UploadLease lease = new UploadLease(this, objectId);
        state.lease = lease;
        return lease;
    }

    /** Only releases an upload admitted through the legacy begin API. */
    public synchronized void end(long objectId) {
        State state = objects.get(objectId);
        if (state == null) {
            return;
        }
        if (state.lease != null) {
            throw new IllegalStateException("A leased object upload cannot be ended by ID: " + objectId);
        }
        state.legacyActive = false;
        removeIdleUnfenced(objectId, state);
    }

    /** Prevent any subsequent PUT admission for this object in this process. */
    public synchronized void fence(long objectId) {
        requireObjectId(objectId);
        objects.computeIfAbsent(objectId, ignored -> new State()).fenced = true;
    }

    /** Local-only observation: it is not a distributed GC grant. */
    public synchronized boolean locallyDrainedAfterFence(long objectId) {
        requireObjectId(objectId);
        State state = objects.get(objectId);
        return state != null && state.fenced && !state.legacyActive && state.lease == null;
    }

    public synchronized boolean contains(long objectId) {
        State state = objects.get(objectId);
        return state != null && (state.legacyActive || state.lease != null);
    }

    private State stateForAdmission(long objectId) {
        requireObjectId(objectId);
        State state = objects.computeIfAbsent(objectId, ignored -> new State());
        if (state.fenced) {
            throw new IllegalStateException("Object upload is locally fenced: " + objectId);
        }
        if (state.legacyActive || state.lease != null) {
            throw new IllegalStateException("Object upload is already active: " + objectId);
        }
        return state;
    }

    private synchronized void release(UploadLease lease) {
        State state = objects.get(lease.objectId);
        if (state == null || state.lease != lease) {
            throw new IllegalStateException("Upload lease does not own object: " + lease.objectId);
        }
        state.lease = null;
        removeIdleUnfenced(lease.objectId, state);
    }

    private void removeIdleUnfenced(long objectId, State state) {
        if (!state.fenced && !state.legacyActive && state.lease == null) {
            objects.remove(objectId);
        }
    }

    private static void requireObjectId(long objectId) {
        if (objectId <= 0L) {
            throw new IllegalArgumentException("Physical object ID must be positive");
        }
    }

    private static final class State {
        private boolean legacyActive;
        private UploadLease lease;
        private boolean fenced;
    }

    public static final class UploadLease implements AutoCloseable {
        private final ActiveObjectUploads owner;
        private final long objectId;
        private final AtomicBoolean closed = new AtomicBoolean();

        private UploadLease(ActiveObjectUploads owner, long objectId) {
            this.owner = owner;
            this.objectId = objectId;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release(this);
            }
        }
    }
}
