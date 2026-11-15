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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-local admission and drain fence for physical object uploads.
 *
 * <p>Independent physical object IDs never contend on one global monitor.
 * Writers atomically update only their own identity with ConcurrentHashMap.compute,
 * while read-only observations are lock-free and see immutable state snapshots.
 * compute holds a short, key-local map-bin lock; no I/O or callback runs inside it.
 * A fenced identity stays present forever, preventing remove/reinsert ABA.
 * A lease is identity-scoped and cannot release a newer upload.</p>
 *
 * <p>This is not distributed arbitration or Controller physical-delete
 * authority. Kafka lifecycle fencing is still required between brokers.</p>
 */
public final class ActiveObjectUploads {
    private final ConcurrentHashMap<Long, State> objects = new ConcurrentHashMap<>();

    /** Legacy admission API; new uploaders should retain the lease instead. */
    public void begin(long objectId) {
        requireObjectId(objectId);
        objects.compute(objectId, (id, current) -> {
            requireIdleUnfenced(id, current);
            return new State(true, null, false);
        });
    }

    /** Admit one upload using an identity-scoped idempotent completion token. */
    public UploadLease beginLease(long objectId) {
        requireObjectId(objectId);
        UploadLease lease = new UploadLease(this, objectId);
        objects.compute(objectId, (id, current) -> {
            requireIdleUnfenced(id, current);
            return new State(false, lease, false);
        });
        return lease;
    }

    /** The legacy ID-only completion must never release an active lease. */
    public void end(long objectId) {
        objects.computeIfPresent(objectId, (id, current) -> {
            if (current.lease() != null) {
                throw new IllegalStateException("A leased object upload cannot be ended by ID: " + objectId);
            }
            return current.fenced() ? new State(false, null, true) : null;
        });
    }

    /** Irreversibly prevent new local uploads, without waiting for active I/O. */
    public void fence(long objectId) {
        requireObjectId(objectId);
        objects.compute(objectId, (id, current) ->
            new State(current != null && current.legacyActive(),
                current == null ? null : current.lease(), true)
        );
    }

    /** Read-only local diagnostic; not a distributed GC grant. */
    public boolean locallyDrainedAfterFence(long objectId) {
        requireObjectId(objectId);
        State state = objects.get(objectId);
        return state != null && state.fenced() && !state.legacyActive() && state.lease() == null;
    }

    public boolean contains(long objectId) {
        State state = objects.get(objectId);
        return state != null && (state.legacyActive() || state.lease() != null);
    }

    private static void requireIdleUnfenced(long objectId, State current) {
        if (current == null) {
            return;
        }
        if (current.fenced()) {
            throw new IllegalStateException("Object upload is locally fenced: " + objectId);
        }
        if (current.legacyActive() || current.lease() != null) {
            throw new IllegalStateException("Object upload is already active: " + objectId);
        }
    }

    private void release(UploadLease lease) {
        objects.compute(lease.objectId, (id, current) -> {
            if (current == null || current.lease() != lease) {
                throw new IllegalStateException("Upload lease does not own object: " + lease.objectId);
            }
            return current.fenced() ? new State(false, null, true) : null;
        });
    }

    private static void requireObjectId(long objectId) {
        if (objectId <= 0L) {
            throw new IllegalArgumentException("Physical object ID must be positive");
        }
    }

    private record State(boolean legacyActive, UploadLease lease, boolean fenced) {
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
