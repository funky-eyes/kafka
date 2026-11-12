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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Process-local, non-authorizing physical Range GET admission and drain fence.
 *
 * <p>One KSO2 indexed read can issue multiple overlapping header/footer/data
 * requests. A fast failing header may trigger a second COMMITTED physical copy
 * while the original footer request remains in flight. Counting the enclosing
 * reader Future would incorrectly report the first physical object quiescent.
 * Count the actual ObjectStore Range GET Futures and logical reads
 * (including hot cache hits) instead.</p>
 *
 * <p>Fencing an object is irreversible for this registry instance. A drained
 * local fence is NOT a Controller grant, proof of other brokers' readers,
 * durable retirement record, or permission to issue S3 DELETE.</p>
 */
final class ActiveCommittedObjectReads {
    private final Map<Long, State> objects = new HashMap<>();

    synchronized Optional<ReadLease> tryAcquire(long objectId) {
        requireObjectId(objectId);
        State state = objects.computeIfAbsent(objectId, ignored -> new State());
        if (state.fenced) {
            return Optional.empty();
        }
        state.inFlight = Math.addExact(state.inFlight, 1L);
        return Optional.of(new ReadLease(this, objectId, false));
    }

    /**
     * Admit a logical batch read before consulting either index or DataBlock
     * cache. Otherwise a warm cache can bypass a physical Range GET fence.
     */
    synchronized Optional<ReadLease> tryAcquireLogicalRead(long objectId) {
        requireObjectId(objectId);
        State state = objects.computeIfAbsent(objectId, ignored -> new State());
        if (state.fenced) {
            return Optional.empty();
        }
        state.logicalReads = Math.addExact(state.logicalReads, 1L);
        return Optional.of(new ReadLease(this, objectId, true));
    }

    synchronized void fence(long objectId) {
        requireObjectId(objectId);
        objects.computeIfAbsent(objectId, ignored -> new State()).fenced = true;
    }

    synchronized boolean locallyDrainedAfterFence(long objectId) {
        requireObjectId(objectId);
        State state = objects.get(objectId);
        return state != null && state.fenced && state.inFlight == 0L && state.logicalReads == 0L;
    }

    synchronized long inFlightRangeGets(long objectId) {
        requireObjectId(objectId);
        State state = objects.get(objectId);
        return state == null ? 0L : state.inFlight;
    }

    synchronized long inFlightLogicalReads(long objectId) {
        requireObjectId(objectId);
        State state = objects.get(objectId);
        return state == null ? 0L : state.logicalReads;
    }

    /**
     * Only the Reader's Range GET path is wrapped; this object is not used for
     * uploads, cleanup or remote physical retirement. Do not close the
     * delegate when the reader's tracking view is discarded.
     */
    ObjectStore trackingReaderStore(ObjectStore delegate) {
        Objects.requireNonNull(delegate, "delegate");
        return new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return rejectReaderUpload();
            }

            @Override
            public CompletableFuture<Void> put(long objectId, List<ByteBuffer> parts) {
                return rejectReaderUpload();
            }

            @Override
            public CompletableFuture<Void> put(long objectId, ObjectStore.PartSource source) {
                return rejectReaderUpload();
            }

            @Override
            public CompletableFuture<Void> put(long objectId, long objectSize, ObjectStore.PartSource source) {
                return rejectReaderUpload();
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                Optional<ReadLease> acquired = tryAcquire(objectId);
                if (acquired.isEmpty()) {
                    return CompletableFuture.failedFuture(
                        new IllegalStateException("COMMITTED physical object read is locally fenced: " + objectId)
                    );
                }
                ReadLease lease = acquired.get();
                CompletableFuture<ByteBuffer> transport;
                try {
                    transport = Objects.requireNonNull(
                        delegate.rangeRead(objectId, position, length),
                        "ObjectStore returned null Range GET future"
                    );
                } catch (RuntimeException | Error failure) {
                    // Preserve the ObjectStore's synchronous failure contract.
                    // The index/cache loader converts it to a failed Future
                    // for COMMITTED fallback, without issuing a needless
                    // concurrent KSO2 Footer GET after a failed Header.
                    lease.close();
                    throw failure;
                }
                // Release only when the underlying physical GET settles, NOT
                // when a caller cancels a dependent reader Future or another
                // parallel KSO2 metadata GET fails fast.
                transport.whenComplete((ignored, failure) -> lease.close());
                // A consumer cancellation must not cancel the transport
                // Future and falsely declare the physical object drained.
                return transport.thenApply(bytes -> bytes);
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(
                    new UnsupportedOperationException("Remote read-only view cannot physically delete an object")
                );
            }
        };
    }

    /**
     * Reject all upload overloads before default ObjectStore adapters can
     * drain a PartSource or materialize an entire multipart object in heap.
     */
    private static CompletableFuture<Void> rejectReaderUpload() {
        return CompletableFuture.failedFuture(
            new UnsupportedOperationException("Remote read-only view cannot publish an object")
        );
    }

    private synchronized void release(long objectId, boolean logical) {
        State state = objects.get(objectId);
        if (state == null) {
            throw new IllegalStateException("Read lease released without admission: " + objectId);
        }
        if (logical) {
            if (state.logicalReads <= 0L) {
                throw new IllegalStateException("Logical read lease released without admission: " + objectId);
            }
            state.logicalReads--;
        } else {
            if (state.inFlight <= 0L) {
                throw new IllegalStateException("Physical read lease released without admission: " + objectId);
            }
            state.inFlight--;
        }
        if (!state.fenced && state.inFlight == 0L && state.logicalReads == 0L) {
            objects.remove(objectId);
        }
    }

    private static void requireObjectId(long objectId) {
        if (objectId <= 0L) {
            throw new IllegalArgumentException("Physical object ID must be positive");
        }
    }

    private static final class State {
        private long inFlight;
        private long logicalReads;
        private boolean fenced;
    }

    static final class ReadLease implements AutoCloseable {
        private final ActiveCommittedObjectReads owner;
        private final long objectId;
        private final boolean logical;
        private final AtomicBoolean released = new AtomicBoolean();

        private ReadLease(ActiveCommittedObjectReads owner, long objectId, boolean logical) {
            this.owner = owner;
            this.objectId = objectId;
            this.logical = logical;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                owner.release(objectId, logical);
            }
        }
    }
}
