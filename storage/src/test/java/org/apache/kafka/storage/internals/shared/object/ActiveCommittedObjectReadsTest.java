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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActiveCommittedObjectReadsTest {
    @Test
    void localDrainRequiresFenceAndAllPhysicalGetLeases() {
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        assertFalse(pins.locallyDrainedAfterFence(101L),
            "A missing local reader is not an authorization to delete");

        ActiveCommittedObjectReads.ReadLease first = pins.tryAcquire(101L).orElseThrow();
        ActiveCommittedObjectReads.ReadLease second = pins.tryAcquire(101L).orElseThrow();
        assertEquals(2L, pins.inFlightRangeGets(101L));
        pins.fence(101L);
        assertTrue(pins.tryAcquire(101L).isEmpty(), "An irreversible fence rejects new S3 Range GETs");
        assertFalse(pins.locallyDrainedAfterFence(101L));

        first.close();
        first.close();
        assertEquals(1L, pins.inFlightRangeGets(101L));
        assertFalse(pins.locallyDrainedAfterFence(101L));
        second.close();
        assertEquals(0L, pins.inFlightRangeGets(101L));
        assertTrue(pins.locallyDrainedAfterFence(101L));

        pins.fence(101L);
        assertTrue(pins.tryAcquire(101L).isEmpty());
        assertFalse(pins.locallyDrainedAfterFence(102L),
            "The local state of one object cannot authorize another physical object");
        assertThrows(IllegalArgumentException.class, () -> pins.fence(0L));
        assertThrows(IllegalArgumentException.class, () -> pins.tryAcquire(-1L));
    }

    @Test
    void nullTransportFutureReleasesLeaseAndPreservesSynchronousFailure() {
        ObjectStore returnsNull = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return null;
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        ObjectStore tracked = pins.trackingReaderStore(returnsNull);
        NullPointerException failure = assertThrows(
            NullPointerException.class, () -> tracked.rangeRead(104L, 0L, 1)
        );
        assertTrue(failure.getMessage().contains("null Range GET future"));
        assertEquals(0L, pins.inFlightRangeGets(104L));
        assertFalse(pins.locallyDrainedAfterFence(104L));
        pins.fence(104L);
        assertTrue(pins.locallyDrainedAfterFence(104L));
    }

    @Test
    void cachedLogicalReadLeaseMustDrainAlongsidePhysicalRangeGets() {
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        ActiveCommittedObjectReads.ReadLease cached = pins.tryAcquireLogicalRead(105L).orElseThrow();
        ActiveCommittedObjectReads.ReadLease physical = pins.tryAcquire(105L).orElseThrow();
        assertEquals(1L, pins.inFlightLogicalReads(105L));
        assertEquals(1L, pins.inFlightRangeGets(105L));

        pins.fence(105L);
        assertTrue(pins.tryAcquireLogicalRead(105L).isEmpty(),
            "A newly admitted cached read must not bypass the physical object fence");
        assertFalse(pins.locallyDrainedAfterFence(105L));
        physical.close();
        assertEquals(0L, pins.inFlightRangeGets(105L));
        assertEquals(1L, pins.inFlightLogicalReads(105L));
        assertFalse(pins.locallyDrainedAfterFence(105L),
            "Completion of the last GET cannot erase a pending logical read");
        cached.close();
        cached.close();
        assertEquals(0L, pins.inFlightLogicalReads(105L));
        assertTrue(pins.locallyDrainedAfterFence(105L));
    }

    @Test
    void readerTrackingViewCannotPublishOrDeletePhysicalObjects() {
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger deletes = new AtomicInteger();
        ObjectStore backing = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                writes.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.completedFuture(ByteBuffer.allocate(length));
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                deletes.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }
        };
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        ObjectStore reader = pins.trackingReaderStore(backing);
        CompletionException putFailure = assertThrows(
            CompletionException.class, () -> reader.put(106L, ByteBuffer.wrap(new byte[] {1})).join()
        );
        CompletionException deleteFailure = assertThrows(
            CompletionException.class, () -> reader.delete(106L).join()
        );
        assertTrue(putFailure.getCause() instanceof UnsupportedOperationException);
        assertTrue(deleteFailure.getCause() instanceof UnsupportedOperationException);
        assertEquals(0, writes.get());
        assertEquals(0, deletes.get());
        assertEquals(0L, pins.inFlightRangeGets(106L));
    }

    @Test
    void readOnlyViewRejectsMultipartSourcesBeforeAnyMaterializationOrClose() {
        AtomicInteger writes = new AtomicInteger();
        AtomicInteger sourcePulls = new AtomicInteger();
        AtomicInteger sourceCloses = new AtomicInteger();
        ObjectStore delegate = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                writes.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return CompletableFuture.completedFuture(ByteBuffer.allocate(length));
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        ObjectStore.PartSource source = new ObjectStore.PartSource() {
            @Override
            public ByteBuffer nextPart() {
                sourcePulls.incrementAndGet();
                return ByteBuffer.wrap(new byte[] {1});
            }

            @Override
            public void close() {
                sourceCloses.incrementAndGet();
            }
        };
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        ObjectStore reader = pins.trackingReaderStore(delegate);
        List<CompletableFuture<Void>> rejected = List.of(
            reader.put(110L, List.of(ByteBuffer.wrap(new byte[] {1}))),
            reader.put(110L, source),
            reader.put(110L, 1L, source)
        );
        for (CompletableFuture<Void> attempt : rejected) {
            CompletionException failure = assertThrows(CompletionException.class, attempt::join);
            assertTrue(failure.getCause() instanceof UnsupportedOperationException);
        }
        assertEquals(0, sourcePulls.get(),
            "Read-only views must not drain a PartSource or materialize multipart data");
        assertEquals(0, sourceCloses.get(), "Rejected uploads cannot steal source ownership");
        assertEquals(0, writes.get(), "No upload overload may reach the backing ObjectStore");
        assertEquals(0L, pins.inFlightRangeGets(110L));
    }

    @Test
    void twoReadViewsShareAllTransportLeasesUntilTheirUnderlyingGetsComplete() {
        CompletableFuture<ByteBuffer> first = new CompletableFuture<>();
        CompletableFuture<ByteBuffer> second = new CompletableFuture<>();
        ObjectStore backing = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return position == 0L ? first : second;
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        ActiveCommittedObjectReads registry = new ActiveCommittedObjectReads();
        ObjectStore readerA = registry.trackingReaderStore(backing);
        ObjectStore readerB = registry.trackingReaderStore(backing);
        CompletableFuture<ByteBuffer> a = readerA.rangeRead(111L, 0L, 1);
        CompletableFuture<ByteBuffer> b = readerB.rangeRead(111L, 1L, 1);

        registry.fence(111L);
        assertEquals(2L, registry.inFlightRangeGets(111L));
        assertFalse(registry.locallyDrainedAfterFence(111L));
        first.complete(ByteBuffer.wrap(new byte[] {1}));
        assertEquals(1, a.join().remaining());
        assertEquals(1L, registry.inFlightRangeGets(111L));
        assertFalse(registry.locallyDrainedAfterFence(111L),
            "One finished reader cannot erase another reader's pending transport");
        second.complete(ByteBuffer.wrap(new byte[] {2}));
        assertEquals(1, b.join().remaining());
        assertTrue(registry.locallyDrainedAfterFence(111L));
        assertEquals(0L, registry.inFlightRangeGets(111L));
    }

    @Test
    void cancellingReadViewFutureCannotUnpinUnderlyingTransport() {
        CompletableFuture<ByteBuffer> transport = new CompletableFuture<>();
        ObjectStore backing = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                return transport;
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        ActiveCommittedObjectReads registry = new ActiveCommittedObjectReads();
        ObjectStore reader = registry.trackingReaderStore(backing);
        CompletableFuture<ByteBuffer> result = reader.rangeRead(112L, 0L, 1);
        assertEquals(1L, registry.inFlightRangeGets(112L));
        assertTrue(result.cancel(true));
        assertFalse(transport.isDone());
        registry.fence(112L);
        assertFalse(registry.locallyDrainedAfterFence(112L));
        transport.complete(ByteBuffer.wrap(new byte[] {1}));
        assertTrue(registry.locallyDrainedAfterFence(112L));
        assertTrue(result.isCancelled());
    }

    @Test
    void synchronouslyThrowingRangeGetCannotLeakLocalLease() {
        IllegalStateException injected = new IllegalStateException("range adapter rejected request");
        ObjectStore throwing = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long objectId, ByteBuffer data) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long objectId, long position, int length) {
                throw injected;
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };
        ActiveCommittedObjectReads pins = new ActiveCommittedObjectReads();
        ObjectStore tracked = pins.trackingReaderStore(throwing);
        IllegalStateException failure = assertThrows(
            IllegalStateException.class, () -> tracked.rangeRead(103L, 0L, 1)
        );
        assertSame(injected, failure);
        assertEquals(0L, pins.inFlightRangeGets(103L));

        pins.fence(103L);
        assertTrue(pins.locallyDrainedAfterFence(103L));
        CompletionException rejected = assertThrows(
            CompletionException.class, () -> tracked.rangeRead(103L, 0L, 1).join()
        );
        assertTrue(rejected.getCause().getMessage().contains("locally fenced"));
        assertEquals(0L, pins.inFlightRangeGets(103L));
    }
}
