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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

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
