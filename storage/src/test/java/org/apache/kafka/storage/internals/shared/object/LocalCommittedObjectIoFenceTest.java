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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalCommittedObjectIoFenceTest {
    @Test
    void drainWaitsForPhysicalGetLogicalCacheReadAndWriter() {
        long objectId = 710L;
        ActiveCommittedObjectReads reads = new ActiveCommittedObjectReads();
        ActiveObjectUploads uploads = new ActiveObjectUploads();
        LocalCommittedObjectIoFence fence = new LocalCommittedObjectIoFence(reads, uploads);
        CompletableFuture<ByteBuffer> transport = new CompletableFuture<>();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long id, ByteBuffer bytes) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long id, long position, int length) {
                return transport;
            }

            @Override
            public CompletableFuture<Void> delete(long id) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        ActiveCommittedObjectReads.ReadLease logical =
            reads.tryAcquireLogicalRead(objectId).orElseThrow();
        CompletableFuture<ByteBuffer> cancelled =
            reads.trackingReaderStore(store).rangeRead(objectId, 0L, 1);
        ActiveObjectUploads.UploadLease upload = uploads.beginLease(objectId);
        assertFalse(fence.locallyDrainedAfterFence(objectId));
        fence.fence(objectId);
        assertFalse(fence.locallyDrainedAfterFence(objectId));

        assertTrue(cancelled.cancel(true));
        assertFalse(transport.isDone());
        upload.close();
        logical.close();
        assertFalse(fence.locallyDrainedAfterFence(objectId),
            "A cancelled caller may not unpin the actual pending S3 transport");
        assertEquals(1L, reads.inFlightRangeGets(objectId));
        transport.complete(ByteBuffer.wrap(new byte[] {1}));
        assertEquals(0L, reads.inFlightRangeGets(objectId));
        assertTrue(fence.locallyDrainedAfterFence(objectId));

        assertTrue(reads.tryAcquireLogicalRead(objectId).isEmpty());
        assertTrue(reads.tryAcquire(objectId).isEmpty());
        assertThrows(IllegalStateException.class, () -> uploads.beginLease(objectId));
        fence.fence(objectId);
        assertTrue(fence.locallyDrainedAfterFence(objectId));
    }

    @Test
    void bothRegistriesMustBeFencedAndOtherObjectsRemainIndependent() {
        ActiveCommittedObjectReads reads = new ActiveCommittedObjectReads();
        ActiveObjectUploads uploads = new ActiveObjectUploads();
        LocalCommittedObjectIoFence fence = new LocalCommittedObjectIoFence(reads, uploads);
        assertFalse(fence.locallyDrainedAfterFence(720L),
            "An absent local observation is not permission to retire");

        reads.fence(720L);
        assertFalse(fence.locallyDrainedAfterFence(720L),
            "Read-side drain alone cannot ignore an unfenced writer");
        uploads.fence(721L);
        assertFalse(fence.locallyDrainedAfterFence(721L),
            "Writer-side drain alone cannot ignore new reads");

        ActiveObjectUploads.UploadLease unrelated = uploads.beginLease(722L);
        ActiveCommittedObjectReads.ReadLease unrelatedRead =
            reads.tryAcquireLogicalRead(722L).orElseThrow();
        fence.fence(720L);
        assertTrue(fence.locallyDrainedAfterFence(720L));
        assertTrue(uploads.contains(722L));
        assertEquals(1L, reads.inFlightLogicalReads(722L));

        fence.fence(722L);
        assertFalse(fence.locallyDrainedAfterFence(722L));
        unrelatedRead.close();
        assertFalse(fence.locallyDrainedAfterFence(722L));
        unrelated.close();
        assertTrue(fence.locallyDrainedAfterFence(722L));
        assertThrows(IllegalArgumentException.class, () -> fence.fence(0L));
    }
}
