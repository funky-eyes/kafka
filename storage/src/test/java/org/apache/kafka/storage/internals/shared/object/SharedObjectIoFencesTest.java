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

import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedObjectIoFencesTest {
    @Test
    void actualReaderAndUploaderShareBrokerLifetimeDrainAcrossCancellation() {
        long objectId = 1001L;
        byte[] data = {1, 2, 3};
        CRC32C crc = new CRC32C();
        crc.update(data);
        SharedPartitionId partition = new SharedPartitionId(10L, 20L, 0);
        SharedObjectRange range = new SharedObjectRange(
            partition, new OffsetRange(0L, 1L), 1, 0L, data.length, crc.getValue());
        RemoteObjectIndex index = new RemoteObjectIndex();
        index.restore(List.of(new RemoteObjectIndex.RangeReference(objectId, range)));
        CompletableFuture<ByteBuffer> physicalGet = new CompletableFuture<>();
        ObjectStore store = new ObjectStore() {
            @Override
            public CompletableFuture<Void> put(long id, ByteBuffer bytes) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }

            @Override
            public CompletableFuture<ByteBuffer> rangeRead(long id, long position, int length) {
                return physicalGet;
            }

            @Override
            public CompletableFuture<Void> delete(long id) {
                return CompletableFuture.failedFuture(new UnsupportedOperationException());
            }
        };

        SharedObjectIoFences shared = new SharedObjectIoFences();
        SharedObjectReader reader = shared.newReader(store, index, 0, 0L);
        ActiveObjectUploads.UploadLease uploader = shared.uploads().beginLease(objectId);
        CompletableFuture<?> read = reader.read(partition, 0L);
        assertFalse(read.isDone());
        assertFalse(shared.locallyDrainedAfterFence(objectId));

        shared.fence(objectId);
        assertFalse(shared.locallyDrainedAfterFence(objectId));
        assertTrue(read.cancel(true));
        assertFalse(physicalGet.isDone(), "Caller cancellation must not fake S3 completion");
        assertThrows(CompletionException.class, () -> reader.read(partition, 0L).join());
        assertThrows(IllegalStateException.class, () -> shared.uploads().beginLease(objectId));
        uploader.close();
        assertFalse(shared.locallyDrainedAfterFence(objectId));

        physicalGet.complete(ByteBuffer.wrap(data));
        assertTrue(shared.locallyDrainedAfterFence(objectId));
        assertFalse(shared.uploads().contains(objectId));

        // A different physical object must remain independent.
        try (ActiveObjectUploads.UploadLease unrelated = shared.uploads().beginLease(objectId + 1)) {
            assertTrue(shared.uploads().contains(objectId + 1));
        }
    }

    @Test
    void allReadPathsRemainFencedAfterDrain() {
        long objectId = 2002L;
        SharedObjectIoFences shared = new SharedObjectIoFences();
        assertFalse(shared.locallyDrainedAfterFence(objectId));
        shared.fence(objectId);
        assertTrue(shared.locallyDrainedAfterFence(objectId));
        assertThrows(IllegalStateException.class, () -> shared.uploads().beginLease(objectId));
        assertThrows(IllegalArgumentException.class, () -> shared.fence(0L));
    }
}
