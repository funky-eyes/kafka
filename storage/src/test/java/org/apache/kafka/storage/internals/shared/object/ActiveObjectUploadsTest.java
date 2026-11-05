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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ActiveObjectUploadsTest {
    @Test
    void localUploadFenceWaitsForLeaseAndBlocksReentry() {
        ActiveObjectUploads uploads = new ActiveObjectUploads();
        assertFalse(uploads.locallyDrainedAfterFence(501L),
            "An unknown object has not been fenced by a controller");

        ActiveObjectUploads.UploadLease ongoing = uploads.beginLease(501L);
        assertTrue(uploads.contains(501L));
        uploads.fence(501L);
        assertFalse(uploads.locallyDrainedAfterFence(501L));
        assertThrows(IllegalStateException.class, () -> uploads.begin(501L));
        assertThrows(IllegalStateException.class, () -> uploads.beginLease(501L));

        ongoing.close();
        ongoing.close();
        assertTrue(uploads.locallyDrainedAfterFence(501L));
        assertFalse(uploads.contains(501L));
        assertThrows(IllegalStateException.class, () -> uploads.beginLease(501L));
        assertFalse(uploads.locallyDrainedAfterFence(502L));
        assertThrows(IllegalArgumentException.class, () -> uploads.fence(0L));
    }

    @Test
    void oldLeaseAndLegacyEndCannotReleaseCurrentUpload() {
        ActiveObjectUploads uploads = new ActiveObjectUploads();
        ActiveObjectUploads.UploadLease old = uploads.beginLease(601L);
        old.close();
        ActiveObjectUploads.UploadLease current = uploads.beginLease(601L);
        old.close();
        assertTrue(uploads.contains(601L));
        assertThrows(IllegalStateException.class, () -> uploads.end(601L),
            "The legacy ID-only release must not revoke a leased upload");

        uploads.fence(601L);
        assertFalse(uploads.locallyDrainedAfterFence(601L));
        current.close();
        assertTrue(uploads.locallyDrainedAfterFence(601L));

        uploads.begin(602L);
        assertTrue(uploads.contains(602L));
        uploads.end(602L);
        uploads.end(602L);
        assertFalse(uploads.contains(602L));
        ActiveObjectUploads.UploadLease newLease = uploads.beginLease(602L);
        assertTrue(uploads.contains(602L));
        newLease.close();
        assertFalse(uploads.contains(602L));
    }

    @Test
    void concurrentAdmissionAndFenceLinearizeForEachPhysicalObject() throws Exception {
        ActiveObjectUploads uploads = new ActiveObjectUploads();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            for (int attempt = 0; attempt < 32; attempt++) {
                long objectId = 1_000L + attempt;
                CountDownLatch start = new CountDownLatch(1);
                Future<Optional<ActiveObjectUploads.UploadLease>> admitted = workers.submit(() -> {
                    start.await();
                    try {
                        return Optional.of(uploads.beginLease(objectId));
                    } catch (IllegalStateException alreadyFenced) {
                        return Optional.empty();
                    }
                });
                Future<?> fencing = workers.submit(() -> {
                    start.await();
                    uploads.fence(objectId);
                    return null;
                });
                start.countDown();
                Optional<ActiveObjectUploads.UploadLease> lease = admitted.get(10, TimeUnit.SECONDS);
                fencing.get(10, TimeUnit.SECONDS);

                assertEquals(lease.isPresent(), uploads.contains(objectId));
                if (lease.isPresent()) {
                    assertFalse(uploads.locallyDrainedAfterFence(objectId),
                        "An admitted upload must survive a concurrent fence");
                    lease.get().close();
                }
                assertTrue(uploads.locallyDrainedAfterFence(objectId));
                assertThrows(IllegalStateException.class, () -> uploads.beginLease(objectId));
            }
        } finally {
            workers.shutdownNow();
        }
    }
}
