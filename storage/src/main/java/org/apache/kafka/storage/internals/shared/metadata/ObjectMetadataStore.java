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
package org.apache.kafka.storage.internals.shared.metadata;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface ObjectMetadataStore extends AutoCloseable {
    CompletableFuture<Void> prepare(long objectId, long createdTimeMs);

    CompletableFuture<Void> commit(SharedObjectMetadata metadata);

    CompletableFuture<Void> delete(long objectId);

    /**
     * Atomically claims a PREPARED object for orphan cleanup. A concurrent COMMIT and cleanup claim must be
     * serialized by the authoritative metadata store so exactly one transition wins. A false result means the object
     * is no longer the PREPARED generation observed by the caller and therefore must not be physically deleted.
     */
    CompletableFuture<Boolean> claimCleanup(long objectId, long expectedCreatedTimeMs);

    /**
     * Marks a successfully deleted cleanup claim complete. Implementations must retain a terminal fence so a delayed
     * COMMIT can never resurrect an object that has already been physically deleted.
     */
    CompletableFuture<Void> completeCleanup(long objectId);

    List<SharedObjectMetadata> committedObjects();

    List<PreparedObject> preparedObjects();

    /** Returns cleanup claims whose physical object deletion still needs to be completed or retried. */
    List<PreparedObject> cleanupClaimedObjects();

    @Override
    default void close() throws Exception {
    }

    record PreparedObject(long objectId, long createdTimeMs) {
        public PreparedObject {
            if (objectId <= 0) {
                throw new IllegalArgumentException("objectId must be positive");
            }
            if (createdTimeMs < 0) {
                throw new IllegalArgumentException("createdTimeMs must be non-negative");
            }
        }
    }
}
