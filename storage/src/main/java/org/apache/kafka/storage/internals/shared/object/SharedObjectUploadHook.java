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

import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous lifecycle hook for deterministic fault injection around the immutable-object publish protocol.
 *
 * <p>Production callers use {@link #NOOP}. Tests may return an incomplete future to stop an upload exactly after a
 * durable protocol transition without blocking the uploader thread. The upload remains registered in
 * {@link ActiveObjectUploads} until the returned future completes or the process terminates.</p>
 */
@FunctionalInterface
public interface SharedObjectUploadHook {
    SharedObjectUploadHook NOOP = (phase, context) -> CompletableFuture.completedFuture(null);

    CompletableFuture<Void> onPhase(Phase phase, UploadContext context);

    enum Phase {
        AFTER_PREPARE,
        AFTER_PUT,
        AFTER_COMMIT
    }

    record UploadContext(
        long objectId,
        long createdTimeMs,
        SharedObjectMetadata metadata
    ) {
        public UploadContext {
            if (objectId <= 0) {
                throw new IllegalArgumentException("objectId must be positive");
            }
            if (createdTimeMs < 0) {
                throw new IllegalArgumentException("createdTimeMs must be non-negative");
            }
            Objects.requireNonNull(metadata, "metadata");
            if (metadata.objectId() != objectId) {
                throw new IllegalArgumentException(
                    "metadata objectId " + metadata.objectId() + " does not match upload objectId " + objectId);
            }
        }
    }
}
