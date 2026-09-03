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

    /**
     * Upload lifecycle context. Metadata is absent before the object byte stream has been fully produced.
     * The planned object size is exact and available at every phase.
     */
    record UploadContext(
        long objectId,
        long createdTimeMs,
        long objectSize,
        SharedObjectMetadata metadata
    ) {
        public UploadContext {
            if (objectId <= 0) {
                throw new IllegalArgumentException("objectId must be positive");
            }
            if (createdTimeMs < 0) {
                throw new IllegalArgumentException("createdTimeMs must be non-negative");
            }
            if (objectSize <= 0) {
                throw new IllegalArgumentException("objectSize must be positive");
            }
            if (metadata != null) {
                if (metadata.objectId() != objectId) {
                    throw new IllegalArgumentException(
                        "metadata objectId " + metadata.objectId() + " does not match upload objectId " + objectId);
                }
                if (metadata.objectSize() != objectSize) {
                    throw new IllegalArgumentException(
                        "metadata objectSize " + metadata.objectSize() + " does not match planned size " + objectSize);
                }
            }
        }

        public static UploadContext planned(long objectId, long createdTimeMs, long objectSize) {
            return new UploadContext(objectId, createdTimeMs, objectSize, null);
        }

        public UploadContext withMetadata(SharedObjectMetadata committedMetadata) {
            return new UploadContext(
                objectId,
                createdTimeMs,
                objectSize,
                Objects.requireNonNull(committedMetadata, "committedMetadata")
            );
        }

        public SharedObjectMetadata requiredMetadata() {
            return Objects.requireNonNull(metadata, "metadata is not available before object publication");
        }
    }
}
