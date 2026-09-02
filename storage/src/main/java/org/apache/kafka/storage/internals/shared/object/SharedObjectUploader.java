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

import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.metadata.ObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.Phase;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.UploadContext;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Enforces the remote durability boundary: an object becomes visible to the storage engine only after both
 * object PUT and metadata COMMIT succeed. A successful PUT followed by a failed commit remains an orphan and
 * must never advance remote coverage.
 */
public final class SharedObjectUploader {
    private final ObjectStore objectStore;
    private final ObjectMetadataStore metadataStore;
    private final SharedObjectPacker packer;
    private final SharedStorageEngine engine;
    private final ActiveObjectUploads activeUploads;
    private final SharedObjectUploadHook uploadHook;

    public SharedObjectUploader(
        ObjectStore objectStore,
        ObjectMetadataStore metadataStore,
        SharedObjectPacker packer,
        SharedStorageEngine engine
    ) {
        this(
            objectStore,
            metadataStore,
            packer,
            engine,
            new ActiveObjectUploads(),
            SharedObjectUploadHook.NOOP
        );
    }

    public SharedObjectUploader(
        ObjectStore objectStore,
        ObjectMetadataStore metadataStore,
        SharedObjectPacker packer,
        SharedStorageEngine engine,
        ActiveObjectUploads activeUploads
    ) {
        this(objectStore, metadataStore, packer, engine, activeUploads, SharedObjectUploadHook.NOOP);
    }

    public SharedObjectUploader(
        ObjectStore objectStore,
        ObjectMetadataStore metadataStore,
        SharedObjectPacker packer,
        SharedStorageEngine engine,
        ActiveObjectUploads activeUploads,
        SharedObjectUploadHook uploadHook
    ) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.metadataStore = Objects.requireNonNull(metadataStore, "metadataStore");
        this.packer = Objects.requireNonNull(packer, "packer");
        this.engine = Objects.requireNonNull(engine, "engine");
        this.activeUploads = Objects.requireNonNull(activeUploads, "activeUploads");
        this.uploadHook = Objects.requireNonNull(uploadHook, "uploadHook");
    }

    public CompletableFuture<SharedObjectMetadata> upload(
        long objectId,
        long createdTimeMs,
        List<SharedStorageEngine.UploadCandidate> candidates
    ) {
        final PackedObject packed;
        final UploadContext context;
        try {
            packed = packer.pack(objectId, candidates, engine);
            context = new UploadContext(objectId, createdTimeMs, packed.metadata());
            activeUploads.begin(objectId);
        } catch (IOException | RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<SharedObjectMetadata> result;
        try {
            result = metadataStore.prepare(objectId, createdTimeMs)
                .thenCompose(ignored -> invokeHook(Phase.AFTER_PREPARE, context))
                .thenCompose(ignored -> objectStore.put(objectId, packed.parts()))
                .thenCompose(ignored -> invokeHook(Phase.AFTER_PUT, context))
                .thenCompose(ignored -> metadataStore.commit(packed.metadata()))
                .thenCompose(ignored -> invokeHook(Phase.AFTER_COMMIT, context))
                .thenApply(ignored -> {
                    engine.commitRemoteObject(packed.metadata());
                    return packed.metadata();
                });
        } catch (RuntimeException e) {
            activeUploads.end(objectId);
            return CompletableFuture.failedFuture(e);
        }
        return result.whenComplete((ignored, error) -> activeUploads.end(objectId));
    }

    private CompletableFuture<Void> invokeHook(Phase phase, UploadContext context) {
        return Objects.requireNonNull(
            uploadHook.onPhase(phase, context),
            "SharedObjectUploadHook returned null for " + phase
        );
    }
}
