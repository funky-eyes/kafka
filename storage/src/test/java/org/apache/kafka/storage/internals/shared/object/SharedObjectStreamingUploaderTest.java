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
import org.apache.kafka.storage.internals.shared.metadata.InMemoryObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.object.SharedObjectUploadHook.Phase;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedObjectStreamingUploaderTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(21, 22, 0);

    @TempDir
    Path tempDir;

    @Test
    void shouldExposePlannedSizeBeforePutAndFinalMetadataAfterPut() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        List<Phase> phases = new ArrayList<>();
        List<SharedObjectUploadHook.UploadContext> contexts = new ArrayList<>();
        SharedObjectUploadHook hook = (phase, context) -> {
            phases.add(phase);
            contexts.add(context);
            return CompletableFuture.completedFuture(null);
        };

        try (SharedStorageEngine engine = new SharedStorageEngine(
            new FileSharedWal(tempDir.resolve("streaming-uploader"), 1024 * 1024, 4096))) {
            engine.appendData(P0, 4, 0, 9, ByteBuffer.wrap(new byte[] {1, 2, 3, 4}))
                .get(10, TimeUnit.SECONDS);
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore,
                metadataStore,
                new SharedObjectPacker(),
                engine,
                new ActiveObjectUploads(),
                hook
            );

            SharedObjectMetadata metadata = uploader.upload(8_101L, 123L, engine.uploadCandidates(P0, 0, 10))
                .get(10, TimeUnit.SECONDS);

            assertEquals(List.of(Phase.AFTER_PREPARE, Phase.AFTER_PUT, Phase.AFTER_COMMIT), phases);
            assertNull(contexts.get(0).metadata());
            assertTrue(contexts.get(0).objectSize() > 0);
            assertEquals(metadata.objectSize(), contexts.get(0).objectSize());
            assertSame(metadata, contexts.get(1).metadata());
            assertSame(metadata, contexts.get(2).metadata());
            assertTrue(metadataStore.isCommitted(8_101L));
            assertTrue(objectStore.contains(8_101L));
        }
    }
}
