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
import org.apache.kafka.storage.internals.shared.metadata.ObjectMetadataStore;
import org.apache.kafka.storage.internals.shared.metadata.OffsetRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectMetadata;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedObjectUploaderTest {
    private static final SharedPartitionId P0 = new SharedPartitionId(1, 2, 0);
    private static final SharedPartitionId P1 = new SharedPartitionId(3, 4, 1);

    @TempDir
    Path tempDir;

    @Test
    void shouldPackMultiplePartitionsAndExposeRemoteOnlyAfterMetadataCommit() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore metadataStore = new InMemoryObjectMetadataStore();
        try (SharedStorageEngine engine = engine("success")) {
            append(engine, P0, 10, 19, new byte[]{1, 2, 3});
            append(engine, P1, 30, 39, new byte[]{4, 5});

            List<SharedStorageEngine.UploadCandidate> candidates = List.of(
                engine.uploadCandidates(P0, 10, 20).get(0),
                engine.uploadCandidates(P1, 30, 40).get(0)
            );
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore, metadataStore, new SharedObjectPacker(), engine);

            SharedObjectMetadata metadata = uploader.upload(100, 1_000, candidates)
                .get(10, TimeUnit.SECONDS);

            assertTrue(objectStore.contains(100));
            assertTrue(metadataStore.isCommitted(100));
            assertTrue(engine.remoteIndex().coverage(P0).covers(new OffsetRange(10, 20)));
            assertTrue(engine.remoteIndex().coverage(P1).covers(new OffsetRange(30, 40)));

            for (var range : metadata.ranges()) {
                ByteBuffer storedPayload = objectStore.rangeRead(
                    metadata.objectId(), range.objectPosition(), range.objectLength()).get(10, TimeUnit.SECONDS);
                if (range.partition().equals(P0)) {
                    assertArrayEquals(new byte[]{1, 2, 3}, bytes(storedPayload));
                } else {
                    assertArrayEquals(new byte[]{4, 5}, bytes(storedPayload));
                }
            }
        }
    }

    @Test
    void putWithoutMetadataCommitMustRemainLogicallyInvisible() throws Exception {
        InMemoryObjectStore objectStore = new InMemoryObjectStore();
        InMemoryObjectMetadataStore preparedStore = new InMemoryObjectMetadataStore();
        ObjectMetadataStore failingCommitStore = new ObjectMetadataStore() {
            @Override
            public CompletableFuture<Void> prepare(long objectId, long createdTimeMs) {
                return preparedStore.prepare(objectId, createdTimeMs);
            }

            @Override
            public CompletableFuture<Void> commit(SharedObjectMetadata metadata) {
                return CompletableFuture.failedFuture(new RuntimeException("injected metadata commit failure"));
            }

            @Override
            public CompletableFuture<Void> delete(long objectId) {
                return preparedStore.delete(objectId);
            }

            @Override
            public List<SharedObjectMetadata> committedObjects() {
                return List.of();
            }
        };

        try (SharedStorageEngine engine = engine("commit-failure")) {
            append(engine, P0, 10, 19, new byte[]{9});
            SharedObjectUploader uploader = new SharedObjectUploader(
                objectStore, failingCommitStore, new SharedObjectPacker(), engine);

            ExecutionException error = assertThrows(ExecutionException.class, () ->
                uploader.upload(200, 2_000, engine.uploadCandidates(P0, 10, 20))
                    .get(10, TimeUnit.SECONDS));
            assertTrue(error.getCause().getMessage().contains("metadata commit failure"));

            assertTrue(objectStore.contains(200)); // orphan physical object
            assertTrue(preparedStore.isPrepared(200));
            assertFalse(engine.remoteIndex().coverage(P0).covers(new OffsetRange(10, 20)));
            assertFalse(engine.uploadCandidates(P0, 10, 20).isEmpty());
        }
    }

    private SharedStorageEngine engine(String name) throws Exception {
        return new SharedStorageEngine(new FileSharedWal(tempDir.resolve(name), 1024 * 1024, 4096));
    }

    private static void append(
        SharedStorageEngine engine,
        SharedPartitionId partition,
        long first,
        long last,
        byte[] payload
    ) throws Exception {
        engine.appendData(partition, 3, first, last, ByteBuffer.wrap(payload)).get(10, TimeUnit.SECONDS);
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }
}
