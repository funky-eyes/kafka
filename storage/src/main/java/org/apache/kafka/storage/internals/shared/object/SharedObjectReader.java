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

import org.apache.kafka.storage.internals.shared.metadata.RemoteObjectIndex;
import org.apache.kafka.storage.internals.shared.metadata.SharedObjectRange;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.zip.CRC32C;

public final class SharedObjectReader {
    private final ObjectStore objectStore;
    private final RemoteObjectIndex remoteIndex;

    public SharedObjectReader(ObjectStore objectStore, RemoteObjectIndex remoteIndex) {
        this.objectStore = Objects.requireNonNull(objectStore, "objectStore");
        this.remoteIndex = Objects.requireNonNull(remoteIndex, "remoteIndex");
    }

    /** Returns the complete Kafka RecordBatch containing {@code offset}, if it is remotely covered. */
    public CompletableFuture<Optional<ByteBuffer>> read(SharedPartitionId partition, long offset) {
        Optional<RemoteObjectIndex.RangeReference> reference = remoteIndex.find(partition, offset);
        if (reference.isEmpty()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        RemoteObjectIndex.RangeReference ref = reference.get();
        SharedObjectRange range = ref.range();
        return objectStore.rangeRead(ref.objectId(), range.objectPosition(), range.objectLength())
            .thenApply(bytes -> {
                long actualChecksum = crc32c(bytes);
                if (actualChecksum != range.checksum()) {
                    throw new CompletionException(new RemoteObjectCorruptionException(
                        "Remote block checksum mismatch for object=" + ref.objectId() +
                            ", partition=" + partition + ", offsets=" + range.offsets() +
                            ", expected=" + range.checksum() + ", actual=" + actualChecksum));
                }
                return Optional.of(bytes.asReadOnlyBuffer());
            });
    }

    private static long crc32c(ByteBuffer data) {
        CRC32C crc = new CRC32C();
        crc.update(data.duplicate());
        return crc.getValue();
    }
}
