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
package org.apache.kafka.storage.internals.shared.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Physical I/O boundary for the shared WAL.
 *
 * <p>The backend owns positional I/O and the durability/lifecycle operations of a physical WAL unit. It deliberately
 * does not know about Kafka offsets, append groups, GROUP_COMMIT records, capacity admission, or reclaim policy.
 * Those semantics remain in the WAL state machine.</p>
 */
public interface WalIoBackend extends AutoCloseable {
    int MAX_WRITE_BATCH_REQUESTS = 64;
    long MAX_WRITE_BATCH_BYTES = 16L * 1024L * 1024L;

    /** Opens an existing physical unit for positional reads only. */
    Handle openRead(Path path) throws IOException;

    /** Reopens an existing physical unit for positional reads and writes. */
    Handle reopen(Path path) throws IOException;

    /** Creates a new physical unit and fails if the path already exists. */
    Handle create(Path path) throws IOException;

    /** Returns the current physical size of a unit. */
    long size(Path path) throws IOException;

    /** Whether this backend can materialize the requested physical WAL capacity during fresh-file creation. */
    default boolean supportsPreallocation() {
        return false;
    }

    /** Whether this backend performs direct I/O rather than page-cache-backed I/O. */
    default boolean supportsDirectIo() {
        return false;
    }

    /**
     * A physical positional write. Its buffer must remain owned by the caller until
     * {@link Handle#writeBatch(List)} returns. Batch entries must not overlap.
     */
    record WriteRequest(ByteBuffer source, long position) {
        public WriteRequest {
            Objects.requireNonNull(source, "source");
            if (position < 0L) {
                throw new IllegalArgumentException("WAL write position must be non-negative");
            }
            if (position > Long.MAX_VALUE - source.remaining()) {
                throw new IllegalArgumentException("WAL positional write extends beyond Long.MAX_VALUE");
            }
        }
    }

    /**
     * Reject overlapping writes before submitting any I/O. WAL records may be
     * completed out of order at the device level only when their physical ranges
     * are disjoint.
     */
    static void validateWriteBatch(List<WriteRequest> writes) {
        Objects.requireNonNull(writes, "writes");
        if (writes.size() > MAX_WRITE_BATCH_REQUESTS) {
            throw new IllegalArgumentException("WAL write batch exceeds maximum request count");
        }
        long totalBytes = 0L;
        for (int i = 0; i < writes.size(); i++) {
            WriteRequest current = Objects.requireNonNull(writes.get(i), "write");
            long start = current.position();
            int bytes = current.source().remaining();
            if (start > Long.MAX_VALUE - bytes) {
                throw new IllegalArgumentException("WAL write range exceeds Long.MAX_VALUE");
            }
            long end = start + bytes;
            totalBytes += bytes;
            if (totalBytes > MAX_WRITE_BATCH_BYTES) {
                throw new IllegalArgumentException("WAL write batch exceeds maximum byte count");
            }
            for (int j = 0; j < i; j++) {
                WriteRequest previous = writes.get(j);
                long otherStart = previous.position();
                long otherEnd = otherStart + previous.source().remaining();
                if (start < otherEnd && otherStart < end) {
                    throw new IllegalArgumentException("Overlapping WAL positional writes in a batch");
                }
            }
        }
    }

    /** Backends may own shared resources; the portable backend does not. */
    @Override
    default void close() throws IOException {
    }

    /**
     * Open handle to one physical WAL unit.
     *
     * <p>All read/write offsets are absolute within the unit. {@link #force()} is the durability boundary for bytes
     * written through this handle. {@link #seal()} durably closes a unit before the state machine rotates away from
     * it.</p>
     */
    interface Handle extends AutoCloseable {
        long size() throws IOException;

        int read(ByteBuffer destination, long position) throws IOException;

        int write(ByteBuffer source, long position) throws IOException;

        /**
         * Submit independent positional writes as a batch. Return only after every
         * submitted write completes, including on failure; callers may then safely
         * release the ByteBuffers. This is NOT a durability acknowledgement:
         * callers must still force and publish the WAL checkpoint separately.
         */
        default void writeBatch(List<WriteRequest> writes) throws IOException {
            WalIoBackend.validateWriteBatch(writes);
            for (WriteRequest request : writes) {
                ByteBuffer source = request.source();
                long position = request.position();
                while (source.hasRemaining()) {
                    int written = write(source, position);
                    if (written <= 0) {
                        throw new IOException("WAL batch write made no progress at " + position);
                    }
                    position = Math.addExact(position, written);
                }
            }
        }

        void truncate(long size) throws IOException;

        /**
         * Materializes a newly-created empty physical unit to the requested capacity.
         *
         * <p>Callers must check {@link WalIoBackend#supportsPreallocation()} first. Implementations may use native
         * allocation primitives or a portable full-file materialization strategy, but a successful return must ensure
         * that subsequent {@link #force()} can surface allocation failures during initialization instead of deferring
         * them until normal WAL reuse.</p>
         */
        default void preallocate(long size) throws IOException {
            throw new UnsupportedOperationException("WAL I/O backend does not support preallocation");
        }

        void force() throws IOException;

        void seal() throws IOException;

        @Override
        void close() throws IOException;
    }
}
