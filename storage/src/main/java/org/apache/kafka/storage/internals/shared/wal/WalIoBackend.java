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

/**
 * Physical I/O boundary for the shared WAL.
 *
 * <p>The backend owns positional I/O and the durability/lifecycle operations of a physical WAL unit. It deliberately
 * does not know about Kafka offsets, append groups, GROUP_COMMIT records, capacity admission, or reclaim policy.
 * Those semantics remain in the WAL state machine.</p>
 */
public interface WalIoBackend extends AutoCloseable {

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
