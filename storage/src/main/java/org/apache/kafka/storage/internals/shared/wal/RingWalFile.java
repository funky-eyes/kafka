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

import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Fixed-length physical file used by the circular WAL state machine.
 *
 * <p>Data writes are staged first. A new logical head/tail becomes recoverable only through
 * {@link #forceAndCheckpoint(long, long)}, which forces data, writes the next alternating superblock, then forces the
 * superblock. Recovery therefore observes either the previous durable window or the new one, never metadata pointing
 * beyond data that skipped the durability barrier.</p>
 *
 * <p>If initialization of a newly created file fails before this object becomes usable, the incomplete file is removed
 * and the parent directory is flushed so a later broker start can retry cleanly. Recovery failures for files that
 * existed before this object was opened remain fail-closed and never delete the existing WAL.</p>
 *
 * <p>This class establishes the configured file length but does not claim that the portable FileChannel backend has
 * physically reserved every filesystem block. True fallocate/preallocation is a separate backend capability and can be
 * added without changing the ring format.</p>
 */
final class RingWalFile implements AutoCloseable {
    private final Path path;
    private final RingWalLayout layout;
    private final WalIoBackend backend;
    private final WalIoBackend.Handle handle;
    private RingWalSuperblock.State state;
    private boolean closed;

    RingWalFile(Path path, long totalCapacityBytes) throws IOException {
        this(path, totalCapacityBytes, new FileChannelWalIoBackend());
    }

    RingWalFile(Path path, long totalCapacityBytes, WalIoBackend backend) throws IOException {
        this.path = Objects.requireNonNull(path, "path");
        this.layout = new RingWalLayout(totalCapacityBytes);
        this.backend = Objects.requireNonNull(backend, "backend");

        Path parent = path.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        boolean exists = Files.exists(path);
        WalIoBackend.Handle opened = exists ? backend.reopen(path) : backend.create(path);
        try {
            if (exists) {
                validateExistingLength(opened);
                this.state = recoverState(opened);
            } else {
                initializeNewFile(opened);
                this.state = new RingWalSuperblock.State(0L, 0L, 0L, layout.dataCapacityBytes());
            }
            this.handle = opened;
        } catch (Throwable failure) {
            closeAfterOpenFailure(opened, failure);
            if (!exists) {
                cleanupFailedInitialization(failure);
            }
            if (failure instanceof IOException ioException) {
                throw ioException;
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new IOException("Unexpected failure opening ring WAL file", failure);
        }
    }

    RingWalLayout layout() {
        return layout;
    }

    synchronized RingWalSuperblock.State state() {
        ensureOpen();
        return state;
    }

    synchronized void write(RingWalLayout.Allocation allocation, ByteBuffer source) throws IOException {
        ensureOpen();
        Objects.requireNonNull(allocation, "allocation");
        Objects.requireNonNull(source, "source");
        ByteBuffer bytes = source.duplicate();
        if (bytes.remaining() != allocation.encodedLength()) {
            throw new IllegalArgumentException(
                "ring WAL write length mismatch: expected=" + allocation.encodedLength() +
                    ", actual=" + bytes.remaining());
        }
        RingWalLayout.PhysicalAddress expected = layout.address(allocation.walOffset());
        if (expected.generation() != allocation.generation() ||
            expected.position() != allocation.physicalPosition()) {
            throw new IllegalArgumentException("allocation does not match ring WAL logical address");
        }
        if (allocation.walOffset() < state.tailOffset()) {
            throw new IllegalArgumentException(
                "ring WAL allocation moves behind durable tail: durableTail=" + state.tailOffset() +
                    ", allocation=" + allocation.walOffset());
        }
        layout.retainedBytes(state.headOffset(), allocation.nextTailOffset());
        ensureContiguous(allocation.walOffset(), allocation.encodedLength());
        writeFully(bytes, allocation.physicalPosition());
    }

    synchronized ByteBuffer read(long walOffset, int length) throws IOException {
        ensureOpen();
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        long endOffset = Math.addExact(walOffset, length);
        if (walOffset < state.headOffset() || endOffset > state.tailOffset()) {
            throw new IllegalArgumentException(
                "ring WAL read is outside durable window: range=[" + walOffset + ", " + endOffset +
                    "), durable=[" + state.headOffset() + ", " + state.tailOffset() + ")");
        }
        ensureContiguous(walOffset, length);
        RingWalLayout.PhysicalAddress address = layout.address(walOffset);
        ByteBuffer result = ByteBuffer.allocate(length);
        readFully(result, address.position());
        result.flip();
        return result.asReadOnlyBuffer();
    }

    synchronized RingWalSuperblock.State forceAndCheckpoint(long headOffset, long tailOffset) throws IOException {
        ensureOpen();
        validateForwardCheckpoint(headOffset, tailOffset);
        RingWalSuperblock.State next = state.next(headOffset, tailOffset);

        handle.force();
        writeSuperblock(next);
        handle.force();
        state = next;
        return state;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOException failure = null;
        try {
            handle.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            backend.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void initializeNewFile(WalIoBackend.Handle opened) throws IOException {
        ByteBuffer lastByte = ByteBuffer.allocate(1);
        writeFully(opened, lastByte, layout.totalCapacityBytes() - 1L);
        RingWalSuperblock.State initial =
            new RingWalSuperblock.State(0L, 0L, 0L, layout.dataCapacityBytes());
        writeSuperblock(opened, initial);
        opened.force();
        long actualSize = opened.size();
        if (actualSize != layout.totalCapacityBytes()) {
            throw new IOException(
                "Unable to establish fixed ring WAL length: expected=" + layout.totalCapacityBytes() +
                    ", actual=" + actualSize);
        }
    }

    private void validateExistingLength(WalIoBackend.Handle opened) throws IOException {
        long actualSize = opened.size();
        if (actualSize != layout.totalCapacityBytes()) {
            throw new WalCorruptionException(
                "Ring WAL file length does not match configured capacity: expected=" + layout.totalCapacityBytes() +
                    ", actual=" + actualSize);
        }
    }

    private RingWalSuperblock.State recoverState(WalIoBackend.Handle opened) throws IOException {
        ByteBuffer first = readSuperblock(opened, 0);
        ByteBuffer second = readSuperblock(opened, 1);
        return RingWalSuperblock.selectNewest(first, second, layout.dataCapacityBytes());
    }

    private ByteBuffer readSuperblock(WalIoBackend.Handle opened, int copyIndex) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(RingWalSuperblock.SUPERBLOCK_BYTES);
        readFully(opened, buffer, superblockPosition(copyIndex));
        buffer.flip();
        return buffer;
    }

    private void writeSuperblock(RingWalSuperblock.State checkpoint) throws IOException {
        writeSuperblock(handle, checkpoint);
    }

    private void writeSuperblock(WalIoBackend.Handle target, RingWalSuperblock.State checkpoint) throws IOException {
        ByteBuffer encoded = RingWalSuperblock.encode(checkpoint);
        writeFully(target, encoded, superblockPosition(RingWalSuperblock.copyIndex(checkpoint.sequence())));
    }

    private static long superblockPosition(int copyIndex) {
        if (copyIndex < 0 || copyIndex >= RingWalLayout.SUPERBLOCK_COPIES) {
            throw new IllegalArgumentException("invalid superblock copy index: " + copyIndex);
        }
        return (long) copyIndex * RingWalSuperblock.SUPERBLOCK_BYTES;
    }

    private void validateForwardCheckpoint(long headOffset, long tailOffset) {
        if (headOffset < state.headOffset()) {
            throw new IllegalArgumentException(
                "ring WAL head cannot move backwards: current=" + state.headOffset() + ", new=" + headOffset);
        }
        if (tailOffset < state.tailOffset()) {
            throw new IllegalArgumentException(
                "ring WAL tail cannot move backwards: current=" + state.tailOffset() + ", new=" + tailOffset);
        }
        layout.retainedBytes(headOffset, tailOffset);
    }

    private void ensureContiguous(long walOffset, int length) {
        if (walOffset < 0 || length <= 0) {
            throw new IllegalArgumentException("invalid ring WAL data range");
        }
        long positionInRing = walOffset % layout.dataCapacityBytes();
        if (positionInRing + length > layout.dataCapacityBytes()) {
            throw new IllegalArgumentException(
                "ring WAL data range crosses physical wrap boundary: offset=" + walOffset + ", length=" + length);
        }
    }

    private void writeFully(ByteBuffer source, long position) throws IOException {
        writeFully(handle, source, position);
    }

    private static void writeFully(WalIoBackend.Handle target, ByteBuffer source, long position) throws IOException {
        long currentPosition = position;
        while (source.hasRemaining()) {
            int written = target.write(source, currentPosition);
            if (written <= 0) {
                throw new IOException("Unable to make progress writing ring WAL at position " + currentPosition);
            }
            currentPosition = Math.addExact(currentPosition, written);
        }
    }

    private void readFully(ByteBuffer destination, long position) throws IOException {
        readFully(handle, destination, position);
    }

    private static void readFully(WalIoBackend.Handle source, ByteBuffer destination, long position) throws IOException {
        long currentPosition = position;
        while (destination.hasRemaining()) {
            int read = source.read(destination, currentPosition);
            if (read < 0) {
                throw new WalCorruptionException("Unexpected EOF reading ring WAL at position " + currentPosition);
            }
            if (read == 0) {
                throw new IOException("Unable to make progress reading ring WAL at position " + currentPosition);
            }
            currentPosition = Math.addExact(currentPosition, read);
        }
    }

    private void closeAfterOpenFailure(WalIoBackend.Handle opened, Throwable failure) {
        try {
            opened.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        try {
            backend.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private void cleanupFailedInitialization(Throwable failure) {
        try {
            if (Files.deleteIfExists(path)) {
                Path parent = path.toAbsolutePath().normalize().getParent();
                if (parent != null) {
                    Utils.flushDir(parent);
                }
            }
        } catch (Throwable cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Ring WAL file is closed");
        }
    }
}
