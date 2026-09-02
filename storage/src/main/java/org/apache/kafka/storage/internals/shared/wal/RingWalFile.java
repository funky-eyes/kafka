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
import java.nio.file.StandardCopyOption;
import java.util.Objects;

/**
 * Fixed-length physical file used by the circular WAL state machine.
 *
 * <p>Data writes are staged first. A new logical head/tail becomes recoverable only through
 * {@link #forceAndCheckpoint(long, long)}, which forces data, writes the next alternating superblock, then forces the
 * superblock. Recovery therefore observes either the previous durable window or the new one, never metadata pointing
 * beyond data that skipped the durability barrier. When a checkpoint advances the durable head, the same new state is
 * forced into the fallback superblock before the head becomes reusable. Recovery likewise repairs an older or invalid
 * fallback copy from the selected state. A CRC-valid fallback can therefore never keep references to physical slots
 * that a newer generation is allowed to overwrite.</p>
 *
 * <p>A brand-new WAL is fully initialized and forced through a sibling staging file before an atomic rename publishes
 * it at the durable path. The final path therefore never names a half-initialized WAL, even if the broker is SIGKILLed
 * or the machine loses power during first creation. A stale staging file is non-authoritative and is discarded when the
 * final WAL does not exist. Recovery failures for an already-published WAL remain fail-closed and never delete it.</p>
 *
 * <p>Fresh files ask the I/O backend to materialize the full configured capacity when it advertises preallocation
 * support. The portable FileChannel backend does this by writing the complete staging file before the initialization
 * force, so disk-allocation failures surface before the WAL is published instead of during later ring reuse. Backends
 * without preallocation support retain the logical-length fallback and must not claim physical reservation.</p>
 */
final class RingWalFile implements AutoCloseable {
    private static final String INITIALIZING_SUFFIX = ".initializing";

    private final Path path;
    private final RingWalLayout layout;
    private final WalIoBackend backend;
    private final WalIoBackend.Handle handle;
    private RingWalSuperblock.State state;
    private Throwable checkpointFailure;
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

        WalIoBackend.Handle opened = null;
        try {
            if (Files.exists(path)) {
                opened = backend.reopen(path);
                validateExistingLength(opened);
                this.state = recoverState(opened);
            } else {
                opened = createAndPublishNewFile();
                this.state = initialState();
            }
            this.handle = opened;
        } catch (Throwable failure) {
            closeAfterOpenFailure(opened, failure);
            throw propagateOpenFailure(failure);
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
        RingWalSuperblock.State current = state;
        RingWalSuperblock.State next = current.next(headOffset, tailOffset);

        try {
            handle.force();
            writeSuperblock(next);
            handle.force();
            if (headOffset > current.headOffset()) {
                mirrorSuperblock(next);
            }
            state = next;
            return state;
        } catch (IOException | RuntimeException failure) {
            checkpointFailure = failure;
            throw failure;
        }
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

    private WalIoBackend.Handle createAndPublishNewFile() throws IOException {
        Path stagingPath = initializationPath(path);
        removeStaleInitialization(stagingPath);

        WalIoBackend.Handle stagingHandle = backend.create(stagingPath);
        boolean stagingClosed = false;
        boolean published = false;
        try {
            initializeNewFile(stagingHandle);
            stagingHandle.close();
            stagingClosed = true;

            Files.move(stagingPath, path, StandardCopyOption.ATOMIC_MOVE);
            published = true;
            flushParentDirectory(path);
            return backend.reopen(path);
        } catch (Throwable failure) {
            if (!stagingClosed) {
                try {
                    stagingHandle.close();
                } catch (Throwable closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (!published) {
                cleanupStagedInitialization(stagingPath, failure);
            }
            throw propagateOpenFailure(failure);
        }
    }

    private RingWalSuperblock.State initialState() {
        return new RingWalSuperblock.State(0L, 0L, 0L, layout.dataCapacityBytes());
    }

    private void initializeNewFile(WalIoBackend.Handle opened) throws IOException {
        if (backend.supportsPreallocation()) {
            opened.preallocate(layout.totalCapacityBytes());
        } else {
            ByteBuffer lastByte = ByteBuffer.allocate(1);
            writeFully(opened, lastByte, layout.totalCapacityBytes() - 1L);
        }
        writeSuperblock(opened, initialState());
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
        RingWalSuperblock.State firstState = decodeSuperblock(first);
        RingWalSuperblock.State secondState = decodeSuperblock(second);
        RingWalSuperblock.State recovered = RingWalSuperblock.selectNewest(
            first,
            second,
            layout.dataCapacityBytes()
        );
        repairFallbackCopy(opened, recovered, firstState, secondState);
        return recovered;
    }

    private RingWalSuperblock.State decodeSuperblock(ByteBuffer bytes) {
        try {
            RingWalSuperblock.State decoded = RingWalSuperblock.decode(bytes);
            return decoded.dataCapacityBytes() == layout.dataCapacityBytes() ? decoded : null;
        } catch (WalCorruptionException e) {
            return null;
        }
    }

    private void repairFallbackCopy(
        WalIoBackend.Handle opened,
        RingWalSuperblock.State recovered,
        RingWalSuperblock.State firstState,
        RingWalSuperblock.State secondState
    ) throws IOException {
        if (recovered.equals(firstState) && recovered.equals(secondState)) {
            return;
        }
        int repairCopy = recovered.equals(firstState) ? 1 : 0;
        writeSuperblock(opened, recovered, repairCopy);
        opened.force();
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

    private void mirrorSuperblock(RingWalSuperblock.State checkpoint) throws IOException {
        int canonicalCopy = RingWalSuperblock.copyIndex(checkpoint.sequence());
        writeSuperblock(handle, checkpoint, 1 - canonicalCopy);
        handle.force();
    }

    private void writeSuperblock(WalIoBackend.Handle target, RingWalSuperblock.State checkpoint) throws IOException {
        writeSuperblock(target, checkpoint, RingWalSuperblock.copyIndex(checkpoint.sequence()));
    }

    private void writeSuperblock(
        WalIoBackend.Handle target,
        RingWalSuperblock.State checkpoint,
        int copyIndex
    ) throws IOException {
        ByteBuffer encoded = RingWalSuperblock.encode(checkpoint);
        writeFully(target, encoded, superblockPosition(copyIndex));
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

    static Path initializationPath(Path durablePath) {
        Objects.requireNonNull(durablePath, "durablePath");
        Path fileName = durablePath.getFileName();
        if (fileName == null) {
            throw new IllegalArgumentException("Ring WAL path must name a file: " + durablePath);
        }
        return durablePath.resolveSibling(fileName.toString() + INITIALIZING_SUFFIX);
    }

    private static void removeStaleInitialization(Path stagingPath) throws IOException {
        if (Files.deleteIfExists(stagingPath)) {
            flushParentDirectory(stagingPath);
        }
    }

    private static void cleanupStagedInitialization(Path stagingPath, Throwable failure) {
        try {
            if (Files.deleteIfExists(stagingPath)) {
                flushParentDirectory(stagingPath);
            }
        } catch (Throwable cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
    }

    private static void flushParentDirectory(Path target) throws IOException {
        Path parent = target.toAbsolutePath().normalize().getParent();
        if (parent != null) {
            Utils.flushDir(parent);
        }
    }

    private void closeAfterOpenFailure(WalIoBackend.Handle opened, Throwable failure) {
        if (opened != null) {
            try {
                opened.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
        try {
            backend.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static IOException propagateOpenFailure(Throwable failure) {
        if (failure instanceof IOException ioException) {
            return ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        return new IOException("Unexpected failure opening ring WAL file", failure);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Ring WAL file is closed");
        }
        if (checkpointFailure != null) {
            throw new IllegalStateException(
                "Ring WAL file is fenced after checkpoint failure; reopen required",
                checkpointFailure
            );
        }
    }
}
