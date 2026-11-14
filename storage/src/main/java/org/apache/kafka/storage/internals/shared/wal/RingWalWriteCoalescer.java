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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Coalesces physically adjacent, small Ring WAL fragments before NIO/AIO
 * submission. Does not re-order writes or change the GROUP_COMMIT barrier.
 *
 * <p>Only contiguous physical addresses are eligible; a ring wrap changes the
 * physical position and therefore cannot be merged across generations.
 * Large payloads remain zero-copy views of their existing backing buffers.
 * Temporary compacted buffers are capped at 64 KiB each and remain valid
 * until the synchronous writeBatch contract drains all AIO completions.</p>
 */
final class RingWalWriteCoalescer {
    static final int MAX_COALESCED_BYTES = 64 * 1024;

    private RingWalWriteCoalescer() {
    }

    static List<WalIoBackend.WriteRequest> coalesce(List<WalIoBackend.WriteRequest> writes) {
        WalIoBackend.validateWriteBatch(writes);
        List<WalIoBackend.WriteRequest> compacted = new ArrayList<>(writes.size());
        int index = 0;
        while (index < writes.size()) {
            WalIoBackend.WriteRequest first = writes.get(index);
            int total = first.source().remaining();
            if (total >= MAX_COALESCED_BYTES) {
                compacted.add(first);
                index++;
                continue;
            }

            long nextPosition = first.position() + total;
            int end = index + 1;
            while (end < writes.size()) {
                WalIoBackend.WriteRequest next = writes.get(end);
                int nextBytes = next.source().remaining();
                if (next.position() != nextPosition || nextBytes > MAX_COALESCED_BYTES - total) {
                    break;
                }
                total += nextBytes;
                nextPosition += nextBytes;
                end++;
            }
            if (end == index + 1) {
                compacted.add(first);
                index++;
                continue;
            }

            ByteBuffer combined = ByteBuffer.allocate(total);
            for (int i = index; i < end; i++) {
                // Duplicates preserve positions of every original request and
                // all user RecordBatch buffers while copying small fragments.
                combined.put(writes.get(i).source().duplicate());
            }
            combined.flip();
            compacted.add(new WalIoBackend.WriteRequest(combined, first.position()));
            index = end;
        }
        return compacted;
    }
}
