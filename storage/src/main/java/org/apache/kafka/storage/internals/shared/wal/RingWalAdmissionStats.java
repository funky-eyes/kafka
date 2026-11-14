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

/**
 * Snapshot of Ring WAL append admission credits, including groups that have
 * left the queue but not yet completed the writer's terminal I/O path.
 *
 * <p>Bytes measure retained encoded record payload/headers, not JVM heap or
 * physical device queue depth; a caller's own RecordBatch memory is excluded.
 * A rejected group is not an ACK or a physical WAL failure.</p>
 */
public record RingWalAdmissionStats(
    int outstandingGroups,
    long outstandingBytes,
    long rejectedGroups,
    int maxOutstandingGroups,
    long maxOutstandingBytes
) {
    public RingWalAdmissionStats {
        if (outstandingGroups < 0 || outstandingBytes < 0L || rejectedGroups < 0L ||
            maxOutstandingGroups <= 0 || maxOutstandingBytes <= 0L) {
            throw new IllegalArgumentException("Ring WAL admission counters and limits must be valid");
        }
    }
}
