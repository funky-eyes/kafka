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
package org.apache.kafka.storage.internals.shared.metadata;

/** Immutable mapping from one Kafka logical offset range to bytes in a shared object. */
public record SharedObjectRange(
    SharedPartitionId partition,
    OffsetRange offsets,
    int leaderEpoch,
    long objectPosition,
    int objectLength,
    long checksum
) {
    public SharedObjectRange {
        if (partition == null || offsets == null) {
            throw new IllegalArgumentException("partition and offsets must not be null");
        }
        if (objectPosition < 0 || objectLength <= 0) {
            throw new IllegalArgumentException("invalid object byte range");
        }
    }
}
