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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/** Request-thread scope for an opt-in tracked append under Kafka's log lock. */
final class SharedAppendScope implements AutoCloseable {
    private static final ThreadLocal<SharedAppendScope> CURRENT = new ThreadLocal<>();
    private final PartitionDurableOffsetTracker tracker;
    private final SharedPartitionId partition;
    private final long generation;
    private boolean consumed;
    private boolean reserved;
    private final CompletableFuture<Void> published = new CompletableFuture<>();

    private SharedAppendScope(PartitionDurableOffsetTracker tracker, SharedPartitionId partition, long generation) {
        this.tracker = tracker;
        this.partition = partition;
        this.generation = generation;
    }

    static SharedAppendScope open(PartitionDurableOffsetTracker tracker, SharedPartitionId partition, long generation) {
        Objects.requireNonNull(tracker, "tracker");
        Objects.requireNonNull(partition, "partition");
        if (CURRENT.get() != null) {
            throw new IllegalStateException("nested shared append scope");
        }
        SharedAppendScope scope = new SharedAppendScope(tracker, partition, generation);
        CURRENT.set(scope);
        return scope;
    }

    static SharedAppendScope current() {
        return CURRENT.get();
    }

    void claimPartition(SharedPartitionId actual) {
        if (!partition.equals(actual) || consumed) {
            throw new IllegalStateException("tracked append scope is for another partition or already consumed");
        }
        consumed = true;
    }

    void markReserved() {
        if (!consumed) {
            throw new IllegalStateException("tracked append scope was not claimed");
        }
        reserved = true;
    }

    boolean hasReservation() {
        return reserved;
    }

    CompletableFuture<Void> published() {
        return published;
    }

    void publish() {
        published.complete(null);
    }

    void fail(Throwable error) {
        published.completeExceptionally(error);
    }

    PartitionDurableOffsetTracker tracker() {
        return tracker;
    }

    long generation() {
        return generation;
    }

    @Override
    public void close() {
        if (CURRENT.get() != this) {
            throw new IllegalStateException("shared append scope closed by a different thread or out of order");
        }
        CURRENT.remove();
        if (reserved && !published.isDone()) {
            IllegalStateException failure = new IllegalStateException("tracked append exited before Kafka state publication");
            tracker.fence(failure);
            published.completeExceptionally(failure);
        }
    }
}
