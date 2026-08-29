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
package org.apache.kafka.storage.internals.log;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Lifecycle contract for an optional broker storage extension.
 *
 * <p>The Kafka broker owns this lifecycle. An extension is started before LogManager loads any partition and is closed
 * only after LogManager has shut down. The extension supplies a {@link UnifiedLogFactory}; it does not participate in
 * ReplicaManager, Partition, ISR, HW or Kafka protocol processing.</p>
 */
public interface KafkaStorageExtension extends AutoCloseable {
    void start(StorageExtensionContext context) throws IOException;

    UnifiedLogFactory unifiedLogFactory();

    /**
     * Returns the non-blocking observer for Kafka's completed local leader/follower transitions.
     *
     * <p>The default is a no-op so classic storage and extensions that do not need role ownership are unchanged.</p>
     */
    default StoragePartitionRoleListener partitionRoleListener() {
        return StoragePartitionRoleListener.NO_OP;
    }

    /**
     * Called after Kafka has opened its SocketServer acceptors but before BrokerServer transitions to STARTED.
     *
     * <p>Extensions that need ordinary Kafka clients can initialize them here. Broker startup waits for the returned
     * future using Kafka's normal startup deadline, so failing the future fails broker startup rather than exposing an
     * extension with partially recovered authoritative metadata. The default implementation is an immediate no-op.</p>
     */
    default CompletableFuture<Void> onBrokerReady(StorageExtensionBrokerContext context) {
        Objects.requireNonNull(context, "context");
        return CompletableFuture.completedFuture(null);
    }

    @Override
    void close() throws IOException;
}
