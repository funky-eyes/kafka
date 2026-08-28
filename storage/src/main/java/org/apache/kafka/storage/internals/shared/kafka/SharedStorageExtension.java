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

import org.apache.kafka.storage.internals.log.KafkaStorageExtension;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;
import org.apache.kafka.storage.internals.shared.SharedStorageEngine;
import org.apache.kafka.storage.internals.shared.wal.FileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;

import java.io.IOException;
import java.util.Objects;

/**
 * Broker-wide shared WAL extension.
 *
 * <p>Exactly one WAL/engine is created per broker extension instance. All selected topic-partitions on the broker share
 * that physical WAL; topic routing only controls which UnifiedLog implementation is created.</p>
 */
public final class SharedStorageExtension implements KafkaStorageExtension {
    private SharedStorageEngine storage;
    private UnifiedLogFactory unifiedLogFactory;

    @Override
    public synchronized void start(StorageExtensionContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        if (storage != null) {
            throw new IllegalStateException("Shared storage extension is already started");
        }

        SharedStorageConfiguration configuration = SharedStorageConfiguration.from(context);
        SharedWal wal = new FileSharedWal(
            configuration.walDir(),
            configuration.walCapacityBytes(),
            configuration.walSegmentBytes()
        );
        try {
            storage = new SharedStorageEngine(wal);
            unifiedLogFactory = new RoutingUnifiedLogFactory(
                configuration,
                new SharedUnifiedLogFactory(storage)
            );
        } catch (Throwable t) {
            try {
                wal.close();
            } catch (Throwable closeError) {
                t.addSuppressed(closeError);
            }
            if (t instanceof IOException ioException) {
                throw ioException;
            }
            if (t instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (t instanceof Error error) {
                throw error;
            }
            throw new IOException("Unable to initialize shared storage extension", t);
        }
    }

    @Override
    public synchronized UnifiedLogFactory unifiedLogFactory() {
        if (unifiedLogFactory == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        return unifiedLogFactory;
    }

    public synchronized SharedStorageEngine storage() {
        if (storage == null) {
            throw new IllegalStateException("Shared storage extension has not been started");
        }
        return storage;
    }

    @Override
    public synchronized void close() throws IOException {
        SharedStorageEngine currentStorage = storage;
        storage = null;
        unifiedLogFactory = null;
        if (currentStorage != null) {
            currentStorage.close();
        }
    }
}
