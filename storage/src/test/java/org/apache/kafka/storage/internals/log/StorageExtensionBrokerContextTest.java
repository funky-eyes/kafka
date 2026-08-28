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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StorageExtensionBrokerContextTest {
    @Test
    void snapshotsListenersAndNullableBrokerOriginals() {
        Endpoint endpoint = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9092);
        Map<String, Object> originals = new HashMap<>();
        originals.put("nullable", null);
        originals.put("key", "value");

        StorageExtensionBrokerContext context = new StorageExtensionBrokerContext(
            "cluster-a",
            7,
            List.of(endpoint),
            originals,
            Time.SYSTEM
        );
        originals.put("key", "changed");

        assertEquals("cluster-a", context.clusterId());
        assertEquals(7, context.brokerId());
        assertEquals(List.of(endpoint), context.listeners());
        assertEquals("value", context.originals().get("key"));
        assertNull(context.originals().get("nullable"));
        assertThrows(UnsupportedOperationException.class, () -> context.originals().put("x", "y"));
        assertThrows(UnsupportedOperationException.class, () -> context.listeners().add(endpoint));
    }

    @Test
    void rejectsMissingNetworkIdentity() {
        Endpoint endpoint = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9092);
        assertThrows(
            IllegalArgumentException.class,
            () -> new StorageExtensionBrokerContext(" ", 1, List.of(endpoint), Map.of(), Time.SYSTEM)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> new StorageExtensionBrokerContext("cluster-a", 1, List.of(), Map.of(), Time.SYSTEM)
        );
    }

    @Test
    void defaultBrokerReadyCallbackIsImmediateNoOp() throws Exception {
        Endpoint endpoint = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9092);
        StorageExtensionBrokerContext context = new StorageExtensionBrokerContext(
            "cluster-a",
            1,
            List.of(endpoint),
            Map.of(),
            Time.SYSTEM
        );
        KafkaStorageExtension extension = new KafkaStorageExtension() {
            @Override
            public void start(StorageExtensionContext ignored) throws IOException {
            }

            @Override
            public UnifiedLogFactory unifiedLogFactory() {
                return UnifiedLogFactory.DEFAULT;
            }

            @Override
            public void close() throws IOException {
            }
        };

        assertNull(extension.onBrokerReady(context).get());
    }
}
