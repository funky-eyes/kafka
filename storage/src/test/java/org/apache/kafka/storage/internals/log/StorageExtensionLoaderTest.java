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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageExtensionLoaderTest {
    @AfterEach
    void resetState() {
        TestStorageExtension.STARTED.set(false);
        TestStorageExtension.CLOSED.set(false);
    }

    @Test
    void returnsEmptyWhenNoExtensionIsConfigured() throws IOException {
        StorageExtensionContext context = context(Map.of());
        assertTrue(StorageExtensionLoader.load(context).isEmpty());
    }

    @Test
    void loadsStartsAndClosesConfiguredExtension() throws IOException {
        StorageExtensionContext context = context(Map.of(
            StorageExtensionLoader.STORAGE_EXTENSION_CLASS_CONFIG,
            TestStorageExtension.class
        ));

        Optional<KafkaStorageExtension> extension = StorageExtensionLoader.load(context);
        assertTrue(extension.isPresent());
        assertTrue(TestStorageExtension.STARTED.get());
        assertFalse(TestStorageExtension.CLOSED.get());
        extension.get().close();
        assertTrue(TestStorageExtension.CLOSED.get());
    }

    @Test
    void rejectsClassesThatDoNotImplementStorageExtension() {
        StorageExtensionContext context = context(Map.of(
            StorageExtensionLoader.STORAGE_EXTENSION_CLASS_CONFIG,
            String.class
        ));
        assertThrows(IOException.class, () -> StorageExtensionLoader.load(context));
    }

    private static StorageExtensionContext context(Map<String, ?> originals) {
        File logDir = TestUtils.tempDirectory();
        return new StorageExtensionContext(originals, List.of(logDir), 7, Time.SYSTEM);
    }

    public static final class TestStorageExtension implements KafkaStorageExtension {
        private static final AtomicBoolean STARTED = new AtomicBoolean(false);
        private static final AtomicBoolean CLOSED = new AtomicBoolean(false);

        @Override
        public void start(StorageExtensionContext context) {
            STARTED.set(true);
        }

        @Override
        public UnifiedLogFactory unifiedLogFactory() {
            return UnifiedLogFactory.DEFAULT;
        }

        @Override
        public void close() {
            CLOSED.set(true);
        }
    }
}
