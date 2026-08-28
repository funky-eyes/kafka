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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageConfigurationTest {
    @Test
    void defaultsToAllNonInternalTopicsAndBrokerWideWalDefaults() {
        File logDir = TestUtils.tempDirectory();
        SharedStorageConfiguration config = SharedStorageConfiguration.from(context(Map.of(), logDir));

        assertEquals(SharedStorageConfiguration.DEFAULT_WAL_CAPACITY_BYTES, config.walCapacityBytes());
        assertEquals(SharedStorageConfiguration.DEFAULT_WAL_SEGMENT_BYTES, config.walSegmentBytes());
        assertTrue(config.walDir().startsWith(logDir.toPath().toAbsolutePath()));
        assertTrue(config.useSharedStorage("events"));
        assertFalse(config.useSharedStorage("__consumer_offsets"));
        assertFalse(config.useSharedStorage("__transaction_state"));
        assertFalse(config.useSharedStorage("__shared_storage_metadata"));
    }

    @Test
    void combinesExactAndPatternSelectors() {
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.TOPICS_CONFIG, "orders, payments");
        originals.put(SharedStorageConfiguration.TOPIC_PATTERN_CONFIG, "cdc-.*");
        SharedStorageConfiguration config = SharedStorageConfiguration.from(
            context(originals, TestUtils.tempDirectory())
        );

        assertTrue(config.useSharedStorage("orders"));
        assertTrue(config.useSharedStorage("payments"));
        assertTrue(config.useSharedStorage("cdc-users"));
        assertFalse(config.useSharedStorage("audit"));
        assertFalse(config.useSharedStorage("__internal"));
    }

    @Test
    void validatesWalCapacityAndSegmentSize() {
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, 1024L);
        originals.put(SharedStorageConfiguration.WAL_SEGMENT_BYTES_CONFIG, 2048L);

        assertThrows(
            IllegalArgumentException.class,
            () -> SharedStorageConfiguration.from(context(originals, TestUtils.tempDirectory()))
        );
    }

    private static StorageExtensionContext context(Map<String, ?> originals, File logDir) {
        return new StorageExtensionContext(originals, List.of(logDir), 3, Time.SYSTEM);
    }
}
