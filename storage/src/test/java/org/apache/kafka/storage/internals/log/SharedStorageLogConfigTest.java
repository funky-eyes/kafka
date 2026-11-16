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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageLogConfigTest {
    @Test
    void ordinaryTopicDefaultsToLocalStorage() {
        assertFalse(new LogConfig(Map.of()).sharedStorageEnable());
        assertFalse(new LogConfig(Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, "false")).sharedStorageEnable());
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "TRUE", " true "})
    void explicitTopicSelectionIsParsedAsBoolean(String value) {
        assertTrue(new LogConfig(Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, value)).sharedStorageEnable());
        assertThrows(ConfigException.class,
            () -> new LogConfig(Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, "enabled")));
    }

    @Test
    void cannotCombineSharedAndTieredStorage() {
        assertThrows(InvalidConfigurationException.class, () -> LogConfig.validate(Map.of(
            TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, "true",
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")));
        LogConfig.validate(Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, "false",
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
    }
}
