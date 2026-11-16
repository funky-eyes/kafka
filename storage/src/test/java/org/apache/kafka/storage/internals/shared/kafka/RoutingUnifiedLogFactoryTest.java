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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RoutingUnifiedLogFactoryTest {
    @TempDir
    Path tempDir;

    @Test
    void absentAndExplicitFalseTopicConfigsSelectNativeFactory() throws Exception {
        for (Map<String, ?> props : List.of(Map.<String, Object>of(),
            Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, false))) {
            UnifiedLogFactory shared = mock(UnifiedLogFactory.class);
            UnifiedLogFactory classic = mock(UnifiedLogFactory.class);
            UnifiedLogCreationContext context = context("events-0", props);
            UnifiedLog expected = mock(UnifiedLog.class);
            when(classic.create(context)).thenReturn(expected);
            assertSame(expected, new RoutingUnifiedLogFactory(configuration(false), shared, classic).create(context));
            verify(shared, never()).create(context);
        }
    }

    @Test
    void explicitTrueSelectsSharedFactoryWithoutBrokerWhitelist() throws Exception {
        UnifiedLogFactory shared = mock(UnifiedLogFactory.class);
        UnifiedLogFactory classic = mock(UnifiedLogFactory.class);
        UnifiedLogCreationContext context = context("events-0", Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, true));
        UnifiedLog expected = mock(UnifiedLog.class);
        when(shared.create(context)).thenReturn(expected);
        assertSame(expected, new RoutingUnifiedLogFactory(configuration(false), shared, classic).create(context));
        verify(classic, never()).create(context);
    }

    @Test
    void existingSharedMarkerRejectsFalseBeforeNativeFactoryCanReadPlaceholder() throws Exception {
        UnifiedLogFactory shared = mock(UnifiedLogFactory.class);
        UnifiedLogFactory classic = mock(UnifiedLogFactory.class);
        UnifiedLogCreationContext context = context("events-0", Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, false));
        SharedStorageModeMarker.prepareShared(context, false);
        assertThrows(IOException.class, () -> new RoutingUnifiedLogFactory(configuration(false), shared, classic).create(context));
        verify(classic, never()).create(context);
    }

    @Test
    void legacyDefaultAllStillHonorsExplicitFalseOnNewTopic() throws Exception {
        UnifiedLogFactory shared = mock(UnifiedLogFactory.class);
        UnifiedLogFactory classic = mock(UnifiedLogFactory.class);
        UnifiedLogCreationContext context = context("events-0", Map.of(TopicConfig.SHARED_STORAGE_ENABLE_CONFIG, false));
        UnifiedLog expected = mock(UnifiedLog.class);
        when(classic.create(context)).thenReturn(expected);
        RoutingUnifiedLogFactory routing = new RoutingUnifiedLogFactory(configuration(true), shared, classic);
        assertSame(expected, routing.create(context));
        assertSame(expected, routing.create(context));
        verify(shared, never()).create(context);
    }

    private SharedStorageConfiguration configuration(boolean legacy) {
        return SharedStorageConfiguration.from(new StorageExtensionContext(
            Map.of(SharedStorageConfiguration.LEGACY_SELECTORS_ENABLED_CONFIG, legacy),
            List.of(tempDir.resolve("logs").toFile()), 1, Time.SYSTEM));
    }

    private UnifiedLogCreationContext context(String name, Map<String, ?> props) {
        UnifiedLogCreationContext context = mock(UnifiedLogCreationContext.class);
        when(context.dir()).thenReturn(tempDir.resolve(name).toFile());
        when(context.topicId()).thenReturn(Optional.of(new Uuid(1L, 2L)));
        when(context.config()).thenReturn(new LogConfig(props));
        when(context.logDirFailureChannel()).thenReturn(new LogDirFailureChannel(1));
        return context;
    }
}
