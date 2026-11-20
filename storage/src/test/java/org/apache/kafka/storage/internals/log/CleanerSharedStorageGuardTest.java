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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.shared.kafka.SharedUnifiedLog;
import org.apache.kafka.storage.internals.utils.Throttler;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CleanerSharedStorageGuardTest {
    @Test
    void rejectsSharedStorageBeforeReadingOrCreatingReplacement() {
        MockTime time = new MockTime();
        OffsetMap map = new LogTestUtils.FakeOffsetMap();
        Cleaner cleaner = new Cleaner(0, map, 1024, 1024 * 1024, 0.75,
            new Throttler(Double.MAX_VALUE, Long.MAX_VALUE, "cleaner", "bytes", time), time, tp -> { });
        UnifiedLog shared = mock(SharedUnifiedLog.class, CALLS_REAL_METHODS);
        doReturn(new TopicPartition("shared", 0)).when(shared).topicPartition();
        LogToClean cleanable = mock(LogToClean.class);
        when(cleanable.log()).thenReturn(shared);
        CleanerStats stats = new CleanerStats(time);

        assertThrows(IllegalStateException.class, () -> cleaner.doClean(cleanable, time.milliseconds()));
        assertThrows(IllegalStateException.class, () -> cleaner.buildOffsetMap(shared, 0, 10, map, stats));
        assertThrows(IllegalStateException.class, () -> cleaner.cleanSegments(
            shared, List.of(), map, time.milliseconds(), stats, new CleanedTransactionMetadata(), 0, 10));
        verify(shared, never()).logSegments(anyLong(), anyLong());
        verify(shared, never()).dir();
    }
    @Test
    void sharedLogIsExcludedFromCompactionButStillEligibleForRetention() {
        TopicPartition tp = new TopicPartition("shared-delete", 0);
        UnifiedLog shared = mock(SharedUnifiedLog.class, CALLS_REAL_METHODS);
        Properties props = new Properties();
        props.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete");
        doReturn(new LogConfig(props)).when(shared).config();
        doReturn("/test-log-dir").when(shared).parentDir();
        ConcurrentHashMap<TopicPartition, UnifiedLog> logs = new ConcurrentHashMap<>();
        logs.put(tp, shared);
        LogCleanerManager manager = new LogCleanerManager(List.of(), logs, new LogDirFailureChannel(1));
        assertTrue(manager.grabFilthiestCompactedLog(new MockTime(), new PreCleanStats()).isEmpty());
        assertTrue(manager.grabFilthiestCompactedLog(new MockTime(), new PreCleanStats()).isEmpty());
        assertTrue(manager.uncleanablePartitions("/test-log-dir").isEmpty());
        assertEquals(shared, manager.deletableLogs().get(tp));
    }

}
