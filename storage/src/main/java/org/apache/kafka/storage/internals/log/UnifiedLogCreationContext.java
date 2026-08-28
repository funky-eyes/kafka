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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.File;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

/**
 * Stable carrier for parameters needed to create a {@link UnifiedLog}.
 *
 * <p>Keeping the factory API to one context object localizes Kafka-internal constructor churn to context construction
 * when upgrading Kafka versions. Storage implementations do not need a new SPI method signature every time Kafka adds
 * another creation parameter.</p>
 */
public record UnifiedLogCreationContext(
    File dir,
    LogConfig config,
    long logStartOffset,
    long recoveryPoint,
    Scheduler scheduler,
    BrokerTopicStats brokerTopicStats,
    Time time,
    int maxTransactionTimeoutMs,
    ProducerStateManagerConfig producerStateManagerConfig,
    int producerIdExpirationCheckIntervalMs,
    LogDirFailureChannel logDirFailureChannel,
    boolean lastShutdownClean,
    Optional<Uuid> topicId,
    ConcurrentMap<String, Integer> numRemainingSegments,
    boolean remoteStorageSystemEnable,
    LogOffsetsListener logOffsetsListener
) {
    public UnifiedLogCreationContext {
        Objects.requireNonNull(dir, "dir");
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(brokerTopicStats, "brokerTopicStats");
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(producerStateManagerConfig, "producerStateManagerConfig");
        Objects.requireNonNull(logDirFailureChannel, "logDirFailureChannel");
        Objects.requireNonNull(topicId, "topicId");
        Objects.requireNonNull(numRemainingSegments, "numRemainingSegments");
        Objects.requireNonNull(logOffsetsListener, "logOffsetsListener");
    }
}
