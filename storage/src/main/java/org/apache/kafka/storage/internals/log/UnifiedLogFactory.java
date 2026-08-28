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

/**
 * Narrow creation seam for {@link UnifiedLog} implementations.
 *
 * <p>The default implementation is exactly Kafka's existing {@link UnifiedLog#create} path. Alternate physical
 * storage implementations can plug in without changing ReplicaManager, Partition, ISR/HW or request protocols.</p>
 */
@FunctionalInterface
public interface UnifiedLogFactory {
    UnifiedLogFactory DEFAULT = context -> UnifiedLog.create(
        context.dir(),
        context.config(),
        context.logStartOffset(),
        context.recoveryPoint(),
        context.scheduler(),
        context.brokerTopicStats(),
        context.time(),
        context.maxTransactionTimeoutMs(),
        context.producerStateManagerConfig(),
        context.producerIdExpirationCheckIntervalMs(),
        context.logDirFailureChannel(),
        context.lastShutdownClean(),
        context.topicId(),
        context.numRemainingSegments(),
        context.remoteStorageSystemEnable(),
        context.logOffsetsListener()
    );

    UnifiedLog create(UnifiedLogCreationContext context) throws IOException;
}
