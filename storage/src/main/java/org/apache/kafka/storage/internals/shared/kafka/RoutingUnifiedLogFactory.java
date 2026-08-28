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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.internals.log.UnifiedLogCreationContext;
import org.apache.kafka.storage.internals.log.UnifiedLogFactory;

import java.io.IOException;
import java.util.Objects;

/** Routes log creation while keeping classic Kafka as the default implementation for non-selected topics. */
public final class RoutingUnifiedLogFactory implements UnifiedLogFactory {
    private final SharedStorageConfiguration configuration;
    private final UnifiedLogFactory sharedFactory;
    private final UnifiedLogFactory classicFactory;

    public RoutingUnifiedLogFactory(
        SharedStorageConfiguration configuration,
        UnifiedLogFactory sharedFactory
    ) {
        this(configuration, sharedFactory, UnifiedLogFactory.DEFAULT);
    }

    RoutingUnifiedLogFactory(
        SharedStorageConfiguration configuration,
        UnifiedLogFactory sharedFactory,
        UnifiedLogFactory classicFactory
    ) {
        this.configuration = Objects.requireNonNull(configuration, "configuration");
        this.sharedFactory = Objects.requireNonNull(sharedFactory, "sharedFactory");
        this.classicFactory = Objects.requireNonNull(classicFactory, "classicFactory");
    }

    @Override
    public UnifiedLog create(UnifiedLogCreationContext context) throws IOException {
        Objects.requireNonNull(context, "context");
        TopicPartition topicPartition = UnifiedLog.parseTopicPartitionName(context.dir());
        UnifiedLogFactory selectedFactory = configuration.useSharedStorage(topicPartition.topic())
            ? sharedFactory
            : classicFactory;
        return selectedFactory.create(context);
    }
}
