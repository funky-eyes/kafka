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

import org.apache.kafka.common.TopicIdPartition;

import java.util.Collection;

/**
 * Notification seam for physical storage implementations that need to know which local replicas currently own
 * leader-only background work.
 *
 * <p>Kafka remains the sole authority for leader election. Implementations must treat this callback as a notification
 * only: it may run while ReplicaManager state-change synchronization is held, so it must not block or perform I/O.
 * The default listener is a no-op and therefore leaves classic Kafka behavior unchanged.</p>
 */
@FunctionalInterface
public interface StoragePartitionRoleListener {
    StoragePartitionRoleListener NO_OP = (leaders, followers) -> { };

    /**
     * Reports local partitions that have completed a transition to leader or follower.
     *
     * @param leaders local replicas that are leaders after the state transition
     * @param followers local replicas that are followers after the state transition
     */
    void onLeadershipChange(
        Collection<TopicIdPartition> leaders,
        Collection<TopicIdPartition> followers
    );
}
