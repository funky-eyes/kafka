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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StoragePartitionRoleListenerEpochCompatibilityTest {
    @Test
    void epochAwareMethodPreservesTheLegacyFunctionalCallback() {
        TopicIdPartition leader = partition(0);
        TopicIdPartition follower = partition(1);
        List<TopicIdPartition> leaderEvents = new ArrayList<>();
        List<TopicIdPartition> followerEvents = new ArrayList<>();
        AtomicInteger callbacks = new AtomicInteger();
        StoragePartitionRoleListener listener = (leaders, followers) -> {
            callbacks.incrementAndGet();
            leaderEvents.addAll(leaders);
            followerEvents.addAll(followers);
        };

        listener.onLeadershipChangeWithEpochs(Map.of(leader, 12), Map.of(follower, 13));

        assertEquals(1, callbacks.get());
        assertEquals(List.of(leader), leaderEvents);
        assertEquals(List.of(follower), followerEvents);
    }

    @Test
    void defaultNoOpListenerAlsoAcceptsEpochAwareCallback() {
        StoragePartitionRoleListener.NO_OP.onLeadershipChangeWithEpochs(
            Map.of(partition(0), 0), Map.of(partition(1), 1)
        );
    }

    private static TopicIdPartition partition(int partition) {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", partition));
    }
}
