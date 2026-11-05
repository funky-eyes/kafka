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

package org.apache.kafka.metadata;

import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;

/**
 * @param isCaughtUp           True if the heartbeat reply should tell the broker that it has caught up.
 * @param isFenced             True if the heartbeat reply should tell the broker that it is fenced.
 * @param inControlledShutdown True if the broker is currently in a controlled shutdown state.
 * @param shouldShutDown       True if the heartbeat reply should tell the broker that it should shut down.
 * @param sourceWalProbeChallenge An optional, non-authorizing diagnostic request.
 *                               Only a Controller-validated v3 heartbeat may carry it.
 */
public record BrokerHeartbeatReply(boolean isCaughtUp, boolean isFenced, boolean inControlledShutdown,
                                   boolean shouldShutDown,
                                   PartitionRetirementSourceProbe.Request sourceWalProbeChallenge) {
    /**
     * Keep the original four-argument broker heartbeat response contract for
     * all existing callers and for the stable v0-v2 wire versions.
     */
    public BrokerHeartbeatReply(boolean isCaughtUp, boolean isFenced, boolean inControlledShutdown,
                                boolean shouldShutDown) {
        this(isCaughtUp, isFenced, inControlledShutdown, shouldShutDown, null);
    }
}
