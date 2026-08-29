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
package org.apache.kafka.storage.internals.shared.object;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Broker-local registry used to keep orphan cleanup away from uploads that are still executing in this process. */
public final class ActiveObjectUploads {
    private final Set<Long> objectIds = ConcurrentHashMap.newKeySet();

    public void begin(long objectId) {
        if (objectId <= 0) {
            throw new IllegalArgumentException("objectId must be positive");
        }
        if (!objectIds.add(objectId)) {
            throw new IllegalStateException("Object upload is already active: " + objectId);
        }
    }

    public void end(long objectId) {
        objectIds.remove(objectId);
    }

    public boolean contains(long objectId) {
        return objectIds.contains(objectId);
    }
}
