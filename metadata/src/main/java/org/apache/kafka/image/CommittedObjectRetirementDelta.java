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
package org.apache.kafka.image;

import org.apache.kafka.common.metadata.CommittedObjectRetirementRecord;
import org.apache.kafka.metadata.CommittedObjectRetirementState;
import org.apache.kafka.metadata.CommittedObjectRetirementState.Entry;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Broker/KRaft metadata image delta. The authoritative Controller separately
 * verifies exact append offsets; this image only preserves replayed values
 * across log compaction and snapshot generation. Never authorizes DELETE.
 */
public final class CommittedObjectRetirementDelta {
    private final CommittedObjectRetirementImage image;
    private final Map<Long, Entry> changed = new HashMap<>();
    private boolean snapshot = false;

    public CommittedObjectRetirementDelta(CommittedObjectRetirementImage image) {
        this.image = Objects.requireNonNull(image, "image");
    }

    public void replay(CommittedObjectRetirementRecord record) {
        Entry next = Entry.fromRecord(record);
        Entry previous = changed.containsKey(next.objectId())
            ? changed.get(next.objectId()) : image.get(next.objectId());
        // A new image may contain only the last lifecycle stage from a trusted
        // compacted snapshot. When a previous value exists, enforce exact CAS
        // and immutable object identity on every later transition.
        if (previous != null) {
            CommittedObjectRetirementState.validateTransition(previous, next);
        }
        changed.put(next.objectId(), next);
    }

    public void finishSnapshot() {
        // A trusted newer snapshot must not silently forget any previously
        // accepted intent or terminal object-ID fence. Fail the entire
        // metadata load instead of reopening a deleted physical object.
        for (long objectId : image.entries().keySet()) {
            if (!changed.containsKey(objectId)) {
                throw new IllegalStateException(
                    "KRaft snapshot omitted a previously accepted physical object lifecycle: " + objectId
                );
            }
        }
        snapshot = true;
    }

    public CommittedObjectRetirementImage apply() {
        Map<Long, Entry> merged = new HashMap<>();
        if (!snapshot) {
            merged.putAll(image.entries());
        }
        merged.putAll(changed);
        return new CommittedObjectRetirementImage(merged);
    }
}
