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

import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.metadata.CommittedObjectRetirementState.Entry;

import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

/**
 * Full-state immutable snapshot of experimental KRaft COMMITTED-object retirement.
 *
 * <p>The object ID terminal fence survives KRaft snapshot compaction.
 * Never collapse DELETED to absence while a pre-retirement producer or checkpoint
 * might reappear. The snapshot record is replay evidence, not permission to
 * delete an object; there is still no Controller production writer.</p>
 */
public record CommittedObjectRetirementImage(Map<Long, Entry> entries) {
    public static final CommittedObjectRetirementImage EMPTY =
        new CommittedObjectRetirementImage(Map.of());

    public CommittedObjectRetirementImage {
        entries = Map.copyOf(Objects.requireNonNull(entries, "entries"));
        for (Map.Entry<Long, Entry> entry : entries.entrySet()) {
            if (entry.getKey() != entry.getValue().objectId()) {
                throw new IllegalArgumentException("Object lifecycle image key differs from immutable object ID");
            }
        }
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public Entry get(long objectId) {
        return entries.get(objectId);
    }

    public void write(ImageWriter writer) {
        entries.entrySet().stream()
            .sorted(Comparator.comparingLong(Map.Entry::getKey))
            .forEach(entry -> writer.write(0, entry.getValue().toRecord()));
    }
}
