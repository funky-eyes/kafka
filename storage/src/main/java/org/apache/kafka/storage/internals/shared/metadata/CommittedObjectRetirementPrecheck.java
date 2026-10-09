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
package org.apache.kafka.storage.internals.shared.metadata;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Read-only logical retirement precheck for a committed physical object.
 *
 * <p>The precheck requires an explicitly replayed log-start watermark for
 * <em>every</em> range, including ranges packed from other partitions or topic IDs.
 * A range is entirely obsolete only when its exclusive end offset is at or below
 * the durable inclusive log start. Missing watermarks and partially intersecting
 * RecordBatch ranges fail closed.</p>
 *
 * <p>Even {@link Finding#ALL_RANGES_BELOW_LOG_START} is NOT permission to delete:
 * authoritative monotonic writers, in-flight upload fencing, reader quiescence,
 * durable reference retirement, and physical-delete retry have not been wired yet.
 * This class neither mutates the remote index nor accesses an object store.</p>
 */
public final class CommittedObjectRetirementPrecheck {
    private CommittedObjectRetirementPrecheck() {
    }

    public static Finding assess(SharedObjectMetadata object, SharedMetadataImage image) {
        Objects.requireNonNull(object, "object");
        Objects.requireNonNull(image, "image");
        return assessRanges(object, image.partitionLogStartsSnapshot());
    }

    /**
     * Classifies the replayed COMMITTED-object inventory against one consistent watermark snapshot.
     *
     * <p>Every finding is advisory and read-only. In particular, an expired range is not evidence
     * that a reader has quiesced, a writer is fenced, or a remote reference has been retired.</p>
     */
    public static List<ObjectFinding> assessCommitted(SharedMetadataImage image) {
        return assessCommittedSnapshot(image).findings();
    }

    /**
     * Returns an immutable diagnostic classification with its consumed metadata log offset.
     * The offset is not a durable GC authorization or proof of consumer catch-up.
     */
    public static AssessmentSnapshot assessCommittedSnapshot(SharedMetadataImage image) {
        Objects.requireNonNull(image, "image");
        SharedMetadataImage.RetirementEvidenceSnapshot evidence = image.retirementEvidenceSnapshot();
        List<ObjectFinding> findings = evidence.committedObjects().stream()
            .map(object -> new ObjectFinding(
                object.objectId(), assessRanges(object, evidence.partitionLogStarts())
            ))
            .toList();
        return new AssessmentSnapshot(evidence.lastConsumedMetadataOffset(), findings);
    }

    private static Finding assessRanges(SharedObjectMetadata object, Map<SharedPartitionId, Long> watermarks) {
        boolean missingWatermark = false;
        boolean hasUnretiredRange = false;
        for (SharedObjectRange range : object.ranges()) {
            Long startOffset = watermarks.get(range.partition());
            if (startOffset == null) {
                missingWatermark = true;
            } else if (range.offsets().endOffset() > startOffset) {
                hasUnretiredRange = true;
            }
        }
        if (missingWatermark) {
            return Finding.MISSING_WATERMARK;
        }
        if (hasUnretiredRange) {
            return Finding.HAS_UNRETIRED_RANGE;
        }
        return Finding.ALL_RANGES_BELOW_LOG_START;
    }

    public enum Finding {
        MISSING_WATERMARK,
        HAS_UNRETIRED_RANGE,
        ALL_RANGES_BELOW_LOG_START
    }

    public record AssessmentSnapshot(long lastConsumedMetadataOffset, List<ObjectFinding> findings) {
        public AssessmentSnapshot {
            if (lastConsumedMetadataOffset < -1L) {
                throw new IllegalArgumentException("lastConsumedMetadataOffset must be >= -1");
            }
            findings = List.copyOf(Objects.requireNonNull(findings, "findings"));
        }
    }

    public record ObjectFinding(long objectId, Finding finding) {
        public ObjectFinding {
            if (objectId <= 0L) {
                throw new IllegalArgumentException("objectId must be positive");
            }
            Objects.requireNonNull(finding, "finding");
        }
    }
}
