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

import java.util.Objects;

/**
 * Read-only value-domain precheck for a future fenced partition log-start writer.
 *
 * <p>This class neither writes to Kafka nor grants a lease. All candidate findings
 * remain advisory: a future writer must additionally prove topic incarnation,
 * current leadership/generation, mixed-version compatibility, exclusive
 * transactional ownership, metadata catch-up, and source log-start freshness
 * before committing any record. Even a persisted log start is not physical-GC
 * permission.</p>
 */
public final class PartitionLogStartAdvancePrecheck {
    private PartitionLogStartAdvancePrecheck() {
    }

    /**
     * Classifies a candidate against one immutable replay snapshot.
     *
     * @param requestedStartOffset proposed inclusive log start
     * @param observedKafkaLogStart latest log start read from the actual Kafka source log
     * @param requiredMetadataOffset last record offset at a caller-established read-committed
     *                               replay horizon, or -1 when the metadata topic is empty
     */
    public static Finding assess(
        SharedMetadataImage image,
        SharedPartitionId partition,
        long requestedStartOffset,
        long observedKafkaLogStart,
        long requiredMetadataOffset
    ) {
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(partition, "partition");
        if (requestedStartOffset < 0L || observedKafkaLogStart < 0L || requiredMetadataOffset < -1L) {
            throw new IllegalArgumentException("Invalid partition log-start precheck offset");
        }

        SharedMetadataImage.RetirementEvidenceSnapshot evidence = image.retirementEvidenceSnapshot();
        if (evidence.lastConsumedMetadataOffset() < requiredMetadataOffset) {
            return Finding.METADATA_REPLAY_BEHIND;
        }
        if (requestedStartOffset > observedKafkaLogStart) {
            return Finding.EXCEEDS_OBSERVED_KAFKA_LOG_START;
        }

        Long previous = evidence.partitionLogStarts().get(partition);
        if (previous == null) {
            // First write for a topic incarnation must explicitly persist zero.
            // Never infer a missing generation-specific watermark from another topic.
            return requestedStartOffset == 0L
                ? Finding.INITIAL_ZERO_CANDIDATE
                : Finding.INITIAL_ZERO_REQUIRED;
        }
        if (requestedStartOffset < previous) {
            return Finding.REGRESSED_BELOW_REPLAYED_WATERMARK;
        }
        if (requestedStartOffset == previous) {
            return Finding.ALREADY_REPLAYED;
        }
        return Finding.VALUE_DOMAIN_CANDIDATE;
    }

    public enum Finding {
        METADATA_REPLAY_BEHIND,
        EXCEEDS_OBSERVED_KAFKA_LOG_START,
        INITIAL_ZERO_REQUIRED,
        INITIAL_ZERO_CANDIDATE,
        REGRESSED_BELOW_REPLAYED_WATERMARK,
        ALREADY_REPLAYED,
        VALUE_DOMAIN_CANDIDATE
    }
}
