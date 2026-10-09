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

import org.junit.jupiter.api.Test;

import static org.apache.kafka.storage.internals.shared.metadata.PartitionLogStartAdvancePrecheck.Finding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PartitionLogStartAdvancePrecheckTest {
    private static final SharedPartitionId PARTITION = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId RECREATED = new SharedPartitionId(1L, 3L, 0);

    @Test
    void missingInitialWatermarkRequiresAnExplicitZeroEvenForAdvancedKafkaLog() {
        SharedMetadataImage image = readyImage();

        assertEquals(Finding.INITIAL_ZERO_REQUIRED, assess(image, PARTITION, 10L, 10L, -1L));
        assertEquals(Finding.INITIAL_ZERO_CANDIDATE, assess(image, PARTITION, 0L, 10L, -1L));
        assertEquals(0, image.partitionLogStartsSnapshot().size());
    }

    @Test
    void candidateMustNotExceedKafkaObservedLogStart() {
        SharedMetadataImage image = readyImage();
        assertEquals(Finding.EXCEEDS_OBSERVED_KAFKA_LOG_START, assess(image, PARTITION, 11L, 10L, -1L));
    }

    @Test
    void metadataReplayMustReachTheRequiredHorizon() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(5L),
            10L
        );
        image.markReady();

        assertEquals(Finding.METADATA_REPLAY_BEHIND, assess(image, PARTITION, 6L, 6L, 11L));
        assertEquals(Finding.VALUE_DOMAIN_CANDIDATE, assess(image, PARTITION, 6L, 6L, 10L));
    }

    @Test
    void lowerThanDurableWatermarkIsNeverAnAdmissibleValue() {
        SharedMetadataImage image = imageWithWatermark(20L);
        assertEquals(Finding.REGRESSED_BELOW_REPLAYED_WATERMARK, assess(image, PARTITION, 19L, 30L, 5L));
        assertEquals(20L, image.partitionLogStartOffset(PARTITION));
    }

    @Test
    void equalityIsIdempotentButDoesNotWrite() {
        SharedMetadataImage image = imageWithWatermark(20L);
        assertEquals(Finding.ALREADY_REPLAYED, assess(image, PARTITION, 20L, 20L, 5L));
    }

    @Test
    void valueDomainCandidateNeverChangesTheReplayImage() {
        SharedMetadataImage image = imageWithWatermark(20L);
        assertEquals(Finding.VALUE_DOMAIN_CANDIDATE, assess(image, PARTITION, 30L, 30L, 5L));
        assertEquals(20L, image.partitionLogStartOffset(PARTITION));
    }

    @Test
    void recreatedTopicDoesNotInheritTheOriginalTopicWatermark() {
        SharedMetadataImage image = imageWithWatermark(20L);
        assertEquals(Finding.INITIAL_ZERO_REQUIRED, assess(image, RECREATED, 10L, 10L, 5L));
        assertEquals(Finding.INITIAL_ZERO_CANDIDATE, assess(image, RECREATED, 0L, 10L, 5L));
    }

    @Test
    void recoveringAndFailedImagesFailClosed() {
        SharedMetadataImage image = new SharedMetadataImage();
        assertThrows(IllegalStateException.class, () -> assess(image, PARTITION, 0L, 0L, -1L));
        image.markFailed(new IllegalStateException("replay failed"));
        assertThrows(IllegalStateException.class, () -> assess(image, PARTITION, 0L, 0L, -1L));
    }

    @Test
    void invalidOffsetsAreNotSilentlyNormalized() {
        SharedMetadataImage image = readyImage();
        assertThrows(IllegalArgumentException.class, () -> assess(image, PARTITION, -1L, 0L, -1L));
        assertThrows(IllegalArgumentException.class, () -> assess(image, PARTITION, 0L, -1L, -1L));
        assertThrows(IllegalArgumentException.class, () -> assess(image, PARTITION, 0L, 0L, -2L));
    }

    private static SharedMetadataImage readyImage() {
        SharedMetadataImage image = new SharedMetadataImage();
        image.markReady();
        return image;
    }

    private static SharedMetadataImage imageWithWatermark(long offset) {
        SharedMetadataImage image = new SharedMetadataImage();
        image.applyFromMetadataLog(
            SharedMetadataRecordCodec.partitionLogStartKey(PARTITION),
            SharedMetadataRecordCodec.partitionLogStartValue(offset),
            5L
        );
        image.markReady();
        return image;
    }

    private static Finding assess(
        SharedMetadataImage image,
        SharedPartitionId partition,
        long requestedStartOffset,
        long observedLogStart,
        long requiredMetadataOffset
    ) {
        return PartitionLogStartAdvancePrecheck.assess(
            image, partition, requestedStartOffset, observedLogStart, requiredMetadataOffset
        );
    }
}
