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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRetirementAuthorityRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Key;
import org.apache.kafka.metadata.PartitionRetirementAuthorityState.Value;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.PartitionRetirementAuthorityVersion;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementAuthorityImageTest {
    private static final Key PARTITION = new Key(Uuid.randomUuid(), 0);
    private static final Value ELECTED = new Value(10L, 6, 1, 40L, false);

    @Test
    void brokerMetadataDeltaActuallyReplaysTheGeneratedKRaftRecord() {
        MetadataDelta delta = negotiatedDelta();
        delta.replay(record(PARTITION, ELECTED));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        assertEquals(ELECTED, image.partitionRetirements().get(PARTITION));
        assertTrue(!image.isEmpty());
        // Without an explicitly negotiated cluster capability, any attempt to
        // publish the new record in a production metadata snapshot fails closed.
        assertThrows(IllegalStateException.class, () -> image.write(
            new RecordListWriter(),
            new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build()
        ));
    }

    @Test
    void snapshotWritesAndRestoresDeletedTopicIdTombstone() {
        Value deleted = new Value(20L, 7, -1, 40L, true);
        PartitionRetirementAuthorityImage image = new PartitionRetirementAuthorityImage(
            Map.of(PARTITION, deleted)
        );
        RecordListWriter writer = new RecordListWriter();
        image.write(writer);
        assertEquals(1, writer.records().size());

        MetadataDelta loaded = negotiatedDelta();
        loaded.replay(writer.records().get(0).message());
        loaded.finishSnapshot();
        MetadataImage restored = loaded.apply(MetadataProvenance.EMPTY);
        assertEquals(deleted, restored.partitionRetirements().get(PARTITION));
        assertThrows(IllegalStateException.class, () ->
            new PartitionRetirementAuthorityDelta(restored.partitionRetirements()).replay(
                record(PARTITION, new Value(21L, 8, 2, 40L, false))
            )
        );
    }

    @Test
    void snapshotFinishDropsAKeyOmittedFromFreshSnapshot() {
        Key other = new Key(Uuid.randomUuid(), 1);
        PartitionRetirementAuthorityImage old = new PartitionRetirementAuthorityImage(
            Map.of(PARTITION, ELECTED, other, new Value(10L, 6, 1, -1L, false))
        );
        PartitionRetirementAuthorityDelta next = new PartitionRetirementAuthorityDelta(old);
        next.finishSnapshot();
        assertTrue(next.apply().isEmpty());

        PartitionRetirementAuthorityDelta partial = new PartitionRetirementAuthorityDelta(old);
        partial.replay(record(PARTITION, new Value(11L, 7, 2, 40L, false)));
        partial.finishSnapshot();
        assertEquals(1, partial.apply().entries().size());
        assertEquals(2, partial.apply().get(PARTITION).brokerId());
    }

    @Test
    void metadataReplayRejectsWatermarkRegression() {
        MetadataDelta delta = negotiatedDelta();
        delta.replay(record(PARTITION, ELECTED));
        assertThrows(IllegalStateException.class, () ->
            delta.replay(record(PARTITION, new Value(11L, 7, 2, 20L, false)))
        );
    }

    @Test
    void sameEpochDemotionDoesNotPermitLateElection() {
        PartitionRetirementAuthorityDelta delta = new PartitionRetirementAuthorityDelta(
            PartitionRetirementAuthorityImage.EMPTY
        );
        delta.replay(record(PARTITION, ELECTED));
        delta.replay(record(PARTITION, new Value(11L, 6, -1, 40L, false)));
        assertThrows(IllegalStateException.class, () ->
            delta.replay(record(PARTITION, new Value(12L, 6, 1, 40L, false)))
        );
    }

    @Test
    void aRecreatedTopicHasAnIndependentAuthorityKey() {
        Key recreated = new Key(Uuid.randomUuid(), 0);
        PartitionRetirementAuthorityDelta delta = new PartitionRetirementAuthorityDelta(
            PartitionRetirementAuthorityImage.EMPTY
        );
        delta.replay(record(PARTITION, new Value(11L, 6, -1, 40L, true)));
        delta.replay(record(recreated, new Value(0L, 1, 2, -1L, false)));
        assertEquals(2, delta.apply().entries().size());
        assertEquals(-1L, delta.apply().get(recreated).logStartOffset());
    }

    @Test
    void imageDefensivelyCopiesMutableCallerMap() {
        Map<Key, Value> mutable = new HashMap<>();
        mutable.put(PARTITION, ELECTED);
        PartitionRetirementAuthorityImage image = new PartitionRetirementAuthorityImage(mutable);
        mutable.clear();
        assertEquals(ELECTED, image.get(PARTITION));
        assertThrows(UnsupportedOperationException.class, () -> image.entries().clear());
    }

    @Test
    void missingFinalizedFeatureRejectsNewKRaftRecord() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        assertThrows(IllegalStateException.class, () -> delta.replay(record(PARTITION, ELECTED)));
    }

    @Test
    void oldMetadataVersionCannotEnableRecordDespiteFeatureClaim() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_3_IV0.featureLevel()));
        delta.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 1));
        assertThrows(IllegalStateException.class, () -> delta.replay(record(PARTITION, ELECTED)));
    }

    @Test
    void negotiatedVersionMustPrecedeAuthorityRecordInReplayOrder() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 1));
        assertThrows(IllegalStateException.class, () -> delta.replay(record(PARTITION, ELECTED)));
        delta.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV0.featureLevel()));
        delta.replay(record(PARTITION, ELECTED));
        assertEquals(ELECTED, delta.apply(MetadataProvenance.EMPTY).partitionRetirements().get(PARTITION));
    }

    @Test
    void negotiatedFullSnapshotRoundTripsNewRecordAndTerminalTombstone() {
        MetadataDelta delta = negotiatedDelta();
        Value tombstone = new Value(12L, 7, -1, 40L, true);
        delta.replay(record(PARTITION, tombstone));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.IBP_4_4_IV0).build());

        MetadataDelta recovered = new MetadataDelta.Builder().build();
        writer.records().forEach(record -> recovered.replay(record.message()));
        recovered.finishSnapshot();
        MetadataImage result = recovered.apply(MetadataProvenance.EMPTY);
        assertEquals(tombstone, result.partitionRetirements().get(PARTITION));
        assertTrue(result.features().isPartitionRetirementAuthorityEnabled());
    }

    @Test
    void negotiatedImageRejectsDowngradeSnapshotTarget() {
        MetadataDelta delta = negotiatedDelta();
        delta.replay(record(PARTITION, ELECTED));
        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        assertThrows(IllegalStateException.class, () -> image.write(
            new RecordListWriter(),
            new ImageWriterOptions.Builder(MetadataVersion.IBP_4_3_IV0).build()
        ));
    }

    private static FeatureLevelRecord feature(String name, short level) {
        return new FeatureLevelRecord().setName(name).setFeatureLevel(level);
    }

    private static MetadataDelta negotiatedDelta() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV0.featureLevel()));
        delta.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 1));
        return delta;
    }

    private static PartitionRetirementAuthorityRecord record(Key key, Value value) {
        return PartitionRetirementAuthorityState.record(key, value);
    }
}
