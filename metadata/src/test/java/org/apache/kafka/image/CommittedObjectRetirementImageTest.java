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
import org.apache.kafka.common.metadata.CommittedObjectRetirementRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.CommittedObjectRetirementState;
import org.apache.kafka.metadata.CommittedObjectRetirementState.Entry;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.PartitionRetirementAuthorityVersion;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommittedObjectRetirementImageTest {
    private static final Uuid CATALOG = Uuid.randomUuid();
    private static final String HASH = "0123456789abcdef".repeat(4);

    @Test
    void snapshotsRetainTerminalObjectIdAndExactOriginalKRaftCoordinates() {
        Entry terminal = Entry.fromRecord(record(100L, 30L, 29L, 4, 30L));
        CommittedObjectRetirementImage source = new CommittedObjectRetirementImage(
            Map.of(100L, terminal)
        );
        RecordListWriter writer = new RecordListWriter();
        source.write(writer);
        assertEquals(1, writer.records().size());

        MetadataDelta delta = negotiated();
        delta.replay(writer.records().get(0).message());
        delta.finishSnapshot();
        MetadataImage restored = delta.apply(MetadataProvenance.EMPTY);
        assertEquals(terminal, restored.committedObjectRetirements().get(100L));
        assertEquals(CommittedObjectRetirementState.Stage.DELETED,
            restored.committedObjectRetirements().get(100L).stage());
        assertThrows(IllegalStateException.class, () ->
            new CommittedObjectRetirementDelta(restored.committedObjectRetirements()).replay(
                record(100L, 31L, 30L, 2, 30L)));
    }

    @Test
    void fullMetadataSnapshotActuallyEmitsAndReplaysObjectTerminalFence() {
        MetadataDelta assembled = negotiated();
        assembled.replay(record(100L, 30L, 29L, 4, 30L));
        MetadataImage image = assembled.apply(MetadataProvenance.EMPTY);
        assertFalse(image.isEmpty());
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.IBP_4_4_IV0).build());

        MetadataDelta loaded = new MetadataDelta.Builder().build();
        for (ApiMessageAndVersion record : writer.records()) {
            loaded.replay(record.message());
        }
        loaded.finishSnapshot();
        MetadataImage restored = loaded.apply(MetadataProvenance.EMPTY);
        assertEquals(image.committedObjectRetirements(), restored.committedObjectRetirements());
    }

    @Test
    void snapshotRefusesProductionAndDowngradedMetadataFormatsWithoutPartialWrites() {
        MetadataDelta delta = negotiated();
        delta.replay(record(100L, 30L, 29L, 4, 30L));
        MetadataImage gated = delta.apply(MetadataProvenance.EMPTY);
        assertThrows(IllegalStateException.class, () -> gated.write(
            new RecordListWriter(),
            new ImageWriterOptions.Builder(MetadataVersion.latestProduction()).build()
        ));
        MetadataImage unnegotiated = new MetadataImage(
            gated.provenance(), FeaturesImage.EMPTY, gated.cluster(), gated.topics(),
            gated.configs(), gated.clientQuotas(), gated.producerIds(),
            gated.acls(), gated.scram(), gated.delegationTokens(),
            gated.partitionRetirements(), gated.committedObjectRetirements()
        );
        assertThrows(IllegalStateException.class, () -> unnegotiated.write(
            new RecordListWriter(),
            new ImageWriterOptions.Builder(MetadataVersion.IBP_4_4_IV0).build()
        ));
    }

    @Test
    void snapshotOmittingPreviouslyAcceptedObjectLifecycleFailsClosed() {
        Entry terminal = Entry.fromRecord(record(100L, 30L, 29L, 4, 30L));
        CommittedObjectRetirementDelta delta = new CommittedObjectRetirementDelta(
            new CommittedObjectRetirementImage(Map.of(100L, terminal))
        );
        assertThrows(IllegalStateException.class, delta::finishSnapshot);
        assertEquals(terminal, delta.apply().get(100L));
    }

    @Test
    void incompleteSnapshotCannotRemoveUnmentionedObjectsInOrdinaryDelta() {
        Entry first = Entry.fromRecord(record(100L, 10L, -1L, 1, -1L));
        Entry second = Entry.fromRecord(record(101L, 11L, -1L, 1, -1L));
        CommittedObjectRetirementDelta delta = new CommittedObjectRetirementDelta(
            new CommittedObjectRetirementImage(Map.of(100L, first, 101L, second))
        );
        delta.replay(record(100L, 12L, 10L, 2, 30L));
        assertEquals(2, delta.apply().entries().size());
    }

    @Test
    void repeatedSnapshotPreservesUnchangedTerminalObjectFence() {
        Entry terminal = Entry.fromRecord(record(100L, 30L, 29L, 4, 30L));
        CommittedObjectRetirementDelta delta = new CommittedObjectRetirementDelta(
            new CommittedObjectRetirementImage(Map.of(100L, terminal))
        );
        delta.replay(terminal.toRecord());
        delta.finishSnapshot();
        assertEquals(terminal, delta.apply().get(100L));
    }

    @Test
    void imageDeltaRejectsChangedDescriptorOrSkippedStages() {
        Entry first = Entry.fromRecord(record(100L, 10L, -1L, 1, -1L));
        CommittedObjectRetirementDelta delta = new CommittedObjectRetirementDelta(
            new CommittedObjectRetirementImage(Map.of(100L, first))
        );
        assertThrows(IllegalStateException.class, () ->
            delta.replay(record(100L, 11L, 10L, 3, 30L)));
        assertThrows(IllegalStateException.class, () ->
            delta.replay(record(100L, 11L, 10L, 2, 30L).setDescriptorSha256("f".repeat(64))));
        assertEquals(first, delta.apply().get(100L));
    }

    @Test
    void unnegotiatedMetadataLoaderCannotReplayRetirement() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        assertThrows(IllegalStateException.class, () ->
            delta.replay(record(100L, 10L, -1L, 1, -1L)));
    }

    @Test
    void snapshotWriterUsesStableUnsignedObjectIdsSortOrder() {
        Entry larger = Entry.fromRecord(record(101L, 12L, -1L, 1, -1L));
        Entry smaller = Entry.fromRecord(record(100L, 10L, -1L, 1, -1L));
        CommittedObjectRetirementImage source = new CommittedObjectRetirementImage(
            Map.of(101L, larger, 100L, smaller)
        );
        RecordListWriter writer = new RecordListWriter();
        source.write(writer);
        assertEquals(100L,
            ((CommittedObjectRetirementRecord) writer.records().get(0).message()).objectId());
        assertEquals(101L,
            ((CommittedObjectRetirementRecord) writer.records().get(1).message()).objectId());
    }

    private static MetadataDelta negotiated() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        delta.replay(new FeatureLevelRecord()
            .setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersion.IBP_4_4_IV0.featureLevel()));
        delta.replay(new FeatureLevelRecord()
            .setName(PartitionRetirementAuthorityVersion.FEATURE_NAME)
            .setFeatureLevel(PartitionRetirementAuthorityVersion.PRAV_1.featureLevel()));
        return delta;
    }

    private static CommittedObjectRetirementRecord record(
        long id,
        long offset,
        long prev,
        int stage,
        long safe
    ) {
        return new CommittedObjectRetirementRecord()
            .setObjectId(id)
            .setAcceptedKRaftOffset(offset)
            .setPreviousKRaftOffset(prev)
            .setRetirementEpoch(7L)
            .setCatalogTopicId(CATALOG)
            .setCatalogCommitOffset(8L)
            .setSealedCatalogPositionExclusive(12L)
            .setObjectSize(4096L)
            .setObjectChecksum(42L)
            .setDescriptorSha256(HASH)
            .setStage((byte) stage)
            .setSafeEpoch(safe);
    }
}
