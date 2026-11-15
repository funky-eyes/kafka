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
package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.CommittedObjectRetirementRecord;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;

import java.util.Objects;
import java.util.Optional;

/**
 * Replay-only, snapshot-rollback-aware KRaft object-lifecycle ledger.
 *
 * <p>This class never emits KRaft records and cannot authorize S3/MinIO DELETE.
 * The catalog position, descriptor digest and safe epoch in a replayed record
 * are only claims: no bridge currently authenticates the separate compacted
 * Kafka COMMITTED catalog, prevents late PUT/COMMIT, or proves every Broker
 * incarnation has drained. A future writer must establish those properties
 * inside a controller-serialized, version-negotiated protocol before the
 * worker can be wired into production.</p>
 */
final class CommittedObjectRetirementControlManager {
    private final TimelineHashMap<Long, Entry> entries;

    CommittedObjectRetirementControlManager(SnapshotRegistry registry) {
        entries = new TimelineHashMap<>(Objects.requireNonNull(registry, "registry"), 0);
    }

    void replayFromKRaft(
        CommittedObjectRetirementRecord record,
        Optional<OffsetAndEpoch> snapshotId,
        long recordOffset
    ) {
        Objects.requireNonNull(snapshotId, "snapshotId");
        Entry next = Entry.fromRecord(record);
        if (snapshotId.isPresent()) {
            // The snapshot contains a single latest value per physical object,
            // possibly a terminal state whose older transitions were compacted.
            // The position is historical, NOT the snapshot reader's current offset.
            if (next.acceptedKRaftOffset() >= snapshotId.get().offset()) {
                throw new IllegalStateException("Object retirement is ahead of its KRaft snapshot horizon");
            }
            Entry previous = entries.get(next.objectId());
            if (previous != null && !previous.equals(next)) {
                throw new IllegalStateException("Conflicting object retirement entries in KRaft snapshot");
            }
        } else {
            if (recordOffset < 0L || next.acceptedKRaftOffset() != recordOffset) {
                throw new IllegalStateException("Object retirement offset does not match KRaft append position");
            }
            validateTransition(entries.get(next.objectId()), next);
        }
        entries.put(next.objectId(), next);
    }

    Entry get(long objectId) {
        return entries.get(objectId);
    }

    private static void validateTransition(Entry previous, Entry next) {
        if (previous == null) {
            if (next.stage() != Stage.RETIRE_INTENT || next.previousKRaftOffset() != -1L) {
                throw new IllegalStateException("First KRaft object retirement transition must be an intent");
            }
            return;
        }
        if (previous.stage() == Stage.DELETED) {
            throw new IllegalStateException("A terminal physical object ID cannot be reused");
        }
        if (next.previousKRaftOffset() != previous.acceptedKRaftOffset()) {
            throw new IllegalStateException("Stale KRaft object retirement generation");
        }
        if (next.stage().wireId() != previous.stage().wireId() + 1) {
            throw new IllegalStateException("Object retirement stages must advance without skipping");
        }
        if (next.retirementEpoch() != previous.retirementEpoch()
            || !next.catalogTopicId().equals(previous.catalogTopicId())
            || next.catalogCommitOffset() != previous.catalogCommitOffset()
            || next.sealedCatalogPositionExclusive() != previous.sealedCatalogPositionExclusive()
            || next.objectSize() != previous.objectSize()
            || next.objectChecksum() != previous.objectChecksum()
            || !next.descriptorSha256().equals(previous.descriptorSha256())) {
            throw new IllegalStateException("Object retirement changed its sealed immutable identity");
        }
        if (next.safeEpoch() < previous.safeEpoch()) {
            throw new IllegalStateException("Object retirement safe epoch regressed");
        }
    }

    enum Stage {
        RETIRE_INTENT(1),
        DELETE_READY(2),
        DELETE_RECORDED(3),
        DELETED(4);

        private final int wireId;

        Stage(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static Stage fromWire(byte id) {
            return switch (id) {
                case 1 -> RETIRE_INTENT;
                case 2 -> DELETE_READY;
                case 3 -> DELETE_RECORDED;
                case 4 -> DELETED;
                default -> throw new IllegalArgumentException("Unknown KRaft object retirement stage: " + id);
            };
        }
    }

    record Entry(
        long objectId,
        long acceptedKRaftOffset,
        long previousKRaftOffset,
        long retirementEpoch,
        Uuid catalogTopicId,
        long catalogCommitOffset,
        long sealedCatalogPositionExclusive,
        long objectSize,
        long objectChecksum,
        String descriptorSha256,
        Stage stage,
        long safeEpoch
    ) {
        Entry {
            Objects.requireNonNull(catalogTopicId, "catalogTopicId");
            Objects.requireNonNull(descriptorSha256, "descriptorSha256");
            Objects.requireNonNull(stage, "stage");
            if (objectId <= 0L || acceptedKRaftOffset < 0L || previousKRaftOffset < -1L
                || previousKRaftOffset >= acceptedKRaftOffset || retirementEpoch <= 0L) {
                throw new IllegalArgumentException("Invalid object retirement identity or KRaft generation");
            }
            if (Uuid.ZERO_UUID.equals(catalogTopicId) || catalogCommitOffset < 0L
                || sealedCatalogPositionExclusive <= catalogCommitOffset) {
                throw new IllegalArgumentException("Retirement requires an immutable catalog and bounded horizon");
            }
            if (objectSize <= 0L || objectChecksum < 0L || !isSha256(descriptorSha256)) {
                throw new IllegalArgumentException("Retirement requires a canonical immutable object descriptor");
            }
            if (stage == Stage.RETIRE_INTENT ? safeEpoch != -1L : safeEpoch < retirementEpoch) {
                throw new IllegalArgumentException("A ready lifecycle stage needs a claimed drained safe epoch");
            }
        }

        static Entry fromRecord(CommittedObjectRetirementRecord record) {
            Objects.requireNonNull(record, "record");
            return new Entry(
                record.objectId(),
                record.acceptedKRaftOffset(),
                record.previousKRaftOffset(),
                record.retirementEpoch(),
                record.catalogTopicId(),
                record.catalogCommitOffset(),
                record.sealedCatalogPositionExclusive(),
                record.objectSize(),
                record.objectChecksum(),
                record.descriptorSha256(),
                Stage.fromWire(record.stage()),
                record.safeEpoch()
            );
        }

        private static boolean isSha256(String hash) {
            if (hash.length() != 64) {
                return false;
            }
            for (int i = 0; i < hash.length(); i++) {
                char c = hash.charAt(i);
                if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
                    return false;
                }
            }
            return true;
        }
    }
}
