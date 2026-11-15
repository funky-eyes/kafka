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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.CommittedObjectRetirementRecord;

import java.util.Objects;

/**
 * Canonical immutable object retirement identity and replay-domain validation
 * shared by the Controller timeline and broker snapshot image.
 *
 * <p>No state, digest, catalog offset or safe epoch in this value model
 * authenticates cross-log publication or all-broker draining. It MUST NEVER
 * be passed as a physical object deletion authorization.</p>
 */
public final class CommittedObjectRetirementState {
    private CommittedObjectRetirementState() {
    }

    public static void validateTransition(Entry previous, Entry next) {
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
        if (!sameCatalogIdentity(previous, next) || !sameObjectDescriptor(previous, next)) {
            throw new IllegalStateException("Object retirement changed its sealed immutable identity");
        }
        if (next.safeEpoch() < previous.safeEpoch()) {
            throw new IllegalStateException("Object retirement safe epoch regressed");
        }
    }

    private static boolean sameCatalogIdentity(Entry previous, Entry next) {
        return previous.retirementEpoch() == next.retirementEpoch()
            && previous.catalogTopicId().equals(next.catalogTopicId())
            && previous.catalogCommitOffset() == next.catalogCommitOffset()
            && previous.sealedCatalogPositionExclusive() == next.sealedCatalogPositionExclusive();
    }

    private static boolean sameObjectDescriptor(Entry previous, Entry next) {
        return previous.objectSize() == next.objectSize()
            && previous.objectChecksum() == next.objectChecksum()
            && previous.descriptorSha256().equals(next.descriptorSha256());
    }

    public enum Stage {
        RETIRE_INTENT(1),
        DELETE_READY(2),
        DELETE_RECORDED(3),
        DELETED(4);

        private final int wireId;

        Stage(int wireId) {
            this.wireId = wireId;
        }

        public int wireId() {
            return wireId;
        }

        public static Stage fromWire(byte id) {
            return switch (id) {
                case 1 -> RETIRE_INTENT;
                case 2 -> DELETE_READY;
                case 3 -> DELETE_RECORDED;
                case 4 -> DELETED;
                default -> throw new IllegalArgumentException("Unknown KRaft object retirement stage: " + id);
            };
        }
    }

    public record Entry(
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
        public Entry {
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

        public static Entry fromRecord(CommittedObjectRetirementRecord record) {
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

        public CommittedObjectRetirementRecord toRecord() {
            return new CommittedObjectRetirementRecord()
                .setObjectId(objectId)
                .setAcceptedKRaftOffset(acceptedKRaftOffset)
                .setPreviousKRaftOffset(previousKRaftOffset)
                .setRetirementEpoch(retirementEpoch)
                .setCatalogTopicId(catalogTopicId)
                .setCatalogCommitOffset(catalogCommitOffset)
                .setSealedCatalogPositionExclusive(sealedCatalogPositionExclusive)
                .setObjectSize(objectSize)
                .setObjectChecksum(objectChecksum)
                .setDescriptorSha256(descriptorSha256)
                .setStage((byte) stage.wireId())
                .setSafeEpoch(safeEpoch);
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
