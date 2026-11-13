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
import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Structural precheck for a not-yet-emittable experimental KRaft record.
 *
 * This class does not authenticate Broker receipts, validate WAL bytes,
 * persist records, integrate with KRaft replay, or authorize acks=all / GC.
 * No production Controller writer or producer ACK consumer calls it.
 */
final class WalReplicaDurabilityCertificateRecordPrecheck {
    static final int MAX_RECEIPTS = 16;
    static final int MAX_SPANS_PER_RECEIPT = 256;
    private static final long MAX_UNSIGNED_CRC32 = 0xffff_ffffL;

    private WalReplicaDurabilityCertificateRecordPrecheck() {
    }

    /**
     * Fail closed on malformed coordinate, duplicate or unsorted receipts,
     * and per-Broker offset gaps, overlaps or foreign span references.
     * A valid payload is still a set of untrusted claims.
     */
    static void requireWellFormedUntrustedRecord(WalReplicaDurabilityCertificateRecord record) {
        requireCoordinates(record);
        Map<Integer, Long> durableEnds = requireReceipts(record.replicaReceipts(), record.endOffset());
        requireSpans(record.walSpans(), durableEnds, record.startOffset(), record.endOffset());
    }

    private static void requireCoordinates(WalReplicaDurabilityCertificateRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("missing WAL certificate KRaft record");
        }
        new WalReplicaDurabilityCertificate.Coordinate(
            record.topicId(), record.partitionId(), record.leaderEpoch(), record.partitionEpoch(),
            record.startOffset(), record.endOffset(), record.controllerEpoch(), record.kRaftOffset());
    }

    private static Map<Integer, Long> requireReceipts(
        List<WalReplicaDurabilityCertificateRecord.ReplicaReceipt> receipts,
        long endOffset
    ) {
        if (receipts == null || receipts.size() < 2 || receipts.size() > MAX_RECEIPTS) {
            throw new IllegalArgumentException("missing or excessive WAL certificate replicas");
        }
        Map<Integer, Long> durableEnds = new HashMap<>();
        Set<Uuid> incarnations = new HashSet<>();
        int previousBroker = -1;
        for (WalReplicaDurabilityCertificateRecord.ReplicaReceipt receipt : receipts) {
            if (receipt == null || receipt.brokerId() <= previousBroker) {
                throw new IllegalArgumentException("replica receipts must be sorted and unique");
            }
            if (receipt.brokerEpoch() < 0 || receipt.storageGeneration() <= 0
                || receipt.durableEndOffset() < endOffset
                || receipt.incarnationId() == null
                || Uuid.ZERO_UUID.equals(receipt.incarnationId())
                || !incarnations.add(receipt.incarnationId())) {
                throw new IllegalArgumentException("invalid WAL replica identity or durable end");
            }
            previousBroker = receipt.brokerId();
            durableEnds.put(receipt.brokerId(), receipt.durableEndOffset());
        }
        return durableEnds;
    }

    private static void requireSpans(
        List<WalReplicaDurabilityCertificateRecord.WalSpan> spans,
        Map<Integer, Long> durableEnds,
        long startOffset,
        long endOffset
    ) {
        if (spans == null || spans.isEmpty()
            || spans.size() > durableEnds.size() * MAX_SPANS_PER_RECEIPT) {
            throw new IllegalArgumentException("missing or excessive WAL certificate spans");
        }
        Map<Integer, Long> cursor = new HashMap<>();
        Map<Integer, Integer> counts = new HashMap<>();
        int previousBroker = -1;
        for (WalReplicaDurabilityCertificateRecord.WalSpan span : spans) {
            if (span == null || span.brokerId() < previousBroker
                || !durableEnds.containsKey(span.brokerId())) {
                throw new IllegalArgumentException("foreign or unsorted WAL span");
            }
            requireNextSpan(span, cursor.getOrDefault(span.brokerId(), startOffset),
                durableEnds.get(span.brokerId()), endOffset);
            int count = counts.merge(span.brokerId(), 1, Integer::sum);
            if (count > MAX_SPANS_PER_RECEIPT) {
                throw new IllegalArgumentException("excessive WAL spans for one Broker");
            }
            previousBroker = span.brokerId();
            cursor.put(span.brokerId(), span.endOffset());
        }
        for (int brokerId : durableEnds.keySet()) {
            if (cursor.getOrDefault(brokerId, -1L) != endOffset) {
                throw new IllegalArgumentException("incomplete independent WAL span coverage");
            }
        }
    }

    private static void requireNextSpan(
        WalReplicaDurabilityCertificateRecord.WalSpan span,
        long expectedStart,
        long durableEnd,
        long certificateEnd
    ) {
        if (span.startOffset() != expectedStart || span.endOffset() <= span.startOffset()
            || span.endOffset() > certificateEnd || span.endOffset() > durableEnd) {
            throw new IllegalArgumentException("WAL span is missing, overlapping or outside its durable range");
        }
        if (span.crc32c() < 0 || span.crc32c() > MAX_UNSIGNED_CRC32) {
            throw new IllegalArgumentException("WAL span checksum is not a 32-bit unsigned CRC");
        }
    }

    /**
     * A necessary replay-origin gate for a future trusted KRaft consumer.
     * The feature flag, reader offsets and committed horizon must originate
     * from the Controller, not from a Broker or record payload. The boolean
     * result is never permission to ACK or to physically reclaim WAL.
     *
     * A log record must match its claimed append offset. A snapshot record
     * may carry an older accepted offset, but never an uncommitted future one.
     */
    static boolean hasCommittedReplayOrigin(
        WalReplicaDurabilityCertificateRecord record,
        short trustedWireVersion,
        MetadataVersion metadataVersion,
        boolean negotiatedExperimentalFeature,
        Optional<OffsetAndEpoch> snapshotId,
        long observedRecordOffset,
        long committedNextOffset
    ) {
        // V0 and V1 have different offset semantics. Never interpret a V1
        // previous-certificate CAS as V0's own claimed append location.
        if (trustedWireVersion != 0 || !negotiatedExperimentalFeature
            || metadataVersion == null || snapshotId == null || committedNextOffset <= 0) {
            return false;
        }
        if (!metadataVersion.isAtLeast(MetadataVersion.IBP_4_4_IV0)) {
            return false;
        }
        try {
            requireWellFormedUntrustedRecord(record);
        } catch (IllegalArgumentException ex) {
            return false;
        }
        if (record.kRaftOffset() >= committedNextOffset) {
            return false;
        }
        if (snapshotId.isPresent()) {
            long snapshotNextOffset = snapshotId.get().offset();
            return snapshotNextOffset > 0 && snapshotNextOffset <= committedNextOffset
                && record.kRaftOffset() < snapshotNextOffset;
        }
        return observedRecordOffset >= 0 && observedRecordOffset < committedNextOffset
            && record.kRaftOffset() == observedRecordOffset;
    }
}
