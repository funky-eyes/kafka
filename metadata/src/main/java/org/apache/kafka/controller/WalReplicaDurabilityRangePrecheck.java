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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Structural, validation-only precheck for gap-free replica WAL spans.
 *
 * A large durableEndOffset alone does not prove that intermediate Kafka
 * offsets survived a crash. Spans must be independently obtained from a
 * trusted durable WAL replay/attestation source, which is not implemented
 * here. Neither this class nor a true result grants acks=all or physical GC.
 */
final class WalReplicaDurabilityRangePrecheck {
    static final int MAX_SPANS_PER_REPLICA = 256;

    /** A half-open Kafka interval attributed to one exact Broker/disk generation. */
    record Span(int brokerId, long brokerEpoch, Uuid incarnationId, long storageGeneration,
                Uuid topicId, int partition, int leaderEpoch, int partitionEpoch, long startOffset,
                long endOffset, boolean checksumVerified) {
        Span {
            Objects.requireNonNull(incarnationId);
            Objects.requireNonNull(topicId);
            if (brokerId < 0 || brokerEpoch < 0 || storageGeneration <= 0) {
                throw new IllegalArgumentException("invalid WAL span Broker identity");
            }
            if (Uuid.ZERO_UUID.equals(incarnationId) || Uuid.ZERO_UUID.equals(topicId)) {
                throw new IllegalArgumentException("reserved WAL span UUID");
            }
            if (partition < 0 || leaderEpoch < 0 || partitionEpoch < 0) {
                throw new IllegalArgumentException("invalid WAL span partition epoch");
            }
            if (startOffset < 0 || endOffset <= startOffset) {
                throw new IllegalArgumentException("invalid WAL span coordinates");
            }
        }
    }

    private WalReplicaDurabilityRangePrecheck() {
    }

    /**
     * Necessary condition only: every counted current Broker must provide
     * a disjoint, ordered series of matching durable spans that exactly
     * covers the proposed Kafka range without a hole or borrowed replica.
     *
     * A caller must separately authenticate every span, its checksum and
     * backend fsync, then establish KRaft committed replay provenance and
     * Kafka ISR/HW policy. This precheck cannot infer any of those facts.
     */
    static boolean hasCompleteIndependentRanges(
        WalReplicaDurabilityCertificate.Coordinate coordinate,
        List<WalReplicaDurabilityCertificate.Receipt> receipts,
        Set<Integer> currentIsr,
        Map<Integer, WalReplicaDurabilityCertificate.CurrentRegistration> registrations,
        Map<Integer, List<Span>> perBrokerSpans,
        int requiredReplicas
    ) {
        if (perBrokerSpans == null || receipts == null
            || perBrokerSpans.size() != receipts.size()) {
            return false;
        }
        if (!WalReplicaDurabilityCertificate.hasCurrentRegisteredCompleteReceipts(
            coordinate, receipts, currentIsr, registrations, requiredReplicas)) {
            return false;
        }
        for (WalReplicaDurabilityCertificate.Receipt receipt : receipts) {
            List<Span> spans = perBrokerSpans.get(receipt.brokerId());
            if (spans == null || spans.isEmpty() || spans.size() > MAX_SPANS_PER_REPLICA) {
                return false;
            }
            long nextOffset = coordinate.startOffset();
            for (Span span : spans) {
                if (!matchesCurrentContiguousSpan(span, coordinate, receipt, nextOffset)) {
                    return false;
                }
                nextOffset = span.endOffset();
            }
            if (nextOffset != coordinate.endOffset()) {
                return false;
            }
        }
        return true;
    }

    private static boolean matchesCurrentContiguousSpan(
        Span span,
        WalReplicaDurabilityCertificate.Coordinate coordinate,
        WalReplicaDurabilityCertificate.Receipt receipt,
        long expectedOffset
    ) {
        if (span == null || !span.checksumVerified()
            || span.startOffset() != expectedOffset || span.endOffset() > coordinate.endOffset()
            || span.endOffset() > receipt.durableEndOffset()) {
            return false;
        }
        if (span.brokerId() != receipt.brokerId()
            || span.brokerEpoch() != receipt.brokerEpoch()
            || !span.incarnationId().equals(receipt.incarnationId())
            || span.storageGeneration() != receipt.storageGeneration()) {
            return false;
        }
        return span.topicId().equals(coordinate.topicId())
            && span.partition() == coordinate.partition()
            && span.leaderEpoch() == coordinate.leaderEpoch()
            && span.partitionEpoch() == coordinate.partitionEpoch();
    }
}
