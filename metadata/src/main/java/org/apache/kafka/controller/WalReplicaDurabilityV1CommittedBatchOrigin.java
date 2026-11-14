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

import org.apache.kafka.common.metadata.WalReplicaDurabilityCertificateRecord;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.ArrayList;
import java.util.List;

/**
 * Validation-only adapter for the actual KRaft data-batch offset and wire
 * version. The standby Controller observes it only from the trusted Raft
 * handleCommit callback. This class cannot prove callback provenance.
 *
 * It never emits metadata records, invokes the production Controller replay
 * switch, authenticates WAL bytes, authorizes Producer ACK, or permits GC.
 */
final class WalReplicaDurabilityV1CommittedBatchOrigin {
    /**
     * The record index refers to the original KRaft batch; no mutable
     * generated metadata record is retained by this position object.
     */
    record ObservedRecord(
        int index,
        long assignedOffset,
        short wireVersion,
        WalReplicaDurabilityV1PositionPrecheck.CommitBatch committedBatch
    ) {
    }

    private WalReplicaDurabilityV1CommittedBatchOrigin() {
    }

    /**
     * Convert the KRaft batch's offset range, epoch and ApiMessageAndVersion
     * into immutable V1 position observations. Batch data must come from a
     * trusted *committed* callback, not a speculative append or a Broker.
     *
     * A control batch has no data messages and supplies no certificates.
     * Legacy V0 and unknown versions are rejected rather than coerced to V1.
     */
    static List<ObservedRecord> inspectCommittedDataBatch(Batch<ApiMessageAndVersion> batch) {
        if (batch == null) {
            throw new IllegalArgumentException("missing committed KRaft batch");
        }
        List<ApiMessageAndVersion> records = batch.records();
        if (records == null) {
            throw new IllegalArgumentException("missing committed KRaft records");
        }
        if (records.isEmpty()) {
            return List.of();
        }
        requireContiguousDataBatch(batch, records.size());
        var context = new WalReplicaDurabilityV1PositionPrecheck.CommitBatch(
            batch.baseOffset(), batch.lastOffset(), batch.epoch(), batch.lastOffset() + 1);
        List<ObservedRecord> positions = new ArrayList<>();
        for (int i = 0; i < records.size(); i++) {
            ApiMessageAndVersion message = records.get(i);
            if (message == null || message.message() == null) {
                throw new IllegalArgumentException("null metadata record in KRaft batch");
            }
            if (message.message().apiKey() == new WalReplicaDurabilityCertificateRecord().apiKey()) {
                if (!(message.message() instanceof WalReplicaDurabilityCertificateRecord)
                    || message.version() != 1) {
                    throw new IllegalArgumentException("non-V1 WAL certificate in committed batch");
                }
                positions.add(new ObservedRecord(
                    i, batch.baseOffset() + i, message.version(), context));
            }
        }
        return List.copyOf(positions);
    }

    private static void requireContiguousDataBatch(Batch<ApiMessageAndVersion> batch, int count) {
        long start = batch.baseOffset();
        long end = batch.lastOffset();
        if (start < 0 || end < start || end == Long.MAX_VALUE || batch.epoch() < 0) {
            throw new IllegalArgumentException("invalid committed KRaft data batch coordinates");
        }
        if ((long) count - 1 > Long.MAX_VALUE - start
            || start + count - 1 != end) {
            throw new IllegalArgumentException("inconsistent committed KRaft data batch record offsets");
        }
    }
}
