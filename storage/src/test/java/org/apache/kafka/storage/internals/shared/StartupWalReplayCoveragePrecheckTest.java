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
package org.apache.kafka.storage.internals.shared;

import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalAppendResult;
import org.apache.kafka.storage.internals.shared.wal.WalLocation;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;
import org.apache.kafka.storage.internals.shared.wal.WalReplayConsumer;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck.Finding;
import static org.apache.kafka.storage.internals.shared.StartupWalReplayCoveragePrecheck.assess;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class StartupWalReplayCoveragePrecheckTest {
    private static final SharedPartitionId FIRST = new SharedPartitionId(1L, 2L, 0);
    private static final SharedPartitionId SECOND = new SharedPartitionId(1L, 2L, 1);

    @Test
    void exactRecoveredRecordBatchWindowIsLocallyCovered() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L), data(FIRST, 10L, 19L)));
        assertEquals(Finding.RECOVERED_BATCH_WINDOW, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 10L, 19L)), 0L, 20L));
    }

    @Test
    void gappedKafkaBatchOffsetsCannotBeCalledCompleteRecovery() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L), data(FIRST, 20L, 29L)));
        assertEquals(Finding.KAFKA_BATCH_GAP_OR_OVERLAP, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 20L, 29L)), 0L, 30L));
    }

    @Test
    void partialFirstRecordBatchCannotBeUsedAsTheSourceStart() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L), data(FIRST, 10L, 19L)));
        assertEquals(Finding.KAFKA_BATCH_GAP_OR_OVERLAP, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 10L, 19L)), 5L, 20L));
    }

    @Test
    void sourceHighWatermarkInsideRecoveredBatchIsNotACompleteBoundary() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L), data(FIRST, 10L, 19L)));
        assertEquals(Finding.SOURCE_END_SPLITS_BATCH, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 10L, 19L)), 0L, 15L));
    }

    @Test
    void noStartupReplayDataCannotCertifyADataWindow() {
        assertEquals(Finding.NO_REPLAYED_DATA, assess(StartupWalReplayEvidence.EMPTY,
            FIRST, List.of(location(0L, 0L, 9L)), 0L, 10L));
    }

    @Test
    void replayedOtherPartitionCannotSubstituteForTargetPartition() throws Exception {
        var replay = replayed(List.of(data(SECOND, 0L, 9L)));
        assertEquals(Finding.NO_REPLAYED_DATA, assess(replay,
            FIRST, List.of(location(0L, 0L, 9L)), 0L, 10L));
    }

    @Test
    void missingTailAtRecoveryCannotCertifyTheFullSourceWindow() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L), data(FIRST, 10L, 19L)));
        assertEquals(Finding.SOURCE_WINDOW_NOT_RECOVERED, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 10L, 19L)), 0L, 30L));
    }

    @Test
    void liveAppendAfterStartupCannotFillTheRecoveryWindow() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L)));
        assertEquals(Finding.POST_START_WAL_BATCH, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 10L, 19L)), 0L, 20L));
    }

    @Test
    void historicalTruncateWithoutAnyLiveIndexCannotCertifySourceWindow() throws Exception {
        var replay = replayed(List.of(data(FIRST, 0L, 9L), WalRecord.truncate(1L, 2L, 0, 8, 0L)));
        assertEquals(Finding.SOURCE_WINDOW_NOT_RECOVERED, assess(replay, FIRST,
            List.of(), 0L, 10L));
    }

    @Test
    void extraDataBatchAfterTheNativeHighWatermarkMustNotBeIgnored() throws Exception {
        var replay = replayed(List.of(
            data(FIRST, 0L, 9L), data(FIRST, 10L, 19L), data(FIRST, 20L, 29L)
        ));
        assertEquals(Finding.SOURCE_END_SPLITS_BATCH, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(61L, 10L, 19L),
                location(122L, 20L, 29L)), 0L, 20L));
    }

    @Test
    void interleavedPartitionWalAddressesMayStillRecoverCompleteTargetWindow() throws Exception {
        var replay = replayed(List.of(
            data(FIRST, 0L, 9L), data(SECOND, 0L, 9L), data(FIRST, 10L, 19L)
        ));
        assertEquals(Finding.RECOVERED_BATCH_WINDOW, assess(replay, FIRST,
            List.of(location(0L, 0L, 9L), location(122L, 10L, 19L)), 0L, 20L));
    }

    @Test
    void emptySourceWindowCannotBePresentedAsRecoveryProof() {
        assertEquals(Finding.EMPTY_SOURCE_WINDOW, assess(
            StartupWalReplayEvidence.EMPTY, FIRST, List.of(), 10L, 10L));
    }

    @Test
    void invalidNativeKafkaBoundsAreRejected() {
        assertThrows(IllegalArgumentException.class, () ->
            assess(StartupWalReplayEvidence.EMPTY, FIRST, List.of(), -1L, 1L));
        assertThrows(IllegalArgumentException.class, () ->
            assess(StartupWalReplayEvidence.EMPTY, FIRST, List.of(), 20L, 19L));
    }

    private static WalLocation location(long position, long first, long last) {
        return new WalLocation(position, 61, 8, first, last);
    }

    private static WalRecord data(SharedPartitionId p, long first, long last) {
        return WalRecord.data(p.topicIdHigh(), p.topicIdLow(), p.partition(),
            8, first, last, new byte[]{1});
    }

    private static StartupWalReplayEvidence replayed(List<WalRecord> records) throws Exception {
        SharedWal wal = mock(SharedWal.class);
        doAnswer(invocation -> {
            WalReplayConsumer consume = invocation.getArgument(0);
            long logicalAddress = 0L;
            for (WalRecord record : records) {
                int encodedLength = record.type() == org.apache.kafka.storage.internals.shared.wal.WalRecordType.DATA
                    ? 61 : 60;
                consume.accept(record, new WalAppendResult(logicalAddress, encodedLength));
                logicalAddress += encodedLength;
            }
            return null;
        }).when(wal).replay(any(WalReplayConsumer.class));
        try (SharedStorageEngine engine = new SharedStorageEngine(wal)) {
            return engine.startupWalReplayEvidence();
        }
    }
}
