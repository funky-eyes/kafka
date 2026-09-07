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
package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.common.message.{FetchResponseData, OffsetForLeaderEpochRequestData}
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData.EpochEndOffset
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.{LeaderEndPoint, PartitionFetchState, ReplicaFetch, ResultWithPartitions}
import org.apache.kafka.storage.internals.log.LogAppendInfo
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, Test}

import java.util.Optional

class AbstractFetcherThreadRecoveredOffsetTest {

  @AfterEach
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test
  def shouldKeepAdjustedRecoveredOffsetWhenLeaderLatestOffsetIsLower(): Unit = {
    val topicPartition = new TopicPartition("shared-recovered-out-of-range", 0)
    val leader = new RecoveryLeaderEndPoint(new BrokerEndPoint(0, "localhost", 1000))
    val fetcher = new RecoveryFetcherThread(leader, recoveredOffset = 160L)

    fetcher.addPartitions(Map(
      topicPartition -> InitialFetchState(
        topicId = None,
        leader = leader.brokerEndPoint(),
        currentLeaderEpoch = 1,
        initOffset = -1L
      )
    ))

    assertEquals(160L, fetcher.truncatedOffset)
    assertEquals(160L, fetcher.fetchState(topicPartition).get.fetchOffset)
  }

  private class RecoveryFetcherThread(leaderEndpoint: LeaderEndPoint, recoveredOffset: Long)
    extends AbstractFetcherThread(
      name = "shared-recovered-offset-fetcher",
      clientId = "shared-recovered-offset-fetcher",
      leader = leaderEndpoint,
      failedPartitions = new FailedPartitions,
      fetchTierStateMachine = new TierStateMachine(leaderEndpoint, null, false),
      fetchBackOffMs = 0,
      brokerTopicStats = new BrokerTopicStats
    ) {

    @volatile var truncatedOffset: Long = -1L

    override protected def processPartitionData(
      topicPartition: TopicPartition,
      fetchOffset: Long,
      partitionLeaderEpoch: Int,
      partitionData: FetchData
    ): Option[LogAppendInfo] = None

    override protected def truncate(topicPartition: TopicPartition, truncationState: OffsetTruncationState): Unit = {
      truncatedOffset = truncationState.offset
    }

    override protected def adjustTruncationState(
      topicPartition: TopicPartition,
      truncationState: OffsetTruncationState
    ): OffsetTruncationState = OffsetTruncationState(recoveredOffset, truncationState.truncationCompleted)

    override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
      throw new AssertionError("full truncation is not expected")
    }

    override protected def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = Optional.of(1)

    override protected def logStartOffset(topicPartition: TopicPartition): Long = 0L

    override protected def logEndOffset(topicPartition: TopicPartition): Long = recoveredOffset

    override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] =
      Optional.of(new OffsetAndEpoch(recoveredOffset, epoch))

    override protected def shouldFetchFromLastTieredOffset(
      topicPartition: TopicPartition,
      leaderEndOffset: Long,
      replicaEndOffset: Long
    ): Boolean = false
  }

  private class RecoveryLeaderEndPoint(endpoint: BrokerEndPoint) extends LeaderEndPoint {
    override def initiateClose(): Unit = {}

    override def close(): Unit = {}

    override def brokerEndPoint(): BrokerEndPoint = endpoint

    override def fetch(fetchRequest: FetchRequest.Builder): java.util.Map[TopicPartition, FetchResponseData.PartitionData] =
      java.util.Map.of()

    override def fetchEarliestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
      new OffsetAndEpoch(0L, currentLeaderEpoch)

    override def fetchLatestOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
      new OffsetAndEpoch(0L, currentLeaderEpoch)

    override def fetchEpochEndOffsets(
      partitions: java.util.Map[TopicPartition, OffsetForLeaderEpochRequestData.OffsetForLeaderPartition]
    ): java.util.Map[TopicPartition, EpochEndOffset] = java.util.Map.of()

    override def buildFetch(
      partitions: java.util.Map[TopicPartition, PartitionFetchState]
    ): ResultWithPartitions[Optional[ReplicaFetch]] =
      new ResultWithPartitions(Optional.empty[ReplicaFetch](), java.util.Set.of())

    override val isTruncationOnFetchSupported: Boolean = true

    override def fetchEarliestLocalOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
      new OffsetAndEpoch(0L, currentLeaderEpoch)

    override def fetchEarliestPendingUploadOffset(topicPartition: TopicPartition, currentLeaderEpoch: Int): OffsetAndEpoch =
      new OffsetAndEpoch(-1L, -1)
  }
}
