/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.cluster.Partition
import kafka.server.QuotaFactory.UNBOUNDED_QUOTA
import kafka.utils.TestUtils
import org.apache.kafka.clients.FetchSessionHandler
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.record.internal.{MemoryRecords, SimpleRecord}
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.network.BrokerEndPoint
import org.apache.kafka.server.storage.log.UnexpectedAppendOffsetException
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.apache.kafka.storage.internals.shared.kafka.SharedLogSegment
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.ArgumentMatchers.{any, anyBoolean, anyInt}
import org.mockito.Mockito.{mock, when}

import java.nio.charset.StandardCharsets
import java.util.Optional

/**
 * Focused recovery-race coverage for ReplicaFetcherThread.
 *
 * The production local-state-loss E2E is the authoritative process-level proof. This unit test pins the narrow
 * scheduling window where the fetcher pre-check observes the old LEO, remote recovery wins the SharedUnifiedLog fence,
 * and UnifiedLog rejects the already-issued fetch response as stale during append.
 */
class ReplicaFetcherThreadRecoveryTest {

  @AfterEach
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test
  def shouldReseedSharedFollowerWhenRemoteRecoveryWinsBeforeAppend(): Unit = {
    val topicPartition = new TopicPartition("shared-recovery-race", 0)
    val topicId = Uuid.randomUuid()
    val recoveredLogEndOffset = 160L
    val leaderEpoch = 1

    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val brokerEndPoint = new BrokerEndPoint(0, "localhost", 1000)
    val blockingSend = mock(classOf[BlockingSend])
    when(blockingSend.brokerEndPoint()).thenReturn(brokerEndPoint)

    val brokerTopicStats = new BrokerTopicStats
    val replicaManager = mock(classOf[ReplicaManager])
    when(replicaManager.brokerTopicStats).thenReturn(brokerTopicStats)

    val log = mock(classOf[UnifiedLog])
    val sharedSegment = mock(classOf[SharedLogSegment])
    when(log.activeSegment).thenReturn(sharedSegment)
    // The pre-check sees no materialized remote prefix yet. Recovery completes only after this point.
    when(sharedSegment.readNextOffset()).thenReturn(0L)
    when(log.logEndOffset).thenReturn(0L).thenReturn(recoveredLogEndOffset)
    when(log.topicId()).thenReturn(Optional.of(topicId))

    val partition = mock(classOf[Partition])
    when(partition.localLogOrException).thenReturn(log)
    when(replicaManager.getPartitionOrException(topicPartition)).thenReturn(partition)
    when(replicaManager.localLogOrException(topicPartition)).thenReturn(log)

    val records = MemoryRecords.withRecords(
      Compression.NONE,
      new SimpleRecord(1000L, "stale".getBytes(StandardCharsets.UTF_8))
    )
    when(partition.appendRecordsToFollowerOrFutureReplica(
      any[MemoryRecords],
      anyBoolean(),
      anyInt()
    )).thenThrow(new UnexpectedAppendOffsetException(
      "remote recovery advanced the shared follower before append",
      0L,
      0L
    ))

    val logContext = new LogContext(
      s"[ReplicaFetcher replicaId=${config.brokerId}, leaderId=${brokerEndPoint.id}, fetcherId=0] "
    )
    val fetchSessionHandler = new FetchSessionHandler(logContext, brokerEndPoint.id)
    val leader = new RemoteLeaderEndPoint(
      logContext.logPrefix,
      blockingSend,
      fetchSessionHandler,
      config,
      replicaManager,
      UNBOUNDED_QUOTA,
      () => MetadataVersion.MINIMUM_VERSION,
      () => 1
    )
    val thread = new ReplicaFetcherThread(
      "shared-recovery-race-fetcher",
      leader,
      config,
      new FailedPartitions,
      replicaManager,
      UNBOUNDED_QUOTA,
      logContext.logPrefix
    )

    thread.addPartitions(Map(
      topicPartition -> InitialFetchState(
        topicId = Some(topicId),
        leader = brokerEndPoint,
        initOffset = 0L,
        currentLeaderEpoch = leaderEpoch
      )
    ))

    val partitionData: thread.FetchData = new FetchResponseData.PartitionData()
      .setPartitionIndex(topicPartition.partition)
      .setLogStartOffset(0L)
      .setHighWatermark(recoveredLogEndOffset)
      .setLastStableOffset(recoveredLogEndOffset)
      .setRecords(records)

    val appendResult = thread.processPartitionData(
      topicPartition,
      fetchOffset = 0L,
      partitionLeaderEpoch = leaderEpoch,
      partitionData
    )

    assertEquals(None, appendResult)
    assertTrue(thread.fetchState(topicPartition).isDefined)
    assertEquals(recoveredLogEndOffset, thread.fetchState(topicPartition).get.fetchOffset)
  }
}
