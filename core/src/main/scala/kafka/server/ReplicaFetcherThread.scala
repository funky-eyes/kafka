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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.InvalidOffsetException
import org.apache.kafka.common.requests.FetchResponse
import org.apache.kafka.server.common.OffsetAndEpoch
import org.apache.kafka.server.storage.log.UnexpectedAppendOffsetException
import org.apache.kafka.storage.internals.log.{LogAppendInfo, LogStartOffsetIncrementReason, UnifiedLog}
import org.apache.kafka.storage.internals.shared.kafka.SharedLogSegment
import org.apache.kafka.server.LeaderEndPoint

import java.util.Optional
import scala.collection.mutable

class ReplicaFetcherThread(name: String,
                           leader: LeaderEndPoint,
                           brokerConfig: KafkaConfig,
                           failedPartitions: FailedPartitions,
                           replicaMgr: ReplicaManager,
                           quota: ReplicaQuota,
                           logPrefix: String)
  extends AbstractFetcherThread(name = name,
                                clientId = name,
                                leader = leader,
                                failedPartitions,
                                fetchTierStateMachine = new TierStateMachine(leader, replicaMgr, false),
                                fetchBackOffMs = brokerConfig.replicaFetchBackoffMs,
                                isInterruptible = false,
                                replicaMgr.brokerTopicStats) {

  this.logIdent = logPrefix

  // Visible for testing
  private[server] val partitionsWithNewHighWatermark = mutable.Buffer[TopicPartition]()

  override protected def latestEpoch(topicPartition: TopicPartition): Optional[Integer] = {
    replicaMgr.localLogOrException(topicPartition).latestEpoch
  }

  override protected def logStartOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logStartOffset
  }

  override protected def logEndOffset(topicPartition: TopicPartition): Long = {
    replicaMgr.localLogOrException(topicPartition).logEndOffset
  }

  override protected def endOffsetForEpoch(topicPartition: TopicPartition, epoch: Int): Optional[OffsetAndEpoch] = {
    replicaMgr.localLogOrException(topicPartition).endOffsetForEpoch(epoch)
  }

  override protected def adjustTruncationState(
    topicPartition: TopicPartition,
    truncationState: OffsetTruncationState
  ): OffsetTruncationState = {
    val log = replicaMgr.localLogOrException(topicPartition)
    val recoveredHighWatermark = log.highWatermark
    if (log.activeSegment.isInstanceOf[SharedLogSegment] && truncationState.offset < recoveredHighWatermark) {
      val adjusted = OffsetTruncationState(recoveredHighWatermark, truncationState.truncationCompleted)
      info(s"Raising stale shared-storage truncation for $topicPartition from ${truncationState.offset} " +
        s"to recovered high watermark $recoveredHighWatermark so the follower log and fetch cursor advance atomically")
      adjusted
    } else {
      truncationState
    }
  }

  override protected[server] def shouldRetryFencedLeaderEpoch(topicPartition: TopicPartition): Boolean =
    replicaMgr.localLog(topicPartition).exists(_.activeSegment.isInstanceOf[SharedLogSegment])

  override protected[server] def shouldFetchFromLastTieredOffset(topicPartition: TopicPartition, leaderEndOffset: Long, replicaEndOffset: Long): Boolean = {
    val isCompactTopic = replicaMgr.localLog(topicPartition).exists(_.config.compact)
    val remoteStorageEnabled = replicaMgr.localLog(topicPartition).exists(_.remoteLogEnabled())

    brokerConfig.followerFetchLastTieredOffsetEnable &&
      remoteStorageEnabled &&
      !isCompactTopic &&
      replicaEndOffset == 0 &&
      leaderEndOffset != 0
  }

  override def initiateShutdown(): Boolean = {
    val justShutdown = super.initiateShutdown()
    if (justShutdown) {
      // This is thread-safe, so we don't expect any exceptions, but catch and log any errors
      // to avoid failing the caller, especially during shutdown. We will attempt to close
      // leaderEndpoint after the thread terminates.
      try {
        leader.initiateClose()
      } catch {
        case t: Throwable =>
          error(s"Failed to initiate shutdown of $leader after initiating replica fetcher thread shutdown", t)
      }
    }
    justShutdown
  }

  override def awaitShutdown(): Unit = {
    super.awaitShutdown()
    // We don't expect any exceptions here, but catch and log any errors to avoid failing the caller,
    // especially during shutdown. It is safe to catch the exception here without causing correctness
    // issue because we are going to shutdown the thread and will not re-use the leaderEndpoint anyway.
    try {
      leader.close()
    } catch {
      case t: Throwable =>
        error(s"Failed to close $leader after shutting down replica fetcher thread", t)
    }
  }

  override def doWork(): Unit = {
    super.doWork()
    completeDelayedFetchRequests()
  }

  // process fetched data
  override def processPartitionData(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    partitionLeaderEpoch: Int,
    partitionData: FetchData
  ): Option[LogAppendInfo] = {
    val logTrace = isTraceEnabled
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    val log = partition.localLogOrException
    val records = toMemoryRecords(FetchResponse.recordsOrFail(partitionData))
    val currentLogEndOffset = log.logEndOffset

    log.activeSegment match {
      case sharedSegment: SharedLogSegment =>
        val materializedEndOffset = sharedSegment.readNextOffset()
        if (fetchOffset < materializedEndOffset && materializedEndOffset > currentLogEndOffset) {
          // Metadata replay rebuilds SharedLogSegment's readable batch/index view before it publishes the recovered
          // Kafka-visible LEO. A fetch response issued before recovery may arrive in that short interval. Appending it
          // would duplicate already-materialized remote batches and corrupt the monotonically increasing offset index.
          // Do not re-seed beyond the Kafka-visible LEO here; returning None keeps the current fetch cursor unchanged.
          // Once recovery publishes the LEO, the existing stale-response branch below will atomically re-seed it.
          info(s"Shared-storage recovery is materializing $topicPartition through offset $materializedEndOffset " +
            s"while local log end offset is $currentLogEndOffset; discarding stale fetch response at $fetchOffset")
          return None
        }
      case _ =>
    }

    if (fetchOffset != currentLogEndOffset) {
      // Shared-storage metadata replay may restore an acknowledged remote prefix after this fetch request was built.
      // The response is valid for the old cursor but must not be appended again into the shared WAL. Re-seed this
      // follower from the recovered LEO and discard the stale response. Ordinary Kafka logs retain the strict offset
      // mismatch failure below.
      if (fetchOffset < currentLogEndOffset && log.activeSegment.isInstanceOf[SharedLogSegment]) {
        val topicId = if (log.topicId().isPresent) Some(log.topicId().get()) else None
        removePartitions(Set(topicPartition))
        addPartitions(Map(
          topicPartition -> InitialFetchState(
            topicId,
            leader.brokerEndPoint(),
            partitionLeaderEpoch,
            currentLogEndOffset
          )
        ))
        info(s"Shared-storage recovery advanced $topicPartition from fetch offset $fetchOffset " +
          s"to local log end offset $currentLogEndOffset; discarding the stale fetch response and resuming there")
        return None
      }
      throw new IllegalStateException("Offset mismatch for partition %s: fetched offset = %d, log end offset = %d.".format(
        topicPartition, fetchOffset, currentLogEndOffset))
    }

    if (logTrace)
      trace("Follower has replica log end offset %d for partition %s. Received %d bytes of messages and leader hw %d"
        .format(log.logEndOffset, topicPartition, records.sizeInBytes, partitionData.highWatermark))

    // Append the leader's messages to the log. SharedUnifiedLog serializes this full append critical section against
    // remote recovery. If recovery wins after the pre-checks above, the stale response may be rejected either by
    // UnifiedLog's offset validation or by the shared segment's monotonic offset index. Only suppress either exception
    // when a catch-time re-read proves that recovery advanced the shared log after the pre-check.
    val logAppendInfo = try {
      partition.appendRecordsToFollowerOrFutureReplica(records, isFuture = false, partitionLeaderEpoch)
    } catch {
      case e: UnexpectedAppendOffsetException =>
        if (handleSharedStorageRecoveryRace(topicPartition, fetchOffset, partitionLeaderEpoch, log, e.getClass.getSimpleName))
          return None
        throw e
      case e: InvalidOffsetException =>
        if (handleSharedStorageRecoveryRace(topicPartition, fetchOffset, partitionLeaderEpoch, log, e.getClass.getSimpleName))
          return None
        throw e
    }

    if (logTrace)
      trace("Follower has replica log end offset %d after appending %d bytes of messages for partition %s"
        .format(log.logEndOffset, records.sizeInBytes, topicPartition))
    val leaderLogStartOffset = partitionData.logStartOffset

    // For the follower replica, we do not need to keep its segment base offset and physical position.
    // These values will be computed upon becoming leader or handling a preferred read replica fetch.
    var maybeUpdateHighWatermarkMessage = s"but did not update replica high watermark"
    log.maybeUpdateHighWatermark(partitionData.highWatermark).ifPresent { newHighWatermark =>
      maybeUpdateHighWatermarkMessage = s"and updated replica high watermark to $newHighWatermark"
      partitionsWithNewHighWatermark += topicPartition
    }

    log.maybeIncrementLogStartOffset(leaderLogStartOffset, LogStartOffsetIncrementReason.LeaderOffsetIncremented)
    if (logTrace)
      trace(s"Follower received high watermark ${partitionData.highWatermark} from the leader " +
        s"$maybeUpdateHighWatermarkMessage for partition $topicPartition")

    // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
    // traffic doesn't exceed quota.
    if (quota.isThrottled(topicPartition))
      quota.record(records.sizeInBytes)

    if (partition.isReassigning && partition.isAddingLocalReplica)
      brokerTopicStats.updateReassignmentBytesIn(records.sizeInBytes)

    brokerTopicStats.updateReplicationBytesIn(records.sizeInBytes)

    logAppendInfo
  }

  private def handleSharedStorageRecoveryRace(
    topicPartition: TopicPartition,
    fetchOffset: Long,
    partitionLeaderEpoch: Int,
    log: UnifiedLog,
    rejectedBy: String
  ): Boolean = {
    log.activeSegment match {
      case sharedSegment: SharedLogSegment =>
        // Re-read after the append failure. A recovery which has already published the Kafka-visible LEO can safely
        // re-seed the follower there. This is the strongest proof and keeps fetch state atomic with the recovered log.
        val recoveredLogEndOffset = log.logEndOffset
        if (fetchOffset < recoveredLogEndOffset) {
          val topicId = if (log.topicId().isPresent) Some(log.topicId().get()) else None
          removePartitions(Set(topicPartition))
          addPartitions(Map(
            topicPartition -> InitialFetchState(
              topicId,
              leader.brokerEndPoint(),
              partitionLeaderEpoch,
              recoveredLogEndOffset
            )
          ))
          info(s"Shared-storage recovery completed while appending a fetch response for $topicPartition; " +
            s"$rejectedBy rejected stale fetch offset $fetchOffset, resuming from recovered LEO $recoveredLogEndOffset")
          true
        } else {
          // Metadata replay materializes the shared batch/index view before publishing the recovered LEO. If the append
          // failed in precisely that window, do not invent a Kafka-visible LEO or advance the fetch cursor. The remote
          // materialized end still proves this response is stale, so discard it and let the next fetch iteration observe
          // the published LEO. If neither re-read proves recovery advanced, the original exception must escape.
          val materializedEndOffset = sharedSegment.readNextOffset()
          if (fetchOffset < materializedEndOffset && materializedEndOffset > recoveredLogEndOffset) {
            info(s"Shared-storage recovery materialized $topicPartition through offset $materializedEndOffset while " +
              s"$rejectedBy rejected stale fetch offset $fetchOffset before recovered LEO publication; discarding the response")
            true
          } else {
            false
          }
        }
      case _ =>
        false
    }
  }

  private def completeDelayedFetchRequests(): Unit = {
    if (partitionsWithNewHighWatermark.nonEmpty) {
      replicaMgr.completeDelayedFetchRequests(partitionsWithNewHighWatermark.toSeq)
      partitionsWithNewHighWatermark.clear()
    }
  }

  /**
   * Truncate the log for each partition's epoch based on leader's returned epoch and offset.
   * The logic for finding the truncation offset is implemented in AbstractFetcherThread.getOffsetTruncationState
   */
  override def truncate(tp: TopicPartition, offsetTruncationState: OffsetTruncationState): Unit = {
    val partition = replicaMgr.getPartitionOrException(tp)
    partition.truncateTo(offsetTruncationState.offset, isFuture = false)

    // mark the future replica for truncation only when we do last truncation
    if (offsetTruncationState.truncationCompleted)
      replicaMgr.replicaAlterLogDirsManager.markPartitionsForTruncation(brokerConfig.brokerId, tp,
        offsetTruncationState.offset)
  }

  override protected def truncateFullyAndStartAt(topicPartition: TopicPartition, offset: Long): Unit = {
    val partition = replicaMgr.getPartitionOrException(topicPartition)
    partition.truncateFullyAndStartAt(offset, isFuture = false)
  }
}
