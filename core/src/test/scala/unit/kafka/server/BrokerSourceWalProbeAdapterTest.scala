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

import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe
import org.apache.kafka.storage.internals.log.UnifiedLog
import org.apache.kafka.storage.internals.shared.kafka.SharedStorageExtension
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock

import java.util.{ArrayDeque, Optional}
import java.util.concurrent.{CompletionException, Executor, RejectedExecutionException}

/**
 * Broker metadata-to-native-source diagnostic lookup. No test enables
 * BrokerHeartbeat SourceWalProbeReport or any KRaft object GC authority.
 */
class BrokerSourceWalProbeAdapterTest {
  private val topicId = new Uuid(3L, 4L)

  @Test
  def testResolutionIsDeferredUntilExecutorAndUsesExactTopicPartition(): Unit = {
    val work = new ArrayDeque[Runnable]()
    var visitedId: Uuid = null
    var visitedPartition: TopicPartition = null
    val adapter = BrokerServer.readOnlySourceWalProbeSampler(
      new SharedStorageExtension(),
      id => {
        visitedId = id
        Optional.of("native-topic")
      },
      tp => {
        visitedPartition = tp
        Some(mock(classOf[UnifiedLog]))
      },
      (task: Runnable) => work.addLast(task)
    )
    val pending = adapter.capture(challenge()).toCompletableFuture
    assertFalse(pending.isDone)
    assertNull(visitedId)
    assertNull(visitedPartition)
    assertEquals(1, work.size())
    val task = new Thread(work.removeFirst(), "broker-source-probe-test")
    task.start()
    task.join()
    assertEquals(topicId, visitedId)
    assertEquals(new TopicPartition("native-topic", 0), visitedPartition)
    // A classic Kafka UnifiedLog cannot become shared-storage evidence.
    assertTrue(pending.join().isEmpty)
  }

  @Test
  def testMissingTopicIdNeverTouchesLogManager(): Unit = {
    val adapter = BrokerServer.readOnlySourceWalProbeSampler(
      new SharedStorageExtension(),
      id => {
        assertEquals(topicId, id)
        Optional.empty[String]()
      },
      _ => throw new AssertionError("Missing TopicId must not query the log"),
      (task: Runnable) => new Thread(task, "broker-source-missing-test").start()
    )
    assertTrue(adapter.capture(challenge()).toCompletableFuture.join().isEmpty)
  }

  @Test
  def testRemovedLocalReplicaCannotSupplySourceWalEvidence(): Unit = {
    val adapter = BrokerServer.readOnlySourceWalProbeSampler(
      new SharedStorageExtension(),
      _ => Optional.of("native-topic"),
      tp => {
        assertEquals(new TopicPartition("native-topic", 0), tp)
        None
      },
      (task: Runnable) => task.run()
    )
    assertTrue(adapter.capture(challenge()).toCompletableFuture.join().isEmpty)
  }

  @Test
  def testRejectedWorkerDoesNotRunNativeSamplingOnHeartbeatThread(): Unit = {
    var resolverInvoked = false
    val rejected: Executor = _ => throw new RejectedExecutionException("worker full")
    val adapter = BrokerServer.readOnlySourceWalProbeSampler(
      new SharedStorageExtension(),
      _ => {
        resolverInvoked = true
        Optional.of("native-topic")
      },
      _ => None,
      rejected
    )
    assertThrows(classOf[RejectedExecutionException], () => {
      adapter.capture(challenge())
      ()
    })
    assertFalse(resolverInvoked)
  }

  @Test
  def testInlineWorkerCannotReadMetadataOrNativeSource(): Unit = {
    var called = false
    val adapter = BrokerServer.readOnlySourceWalProbeSampler(
      new SharedStorageExtension(),
      _ => {
        called = true
        Optional.of("native-topic")
      },
      _ => None,
      (task: Runnable) => task.run()
    )
    val failure = assertThrows(classOf[CompletionException], () => {
      adapter.capture(challenge()).toCompletableFuture.join()
      ()
    })
    assertTrue(failure.getCause.isInstanceOf[IllegalStateException])
    assertFalse(called)
  }

  @Test
  def testNullTopicMetadataResponseNeverFallsBackToLocalLog(): Unit = {
    val adapter = BrokerServer.readOnlySourceWalProbeSampler(
      new SharedStorageExtension(),
      _ => null,
      _ => throw new AssertionError("Unknown metadata must not reach LogManager"),
      (task: Runnable) => new Thread(task, "broker-null-topic-test").start()
    )
    val failure = assertThrows(classOf[CompletionException], () => {
      adapter.capture(challenge()).toCompletableFuture.join()
      ()
    })
    assertTrue(failure.getCause.isInstanceOf[NullPointerException])
  }

  private def challenge(): PartitionRetirementSourceProbe.Request = {
    new PartitionRetirementSourceProbe.Request(
      new Uuid(1L, 2L), "cluster-a", topicId, 0,
      3, 54L, new Uuid(5L, 6L), 8, -1L, 100L, 0L
    )
  }
}
