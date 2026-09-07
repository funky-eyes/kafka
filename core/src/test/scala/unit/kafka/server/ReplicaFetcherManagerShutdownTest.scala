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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.{AfterEach, Test}
import org.mockito.Mockito.mock

class ReplicaFetcherManagerShutdownTest {

  @AfterEach
  def cleanup(): Unit = {
    TestUtils.clearYammerMetrics()
  }

  @Test
  def shouldClearFailedPartitionsWhenFetcherManagerShutsDown(): Unit = {
    val config = KafkaConfig.fromProps(TestUtils.createBrokerConfig(1))
    val metrics = new Metrics()
    val manager = new ReplicaFetcherManager(
      config,
      mock(classOf[ReplicaManager]),
      metrics,
      new MockTime(),
      mock(classOf[ReplicationQuotaManager]),
      () => MetadataVersion.LATEST_PRODUCTION,
      () => 1L
    )
    val topicPartition = new TopicPartition("shared-recovery-restart", 0)

    manager.failedPartitions.add(topicPartition)
    assertTrue(manager.failedPartitions.contains(topicPartition))

    manager.shutdown()

    assertFalse(manager.failedPartitions.contains(topicPartition))
    assertTrue(manager.failedPartitions.partitions().isEmpty)
    metrics.close()
  }
}
