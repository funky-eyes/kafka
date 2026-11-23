/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import java.util.concurrent.{CountDownLatch, Executors, LinkedBlockingQueue, RejectedExecutionException, TimeUnit}
import scala.collection.mutable

class SharedWalConfirmationBatcherTest {
  @Test
  def coalescesAndBoundsNotifications(): Unit = {
    val scheduled = mutable.Queue.empty[Runnable]
    val batches = mutable.Buffer.empty[Vector[Int]]
    val rejected = mutable.Buffer.empty[Int]
    val batcher = new SharedWalConfirmationBatcher[Int](scheduled.enqueue(_), batches += _,
      (item, _) => rejected += item, maxBatch = 2)

    (1 to 5).foreach(batcher.submit)
    assertEquals(1, scheduled.size)
    while (scheduled.nonEmpty) scheduled.dequeue().run()
    assertEquals(List(Vector(1, 2), Vector(3, 4), Vector(5)), batches.toList)
    assertTrue(rejected.isEmpty)
  }

  @Test
  def notificationArrivingDuringDrainIsNotLost(): Unit = {
    val scheduled = mutable.Queue.empty[Runnable]
    val processed = mutable.Buffer.empty[Int]
    var batcher: SharedWalConfirmationBatcher[Int] = null
    batcher = new SharedWalConfirmationBatcher[Int](scheduled.enqueue(_), batch => {
      processed ++= batch
      if (batch.contains(1)) batcher.submit(2)
    }, (_, failure) => fail(failure))

    batcher.submit(1)
    scheduled.dequeue().run()
    assertEquals(1, scheduled.size)
    scheduled.dequeue().run()
    assertEquals(List(1, 2), processed.toList)
    assertTrue(scheduled.isEmpty)
  }

  @Test
  def rejectedRescheduleFailsEveryWaitingNotificationAndCanRecover(): Unit = {
    val scheduled = mutable.Queue.empty[Runnable]
    val processed = mutable.Buffer.empty[Int]
    val rejected = mutable.Buffer.empty[Int]
    var rejectScheduling = false
    val batcher = new SharedWalConfirmationBatcher[Int](task => {
      if (rejectScheduling) throw new RejectedExecutionException("stopped")
      scheduled.enqueue(task)
    }, processed ++= _, (item, _) => rejected += item, maxBatch = 1)

    batcher.submit(1)
    batcher.submit(2)
    batcher.submit(3)
    rejectScheduling = true
    scheduled.dequeue().run()
    assertEquals(List(1), processed.toList)
    assertEquals(List(2, 3), rejected.toList)

    rejectScheduling = false
    batcher.submit(4)
    scheduled.dequeue().run()
    assertEquals(List(1, 4), processed.toList)
  }

  @Test
  def concurrentSubmittersCannotLoseNotifications(): Unit = {
    val tasks = new LinkedBlockingQueue[Runnable]()
    val seen = mutable.Set.empty[Int]
    val finished = new CountDownLatch(2000)
    val batcher = new SharedWalConfirmationBatcher[Int](task => { tasks.add(task); () }, batch => {
      batch.foreach { value =>
        assertTrue(seen.add(value), s"duplicate confirmation $value")
        finished.countDown()
      }
    }, (_, failure) => fail(failure), maxBatch = 32)
    val pool = Executors.newFixedThreadPool(4)
    val start = new CountDownLatch(1)
    try {
      val producers = (0 until 4).map { worker =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            start.await()
            (0 until 500).foreach(i => batcher.submit(worker * 500 + i))
          }
        })
      }
      start.countDown()
      while (finished.getCount > 0) {
        val task = tasks.poll(10, TimeUnit.SECONDS)
        assertNotNull(task, "a pending confirmation must have a scheduled drain")
        task.run()
      }
      producers.foreach(_.get(10, TimeUnit.SECONDS))
      assertEquals(2000, seen.size)
    } finally {
      pool.shutdownNow()
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS))
    }
  }

  @Test
  def aFailureWhileFencingOneNotificationDoesNotSkipOthers(): Unit = {
    val scheduled = mutable.Queue.empty[Runnable]
    val rejected = mutable.Buffer.empty[Int]
    val batcher = new SharedWalConfirmationBatcher[Int](scheduled.enqueue(_), _ => {
      throw new IllegalStateException("injected publication failure")
    }, (item, _) => {
      rejected += item
      if (item == 1) throw new IllegalStateException("injected fence failure")
    })
    batcher.submit(1)
    batcher.submit(2)
    scheduled.dequeue().run()
    assertEquals(List(1, 2), rejected.toList)
    assertTrue(scheduled.isEmpty)
  }
}
