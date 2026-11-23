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

import kafka.utils.Logging

import scala.collection.mutable
import scala.util.control.NonFatal

/** Coalesces completed WAL notifications without executing broker work on a WAL thread. */
private[server] final class SharedWalConfirmationBatcher[A](schedule: Runnable => Unit,
                                                             process: Vector[A] => Unit,
                                                             reject: (A, Throwable) => Unit,
                                                             maxBatch: Int = 1024) extends Logging {
  require(maxBatch > 0)
  private val lock = new Object
  private val pending = mutable.Queue.empty[A]
  private var scheduled = false

  def submit(item: A): Unit = {
    val start = lock.synchronized {
      pending.enqueue(item)
      if (scheduled) false else {
        scheduled = true
        true
      }
    }
    if (start) scheduleDrain()
  }

  private def scheduleDrain(): Unit = {
    try schedule(() => drain())
    catch {
      case NonFatal(failure) =>
        val rejected = lock.synchronized {
          val items = pending.toVector
          pending.clear()
          scheduled = false
          items
        }
        rejectBatch(rejected, failure)
    }
  }

  private def drain(): Unit = {
    val batch = lock.synchronized {
      val builder = Vector.newBuilder[A]
      var count = 0
      while (count < maxBatch && pending.nonEmpty) {
        builder += pending.dequeue()
        count += 1
      }
      builder.result()
    }
    try process(batch)
    catch {
      case NonFatal(failure) => rejectBatch(batch, failure)
    } finally {
      val again = lock.synchronized {
        if (pending.nonEmpty) true else {
          scheduled = false
          false
        }
      }
      if (again) scheduleDrain()
    }
  }

  private def rejectBatch(items: Vector[A], failure: Throwable): Unit = {
    items.foreach { item =>
      try reject(item, failure)
      catch {
        case NonFatal(exception) => error("Unable to fence a rejected shared WAL confirmation", exception)
      }
    }
  }
}
