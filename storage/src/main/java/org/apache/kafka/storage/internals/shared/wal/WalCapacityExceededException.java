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
package org.apache.kafka.storage.internals.shared.wal;

import org.apache.kafka.common.errors.KafkaStorageException;

/**
 * Retriable admission failure raised before a logical append group modifies the shared WAL.
 *
 * <p>This is deliberately not an {@link java.io.IOException}. Kafka treats an IOException from a log append as a
 * physical log-directory failure and takes the directory offline. Capacity pressure is different: the disk and WAL
 * are healthy, but accepting another append would overwrite or exceed the configured durability window. Propagating a
 * Kafka storage error rejects the produce request while keeping the broker and log directory online so asynchronous
 * remote upload or operator action can relieve the pressure.</p>
 */
public final class WalCapacityExceededException extends KafkaStorageException {
    public WalCapacityExceededException(String message) {
        super(message);
    }
}
