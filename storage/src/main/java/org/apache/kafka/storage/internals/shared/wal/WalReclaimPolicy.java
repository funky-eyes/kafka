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

/**
 * Decides whether one durable WAL record may be removed from the local recovery window.
 *
 * <p>The WAL invokes this policy only for user DATA/TRUNCATE records, never for the internal GROUP_COMMIT marker.
 * Reclamation is still constrained by the WAL implementation to a contiguous physical prefix ending at a complete
 * append-group boundary, so a policy can never cause half of an atomic Kafka append to survive on disk.</p>
 */
@FunctionalInterface
public interface WalReclaimPolicy {
    boolean canReclaim(WalRecord record, WalAppendResult appendResult);
}
