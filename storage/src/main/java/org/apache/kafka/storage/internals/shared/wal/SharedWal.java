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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface SharedWal extends AutoCloseable {
    CompletableFuture<WalAppendResult> append(WalRecord record);

    WalRecord read(WalLocation location) throws IOException;

    default List<WalRecord> readBatch(List<WalLocation> locations) throws IOException {
        List<WalRecord> records = new ArrayList<>(locations.size());
        for (WalLocation location : locations) {
            records.add(read(location));
        }
        return List.copyOf(records);
    }

    void replay(WalReplayConsumer consumer) throws IOException;

    long usedBytes();

    long capacityBytes();

    @Override
    void close() throws IOException;
}
