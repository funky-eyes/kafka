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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Version-specific Kafka adapter kept outside the shared-storage engine.
 *
 * <p>Remote identity is defined at Kafka RecordBatch boundaries. This adapter materializes each mutable Kafka batch
 * into one owned buffer using Kafka's public batch serialization API. The engine therefore never depends on
 * MemoryRecords' private backing-buffer layout.</p>
 */
public final class KafkaRecordBatchAdapter {
    private KafkaRecordBatchAdapter() {
    }

    public static List<SerializedBatch> serializeBatches(MemoryRecords records) {
        List<SerializedBatch> result = new ArrayList<>();
        for (MutableRecordBatch batch : records.batches()) {
            ByteBuffer buffer = ByteBuffer.allocate(batch.sizeInBytes());
            ByteBufferOutputStream output = new ByteBufferOutputStream(buffer);
            batch.writeTo(output);
            ByteBuffer serialized = output.buffer();
            serialized.flip();
            if (serialized.remaining() != batch.sizeInBytes()) {
                throw new IllegalStateException(
                    "Serialized Kafka batch size mismatch: expected=" + batch.sizeInBytes() +
                        ", actual=" + serialized.remaining());
            }
            result.add(new SerializedBatch(
                batch.baseOffset(),
                batch.lastOffset(),
                batch.partitionLeaderEpoch(),
                batch.maxTimestamp(),
                serialized.asReadOnlyBuffer()
            ));
        }
        return List.copyOf(result);
    }

    public record SerializedBatch(
        long firstOffset,
        long lastOffset,
        int leaderEpoch,
        long maxTimestamp,
        ByteBuffer bytes
    ) {
        public SerializedBatch {
            if (firstOffset < 0 || lastOffset < firstOffset) {
                throw new IllegalArgumentException("invalid Kafka batch offset range");
            }
            if (bytes == null || !bytes.hasRemaining()) {
                throw new IllegalArgumentException("serialized Kafka batch must not be empty");
            }
            bytes = bytes.asReadOnlyBuffer();
        }
    }
}
