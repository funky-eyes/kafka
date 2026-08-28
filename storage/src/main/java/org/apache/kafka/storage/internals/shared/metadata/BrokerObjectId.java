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
package org.apache.kafka.storage.internals.shared.metadata;

/**
 * Stable 63-bit object ID layout that avoids cross-broker coordination on every S3 upload.
 *
 * <p>The high 24 bits identify the Kafka broker and the low 39 bits are a broker-local durable sequence. Sequence zero
 * is reserved, so every valid object ID is strictly positive. The sequence is persisted/reserved by the metadata plane
 * before use; this class only owns the format and validation.</p>
 */
public final class BrokerObjectId {
    public static final int BROKER_ID_BITS = 24;
    public static final int SEQUENCE_BITS = 39;
    public static final int MAX_BROKER_ID = (1 << BROKER_ID_BITS) - 1;
    public static final long MAX_SEQUENCE = (1L << SEQUENCE_BITS) - 1L;

    private BrokerObjectId() {
    }

    public static long compose(int brokerId, long sequence) {
        if (brokerId < 0 || brokerId > MAX_BROKER_ID) {
            throw new IllegalArgumentException(
                "brokerId must be in [0, " + MAX_BROKER_ID + "]: " + brokerId);
        }
        if (sequence <= 0 || sequence > MAX_SEQUENCE) {
            throw new IllegalArgumentException(
                "sequence must be in [1, " + MAX_SEQUENCE + "]: " + sequence);
        }
        return ((long) brokerId << SEQUENCE_BITS) | sequence;
    }

    public static int brokerId(long objectId) {
        validateObjectId(objectId);
        return (int) (objectId >>> SEQUENCE_BITS);
    }

    public static long sequence(long objectId) {
        validateObjectId(objectId);
        return objectId & MAX_SEQUENCE;
    }

    private static void validateObjectId(long objectId) {
        if (objectId <= 0) {
            throw new IllegalArgumentException("objectId must be positive: " + objectId);
        }
    }
}
