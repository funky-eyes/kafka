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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BrokerObjectIdTest {
    @Test
    void roundTripsBrokerAndSequence() {
        long objectId = BrokerObjectId.compose(42, 123_456L);

        assertEquals(42, BrokerObjectId.brokerId(objectId));
        assertEquals(123_456L, BrokerObjectId.sequence(objectId));
        assertTrue(objectId > 0);
    }

    @Test
    void differentBrokersCannotCollideAtSameSequence() {
        long brokerOne = BrokerObjectId.compose(1, 99L);
        long brokerTwo = BrokerObjectId.compose(2, 99L);

        assertNotEquals(brokerOne, brokerTwo);
        assertEquals(99L, BrokerObjectId.sequence(brokerOne));
        assertEquals(99L, BrokerObjectId.sequence(brokerTwo));
    }

    @Test
    void consumesAllPositiveSignedLongBitsWithoutOverflow() {
        long largest = BrokerObjectId.compose(
            BrokerObjectId.MAX_BROKER_ID,
            BrokerObjectId.MAX_SEQUENCE
        );

        assertEquals(Long.MAX_VALUE, largest);
        assertEquals(BrokerObjectId.MAX_BROKER_ID, BrokerObjectId.brokerId(largest));
        assertEquals(BrokerObjectId.MAX_SEQUENCE, BrokerObjectId.sequence(largest));
    }

    @Test
    void rejectsReservedOrOutOfRangeComponents() {
        assertThrows(IllegalArgumentException.class, () -> BrokerObjectId.compose(-1, 1L));
        assertThrows(
            IllegalArgumentException.class,
            () -> BrokerObjectId.compose(BrokerObjectId.MAX_BROKER_ID + 1, 1L)
        );
        assertThrows(IllegalArgumentException.class, () -> BrokerObjectId.compose(1, 0L));
        assertThrows(
            IllegalArgumentException.class,
            () -> BrokerObjectId.compose(1, BrokerObjectId.MAX_SEQUENCE + 1L)
        );
        assertThrows(IllegalArgumentException.class, () -> BrokerObjectId.brokerId(0L));
        assertThrows(IllegalArgumentException.class, () -> BrokerObjectId.sequence(-1L));
    }
}
