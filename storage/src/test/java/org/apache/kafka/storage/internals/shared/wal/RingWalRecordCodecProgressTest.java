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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RingWalRecordCodecProgressTest {
    @Test
    void failsFastWhenBackendReadMakesNoProgress() {
        IOException failure = assertThrows(
            IOException.class,
            () -> WalRecordCodec.read(new ZeroProgressReadHandle(), 0L)
        );

        assertEquals("Unable to make progress reading WAL at position 0", failure.getMessage());
    }

    private static final class ZeroProgressReadHandle implements WalIoBackend.Handle {
        @Override
        public long size() {
            return WalRecordCodec.PREFIX_BYTES;
        }

        @Override
        public int read(ByteBuffer destination, long position) {
            return 0;
        }

        @Override
        public int write(ByteBuffer source, long position) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void truncate(long size) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void force() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void seal() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
        }
    }
}
