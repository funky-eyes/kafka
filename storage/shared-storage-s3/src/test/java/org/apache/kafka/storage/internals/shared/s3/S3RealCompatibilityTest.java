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
package org.apache.kafka.storage.internals.shared.s3;

import org.apache.kafka.storage.internals.shared.object.ObjectStore;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Compatibility proof against the real AWS S3 service rather than an S3-compatible emulator.
 *
 * <p>The test deliberately leaves endpoint override empty and path-style access disabled. This exercises the AWS SDK
 * default endpoint resolution, TLS, virtual-hosted bucket addressing, workload credentials, Range GET semantics and
 * the known-size streaming multipart path used by the production Shared Object uploader.</p>
 */
@Timeout(value = 5, unit = TimeUnit.MINUTES)
class S3RealCompatibilityTest {
    private static final int SMALL_OBJECT_BYTES = 4 * 1024;
    private static final int MULTIPART_TAIL_BYTES = 4 * 1024;

    @Test
    void roundTripsSingleAndMultipartObjectsAgainstAwsS3() throws Exception {
        String bucket = System.getenv("SHARED_STORAGE_REAL_S3_BUCKET");
        assumeTrue(bucket != null && !bucket.isBlank(), "Real AWS S3 compatibility bucket is not configured");

        String region = environment("SHARED_STORAGE_REAL_S3_REGION", S3ObjectStoreConfig.DEFAULT_REGION);
        String prefix = "ga-compatibility/" + UUID.randomUUID() + "/objects";
        S3ObjectStoreConfig config = new S3ObjectStoreConfig(
            bucket.trim(),
            prefix,
            region,
            Optional.empty(),
            false,
            2
        );

        byte[] small = bytes(SMALL_OBJECT_BYTES, 17);
        byte[] firstPart = bytes(S3ObjectStore.MIN_MULTIPART_PART_BYTES, 41);
        byte[] finalPart = bytes(MULTIPART_TAIL_BYTES, 73);

        try (S3ObjectStore store = new S3ObjectStore(config)) {
            try {
                store.put(1L, ByteBuffer.wrap(small)).get(60, TimeUnit.SECONDS);
                assertArrayEquals(
                    Arrays.copyOfRange(small, 101, 229),
                    bytes(store.rangeRead(1L, 101L, 128).get(60, TimeUnit.SECONDS))
                );

                long multipartBytes = Math.addExact((long) firstPart.length, finalPart.length);
                store.put(
                    2L,
                    multipartBytes,
                    partSource(firstPart, finalPart)
                ).get(120, TimeUnit.SECONDS);

                int boundaryStart = firstPart.length - 64;
                byte[] expectedBoundary = new byte[128];
                System.arraycopy(firstPart, boundaryStart, expectedBoundary, 0, 64);
                System.arraycopy(finalPart, 0, expectedBoundary, 64, 64);
                assertArrayEquals(
                    expectedBoundary,
                    bytes(store.rangeRead(2L, boundaryStart, 128).get(60, TimeUnit.SECONDS))
                );

                assertEquals(0, store.rangeRead(2L, 0L, 0).get(60, TimeUnit.SECONDS).remaining());
            } finally {
                deleteObjects(store, 1L, 2L);
            }
        }
    }

    private static ObjectStore.PartSource partSource(byte[]... parts) {
        return new ObjectStore.PartSource() {
            private int index;

            @Override
            public ByteBuffer nextPart() {
                if (index >= parts.length) {
                    return null;
                }
                return ByteBuffer.wrap(parts[index++]);
            }
        };
    }

    private static void deleteObjects(S3ObjectStore store, long... objectIds) throws Exception {
        Exception failure = null;
        for (long objectId : objectIds) {
            try {
                store.delete(objectId).get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static byte[] bytes(int size, int seed) {
        byte[] value = new byte[size];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index * 31);
        }
        return value;
    }

    private static byte[] bytes(ByteBuffer buffer) {
        ByteBuffer duplicate = buffer.duplicate();
        byte[] value = new byte[duplicate.remaining()];
        duplicate.get(value);
        return value;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }
}
