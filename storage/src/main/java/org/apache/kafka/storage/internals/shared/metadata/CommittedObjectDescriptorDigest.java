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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Versioned canonical SHA-256 identity for one immutable COMMITTED KSO2
 * descriptor, including every ordered packed range.
 *
 * <p>Only the descriptor's bytes are hashed, not an arbitrary Java
 * toString(), hashCode(), map order, or the current metadata replay offset.
 * The fixed-endian encoding and domain tag allow another participant to
 * independently reproduce the same identity without materializing the
 * physical KSO2 object. All fields of every packed range are included.</p>
 *
 * <p>IMPORTANT: matching hashes do NOT authenticate the COMMITTED metadata
 * log, establish a sealed read-committed horizon, fence late PUT/COMMIT,
 * prove global Broker drain, or authorize S3 physical deletion. A future
 * Controller handoff must independently establish those properties before
 * persisting RETIRE_INTENT or DELETE_READY.</p>
 */
public final class CommittedObjectDescriptorDigest {
    private static final byte[] DOMAIN =
        "KSO2-COMMITTED-DESCRIPTOR".getBytes(StandardCharsets.US_ASCII);
    private static final int FORMAT_VERSION = 1;

    private CommittedObjectDescriptorDigest() {
    }

    /** Lowercase SHA-256 hex over the exact, ordered COMMITTED descriptor. */
    public static String sha256(SharedObjectMetadata object) {
        Objects.requireNonNull(object, "object");
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK SHA-256 implementation unavailable", e);
        }
        try (DataOutputStream out = new DataOutputStream(
            new DigestOutputStream(OutputStream.nullOutputStream(), digest)
        )) {
            out.write(DOMAIN);
            out.writeInt(FORMAT_VERSION);
            out.writeLong(object.objectId());
            out.writeLong(object.objectSize());
            out.writeLong(object.objectChecksum());
            out.writeInt(object.ranges().size());
            for (SharedObjectRange range : object.ranges()) {
                SharedPartitionId partition = range.partition();
                out.writeLong(partition.topicIdHigh());
                out.writeLong(partition.topicIdLow());
                out.writeInt(partition.partition());
                out.writeLong(range.offsets().startOffset());
                out.writeLong(range.offsets().endOffset());
                out.writeInt(range.leaderEpoch());
                out.writeLong(range.objectPosition());
                out.writeInt(range.objectLength());
                out.writeLong(range.checksum());
            }
        } catch (IOException impossible) {
            throw new IllegalStateException("Unable to hash in-memory descriptor", impossible);
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
