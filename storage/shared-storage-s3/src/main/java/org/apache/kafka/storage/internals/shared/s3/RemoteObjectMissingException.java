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

/**
 * A previously referenced S3 object was not retrievable. An external Bucket
 * expiration rule, administrator deletion, incorrect key/bucket or remote
 * corruption can all produce this result. Never translate it to an absent
 * Kafka offset or silently move LogStartOffset/HighWatermark.
 */
public final class RemoteObjectMissingException extends IllegalStateException {
    private final long objectId;

    public RemoteObjectMissingException(long objectId, String bucket, Throwable cause) {
        super("Shared-storage physical object " + objectId + " is unavailable (HTTP 404) in bucket "
            + bucket + ". An S3 lifecycle expiration or external deletion may have removed a "
            + "COMMITTED reference; retain Kafka offsets and use any independently verified "
            + "local or alternate physical copy before declaring data unrecoverable.", cause);
        this.objectId = objectId;
    }

    public long objectId() {
        return objectId;
    }
}
