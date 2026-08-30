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

import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaObjectMetadataStoreTest {
    @Test
    void retriesOnlyTransientMetadataTopicCreationFailures() {
        assertTrue(KafkaObjectMetadataStore.isTransientTopicCreationFailure(
            new InvalidReplicationFactorException("not enough brokers yet")));
        assertTrue(KafkaObjectMetadataStore.isTransientTopicCreationFailure(
            new TimeoutException("controller metadata is still converging")));
        assertFalse(KafkaObjectMetadataStore.isTransientTopicCreationFailure(
            new IllegalStateException("permanent configuration failure")));
    }

    @Test
    void retriesTransientRemotePlaneBootstrapFailuresThroughIOExceptionWrapping() {
        assertTrue(S3SharedStorageExtension.isRetriableMetadataBootstrapFailure(
            new IOException(
                "Unable to initialize shared metadata store",
                new UnknownTopicOrPartitionException("metadata partition not locally visible yet")
            )
        ));
        assertTrue(S3SharedStorageExtension.isRetriableMetadataBootstrapFailure(
            new IOException("wrapped", new TimeoutException("metadata still converging"))
        ));
        assertFalse(S3SharedStorageExtension.isRetriableMetadataBootstrapFailure(
            new IOException("permanent", new IllegalStateException("invalid cleanup policy"))
        ));
    }
}
