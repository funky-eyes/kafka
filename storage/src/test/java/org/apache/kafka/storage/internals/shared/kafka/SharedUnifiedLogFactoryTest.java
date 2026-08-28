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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InconsistentTopicIdException;
import org.apache.kafka.storage.internals.checkpoint.PartitionMetadataFile;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedUnifiedLogFactoryTest {
    @Test
    void persistsTopicIdBeforeSharedWalCanBeUsed() throws IOException {
        File dir = TestUtils.tempDirectory();
        TopicPartition topicPartition = new TopicPartition("shared-topic", 0);
        Uuid topicId = Uuid.randomUuid();
        LogDirFailureChannel channel = Mockito.mock(LogDirFailureChannel.class);

        Uuid resolved = SharedUnifiedLogFactory.resolveAndPersistTopicId(
            dir,
            topicPartition,
            Optional.of(topicId),
            channel
        );

        assertEquals(topicId, resolved);
        PartitionMetadataFile metadataFile = new PartitionMetadataFile(
            PartitionMetadataFile.newFile(dir),
            channel
        );
        assertTrue(metadataFile.exists());
        assertEquals(topicId, metadataFile.read().topicId());
    }

    @Test
    void recoversTopicIdFromPartitionMetadataOnRestart() throws IOException {
        File dir = TestUtils.tempDirectory();
        TopicPartition topicPartition = new TopicPartition("shared-topic", 1);
        Uuid topicId = Uuid.randomUuid();
        LogDirFailureChannel channel = Mockito.mock(LogDirFailureChannel.class);

        SharedUnifiedLogFactory.resolveAndPersistTopicId(
            dir,
            topicPartition,
            Optional.of(topicId),
            channel
        );

        Uuid recovered = SharedUnifiedLogFactory.resolveAndPersistTopicId(
            dir,
            topicPartition,
            Optional.empty(),
            channel
        );
        assertEquals(topicId, recovered);
    }

    @Test
    void rejectsTopicIdMismatchBeforeWalRecovery() throws IOException {
        File dir = TestUtils.tempDirectory();
        TopicPartition topicPartition = new TopicPartition("shared-topic", 2);
        Uuid topicId = Uuid.randomUuid();
        LogDirFailureChannel channel = Mockito.mock(LogDirFailureChannel.class);

        SharedUnifiedLogFactory.resolveAndPersistTopicId(
            dir,
            topicPartition,
            Optional.of(topicId),
            channel
        );

        assertThrows(
            InconsistentTopicIdException.class,
            () -> SharedUnifiedLogFactory.resolveAndPersistTopicId(
                dir,
                topicPartition,
                Optional.of(Uuid.randomUuid()),
                channel
            )
        );
    }

    @Test
    void rejectsExistingSharedLogWithoutDurableTopicId() {
        File dir = TestUtils.tempDirectory();
        TopicPartition topicPartition = new TopicPartition("shared-topic", 3);
        LogDirFailureChannel channel = Mockito.mock(LogDirFailureChannel.class);

        IOException error = assertThrows(
            IOException.class,
            () -> SharedUnifiedLogFactory.resolveAndPersistTopicId(
                dir,
                topicPartition,
                Optional.empty(),
                channel
            )
        );
        assertTrue(error.getMessage().contains("requires a durable topic ID"));
    }
}
