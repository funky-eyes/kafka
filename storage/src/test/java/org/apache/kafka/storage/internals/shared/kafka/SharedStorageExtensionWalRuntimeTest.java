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

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;
import org.apache.kafka.storage.internals.log.StorageExtensionContext;
import org.apache.kafka.storage.internals.shared.metadata.SharedPartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageExtensionWalRuntimeTest {
    private static final long WAL_CAPACITY = 64L * 1024L;

    @TempDir
    Path tempDir;

    @Test
    void defaultRuntimeActuallyStartsRingWal() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-ring"));
        Path walDir = tempDir.resolve("shared-wal-ring");
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, walDir, Map.of()));

            Path ringWal = SharedStorageWalFactory.ringWalPath(walDir);
            assertTrue(Files.isRegularFile(ringWal));
            assertEquals(WAL_CAPACITY, Files.size(ringWal));
        } finally {
            extension.close();
        }
    }

    @Test
    void explicitRollbackRuntimeActuallyStartsRotatingWal() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-rotating"));
        Path walDir = tempDir.resolve("shared-wal-rotating");
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(
                logDir,
                walDir,
                Map.of(
                    SharedStorageConfiguration.WAL_ENGINE_CONFIG, "rotating-file",
                    SharedStorageConfiguration.WAL_SEGMENT_BYTES_CONFIG, 16L * 1024L
                )
            ));
            extension.storage().appendData(
                new SharedPartitionId(1L, 2L, 0),
                3,
                0L,
                0L,
                ByteBuffer.wrap(new byte[] {1, 2, 3})
            ).join();

            assertFalse(Files.exists(SharedStorageWalFactory.ringWalPath(walDir)));
            try (var entries = Files.list(walDir)) {
                assertTrue(entries.anyMatch(path ->
                    path.getFileName().toString().matches("wal-\\d{20}\\.log")));
            }
        } finally {
            extension.close();
        }
    }

    @Test
    void bindsRealBrokerReadyIdentityToFrozenWalReplaySnapshot() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-bound"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-bound"), Map.of()));
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
            StorageExtensionBrokerContext registered = registeredContext(3, 54L, new Uuid(10L, 11L));
            extension.onBrokerReady(registered).join();

            BrokerStartupReplayObservation observation =
                extension.brokerStartupReplayObservation().orElseThrow();
            assertEquals("cluster-a", observation.clusterId());
            assertEquals(3, observation.brokerId());
            assertEquals(54L, observation.brokerEpoch());
            assertEquals(registered.brokerIncarnationId(), observation.brokerIncarnationId());
            assertSame(extension.storage().startupWalReplayEvidence(), observation.replay());
        } finally {
            extension.close();
        }
    }

    @Test
    void unboundBrokerReadyIdentityCannotProduceARecoveryPairing() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-unbound"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-unbound"), Map.of()));
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
            assertTrue(extension.brokerStartupReplayObservation().isPresent());
            extension.onBrokerReady(new StorageExtensionBrokerContext(
                "cluster-a", 3, listeners(), Map.of(), Time.SYSTEM
            )).join();
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
        } finally {
            extension.close();
        }
    }

    @Test
    void mismatchedKafkaBrokerIdCannotReusePreviousIdentitySnapshot() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-mismatch"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-mismatch"), Map.of()));
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
            extension.onBrokerReady(registeredContext(4, 54L, new Uuid(10L, 11L))).join();
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
        } finally {
            extension.close();
        }
    }

    @Test
    void reRegistrationRevokesOldProcessLocalPairingWithoutRebinding() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-rebound"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-rebound"), Map.of()));
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
            assertTrue(extension.brokerStartupReplayObservation().isPresent());

            extension.onBrokerReady(registeredContext(3, 55L, new Uuid(12L, 13L))).join();
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
            // ABA to the old identity must not resurrect the old snapshot.
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
        } finally {
            extension.close();
        }
    }

    @Test
    void repeatedIdenticalBrokerReadyIdentityIsIdempotent() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-identical"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-identical"), Map.of()));
            StorageExtensionBrokerContext same = registeredContext(3, 54L, new Uuid(10L, 11L));
            extension.onBrokerReady(same).join();
            BrokerStartupReplayObservation initial =
                extension.brokerStartupReplayObservation().orElseThrow();
            extension.onBrokerReady(same).join();
            assertSame(initial, extension.brokerStartupReplayObservation().orElseThrow());
        } finally {
            extension.close();
        }
    }

    @Test
    void changedClusterIdentityRevokesSnapshotEvenWithSameBrokerEpoch() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-cluster"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-cluster"), Map.of()));
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
            extension.onBrokerReady(new StorageExtensionBrokerContext(
                "different-cluster", 3, listeners(), Map.of(), Time.SYSTEM, 54L, new Uuid(10L, 11L)
            )).join();
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
        } finally {
            extension.close();
        }
    }

    @Test
    void unboundFirstCallbackCannotBeUpgradedWithoutWalRestart() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-unbound-first"));
        SharedStorageExtension extension = new SharedStorageExtension();
        try {
            extension.start(context(logDir, tempDir.resolve("wal-unbound-first"), Map.of()));
            extension.onBrokerReady(new StorageExtensionBrokerContext(
                "cluster-a", 3, listeners(), Map.of(), Time.SYSTEM
            )).join();
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
            assertTrue(extension.brokerStartupReplayObservation().isEmpty());
        } finally {
            extension.close();
        }
    }

    @Test
    void extensionCloseRevokesTheStartupIdentityPairing() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-closed"));
        SharedStorageExtension extension = new SharedStorageExtension();
        extension.start(context(logDir, tempDir.resolve("wal-closed"), Map.of()));
        extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))).join();
        assertTrue(extension.brokerStartupReplayObservation().isPresent());
        extension.close();
        assertTrue(extension.brokerStartupReplayObservation().isEmpty());
    }

    @Test
    void brokerReadyCallbackCannotBindIdentityBeforeWalStartup() {
        SharedStorageExtension extension = new SharedStorageExtension();
        assertThrows(IllegalStateException.class, () ->
            extension.onBrokerReady(registeredContext(3, 54L, new Uuid(10L, 11L))));
        assertTrue(extension.brokerStartupReplayObservation().isEmpty());
    }

    @Test
    void registeredIdentityPairsOnlyRecordsActuallyRecoveredOnRestart() throws Exception {
        Path logDir = Files.createDirectory(tempDir.resolve("kafka-log-recovered"));
        Path walDir = tempDir.resolve("wal-recovered");
        SharedPartitionId partition = new SharedPartitionId(1L, 2L, 0);
        SharedStorageExtension original = new SharedStorageExtension();
        try {
            original.start(context(logDir, walDir, Map.of()));
            original.storage().appendData(
                partition, 3, 0L, 0L, ByteBuffer.wrap(new byte[]{1})
            ).join();
        } finally {
            original.close();
        }

        SharedStorageExtension restarted = new SharedStorageExtension();
        try {
            restarted.start(context(logDir, walDir, Map.of()));
            restarted.onBrokerReady(registeredContext(3, 55L, new Uuid(14L, 15L))).join();
            BrokerStartupReplayObservation observation =
                restarted.brokerStartupReplayObservation().orElseThrow();
            assertEquals(1L, observation.replay().replayedRecords());
            assertEquals(1L, observation.replay().partition(partition).orElseThrow().dataRecords());
            assertEquals(new Uuid(14L, 15L), observation.brokerIncarnationId());
        } finally {
            restarted.close();
        }
    }

    private static List<Endpoint> listeners() {
        return List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "127.0.0.1", 9092));
    }

    private static StorageExtensionBrokerContext registeredContext(int brokerId, long epoch, Uuid incarnation) {
        return new StorageExtensionBrokerContext(
            "cluster-a", brokerId, listeners(), Map.of(), Time.SYSTEM, epoch, incarnation
        );
    }

    private static StorageExtensionContext context(
        Path logDir,
        Path walDir,
        Map<String, ?> extra
    ) {
        HashMap<String, Object> originals = new HashMap<>();
        originals.put(SharedStorageConfiguration.WAL_DIR_CONFIG, walDir.toString());
        originals.put(SharedStorageConfiguration.WAL_CAPACITY_BYTES_CONFIG, WAL_CAPACITY);
        originals.putAll(extra);
        return new StorageExtensionContext(
            originals,
            List.of(logDir.toFile()),
            3,
            Time.SYSTEM
        );
    }
}
