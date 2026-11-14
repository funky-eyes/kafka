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
package org.apache.kafka.jmh.storage;

import org.apache.kafka.storage.internals.shared.wal.AsynchronousFileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.FileChannelWalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.RingSharedWal;
import org.apache.kafka.storage.internals.shared.wal.RotatingFileSharedWal;
import org.apache.kafka.storage.internals.shared.wal.SharedWal;
import org.apache.kafka.storage.internals.shared.wal.WalDurabilityStats;
import org.apache.kafka.storage.internals.shared.wal.WalIoBackend;
import org.apache.kafka.storage.internals.shared.wal.WalRecord;
import org.apache.kafka.storage.internals.shared.wal.WalWriteLatencyStats;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Same-device, local-WAL durable append latency comparison. NOT an acks=all
 * Producer benchmark and NOT an IOPS/throughput emulator.
 *
 * <p>Use the JMH -t option for concurrent appenders. A separate maintenance
 * thread reclaims acknowledged WAL groups to keep the trial running longer
 * than the physical ring capacity. That maintenance is part of the measured
 * system workload and can generate additional checkpoint forces.</p>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
public class SharedWalDurabilityBench {
    private static final long CAPACITY_BYTES = 128L * 1024L * 1024L;
    private static final long SEGMENT_BYTES = 16L * 1024L * 1024L;

    @Param({"1024", "4096", "65536", "131072", "262144", "1048576"})
    public int payloadBytes;

    @Param({"ring", "rotating-file"})
    public String engine;

    @Param({"nio", "aio"})
    public String ioBackend;

    private final AtomicLong nextOffset = new AtomicLong();
    private final AtomicReference<Throwable> maintenanceFailure = new AtomicReference<>();
    private Path workingDirectory;
    private SharedWal wal;
    private ScheduledExecutorService maintenance;
    private byte[] payload;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        workingDirectory = Files.createTempDirectory("shared-wal-durability-jmh-");
        payload = new byte[payloadBytes];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i & 0xff);
        }
        WalIoBackend physical = "aio".equals(ioBackend)
            ? new AsynchronousFileChannelWalIoBackend()
            : new FileChannelWalIoBackend();
        wal = "ring".equals(engine)
            ? new RingSharedWal(workingDirectory.resolve("ring.wal"),
                CAPACITY_BYTES + 8192L, physical)
            : new RotatingFileSharedWal(workingDirectory.resolve("rotating"),
                CAPACITY_BYTES, SEGMENT_BYTES, physical);

        maintenance = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "shared-wal-jmh-maintenance");
            thread.setDaemon(true);
            return thread;
        });
        maintenance.scheduleWithFixedDelay(() -> {
            try {
                if (wal.usedBytes() > CAPACITY_BYTES / 2L) {
                    wal.reclaim((record, ignored) -> true, CAPACITY_BYTES / 4L);
                }
            } catch (Throwable failure) {
                maintenanceFailure.compareAndSet(null, failure);
            }
        }, 10L, 10L, TimeUnit.MILLISECONDS);
    }

    @Benchmark
    public int durableAppend() {
        Throwable failure = maintenanceFailure.get();
        if (failure != null) {
            throw new IllegalStateException("WAL maintenance failed during benchmark", failure);
        }
        long offset = nextOffset.getAndIncrement();
        // Owned immutable payload is deliberately shared across invocations:
        // avoid benchmarking a 1 MiB heap copy instead of WAL persistence.
        WalRecord record = WalRecord.dataOwned(1L, 2L, 0, 0, offset, offset,
            ByteBuffer.wrap(payload).asReadOnlyBuffer());
        return wal.appendBatch(List.of(record)).join().get(0).length();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (maintenance != null) {
            maintenance.shutdown();
            if (!maintenance.awaitTermination(10L, TimeUnit.SECONDS)) {
                maintenance.shutdownNow();
            }
        }
        if (wal != null) {
            WalDurabilityStats durability = wal.durabilityStats();
            WalWriteLatencyStats latency = wal.writeLatencyStats();
            System.out.printf("LOCAL-WAL-JMH engine=%s backend=%s bytes=%d batches=%d " +
                "dataForceNs=%d checkpointForceNs=%d queuedGroups=%d queueNs=%d physicalWriteNs=%d%n",
                engine, ioBackend, payloadBytes, durability.durabilityBatchCount(),
                durability.durabilityDataForceNanos(), durability.durabilityCheckpointForceNanos(),
                latency.queuedAppendGroups(), latency.totalQueueNanos(), latency.totalPhysicalWriteNanos());
            wal.close();
        }
        if (workingDirectory != null) {
            try (Stream<Path> entries = Files.walk(workingDirectory)) {
                for (Path entry : entries.sorted(Comparator.reverseOrder()).toList()) {
                    Files.deleteIfExists(entry);
                }
            }
        }
    }
}
