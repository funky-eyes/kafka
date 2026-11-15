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
package org.apache.kafka.storage.internals.shared.object;

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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.concurrent.TimeUnit;

/**
 * Diagnostic microbenchmark for the actual process-local COMMITTED reader
 * admission/release implementation, not a distributed GC throughput claim.
 *
 * <p>One object ID makes eight readers contend on the same atomic map key.
 * 1024 IDs give each JMH worker an independent key. This isolates the
 * contention pattern without fake S3/Controller latency or a synthetic
 * synchronized baseline with different semantics.</p>
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(8)
public class ActiveCommittedObjectReadsBenchmark {
    @Param({"1", "1024"})
    private int objectCardinality;
    private ActiveCommittedObjectReads reads;

    @Setup(Level.Trial)
    public void setup() {
        reads = new ActiveCommittedObjectReads();
    }

    @Benchmark
    public void physicalRangeGetLease(ThreadParams thread) {
        long objectId = 1L + thread.getThreadIndex() % objectCardinality;
        ActiveCommittedObjectReads.ReadLease lease = reads.tryAcquire(objectId).orElseThrow();
        lease.close();
    }

    @Benchmark
    public void logicalCachedReadLease(ThreadParams thread) {
        long objectId = 1L + thread.getThreadIndex() % objectCardinality;
        ActiveCommittedObjectReads.ReadLease lease = reads.tryAcquireLogicalRead(objectId).orElseThrow();
        lease.close();
    }
}
