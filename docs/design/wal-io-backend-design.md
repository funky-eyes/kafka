# WAL I/O backend: verified implementation and native async design

## Decision and scope (2026-11-14)

**Keep the current `FileChannelWalIoBackend` as the correctness baseline. Do not label the current implementation native AIO, and do not make `io_uring` a prerequisite for Kafka's `acks=all` semantics.** Investigate a native asynchronous backend only behind explicit opt-in and the same durability tests.

This design applies to `shared-wal-s3-4.3.1` and should remain aligned with the tested source, not an aspirational architecture diagram. Existing Kafka Leader/Follower Fetch replication, ISR, `min.insync.replicas`, high watermark and Producer `acks` are authoritative; neither a WAL future nor a controller diagnostic record may independently authorize them.

## Verified data and physical I/O path

```text
Kafka Leader/Follower RecordBatch (native Fetch replication)
  -> SharedLogSegment / SharedStorageEngine
  -> SharedStorageWalFactory
       default shared.storage.wal.engine = ring
         -> RingSharedWal (fixed-capacity reusable circular WAL)
         -> RingWalFile
       optional shared.storage.wal.engine = rotating-file
         -> RotatingFileSharedWal
         -> FileSharedWal
  -> WalIoBackend / Handle
  -> FileChannelWalIoBackend
       FileChannel.read / write (blocking positional I/O)
       FileChannel.force(false) (synchronous durability barrier)
       flush parent directory for newly created physical units
  -> durable append future completion
  -> local WAL index / durability observation
  -> Kafka replication progress / ISR / HW / ACK decisions
  -> independently scheduled shared S3 object upload and safe WAL reclaim
```

The shared WAL does **not** require the Kafka log segment's full RecordBatch payload to be duplicated into an ordinary local partition log file; the adapter retains minimal Kafka-facing logical metadata/index state. A replica persists its received record batches to that Broker's own WAL. S3/MinIO provides shared object-backed committed data; loss of one Broker's WAL is protected by surviving ISR replicas under the configured native Kafka durability contract.

Current verified implementation:

- `SharedStorageConfiguration.DEFAULT_WAL_ENGINE = WalEngine.RING`, `SharedStorageWalFactory` selects `RingSharedWal` or `RotatingFileSharedWal`.
- Both physical WAL implementations use `FileChannelWalIoBackend` by default.
- `WalIoBackend.Handle.read/write/force` is a **synchronous** interface. `appendBatch()` returns `CompletableFuture` because the WAL has a background single-writer loop, batching, GROUP_COMMIT and a durability barrier, **not** because physical I/O uses Java `AsynchronousFileChannel`, Linux `libaio`, or `io_uring`.
- The backend currently advertises `supportsDirectIo() == false`; page-cache-backed positional file I/O is the default. Preallocation capability is distinct from direct I/O or native asynchronous submission.
- There is **no kernel-native-AIO WAL backend**, no runtime `io_uring` selection and no asynchronous `force` completion API. The opt-in Java AIO adapter awaits each operation's completion to preserve the current durability contract. A new backend cannot safely be substituted by renaming a class.

## Relationship to open-source AutoMQ

The *design inspiration* is shared storage with WAL on the synchronous write path and object-backed longer-term storage, not bit-for-bit reuse of AutoMQ's protocol or source. Open-source AutoMQ offers multiple WAL implementations, including object-storage WAL (`s3stream/.../wal/impl/object`). That object WAL buffers and asynchronously uploads batches; its behavior cannot be equated to this project's Broker-local WAL or used as evidence that all AutoMQ WAL variants use Java AIO or Linux `io_uring`.

Our deliberate difference is Kafka-native partition replication of the RecordBatch *into each Broker's WAL*, with Kafka ISR/HW and minISR controlling acknowledgement and failover, rather than inventing another independent replica quorum or WAL-certificate authority. A WAL-only failure/restart test with S3 offline is a required correctness gate, not an optimization benchmark.

## Selectable Java disk backends (experimental AIO)

The physical backend is configurable without changing the WAL data format:

```properties
shared.storage.wal.engine=ring
shared.storage.wal.io.backend=nio
# For experimental Java NIO.2 asynchronous channel reads/writes:
# shared.storage.wal.io.backend=aio
```

`nio` (default) uses synchronous `FileChannel` positional writes and `force(false)`.
`aio` selects `AsynchronousFileChannel` for positional read/write operations.
The existing WAL state-machine `Handle` contract is still synchronous: it awaits
each AIO operation's completion before proceeding to the next write/checkpoint
and calls the synchronous `AsynchronousFileChannel.force(false)` durability
barrier. Thus AIO is a **real Java AIO API**, but **not** a fully pipelined
nonblocking writer or a guarantee of kernel-native AIO on every JDK.

This switch applies to both Ring and rotating-file WAL, with identical durable
record and checkpoint formats. No `java.io.FileInputStream`,
`FileOutputStream` or `RandomAccessFile` is used for physical WAL I/O.
An unknown `bio` backend value is rejected at configuration parsing.

AIO is opt-in until durability/crash-recovery parity and performance benchmarks
have been verified on the target filesystem; never silently switch from a
failed in-flight AIO group to NIO and claim a successful ACK.

## Batched positional submission foundation (Ring integrated; rotating-file pending)

The physical Handle now exposes `writeBatch(List<WriteRequest>)` for
**disjoint** positional writes, bounded to 64 requests and 16 MiB per call. The portable NIO implementation writes them in
sequence; the Java AIO implementation submits independent writes first, then
drains every submitted completion (including partial writes and failure paths)
before returning. Overlapping ranges are rejected before any submission.
The caller retains ownership of all ByteBuffers until the method returns.

As of the merged #54 Ring WAL code, the Ring writer uses this method to
submit batches of disjoint DATA, padding, GROUP_COMMIT headers and payloads.
The rotating-file writer is not yet migrated. All force/checkpoint and Kafka
ACK conditions are unchanged. The API waits for every submitted write to
complete and does **not** make `force(false)` asynchronous; no performance
advantage is assumed without measured benchmarks.

## Ring WAL staged physical AIO batches (stacked follow-up)

The Ring WAL writer stages DATA, GROUP_COMMIT, header/payload and padding
fragments as disjoint physical positional writes. A fixed 64-request / 16 MiB
submission limit bounds each physical batch; any larger logical group is split
into multiple completed physical batches. Ring allocation and retained-capacity
admission remain above I/O. A completed physical batch **does not** complete
a WAL append future. The writer retains the existing strict order:

1. Finish every staged physical write, including all partial writes.
2. Complete the synchronous data `force(false)`.
3. Write the next alternating Ring superblock checkpoint.
4. Complete the synchronous checkpoint `force(false)`, plus required head mirror.
5. Only then complete the WAL append futures; Kafka still owns ISR/HW/acks.

No physical write may cross the Ring data region or overlap another staged
physical write. Even though Java AIO can have multiple writes in flight,
this implementation still waits for the batch to settle, and the WAL writer's
durability barriers remain blocking. Keep default NIO and opt-in AIO until
full exact-HEAD failure testing, crash recovery, wrap and performance validation
pass. The rotating-file WAL is not yet migrated to this staged batch path.

## Portable WAL performance roadmap and source comparisons (2026-11-14)

This repo's **performance bottleneck to test first** is the serialized
`data force(false) -> superblock write -> checkpoint force(false)` path.
Two separate durability barriers are performed on a normal Ring append;
head advancement may require a third force for the mirrored superblock.
The current Ring writer has a 0 ns default singleton linger and an unbounded
`LinkedBlockingQueue`. The AIO backend has a two-worker executor and
`writeBatch` waits until all completions settle.

Comparisons require keeping the same durability guarantee:

| Proven pattern | Reference | Safe takeaway |
| --- | --- | --- |
| Time/size threshold, bounded parallel submissions, ordered recovery | AutoMQ 2024 Delta WAL design | Compare against ~333 us batching window, **not** an assumed 333 us WAL RT; the raw-device path differs from Java file I/O |
| Group commit with one sync for multiple writers, recycled log segments | RocksDB WAL Performance wiki | Join small appends and reduce file metadata I/O; never copy its default nonsync mode for `acks=all` |
| Configurable minimum sync interval, concurrent fsync queue | CockroachDB Pebble `WALMinSyncInterval` | Preserve default zero delay, benchmark an optional microsecond-level window |
| Conditional group-commit delay and separate write/fsync metrics | PostgreSQL 18 WAL documentation | Do not sleep when no contention; record timing by phase |
| Adaptive group-writes with journal-sync safety | Apache BookKeeper configuration | Keep buffered bytes and wait caps independent from durability semantics |
| Periodic vs batch commitlog modes | ScyllaDB | Never acknowledge unsynced periodic writes while promising durable WAL |

Source references:

- https://www.automq.com/blog/principle-analysis-how-automq-implements-high-performance-wal-based-on-raw-devices
- https://github.com/facebook/rocksdb/wiki/WAL-Performance
- https://github.com/cockroachdb/pebble/blob/master/options.go
- https://www.postgresql.org/docs/current/runtime-config-wal.html
- https://github.com/apache/bookkeeper/blob/master/conf/bk_server.conf
- https://docs.scylladb.com/manual/stable/reference/configuration-parameters.html

### Development sequence (avoid premature complexity)

1. **Observe before changing ACK timing**: `WalDurabilityStats` already
   reports aggregate data-force/checkpoint-force duration. The optional
   `WalWriteLatencyStats` API counts Ring append queue wait and physical
   batch completion duration (total and maximum nanoseconds, no fake p99
   estimate). These are per-process aggregate counters, **not** Producer
   `acks=all` percentiles; expose actual histogram/p99 in a future change.
2. **Reduce tiny positional writes without extra forces**: prototype
   `RingWalWriteCoalescer` merges adjacent physical fragments into a
   temporary buffer no larger than 64 KiB. Large payloads retain their
   original zero-copy view; logical wraps and physical gaps stop merging.
   This is a **candidate**, not a measured performance improvement.
   Compare staged versus submitted request counts, CPU allocation/GC and
   latency on the six AutoMQ message sizes before rollout.
3. **Bound outstanding append pressure (candidate)**: Ring WAL now has
   configurable `shared.storage.wal.admission.max.groups` (default 4096)
   and `shared.storage.wal.admission.max.bytes` (default 256 MiB).
   Credits include queued **and writing** groups, and are released only
   by the writer's terminal completion/error handler. Reject pressure
   immediately using `WalCapacityExceededException` rather than
   blocking an append thread; a user-cancelled `CompletableFuture`
   never refunds still-in-flight I/O. These bytes count encoded payloads,
   not complete process RSS or caller-side allocations. The queue is
   operationally bounded by admission even though its backing collection
   remains a `LinkedBlockingQueue`. Measure rejected groups under load
   and provide a retry policy on callers; retain the 0 ns linger default
   until benchmarking establishes an improvement.
4. **Tune *optional* microbatch window**: default 0 ns; test 0, 50,
   100, 250 and 333 us under controlled concurrency, with soft data-byte
   thresholds separate from the existing 64-request/16-MiB hard limits.
   Trigger immediately on enough data and avoid waiting while busy.
5. **Investigate single-force commit as a separate protocol change**:
   current durable superblock tail is the recovery authority, not merely
   an optional index. Simply removing `data force` or delaying the
   superblock loses crash consistency. A valid candidate must prove:
   acknowledged committed groups survive cold restart; a torn/partial
   group never hides a previously ACKed prefix; stale tail scanning
   respects ring generation and reclaimed/overwritten slots; the
   superblock head mirror stays safe on reclaims; all fault windows
   including SIGKILL and permanent old-leader loss preserve `acks=all`.
   Keep double force until an alternative proof, tests and
   same-disk benchmark exist.

A 3000-IOPS device budget gives 333 us as the *average allowed I/O
completion spacing at saturation*, **not** a single I/O latency or a
`force()` completion time. Local WAL and Kafka Producer `acks=all` RT
must be measured separately. Adopt only changes that improve the
latency/throughput frontier while passing exact-head RF2/RF3,
S3-offline and crash recovery gates.

## Current native asynchronous I/O status

| Capability | Current | Interpretation |
| --- | --- | --- |
| Producer-facing asynchronous `CompletableFuture` | Yes | Background writer; not kernel AIO |
| Ordered single-writer WAL group append | Yes | Correctness and sequential batching |
| Batched `force(false)`/durability completion | Yes | Synchronous fsync-like barrier on writer |
| Default circular preallocated Ring WAL | Yes | Physical layout; does not imply `O_DIRECT` |
| Portable FileChannel positional backend | Yes | Known correctness fallback |
| Java `AsynchronousFileChannel` backend | Experimental opt-in | AIO read/write API; existing WAL waits for completion, so not a full nonblocking pipeline |
| Linux direct `O_DIRECT` backend | No | Alignment, platform and buffer ownership still unresolved |
| Native `io_uring` backend | No | Requires explicit implementation and CI |

## Future backend interface requirements

**Do not silently graft a native async implementation onto the current blocking `Handle` API and claim it is async.** A proposed separate capability/interface must express submission *and completion* of positional reads/writes and durability barriers, with explicit lifecycle ownership. Keep Kafka offsets, GROUP_COMMIT, ring generation, reclaim, local-index publication and `acks`/HW decisions entirely above the physical I/O backend.

Required invariants for an experimental native backend:

1. **Durability ordering:** Complete a WAL append future only after *all* DATA and GROUP_COMMIT writes plus the required data and checkpoint durability barriers have succeeded. If writes finish out of order, a later completion must never advance the durable group tail past an earlier unfinished/failed write. A successful page-cache copy is not durable proof.
2. **Failure isolation:** Any write, fsync, checkpoint, timeout, cancellation, closed-handle or ring-broken error must fence the affected writer; fail pending appends without successful ACK and refuse later work. The fate of writes from failed fsync is **unknown**, not necessarily absent. Cold recovery must preserve previously acknowledged prefixes and respect group framing.
3. **Ring safety:** Enforce per-slot generation, aligned bounded reads/writes (if direct I/O), no reuse across still-referenced WAL data, no writes past capacity, and recovery from partial writes, torn checkpoints, unclean SIGKILL and wrap.
4. **Buffer ownership:** Backing memory must remain alive and unmodified until native I/O completes; bound queue depth, direct buffer pool and aggregate memory; never reuse an in-flight buffer. A native callback must not publish WAL index metadata until the durability completion.
5. **Lifecycle:** Cleanly drain or fail outstanding I/O on close, maintain file/directory durability on create/rotate/reopen, and avoid use-after-free of completion queues or registered buffers.
6. **Compatibility:** Choose native mode only by explicit setting after platform/runtime capability probes. Default and all unsupported/error paths must safely select the tested `FileChannelWalIoBackend` *before* admitting writes. No hot fallback mid-append after an ambiguous force failure.
7. **Packaging:** No hidden Linux/JNI requirement in the portable Kafka build; specify native packaging, architecture, kernel support, security permissions and unavailable-backend behavior.
8. **Production invariants:** Never weaken Kafka ISR, high watermark, `acks=all`, `DENY_ALL` experimental authorization fence or disabled COMMITTED physical GC.

## Measurement and acceptance before enabling native I/O

First collect a reproducible **FileChannel baseline**, then run exactly the same workload and fault matrix against any proposed async backend:

- Backends/layouts: Ring (default) and rotating-file; if proposed, direct-I/O and native-`io_uring` separately.
- Hardware/runtime: Linux kernel, JDK, filesystem, NVMe device, cache and mount options, container/direct-I/O permissions, CPU/NUMA, thread and IO queue configurations.
- Workload grid: `acks=1` and `acks=all`, RF2/RF3, minISR, 1/16/64 KiB batches, low/high producer concurrency, sustained WAL pressure and S3 offline.
- Metrics: successfully confirmed Kafka records/s and MiB/s, p50/p95/p99/p99.9 ACK latency, group coalescing, `force` counts and time, CPU/GC, page-cache pressure, native queue depth/backpressure, disk utilization, S3 upload backlog, failed requests and WAL recovery duration.
- Correctness faults: injected data write/force and checkpoint failures, partial group/wrap, unclean SIGKILL plus permanent old-leader WAL deletion, delayed/reordered completions, cancelled/in-flight I/O, S3 outage, ISR shrink/restore and RF3 post-failover `acks=all` writes.
- Reproducibility: run warmups and repeated measured trials, separate throughput from tail-latency results, record all artifact versions. Do **not** invent a speedup threshold or promise performance improvement without measurements.

### Recommended next engineering sequence

1. Keep the integrated FileChannel Ring WAL as the protected correctness implementation and finish any in-flight exact-HEAD CI.
2. Add an **opt-in benchmark harness** that reports the metrics above without asserting unstable throughput thresholds in CI.
3. Prototype a native async submission/completion interface behind a separate implementation and tests; keep production default unchanged.
4. Establish strict failover/durability and cold-replay parity first, then compare performance on real NVMe; only consider an opt-in rollout if it materially improves p99 ACK latency or CPU at matched durability.
5. Do not replace the Kafka-native WAL replication model with an experimental Controller certificate protocol.

This replaces the old text that incorrectly described `WalIoBackend` and the circular WAL as still only proposed; both exist in this source tree.
