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
- The current branch has **no production native-AIO WAL backend**, no runtime `io_uring` selection and no asynchronous `force` completion API. A new backend cannot safely be substituted by renaming a class.

## Relationship to open-source AutoMQ

The *design inspiration* is shared storage with WAL on the synchronous write path and object-backed longer-term storage, not bit-for-bit reuse of AutoMQ's protocol or source. Open-source AutoMQ offers multiple WAL implementations, including object-storage WAL (`s3stream/.../wal/impl/object`). That object WAL buffers and asynchronously uploads batches; its behavior cannot be equated to this project's Broker-local WAL or used as evidence that all AutoMQ WAL variants use Java AIO or Linux `io_uring`.

Our deliberate difference is Kafka-native partition replication of the RecordBatch *into each Broker's WAL*, with Kafka ISR/HW and minISR controlling acknowledgement and failover, rather than inventing another independent replica quorum or WAL-certificate authority. A WAL-only failure/restart test with S3 offline is a required correctness gate, not an optimization benchmark.

## Current native asynchronous I/O status

| Capability | Current | Interpretation |
| --- | --- | --- |
| Producer-facing asynchronous `CompletableFuture` | Yes | Background writer; not kernel AIO |
| Ordered single-writer WAL group append | Yes | Correctness and sequential batching |
| Batched `force(false)`/durability completion | Yes | Synchronous fsync-like barrier on writer |
| Default circular preallocated Ring WAL | Yes | Physical layout; does not imply `O_DIRECT` |
| Portable FileChannel positional backend | Yes | Known correctness fallback |
| Java `AsynchronousFileChannel` backend | No | May use worker threads, not proof of kernel AIO |
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
