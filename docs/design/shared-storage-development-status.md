# Shared Storage Development Status

This document is the durable development checkpoint for the Kafka 4.3.1 shared-storage branch. It records architecture invariants, implemented milestones, current CI evidence, and the next implementation boundary. It is intentionally stricter than a roadmap: a gate is marked green only when there is evidence for the exact code tree or an explicitly identified identical tree.

## Branch

- Repository: `funky-eyes/kafka`
- Branch: `shared-wal-s3-4.3.1`
- Kafka baseline: 4.3.1
- Current canonical commit at this checkpoint: `effe8460d80c3052225ba00586776e320e8ff8d9`
- Current code tree at this checkpoint: `dbfda4c7d1bd7fe96d8575f43a03ceeff3de1c66`
- Canonical author/committer identity: `Jianbin Chen <jianbin@apache.org>`

## Architecture invariants

1. Kafka partition replicas remain authoritative for replication, ISR, high watermark, and leader election.
2. Each broker owns a broker-wide local WAL. Shared-topic payloads are durably appended to the WAL instead of being retained as normal Kafka segment payload.
3. Replica progress cannot advance before the corresponding local WAL durability barrier. Therefore `acks=all` continues to use Kafka replica/HW semantics.
4. `acks=1` guarantees leader-local WAL durability only. A permanently lost leader may lose records that were not replicated, matching Kafka semantics.
5. S3 upload is asynchronous and independent of Kafka log-segment rolling.
6. Authoritative remote-object publication is `PREPARE -> PUT -> COMMIT`. Only COMMIT creates authoritative remote coverage.
7. Only current-leader data strictly below Kafka high watermark is eligible for authoritative upload.
8. WAL physical reclamation requires authoritative remote coverage to be durably represented by the local remote-object checkpoint.
9. Physical S3 orphans from `PUT` success followed by metadata `COMMIT` failure are reclaimed only through race-safe orphan state/claim rules.
10. The design is not Kafka tiered storage. The target is an AutoMQ-like WAL + object-storage data plane while retaining Kafka partition replicas.

## Implemented milestones

### Core shared-storage path

- Shared-topic extension and Kafka log integration.
- Broker-wide WAL append path.
- Crash-atomic logical append groups using DATA/control records followed by `GROUP_COMMIT`.
- `FileChannel.force(false)` durability barrier before append futures complete.
- Directory fsync after creation of new WAL segment directory entries.
- WAL restart recovery and truncation of incomplete final append groups.
- Partition WAL index and remote-object index.
- Kafka-HW-gated upload candidate selection.
- S3 range read with checksum validation and remote fallback after local reclaim.

### Remote object protocol

- `PREPARE -> PUT -> COMMIT` upload protocol.
- Kafka-backed authoritative object metadata.
- Logical deduplication/checksum-conflict handling.
- Race-safe orphan cleanup protocol.
- Upload crash-window coverage for AFTER_PREPARE / AFTER_PUT / AFTER_COMMIT on previously validated identical functional trees.

### Performance phase 1: WAL read cache

- Bounded in-memory WAL DATA cache, default 256 MiB.
- Physical `(logicalSegmentId, position)` identity.
- Cache population only after the WAL durability future succeeds.
- Cache miss falls back to physical WAL.
- Cache is disposable across reclaim/restart.

### Performance phase 2: upload scheduling

Upload scheduling is driven by:

- accumulated eligible bytes reaching target object size;
- maximum linger of the oldest pending candidate;
- WAL pressure threshold.

### Performance phase 3: bounded parallel object upload

- Configurable `shared.storage.upload.max.inflight`, production default 4.
- Physical candidate reservation prevents duplicate concurrent upload of the same WAL location.
- Disjoint object uploads may complete out of order.
- Failed upload releases reservation and permits retry with a new object identity.
- Existing synchronous S3 client I/O pool is reused; this is bounded concurrent synchronous-client I/O, not yet a native async S3 client.

## Performance phase 4A: online WAL reclaim

The former reclaim implementation stopped the WAL data plane:

`block operations -> wait -> close FileSharedWal -> scan -> delete -> reopen FileSharedWal -> clear/replay index`

The current implementation replaces that with online segment lifecycle management.

### Implemented changes

- `SharedWal.reclaimedThroughSegmentId()` exposes the highest monotonically increasing logical segment id physically reclaimed. A backend that cannot expose a boundary returns `-1` and may use replay fallback.
- `PartitionWalIndex` prunes only locations in reclaimed physical logical segments instead of clearing and replaying the entire index.
- `FileSharedWal.usedBytes` is atomic so writer increments and online reclaim decrements cannot lose updates.
- Active segment publication is visible to the reclaim path.
- Reclaim drains readers that may have old segment files open, but does not stop normal append admission for the entire reclaim scan/delete cycle.
- Only immutable segments before the active logical segment are normally scanned/deleted.
- Append groups spanning segment boundaries remain pinned until a complete `GROUP_COMMIT` proves a reclaimable boundary.
- A `sealActiveSegment()` primitive can force/close the current active segment without stopping/restarting the writer thread. This avoids the single-segment capacity deadlock where a safe active segment could not be reclaimed because there was no free capacity to trigger a natural roll.
- Active sealing is conditional: first reclaim an already-immutable safe prefix; seal the active segment only if additional headroom is required and the scan was not blocked by an unsafe oldest group. This prevents segment churn during S3 outage or stalled remote coverage.
- Partial physical deletion accounting is applied before propagating a later delete/fsync error so in-memory capacity accounting cannot diverge from already-deleted durable directory state.
- Reclaim scan and deletion state machines are decomposed into small functions to satisfy Kafka Checkstyle NPath limits while preserving protocol behavior.

### Phase 4A required correctness gates

- `Shared Storage` main Java 25 shared-storage test/static-analysis gate.
- `Shared Storage` MinIO + 3-broker KRaft failover gate.
- `Shared Storage WAL Crash Windows` deterministic crash/restart/checkpoint/reclaim gate.
- `Shared Storage WAL Capacity` S3-outage fail-closed capacity gate.
- `Shared Storage S3 Outage` acks=all outage/failover gate.
- `Shared Storage acks=1 Durability Matrix` RF1/RF2/RF3 external-JVM contract gates.
- `Shared Storage acks=all Durability Matrix` RF1/RF2/RF3 independent-JVM leader-SIGKILL gates.

### Exact-tree evidence at the time this checkpoint was written

For raw commit `2a3ee1cd2954d787f02ac28ef533882afd73762e`, normalized as canonical commit `effe8460d80c3052225ba00586776e320e8ff8d9`, tree `dbfda4c7d1bd7fe96d8575f43a03ceeff3de1c66`:

- Normalize Shared Storage Author #259: SUCCESS.
- Shared Storage WAL Capacity #46: SUCCESS.
- Shared Storage #204: running when this checkpoint was written.
- Shared Storage WAL Crash Windows #34: running when this checkpoint was written.
- Shared Storage acks=1 Durability Matrix #96: running when this checkpoint was written.
- Shared Storage acks=all Durability Matrix #64: running when this checkpoint was written.
- Shared Storage S3 Outage #48: running when this checkpoint was written.

Do not promote the remaining gates to green until their terminal results have been checked.

## WAL I/O direction

Do not equate Java `AsynchronousFileChannel` with native asynchronous Linux disk I/O. On common Linux JDK implementations it may use a thread pool over blocking file operations and therefore does not by itself provide the desired architecture or latency characteristics.

The preferred evolution is:

1. Preserve a single logical WAL writer and natural group commit.
2. Introduce an internal WAL I/O abstraction without changing durability semantics.
3. Keep a FileChannel/NIO backend as the portable correctness baseline.
4. Move WAL storage from unbounded filename rotation toward a fixed-capacity, preallocated circular/block layout.
5. Keep logical segment/generation identity monotonically increasing even when physical slots are reused.
6. Benchmark alternative backends only after the fixed-layout semantics are proven: buffered NIO, preallocation, direct I/O where justified, and Linux io_uring/native backend where deployment/runtime constraints are acceptable.

## Planned phase 4B: I/O abstraction

Introduce a narrow internal backend around physical WAL operations. The abstraction must not own logical append-group semantics; those remain in the WAL state machine.

Expected responsibilities include:

- positional write/read;
- force/durability barrier;
- open/create/seal/close physical storage units;
- optional preallocation capability reporting;
- no change to `WalRecordCodec`, `GROUP_COMMIT`, Kafka ACK semantics, or replay semantics.

The first backend remains FileChannel based. Native/AIO/io_uring experimentation is a later optimization behind the same contract.

## Planned phase 4C: fixed-slot circular WAL

Target physical layout:

- configured total WAL capacity is divided into a fixed number of preallocated slots/blocks;
- physical slots are recycled instead of continually creating/deleting files;
- each reuse receives a new monotonically increasing logical generation/segment id;
- a `WalLocation` must identify generation plus position so a stale location can never alias newly written data in a reused slot;
- head/tail movement is constrained by durable remote coverage and local checkpoint state;
- crash recovery reconstructs only committed logical append groups and the current head/tail/generation state;
- no slot may be overwritten while a local reader/index entry can still legally reference its current generation.

This phase will be accepted only with explicit wrap-around, stale-location, crash-at-wrap, capacity-pressure, restart, failover, and remote-fallback tests.

## Non-goals / claims not yet justified

- Do not claim AutoMQ performance parity.
- Do not claim the current rotating WAL is already a circular WAL.
- Do not claim Java AIO is faster without benchmark evidence.
- Do not claim io_uring is production-ready until a concrete backend, packaging/runtime strategy, fallback path, and crash/durability benchmark suite exist.
- Do not claim a tree is fully green using CI evidence from a different functional tree unless the trees are proven identical.
