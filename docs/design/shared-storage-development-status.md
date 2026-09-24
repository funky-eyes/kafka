# Shared Storage Development Status

This document is the durable development checkpoint for the Kafka 4.3.1 shared-storage branch. It records architecture invariants, implemented milestones, current machine-verifiable GA evidence, and the next implementation boundary. It is intentionally stricter than a roadmap: a gate is marked green only when its production-tree and gate-contract evidence is proven equivalent to the candidate.

## Branch

- Repository: `funky-eyes/kafka`
- Branch: `shared-wal-s3-4.3.1`
- Kafka baseline: 4.3.1
- Latest canonical checkpoint before this document update: `b0a59bbd3f0f2fcead544f34a7e389c4b2733367`
- Shared Storage production fingerprint: `ff65cda02e7f9424e03ec655f2f9f0ec755cfa0b93b7bdf63c6f132554185514`
- Production files fingerprinted by the GA manifest: 100
- Canonical author/committer identity: `Jianbin Chen <jianbin@apache.org>`

Author normalization may rewrite commit IDs while preserving the tree. GA evidence therefore does not bind correctness to a raw commit ID alone: it requires both the Shared Storage production fingerprint and each gate's contract fingerprint to match the release candidate.

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
11. No append future may complete successfully before the bytes and the checkpoint making them reachable have crossed the required durability barrier.
12. Circular physical reuse must never make a stale logical WAL location alias newly written bytes.

## Implemented milestones

### Core shared-storage path

- Shared-topic extension and Kafka log integration.
- Broker-wide WAL append path.
- Crash-atomic logical append groups using DATA/control records followed by `GROUP_COMMIT`.
- Durability barrier before append futures complete.
- WAL restart recovery and fail-closed handling of incomplete/corrupt committed state.
- Partition WAL index and remote-object index.
- Kafka-HW-gated upload candidate selection.
- S3 range read with checksum validation and remote fallback after local reclaim.

### Remote object protocol

- `PREPARE -> PUT -> COMMIT` upload protocol.
- Kafka-backed authoritative object metadata.
- Logical deduplication/checksum-conflict handling.
- Race-safe orphan cleanup protocol.
- Upload crash-window coverage around PREPARE, PUT, and COMMIT.
- S3 outage behavior is fail-closed when local durable capacity cannot safely advance.

### Performance phase 1: WAL read cache

- Bounded in-memory WAL DATA cache, default 256 MiB.
- Physical WAL identity is used to prevent cache ambiguity across reclaim/reuse.
- Cache population occurs only after the WAL durability future succeeds.
- Cache miss falls back to physical WAL.
- Cache contents are disposable across reclaim/restart.

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
- Existing synchronous S3 client I/O is reused behind bounded parallelism; this is not a claim of native async S3 I/O.

### Performance phase 4A: online WAL reclaim

The original reclaim design stopped the WAL data plane while closing, scanning, deleting, reopening, and replaying the WAL. The current implementation replaces that with online segment lifecycle management.

Implemented properties include:

- monotonic physical reclaim progress;
- targeted `PartitionWalIndex` pruning instead of clear-and-replay;
- atomic used-capacity accounting;
- reader draining only where required by physical deletion;
- append groups remain pinned until their complete `GROUP_COMMIT` boundary is reclaimable;
- active-segment sealing is conditional and does not turn S3 outage into segment churn;
- partial physical deletion accounting is applied before propagating a later delete/fsync failure.

### Performance phase 4B: WAL I/O abstraction — implemented

The physical WAL boundary is now represented by `WalIoBackend`.

The abstraction owns:

- positional read/write;
- truncate/preallocation capability;
- force/durability barrier;
- physical-unit seal/close lifecycle.

It deliberately does not own Kafka offsets, append groups, `GROUP_COMMIT`, capacity admission, reclaim policy, or replay semantics.

`FileChannelWalIoBackend` remains the portable correctness baseline. Java `AsynchronousFileChannel` is not treated as equivalent to native asynchronous Linux I/O, and no io_uring performance claim is made.

### Performance phase 4C: fixed-capacity circular Ring WAL — implemented

The branch now contains `RingSharedWal`, `RingWalFile`, `RingWalLayout`, dual superblocks, authenticated wrap padding markers, preallocation handling, and circular logical-to-physical mapping.

Important properties:

- configured WAL capacity is physically bounded;
- logical WAL offsets remain monotonic while physical file positions are reused;
- stale logical locations outside the durable head/tail window are rejected;
- wrap padding is authenticated when large enough to contain a record header and otherwise must remain zero-filled;
- durable head/tail transitions are checkpointed using alternating superblocks;
- checkpoint failure fences the live WAL until reopen/recovery;
- recovery fails closed on invalid magic, partial committed records, malformed append groups, or invalid wrap markers;
- reclaim advances only across complete append groups accepted by the reclaim policy.

The Ring writer already performs natural durability batching: it drains multiple pending append groups and crosses one physical force/checkpoint barrier for the admitted drained batch. Do not introduce another batching rewrite unless measured steady-state evidence demonstrates a remaining bottleneck.

## Ring WAL correctness coverage

The current suite includes explicit coverage for:

- normal append/read/replay;
- physical wrap and stale-padding clearing;
- authenticated wrap markers and corrupted padding;
- group admission when the final `GROUP_COMMIT` cannot fit;
- reclaim stopping before the first unsafe append group;
- process crash and crash windows;
- superblock selection/recovery;
- checkpoint failure and live-WAL fencing;
- preallocation failure;
- fallback/reuse behavior;
- writer lifecycle and close timeout behavior;
- remote recovery integration and TimeIndex recovery regression.

The GA graph additionally requires the focused `Shared Storage Ring WAL Correctness` workflow.

## Performance baseline status

The relative performance gate compares shared storage with classic Kafka inside the same three-broker cluster and alternates timed execution order.

The current measurement contract preconditions both routed produce paths with interleaved warmup records, waits for shared-storage background work to become idle, measures both produce paths before starting either consume measurement, and keeps the shared/classic produce order balanced across repetitions. Consumer timing seeks each partition to the warmup boundary before measurement. This removes the previous cluster-age and cross-phase order bias without changing the workload or release threshold.

Latest validated produce ratios for the corrected paired measurement contract:

- 0.7086
- 0.5228
- 0.9125
- 0.6482
- median: **0.6784**
- required minimum: **0.60**

Latest median consume ratio: **0.7056** against the required minimum **0.50**.

The gate is therefore green with measurable headroom. The same run reported per-sample maximum WAL durability barriers of approximately 3.5-5.0 ms and no abnormal tens-of-milliseconds durability spike. No production WAL durability weakening was introduced to obtain this result and the threshold was not lowered.

Do not start an additional Ring WAL group-commit/durability-barrier optimization solely to create more headroom while this steady-state gate remains green. Reopen that work only if repeated corrected measurements show a stable regression below the release threshold or profiling identifies a separate production bottleneck.

## GA evidence status

The machine-generated normalized-branch GA manifest for canonical release checkpoint `b0a59bbd3f0f2fcead544f34a7e389c4b2733367` reports:

- Result: **PASS**
- Production fingerprint: `ff65cda02e7f9424e03ec655f2f9f0ec755cfa0b93b7bdf63c6f132554185514`
- Production files fingerprinted: **100**
- Mandatory gates: **19/19 PASS**

The mandatory set includes:

- main Shared Storage integration/static-analysis evidence;
- `acks=1` durability matrix;
- `acks=all` durability matrix;
- WAL crash windows;
- WAL capacity;
- S3 outage;
- upload crash points;
- Ring WAL correctness;
- object format correctness;
- Kafka semantics and HA;
- Kafka client failover;
- KRaft controller HA;
- local state loss recovery;
- inflight idempotent produce;
- Kafka multipart E2E;
- topic lifecycle;
- performance baseline;
- soak and chaos;
- rolling upgrade.

The normalized-branch seal workflow evaluates the canonical branch HEAD and uploads `shared-storage-normalized-ga-manifest`. The latest seal completed successfully after the corrected paired performance gate and the full RF1/RF2/RF3 `acks=1` durability matrix passed. Specialized runtime workflows are now prevented by the GA consistency checker from directly owning global Core Checkstyle/SpotBugs tasks; those checks remain owned by the main Shared Storage workflow, avoiding unrelated test-source changes blocking runtime evidence.

Evidence lookup is lazy by Actions page and stops at the newest production+contract-equivalent run rather than preloading up to ten pages for every gate. Obsolete seal jobs are bounded by their own cancel-in-progress concurrency group and no longer block author normalization.

## Real AWS S3 release evidence

Real AWS S3 compatibility remains optional in the default MinIO-oriented GA manifest and becomes mandatory only when `require_real_s3=true`.

The code path is ready for pre-merge evidence:

- workflow: `Shared Storage Real S3 Compatibility`;
- protected environment: `shared-storage-aws-s3`;
- OIDC role secret: `SHARED_STORAGE_AWS_ROLE_ARN`;
- branch-trigger bucket variable: `SHARED_STORAGE_AWS_S3_BUCKET`;
- optional region variable: `SHARED_STORAGE_AWS_S3_REGION` (defaults to `us-east-1`);
- dedicated pre-merge evidence branch: `shared-wal-s3-4.3.1-real-s3`.

Pointing that dedicated evidence branch at the exact candidate SHA triggers the branch-local workflow without requiring the workflow file to exist on the repository default branch. The GA manifest accepts the run only when repository, workflow name/path, event, evidence branch, production fingerprint, and real-S3 gate contract all match.

The dedicated Real S3 evidence branch `shared-wal-s3-4.3.1-real-s3` has now been created at candidate `055a4780d4404517f6f48709664c5f1b20dbbe0d`. Its first compatibility run (`35943806986`) reached the protected environment and failed in the configuration preflight before any AWS credential or S3 operation was attempted. The branch-trigger bucket variable `SHARED_STORAGE_AWS_S3_BUCKET` was empty, and the OIDC role secret `SHARED_STORAGE_AWS_ROLE_ARN` was also unavailable to the job environment.

This is an external release-environment blocker rather than a Shared Storage code failure. Configure those two values in the `shared-storage-aws-s3` protected environment (and optionally `SHARED_STORAGE_AWS_S3_REGION`), then trigger a fresh run from the dedicated evidence branch. Do not claim AWS S3 release compatibility until that run succeeds and a `require_real_s3=true` manifest accepts it.

## Next implementation boundary

The next boundary is release evidence, not a new storage architecture phase:

1. Configure/verify the protected `shared-storage-aws-s3` GitHub Environment and its OIDC role/bucket variables.
2. Point `shared-wal-s3-4.3.1-real-s3` at the exact release candidate to obtain real AWS S3 compatibility evidence.
3. Run the final GA release manifest with `require_real_s3=true` if the release claims AWS S3 support.
4. Treat any subsequent production or gate-contract change as evidence-invalidating and regenerate only the affected evidence according to the manifest rules.
5. Reopen performance implementation work only from corrected steady-state regression evidence or profiling, not from historical cold-start variance.

## Non-goals / claims not yet justified

- Do not claim AutoMQ performance parity.
- Do not claim native async or io_uring performance without a concrete backend, packaging/runtime strategy, fallback path, and benchmark evidence.
- Do not claim real AWS S3 compatibility before the protected Real S3 gate has passed for an equivalent candidate tree.
- Do not lower GA performance thresholds to make a candidate pass.
- Do not treat a green run from a different production or gate-contract fingerprint as release evidence.
- Do not re-implement already completed WAL I/O abstraction or Ring WAL phases based on the obsolete pre-Ring roadmap.
