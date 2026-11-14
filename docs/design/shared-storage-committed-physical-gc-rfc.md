# RFC: Safe physical GC for COMMITTED shared-storage objects

Status: **DESIGN ONLY / NOT IMPLEMENTED**. This RFC does not turn on COMMITTED DELETE, reduce Kafka durability, or extend the sealed MinIO-backed GA claim.

Baseline: `shared-wal-s3-4.3.1` @ `dffb63fdcdcb7af995f6ff63e62ecdca0d5e9e4c` (sealed MinIO GA). Every subsequent code change requires a new source SHA and full verification. Scope: Topic Delete, retention and DeleteRecords physical reclamation on S3-compatible storage; not WAL performance tuning, native AWS S3 certification or io_uring.

## Source-code evidence: what to keep and what not to copy

Source revisions inspected on 2026-11-15:

1. **AutoMQ** @ `fcbfb4c6cfbcde669d1242fa11c27c70c82db880`:
   - [S3ObjectControlManager](https://github.com/AutoMQ/automq/blob/fcbfb4c6cfbcde669d1242fa11c27c70c82db880/metadata/src/main/java/org/apache/kafka/controller/stream/S3ObjectControlManager.java) owns KRaft-replayed object states `PREPARED -> COMMITTED -> MARK_DESTROYED`, a delayed cleanup loop, batched S3 removal and subsequent KRaft `RemoveS3ObjectRecord`. A cycle limits candidate processing (`MAX_DELETE_BATCH_COUNT=2000`), distinct from S3 request batch limits.
   - [StreamControlManager](https://github.com/AutoMQ/automq/blob/fcbfb4c6cfbcde669d1242fa11c27c70c82db880/metadata/src/main/java/org/apache/kafka/controller/stream/StreamControlManager.java) retires stream metadata and marks its object references destroyed within a controller transaction. A deleted stream's **stream-set** objects are not blindly deleted: cross-stream bytes are cleaned via compaction.
   - [TopicDeletionManager](https://github.com/AutoMQ/automq/blob/fcbfb4c6cfbcde669d1242fa11c27c70c82db880/metadata/src/main/java/org/apache/kafka/controller/stream/TopicDeletionManager.java) persists topic-ID cleanup intent and retries until the matching streams are gone.
   - [StreamObjectCompactor](https://github.com/AutoMQ/automq/blob/fcbfb4c6cfbcde669d1242fa11c27c70c82db880/s3stream/src/main/java/com/automq/stream/s3/compact/StreamObjectCompactor.java) identifies expired objects using stream-start boundaries, and copy-on-write compaction restructures live object data. [CompactionManager](https://github.com/AutoMQ/automq/blob/fcbfb4c6cfbcde669d1242fa11c27c70c82db880/s3stream/src/main/java/com/automq/stream/s3/compact/CompactionManager.java) throttles compaction work.
   - **Keep:** one durable control-plane authority, publish/retire transitions before physical side effects, bounded asynchronous batch GC, topic incarnation isolation, object layout that permits eventual reclaim.
   - **Do not copy:** dependence on AutoMQ stream ownership/storage format, composite-object shallow/deep delete semantics, two rolling in-memory timestamp sets as a substitute for a durable GC lease, or cloud-specific WAL assumptions.

2. **Apache Kafka Tiered Storage**: [RemoteLogManager](https://github.com/apache/kafka/blob/trunk/storage/src/main/java/org/apache/kafka/server/log/remote/storage/RemoteLogManager.java) persists `DELETE_SEGMENT_STARTED` before physical removal and `DELETE_SEGMENT_FINISHED` afterward; interrupted deletion is retried. [RemoteLogManagerTest](https://github.com/apache/kafka/blob/trunk/storage/src/test/java/org/apache/kafka/server/log/remote/storage/RemoteLogManagerTest.java) exercises dangling copy/delete and leadership cancellation.
   - **Keep:** explicit durable delete intent, idempotent retry, role turnover recovery.
   - **Do not copy:** per-topic-partition segment ownership as physical-delete proof. Our KSO2 object may pack ranges from multiple topic IDs and partitions.

3. **Apache Pulsar** @ `5bc4c05aac591e60994c17ca3957baf08f525032`: [ManagedLedgerImpl](https://github.com/apache/pulsar/blob/5bc4c05aac591e60994c17ca3957baf08f525032/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerImpl.java) records an offload attempt UUID before upload, conditionally completes it in metadata, and deliberately leaves offloaded data intact when metadata outcome is uncertain. Trimming defers if cursor recovery/metadata state is incomplete.
   - **Keep:** immutable attempt identity, unknown-outcome = retain, recovery-before-trim.
   - **Do not copy:** BookKeeper ledger/cursor lifecycle for Kafka WAL replicas.

4. **Pravega** @ `3142a01075bfae78d66588e46ff87e6e10225744`: [GarbageCollector](https://github.com/pravega/pravega/blob/3142a01075bfae78d66588e46ff87e6e10225744/segmentstore/storage/src/main/java/io/pravega/segmentstore/storage/chunklayer/GarbageCollector.java) uses bounded/background task queues, per-key serialization, metadata-active checks, delayed retries and a failed-task queue.
   - **Keep:** durable retry work, resource isolation and rate limits.
   - **Do not copy:** an absent chunk metadata entry as authority to delete **COMMITTED KSO2** objects. Missing metadata in our design may reflect incomplete replay, not orphanhood.

## The architectural difference that cannot be hidden

AutoMQ's stream and object lifecycle changes share KRaft Controller serialization. **Our current design does not.** Physical object COMMITs and logical range references are replayed from a single-partition compacted Kafka shared metadata topic, whereas the experimental retirement authority is a non-emitting KRaft model. There is no cross-log atomic transaction. A broker-local `RemoteObjectIndex`, a replayed watermark, a local `ActiveCommittedObjectReads` drain, or an unauthenticated report is not a distributed deletion grant.

Current production:
- `OrphanObjectCleaner` safely handles **PREPARED** cleanup; it explicitly does not delete COMMITTED objects based on local index absence.
- `SharedMetadataImage` rejects unproven live COMMITTED metadata tombstones.
- `RemoteObjectIndex` keeps selected **and alternate** physical read paths, and local checkpoints can replay earlier references.
- `PartitionRetirementControlManager` is replay-only; there is no production controller-authorized writer.
- Experimental `partition.retirement.authority.version=1` requires `IBP_4_4_IV0`, which the 4.3.1 baseline does not meet. Production principal policy is `DENY_ALL`. **Never lower/bypass either guard to 'get GC working'.**

## Recommended smallest correct protocol

**Design invariant: COMMITTED objects become eligible for physical DELETE only through a durable, unique object-ID/generation-specific retirement transaction, never from a listing, age check, local index, expired topic name, or unauthenticated broker claim.**

The lifecycle coordinator must make one authoritative order encompassing (or explicitly proving a barrier between) the object COMMIT catalog, immutable Topic ID and partition retirements, and physical DELETE intents. **Recommended lean approach: keep the existing compacted Kafka COMMITTED catalog for normal publication/reads, but make a small, durable KRaft-owned catalog authoritative over candidate object retirement and physical deletion.** Only objects entering retirement need KRaft GC intent records; there is no need to migrate all hot COMMITTED objects or adopt AutoMQ's Stream storage abstraction. The prerequisite is a separately proved, durable **cross-log handoff barrier** fencing all later COMMIT/reference publications for the candidate object and binding its immutable descriptor to a verified Kafka metadata read-committed horizon. Merely pairing two sampled offsets is **not** sufficient. If that bridge cannot be proved under old leaders, compaction and crash recovery, migrate the **minimum necessary object lifecycle catalog** into the same KRaft transaction domain instead of shipping a weakened fence.

Proposed object states (name and wire contract still to be versioned):

```
PREPARED --upload + durable COMMIT--> COMMITTED
PREPARED --expired fenced claim-----> ORPHAN_CLAIMED -> ORPHAN_DELETED
COMMITTED --controller retirement---> RETIRE_INTENT
RETIRE_INTENT --all reference and I/O fences verified--> DELETE_READY
DELETE_READY --physical object DELETE---> DELETE_RECORDED
DELETE_RECORDED --authoritative metadata retirement/compaction--> terminal DELETED
```

A failed or uncertain step leaves the durable intent and retries at that state. A terminal object ID must never be reused/reuploaded. The physical object key and descriptor identity are immutable.

### Phase A — durable source lifetime, once, per immutable Topic ID

- A real Controller event, with a negotiated compatible metadata feature, serializes current Topic ID, partition, leader/partition epoch, BrokerId/Epoch/incarnation, and durable LogStart advances. Topic Delete is terminal for that Topic ID; re-create with the same name receives a distinct independent identity.
- Validate source LogStart against trusted current Kafka leader state **at the serialized commit boundary**; reject stale/restarted brokers and old epochs. A caller-supplied Java model `Snapshot` is never sufficient.
- On startup, SnapshotRegistry replay must restore maximum epoch, terminal topic tombstones, and monotonic watermarks even after KRaft snapshot/compaction. Unknown upgrade support / unknown ack / missing replay horizon means OFF.

### Phase B — freeze COMMIT and retire every reference

- Fence old upload/COMMIT owners for the retiring source generation before deciding the remote catalog horizon. A late PUT, multipart completion, duplicate COMMITTED copy or alternate fallback must not resurrect a retired reference.
- Produce an authenticated, versioned barrier between KRaft lifetime state and the shared metadata COMMIT catalog; retain exact immutable object descriptors (object ID, checksum, size, per-range Topic IDs/offsets, metadata generation). Do not create a GC worker permit until all relevant records before the barrier are resolved.
- For each KSO2 object inspect **all** packed ranges. A normal live range, missing watermark, partially overlapping RecordBatch, alternate reference, unread replica image or uncleared upload => **retain the entire physical object**.
- Durable `RETIRE_INTENT` must contain original descriptor identity, generation, every partition covered, sealed catalog horizon and retry state. It is an object-scoped intent, not a per-partition-local deletion decision.

### Phase C — scalable reader/producer quiescence (batched safe epoch)

- Persist an object-scoped retirement generation `retireEpoch`, but **do not collect one network ACK per object**. Publish batched Controller retirement barriers and require each broker that could serve an old remote index to acknowledge its highest **fully drained epoch** (`drainedEpoch`) for the current authenticated BrokerEpoch/incarnation.
- Upon observing a barrier, each broker installs **no-new-read/no-new-upload** fences for retired object IDs and excludes them from primary and alternate lookup and replayed local checkpoints. Wait for both physical Range GET Futures and logical cache-hit read leases admitted before the barrier, plus any PUT/multipart/COMMIT work. The existing process-local admission registries can be used only when all actual readers/uploaders share them.
- The Controller derives a cohort from the real cluster membership **at the barrier**, authenticates each broker's `drainedEpoch` (including incarnation), and permits a physical object only if every required live reader cohort has advanced at or past `retireEpoch`. One epoch report amortizes many object retirements; this is a proposed QSBR/RCU-style improvement, **not** a feature copied from AutoMQ.
- A disconnected broker is not silently ignored: it must be durably fenced from serving and, on re-registration, complete KRaft retirement replay and rebuild its local remote index before any remote read. The Controller must reject stale incarnation ACKs and heartbeat ABA. Broker removal or reassignment must not revive old readers.
- Never accept ISR quorum alone as proof of global read quiescence: historical/follower remote reads, local checkpoints and recovery readers may exist outside ISR. A missing broker fence or timed-out drain stalls physical GC, **not** data availability.

### Phase D — idempotent, durable physical delete

- KRaft commits `DELETE_READY` **after** all verified fences; a worker with a short, generation-fenced task lease executes an idempotent S3/MinIO `DELETE(objectId)`. Worker shutdown, re-election or timeout replays intent and retries without deleting live bytes.
- Require quiescence of in-flight PUT/multipart completion before delete; a late PUT after DELETE can recreate an object. Object IDs/keys must never be recycled. A terminal/fenced object may still need **reconciliation DELETE retries** if an already-issued remote PUT completes after an uncertain network disconnect; maintain enough durable terminal evidence to find it again. In this case the late PUT must remain unreachable by readers; otherwise a safe GC grant was never established. The delay/grace period is additional defense and cost smoothing, **not** the proof of safety.
- On successful physical delete and the supported storage contract (including required verification where practical), persist `DELETE_RECORDED` then retire the authoritative catalog entry, retaining a compacted terminal fencing record for the whole lifetime in which an old producer or checkpoint could reappear. If DELETE succeeded but the metadata write failed, retry DELETE.
- Bound object IDs per sweep, concurrent DELETEs, bytes/sec and S3 requests/sec. Apply exponential-jittered retries and persistent dead-letter/alert states; use LIST only for reconciliation and observability, **not** GC eligibility.

### Phase E — reclaim packed bytes without deleting live ranges

- If a cross-topic/cross-partition KSO2 object has any live range, keep it until a copy-on-write (COW) compaction writes and verifies surviving complete RecordBatches into **new immutable object IDs**.
- Atomically publish the new references and retirement of old references at the same authoritative generation; preserve primary and alternate fallback read contracts. Only then mark the old object for Phase C/D.
- Avoid AutoMQ composite/shared-child deletion initially: simple physical-object ownership is easier to prove. Prevent future retention pinning by grouping upload packs with similar Topic-ID/TTL when feasible, without breaking batching or WAL durability.

## Minimal implementation order (no speculative refactor)

1. **Authority and catalog handoff (critical path)** — implement a real version-negotiated KRaft retirement-intent writer (not a full object-catalog rewrite), and prove the cross-log generation/barrier against existing COMMITTED metadata before granting any deletion. Keep an explicitly documented KRaft catalog migration as the safe fallback if this cannot be proven. Reject all unproven identities. Tests: old leader re-init, same-name new Topic ID, compaction/snapshot, mixed-version downgrade, late COMMIT, uncertain outcome. **This blocks all subsequent physical DELETE.**
2. **Durable retirement and distributed drain** — persist object intents, fence selected/alternate read references, pending cache reads and uploads across actual brokers/restarts. Tests: overlapping requests, cancellation, local WAL checkpoint restore, offline broker rejoin, partial packed range.
3. **Retryable MinIO worker** — one KRaft generation-fenced worker lease, idempotent physical DELETE, persist completion after remote effect. Inject failure at every state boundary (including DELETE success/metadata timeout and delayed multipart completion). Assertions use physical MinIO HEAD and ListObjectsV2 and unskipped named JUnit evidence.
4. **COW compaction** — only after physical GC is correct. Repack live subsets and assert old packed object is reclaimed after safe ref swap, while the surviving records remain readable on all replicas.
5. **Operational enablement** — default OFF -> shadow candidates/reporting -> limited canary -> explicitly enabled release; monitor stuck intents, estimated live pinned bytes, delete retry age, physical object count, unexpected 404, read fallback failures and consumer equivalence.

### Required failure / correctness gate

- Topic Delete then recreate same name/new Topic ID; never delete new data.
- Retention and DeleteRecords; boundary inside a RecordBatch; preserve whole batch until safely rewritten.
- Packed object with one or more live partitions; must survive; COW replacement then collect old.
- Equivalent primary and alternate COMMITTED copies; don't delete either while remotely readable.
- Concurrent uploads/late multipart PUT, leader SIGKILL, stale broker re-registration, observer ABA.
- Metadata replay lag, compacted metadata, KRaft snapshot install, uncertain controller commit.
- Broker disconnected during fence, broker restart with old local remote checkpoint, cache-hit read and unfinished physical GET.
- S3 DELETE fail, return-success but metadata commit fail, repeated DELETE, MinIO outage/restore.
- RF2/RF3 producer `acks=all`, ISR/HW and WAL recovery remain unchanged.
- Bucket with S3 versioning/object lock: default **unsupported for physical reclamation** until version-aware deletion and retention/legal-hold policy are separately tested; a 404 current-version HEAD does not prove old versions were reclaimed.

## Efficiency and risk limits

- Use immutable packed-object descriptors and a derived **live-range set**, not a mutable distributed refcount prone to drift under failures. GC classification must be `O(total packed ranges)` across a bounded cursor; never `O(all stored objects)` for every producer append.
- Keep Controller work limited to serialized, bounded metadata transitions. Read/merge large object bytes in workers; keep S3 physical DELETE off the Controller event queue.
- Use a **cohort/batched global safe epoch** rather than `O(retired objects × brokers)` synchronous drain RPCs. Keep the much smaller per-object local I/O lease counters.
- Prefer one object = one immutable physical owner and one KRaft retirement key. Defer composite index-sharing or aggressive deep-delete semantics.
- Don't emit a new wire format while rollback-eligible 4.3.1 binaries cannot decode it. Choose a properly versioned upgrade/migration or a separately certified compatibility backport; never silently drop the existing `IBP_4_4_IV0` gate.

## Explicit exclusions

- **No object deletion by retention timestamp alone.** No GC scans across an arbitrary bucket prefix as proof of orphanhood.
- **No introduction of a new WAL or replication protocol.** Preserve Java NIO/AIO, Kafka Fetch/ISR/HW and producer ACK semantics.
- **No automatic performance claims or AWS native S3 claim.** This RFC addresses correctness and eventual space recovery for MinIO-compatible object storage only.
- **No premature production enablement.** A design document, precheck, local lease or green historical GA run does not establish completed physical COMMITTED GC.
