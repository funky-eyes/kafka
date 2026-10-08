# Committed-object lifecycle GC — durable retirement metadata (SDD)

## Scope of this checkpoint

This adds a **read-only** protocol reservation for monotonic, immutable-topic-ID-scoped
partition log-start watermarks. It changes the metadata codec and replay image,
but deliberately adds **no writer**, **no range eviction**, and **no physical DELETE**.
The current MinIO GA still covers orphan and redundant COMMITTED object cleanup,
not physical reclamation following Topic Delete, Retention, or DeleteRecords.

## Persisted key and value

The existing authoritative source is a **single-partition compacted Kafka metadata topic**.
The following layouts are permanent and big-endian:

- Key type `0x04`, layout `type:byte | topicIdHigh:int64 | topicIdLow:int64 |
  partition:int32` (21 bytes). Topic ID prevents accidental cross-incarnation
  retirement when a topic is deleted and recreated with the same name.
- Value version `0x0001`, value type `0x06`, `startOffset:int64` (11 bytes).
  The offset is the inclusive Kafka log start, must be non-negative, and may be zero.
- Equal watermarks are idempotent; an observed backwards transition or a compacted
  tombstone fails replay closed. A compacted snapshot containing only the latest
  watermark is valid.
- Object/cleanup/broker keys and GA-v1 value encodings are unchanged and remain
  protected by golden-byte tests.

**The replayed watermark is metadata only.** It cannot authorize any deletion
and it does not change which remote ranges the read path exposes.

## Required gates for subsequent work

1. **Mixed-version fence.** Older brokers reject unknown keys. Before any producer
   emits this record, all active/rollback-eligible brokers must support the decoder;
   the current release emits none.
2. **Writer serialization.** Kafka compaction does not implement compare-and-set.
   A delayed lower-value write can compact away a higher watermark. Add an
   authoritative, generation-fenced monotonic writer before enabling emission.
3. **Durable logical retirement.** Replay a watermark before filtering reads;
   reject late uploads and only retire full RecordBatch ranges. Topic deletion
   needs an immutable Topic-ID terminal fence as well as partition log-start.
4. **Multi-partition safety.** A physical object may contain ranges from multiple
   topics/partitions. Do not delete its bytes until all ranges are durably retired,
   all live readers are quiesced, and no upload can still publish bytes.
5. **Crash safety.** Persist reference retirement, perform physical DELETE, then
   tombstone committed object metadata. Retain retry evidence across errors and
   broker restarts. Uncertain evidence must mean **retain**, not delete.
6. **MinIO verification.** Add physical HEAD/ListObjectsV2 evidence after
   Topic Delete/Retention/DeleteRecords, and assert retained packed ranges
   remain readable across crash/failover/restart, with mandatory JUnit anti-skip.

This checkpoint intentionally does not yet satisfy gates 1–6: it only locks
the replay contract and the fail-closed starting point. Production release
claims must not extend to post-lifecycle COMMITTED-object physical GC yet.

## Batch 14: read-only per-object retirement precheck

`SharedMetadataImage.partitionLogStartsSnapshot()` returns an immutable, stable
point-in-time copy of **explicitly replayed** watermarks after the image becomes
READY. A missing entry is not silently substituted with offset zero. Failed or
recovering images cannot provide a snapshot.

`CommittedObjectRetirementPrecheck.assess()` applies only logical offset
evidence to an immutable COMMITTED object's full set of RecordBatch ranges:

- `MISSING_WATERMARK`: any partition or topic incarnation lacks an explicitly
  replayed log start; fail closed, including packed objects with other live ranges.
- `HAS_UNRETIRED_RANGE`: every partition has a watermark, but at least one
  range has an exclusive end offset **greater** than its partition's log start;
  partially intersecting RecordBatches retain all their physical bytes.
- `ALL_RANGES_BELOW_LOG_START`: every range ends at or below its matching
  watermark. **This is a logical observation, not deletion authorization.**

The precheck has no object-store or index mutation capability and cannot cause
physical reclamation. Unit tests lock the exclusive-end boundary, multi-partition
reachability, recreated topic-ID isolation, missing-vs-zero evidence, immutable
snapshots, and fail-closed recovery/failed states. All writer, quiescence and
durable retirement fences listed above remain mandatory before physical GC.
