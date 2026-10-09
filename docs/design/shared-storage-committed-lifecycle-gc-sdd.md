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

## Batch 15: atomic read-only committed inventory

`SharedMetadataImage.retirementEvidenceSnapshot()` takes both the sorted COMMITTED
object inventory and all explicitly replayed partition log starts under the same
image monitor. Live replay cannot interleave between these two copies, and the
returned lists/maps are immutable defensive snapshots.

`CommittedObjectRetirementPrecheck.assessCommitted()` classifies that entire
inventory against **one** watermark snapshot rather than re-reading live
watermarks for each object. The results are sorted by object ID, only include
authoritative COMMITTED entries, and do not hide a live range in packed
multi-partition objects. Missing evidence remains distinct from offset zero.

This checkpoint is purely diagnostic: it has **no** writer, reference eviction,
Kafka metadata offset / leader-generation proof, reader-quiescence proof or S3
DELETE. A logically expired observation can become stale immediately, must
never be used directly as deletion authority, and must be revalidated after
the durable retirement and lifetime fences are implemented.

## Batch 16: replay-offset provenance and default-deny writer gate

The single-partition Kafka metadata consumer now applies each replayed record
through `SharedMetadataImage.applyFromMetadataLog(key, value, offset)`. Its
monotonic Kafka consumer offset is recorded under the image lock alongside
the updated object/watermark state. Compaction gaps are allowed; duplicate or
backward offsets, negative offsets and corrupt records fail the image closed.
Direct in-memory `apply()` leaves the provenance offset at `-1`.

`retirementEvidenceSnapshot()` and
`CommittedObjectRetirementPrecheck.assessCommittedSnapshot()` expose the
last **consumed** metadata offset as diagnostic provenance only. It does not
prove catch-up to the latest committed metadata topic offset, leadership,
generation ownership, durable retirement, reader quiescence or deletion safety.

`KafkaObjectMetadataStore.writeRecord()` rejects partition-log-start keys
on its ordinary producer path. No production writer for that key is enabled.
The gate cannot prevent writes from an independent old/external producer; true
cross-broker fencing, mixed-version rollout and monotonic transactional writes
remain mandatory before authorizing this feature. Existing object, cleanup
and sequence metadata protocols are unchanged.

## Batch 17: read-only watermark writer preflight and acks=1 readiness

`PartitionLogStartAdvancePrecheck.assess()` classifies **value-domain**
proposals before any authoritative writer exists. It takes an explicitly
identified topic incarnation and partition, a proposed inclusive start offset,
a caller-observed Kafka log start, and a caller-established read-committed
metadata replay horizon. It rejects a snapshot behind the horizon, offsets
ahead of the source log, and replayed regressions. Equality is idempotent.
A missing topic-incarnation watermark requires an explicit initial zero rather
than silently inheriting another topic's value.

The precheck has no Kafka or MinIO write capability and does not establish the
freshness or authenticity of the caller-provided evidence. In particular it
cannot prove all brokers decode the watermark keys, active broker generation,
exclusive transactional writer ownership, read-committed catch-up at commit
time, or the source log-start value at commit time. `VALUE_DOMAIN_CANDIDATE`
and `INITIAL_ZERO_CANDIDATE` **must not** be treated as permission to write
or to retire references. The ordinary metadata producer continues to reject
log-start writes.

The acks=1 external-JVM test additionally waits for the elected partition leader
to answer a real, read-only ListOffsets request before its single-attempt,
`retries=0`, leader-only produce. This closes the test's controller-metadata
vs local leader-initialization race without weakening the crash/durability proof.

The focused watermark value preflight uses
`SharedMetadataImage.partitionLogStartEvidence(partition)` to capture only one
partition watermark plus replay offset under the image lock. It avoids copying
the entire COMMITTED-object inventory on each tentative watermark proposal
and preserves the missing-versus-explicit-zero distinction. The full inventory
snapshot remains available for batch object-retirement diagnostics.

## Batch 18: local role ABA fence, scoped transaction identity, strict metadata client policy

### Implemented, deliberately non-emitting

- `SharedPartitionRoleListener` invalidates `LocalRetirementLeadershipFence`
  tickets before follower/removal notifications, and issues a new ticket after
  each leader callback. Repeated LEADER callbacks, demotion/re-promotion,
  partition removal/reassignment and topic-ID recreation cannot resurrect
  a previously captured local ticket. Tokens from a different fence instance
  are rejected.
- `SharedMetadataClientConfiguration.retirementProducerProperties(partition)`
  reserves one deterministic transactional ID for each immutable
  `(clusterId, topicIdHigh, topicIdLow, partition)` tuple, independent of
  the broker ID. This is distinct from per-broker sequence-producer IDs.
  A future `initTransactions()` under the same identity can fence a
  previous producer incarnation. No production code instantiates this
  producer or emits retirement records in Batch 18.
- Existing metadata producers now reject unsafe `acks`, disabled
  idempotence, and caller-supplied `transactional.id`. The metadata consumer
  rejects `read_uncommitted`, auto-commit and `latest` auto-offset-reset
  overrides. SASL/SSL and other safe client properties remain configurable.

### Still required before the first watermark write

1. **Finalized mixed-version capability:** demonstrate all active brokers and
   supported rollback binaries decode the reserved log-start key/value. A
   single non-upgraded metadata replay participant can be permanently
   failed by an unexpected key; never use a local boolean as proof.
2. **Authoritative Kafka leader epoch:** extend the broker callback/ownership
   boundary with the source partition's actual KRaft leader epoch and
   validated log-start observation. The current local ticket is only an
   intra-process ABA fence; it contains no Kafka leader epoch.
3. **Single transactional owner:** acquire the immutable-topic-partition
   transaction identity and initialize the Kafka producer to fence the
   previous transactional producer. A stale former leader must never be
   able to re-initialize this identity and overwrite a newer watermark.
   This requires an authoritative persisted generation claim and a
   last-writer-wins/compaction-safe ordering protocol, not just a
   preflight read or a producer mutex.
4. **Read-committed catch-up:** after acquiring ownership, catch up from
   Kafka's authoritative metadata partition to a verified read-committed
   horizon. Recheck the source leader epoch, current log start and the
   persisted watermark/generation; reject absent evidence unless recording
   explicit zero on the first incarnation.
5. **Atomic publication and uncertain outcomes:** validate immediately
   before `commitTransaction()`. On fencing, timeouts, uncertain commit or
   role changes, discard any authority and reconstruct from an up-to-date
   read-committed replay before retrying. An acknowledgement/consumed
   offset alone is not evidence that a superseded writer was legitimate.
6. **No S3 side effects:** only when all above are proven may a separate
   lifecycle phase begin durable RecordBatch reference retirement and
   reader/upload quiescence, followed by crash-retryable MinIO DELETE.

#### Critical safety examples

| Scenario | Requirement |
| --- | --- |
| LEADER -> FOLLOWER -> LEADER on one process | Old local ticket stays invalid |
| Same topic name recreated with new Topic ID | Distinct retirement transaction identity and watermark |
| Different brokers lead the same topic-ID partition | Same transactional ID, able to fence old producer |
| Stale old leader re-initializes after new leader | Must detect lower authoritative leader epoch; no write |
| Metadata topic is compacted between generations | Last retained record remains monotonic and fenced |
| Operator overrides metadata `acks=1` or `read_uncommitted` | Fail fast at metadata client configuration |
| Kafka transaction outcome is unknown | Fail closed; reinitialize and replay before considering retry |
| One live RecordBatch exists in a packed object | Entire MinIO object remains protected |

Batch 18 changes **no persisted key/value encoding** and enables **no**
log-start writer, remote-reference retirement or physical object deletion.
Tests verify only the local ticket, deterministic ID and client-safety
properties; neither the local ticket nor `retirementProducerProperties()`
can authorize a cross-broker transaction.
