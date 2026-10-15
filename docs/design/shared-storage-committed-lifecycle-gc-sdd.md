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

## Batch 19: source KRaft leader epoch notifications (still no writer)

### Code and compatibility

The broker role seam now offers
`StoragePartitionRoleListener.onLeadershipChangeWithEpochs(leaders, followers)`.
`ReplicaManager.applyDelta()` supplies the **post-transition Kafka
`Partition.getLeaderEpoch`** for both leaders and followers, through
`LogManager`. The old callback remains the one abstract method: third-party
implementations and the default no-op listener still receive the original
leader/follower collections through a default delegation. The callback
does not block, access MinIO, or produce Kafka records.

`SharedPartitionRoleListener` forwards known source epochs into
`LocalRetirementLeadershipFence`. Each callback changes a unique **local**
generation, and demotion/partition removal invalidates existing tickets.
A stale lower epoch cannot re-promote a writer; an unversioned callback
after a known epoch cannot erase epoch evidence. Only
`captureEpochRetirementLeader()` can supply a ticket carrying an observed
KRaft epoch, and it rejects legacy notifications with an unknown epoch.

`PartitionRetirementEpochPrecheck` joins a still-current local ticket,
a caller-observed Kafka epoch, and the Batch 17 read-only watermark-value
preflight. Its strongest status is
`LOCAL_EPOCH_OBSERVATION_MATCH`: this **only** describes a local moment
and an advisory value-domain finding. It is explicitly not a write
authorization. Unit tests cover ABA, source-epoch mismatch, missing
epochs, stale lower-epoch notifications, missing initial zero, stale
metadata replay, an offset exceeding source log start, failed image,
and the original functional interface compatibility.

### The remaining cross-broker correctness gap

Kafka's `transactional.id` fences an earlier producer only when the
replacement initializes a new producer epoch. It **does not prevent a
stale old leader from subsequently calling `initTransactions()` and
fencing the current leader**. Nor can Kafka's compacted metadata topic
compare the next value with the previous value atomically: a delayed
lower watermark under the existing compacted partition key can replace
a higher watermark, leaving only the wrong winner after compaction.

This remains a **hard blocker** even now that the actual source leader
epoch reaches the local listener. Future code must prove an authority
protocol that rejects old KRaft epochs at **the durable commit boundary**,
including stale restarts, and preserves monotonic replay **after topic
compaction**. Candidate approaches need their own design and failure
proof (for example, generation-scoped records with safe ordering and
retention, or a KRaft-authoritative controller-coordinated state
machine); the existing one-key last-write-wins record cannot simply
be made safe by an in-memory precheck or the transaction ID alone.

No Batch 19 code creates a retirement transactional producer, emits a
partition log-start record, retires a durable remote reference, changes
RecordBatch reachability, or physically deletes a COMMITTED MinIO
object. The existing 19-gate GA evidence applies only to the previous
release scope, **not** completed COMMITTED lifecycle physical GC.


## Batch 20: epoch regression evidence and performance leader readiness

Batch 19's performance JUnit captured `NOT_LEADER_OR_FOLLOWER` on a
freshly created shared topic before repeated `OUT_OF_ORDER_SEQUENCE_NUMBER`
retries and a 120-second producer delivery timeout. The three completed LZ4
sample produce ratios were 0.5883, 0.7541, and 0.6876; the fourth sample
never completed. This is a **correctness-sensitive idempotent-producer
failure** requiring a production-path investigation if reproduced, not merely
a slow-performance sample to discard or a reason to relax GA thresholds.

The benchmark now requires read-only ListOffsets responses from all six
leaders after RF3/ISR3 metadata convergence for each new topic pair, before
sending idempotent warmup records. This targets the controller-metadata versus
data-plane leader-initialization race; it does not prove that an unrelated
leader transition, producer-state replay, or sequence-validation defect is
fixed. The acks=all, idempotence, throughput thresholds, warmup workload, and
sample count are unchanged.

The Shared Storage Java 25 workflow explicitly selects the two
`StoragePartitionRoleListenerEpochCompatibilityTest` methods. A new
fail-closed JUnit evidence checker requires those two methods plus the 13
local leadership fence, 10 shared role listener, and eight epoch precheck
methods to be present and unskipped. Missing reports, methods, failures,
errors, and skips fail CI; test-result XML is retained as an artifact.

The Batch 20 changes are **not** authoritative monotonic watermark writes,
durable COMMITTED reference retirement, reader/upload quiescence, or MinIO
physical lifecycle GC. A GA manifest PASS for the historical workflow
matrix does not authorize those unimplemented capabilities.


## Batch 21: reject equal-epoch callback reentry; prove compaction unsafety

### Local epoch-fence safety invariant

An epoch-aware LEADER callback may refresh a **currently leading** local
ticket at the same epoch (invalidating any previously captured ticket).
But once this partition has been observed as FOLLOWER at epoch E, a
delayed LEADER callback at E or below must **never re-promote** a local
retirement ticket. A new positive leader transition requires an epoch
strictly greater than the most recently observed follower epoch.

`LocalRetirementLeadershipFence` now applies this rule. Tests cover
LEADER(E) -> FOLLOWER(E) -> delayed LEADER(E), the subsequent valid
LEADER(E+1), and same-epoch duplicate leader notifications. A companion
`SharedPartitionRoleListenerTest` checks the real callback seam. The
mandatory Java 25 JUnit-evidence checker now requires all 39 named
epoch, role, and compaction-safety regression tests, without skips.

**Limitations:** this is only a process-local race fence. The current
`onRemoved()` releases local epoch history to avoid unbounded topic-ID
tombstones. Reassignment after removal, process restart, another broker
and stale producer reinitialization therefore still require an independent
authoritative generation check. Do not interpret these tickets as leases
or emission authorization.

### Real compacted-topic counterexample

`PartitionRetirementCompactionSafetyTest` uses the actual reserved
partition-log-start codec and `SharedMetadataImage`:

1. Replay `startOffset=40` at metadata offset 10, then
   `startOffset=20` at offset 11 for the same compacted key:
   live image replay correctly fails closed on the regression.
2. Kafka may compact away offset 10. A new consumer that only sees
   offset 11 accepts `20` because the higher value's provenance has
   disappeared; the persisted state cannot distinguish a stale writer
   from a legitimate initial value.
3. A consumed metadata offset does not prove a read-committed horizon,
   authoritative source leader epoch, or writer generation. Topic
   recreation remains isolated by immutable topic ID.

This is a **negative safety proof for the existing key**, not a test
that a production writer is working. Simply adding `transactional.id`
cannot fix it: an old broker can call `initTransactions()` after the
new leader and fence its producer in turn.

### Required authoritative protocol before emitting anything

The preferred design direction is a controller/KRaft-serialized partition
generation and watermark state machine, subject to explicit compatibility,
restart and rollback proofs. The **serialized authority** would need to:

1. Bind a generation to immutable topic ID, partition, current KRaft
   leader epoch, and authenticated current leader identity. Validate a
   fresh source log-start observation at the same epoch.
2. Serialize leader transition and watermark advance against one
   durable authority log with a monotonic-value invariant. Reject all
   lower generations and lower start offsets at that log's commit
   boundary; reconstruct those checks from compacted/snapshot state.
3. Prohibit unknown source log start, lost replay horizon, generation
   uncertainty, mixed-version decoder incompatibility, and ambiguous
   commit outcomes. No external metadata Kafka transaction or local
   callback alone may substitute for controller validation.
4. Keep the historical `0x04` compacted log-start key **non-emitting**
   unless its use is proven safe against a late stale last writer.
   Consumer mirrors cannot themselves become deletion authority.
5. Retire COMMITTED references only in a later, separately persisted
   lifecycle phase after every packed RecordBatch range is unreachable
   and reader/upload owners are fenced. Delete physical MinIO bytes
   only after crash-retryable reference retirement.

No controller RPC, KRaft metadata record, authoritative writer, reference
retirement, or MinIO physical COMMITTED lifecycle deletion is enabled in
Batch 21. The historical 19/19 GA PASS remains scoped to the already
implemented release behavior.


## Batch 22: executable authority-transition reference model (still non-emitting)

PartitionRetirementAuthorityModel is a **pure, side-effect-free reference
specification** for a future controller/KRaft-serialized retirement
generation and monotonic watermark protocol. It is intentionally **not**
a controller implementation, has no active production callers and no
Kafka producer, metadata codec writer, reader eviction or MinIO DELETE.

### Required state and ordering assumptions

Every hypothetical authority snapshot is scoped to an **immutable
Topic ID and partition** and retains:

- The offset of the **last accepted authority transition**, not a
  Kafka metadata consumer offset. Offsets must strictly increase
  but may have gaps for unrelated KRaft commands.
- The highest source-partition KRaft leader epoch accepted.
  A no-leader transition retains this epoch: a delayed same-epoch
  election may never recreate a writer.
- The active source broker ID, or explicit NO_LEADER, and an
  explicitly persisted inclusive log start (missing differs from zero).

The model's observeLeader, observeNoLeader and advanceLogStart
operations assume that **one trusted controller has serialized and
durably validated** every accepted transition at the commit boundary.
Each proposal supplies an expected prior authority offset to detect
stale snapshots. An old epoch, wrong broker, mismatched topic ID,
stale expected version, absent first zero, regressed watermark or value
above an observed source log start cannot change the model. Duplicate
leadership and equal watermarks leave the model unchanged; a new epoch
retains the prior watermark across broker changes.

Sixteen unit tests exercise cross-broker handover, stale former
leaders, equal-epoch demotion, authority-version races, monotonic
watermarks across a *hypothetical* compacted durable controller
snapshot, initial zero, topic-ID isolation and noncontiguous offsets.
The mandatory Java 25 anti-skip gate now lists **55 named tests**.

### What this reference does NOT establish

An arbitrary caller could fabricate the reference Snapshot, broker ID,
source epoch, log-start observation or authority offset. The reducer
cannot authenticate them. A future integration MUST obtain trusted
inputs from the current KRaft controller state and validate and
persist at one authoritative serialization point:

1. **Controller integration:** No KRaft record or controller RPC
   currently exists. A versioned record, controller image and
   mixed-version/rollback-capability gate are required first.
2. **Epoch ownership:** Broker callbacks and Kafka transactional IDs
   do not establish controller permission. Stale producers must be
   rejected *at commit*, including after reinitialization or restart.
3. **Durable snapshots:** The Java reference Snapshot is not durable.
   KRaft replay and snapshot restore must preserve maximum watermark,
   topic incarnation and generation despite compaction. The existing
   single-key last-write-wins metadata topic cannot guarantee this.
4. **Fresh source observation:** The method parameter representing
   the source log start is caller-supplied, not an authenticated
   latest source offset. Current KRaft epoch, bound and read-committed
   metadata replay horizon must be revalidated at commit time.
5. **Unknown outcomes:** Timeout, failed replay, lost generation,
   unsupported binary or uncertain controller response must deny
   further progress and reconstruct authority from committed state.
6. **Logical and physical GC:** A durable log-start does not retire
   COMMITTED packed objects by itself. A separate persistent reference
   retirement, read/upload quiescence fence and crash-retryable physical
   deletion phase are still mandatory.

The existing 0x04 log-start compacted key remains **non-emitting**.
The historical 19/19 GA manifest covers the previously delivered
shared-storage behavior and does not certify COMMITTED lifecycle GC.


## Batch 23: irrevocable Topic ID retirement and KRaft controller source preflight

### Topic Delete is terminal, independent of Kafka leader epochs

The Batch 22 pure authority reducer now models a **terminal Topic ID / partition
retirement fence**. A future controller-applied Topic Delete must persist this
terminal state at its authoritative serialized log position, even when the
topic never had an elected leader. Once deleted, the same immutable topic ID
can never gain a leader, accept a watermark, or be revived by a higher source
leader epoch. A duplicate terminal notification is idempotent, and a stale
expected authority offset cannot erase a newer state.

The terminal state preserves the previously accepted watermark for
crash/replay diagnosis and is intentionally separate from a temporary
NO_LEADER event. The reference Snapshot now includes an explicit terminal
flag, and its constructor rejects impossible combinations such as
terminally deleted plus an active leader. Topic recreation with a *new*
topic ID begins from an independent initial state.

Eight new model tests cover deleted-before-election, deleted-after-election,
stale/duplicate deletion, delayed watermark and leader callbacks, topic ID
recreation, immutable checkpoint restore, and invalid resurrection state.

**This is a reference invariant, not a KRaft topic-delete hook.** The current
production controller does not yet write or restore this retirement state.
Kafka Topic Delete by itself is not a COMMITTED physical object GC signal.

### Ground candidate identity in the actual controller registration

The metadata module now contains a read-only
PartitionRetirementControllerPrecheck using
ReplicationControlManager.getPartition(topicId, partitionId). The lookup is
scoped to immutable Topic ID, rather than a recyclable topic name. A candidate
broker and leader epoch are checked against the KRaft controller's current
PartitionRegistration. The gate rejects absent/deleted partitions, no leader,
wrong broker, old/future epochs, an unclean leader still recovering, and
leaders absent from ISR or the replica set.

Controller registration tests include simulated leader handover and deletion
via the actual controller lookup method, as well as fail-closed input handling.
They do not issue Kafka metadata writes or claims. Even
CONTROLLER_IMAGE_MATCH only describes one read from the local controller image
and must **not** be treated as an authorization token.

The Java 25 Shared Storage CI now also executes the targeted metadata-module
controller test class and checks its actual JUnit XML. Combined required
controller, topic-deletion, leadership, compaction and reference-model methods:
**76 executed, zero skipped**. A missing controller-module result or any
failure must block the gate.

### Remaining hard requirements before production enablement

1. A versioned KRaft metadata record and controller-owned replayable image
   for the immutable Topic ID terminal fence, elected generation, monotonic
   log-start watermark and last authoritative transition offset, including
   snapshot retention and mixed-version/rollback fencing.
2. A controller RPC/event-queue implementation validating the *current*
   PartitionRegistration, active broker incarnation and exact source log start,
   then writing the authority transition atomically at the KRaft commit
   boundary. A broker must not be able to claim or pre-empt a newer generation
   solely through transactional.id initialization.
3. Handle uncertain/failed commits by re-reading committed controller state,
   never by trusting an acknowledgement, stale local ticket or old compacted
   metadata record.
4. Persist multi-partition COMMITTED reference retirement separately from
   reader/upload lifetime fences. Reclaim an entire physical MinIO object only
   after every packed range and concurrent reader are proven unreachable.
5. Run restart, controller failover, rolling version, topic recreation,
   retention, DeleteRecords and MinIO physical HEAD/ListObjectsV2 tests with
   fail-closed evidence and no skipped cases.

The legacy 0x04 log-start metadata key remains non-emitting. There is no new
controller event, KRaft record, production watermark producer, remote index
mutation or physical COMMITTED object deletion in Batch 23. Historical GA
19/19 PASS remains limited to the already implemented shared-storage scope.


## Batch 24: keep the authority snapshot invariant fail-closed under Checkstyle

Batch 23 Java 25 Shared Storage failed in the main-storage module's static
analysis, not in Kafka/MinIO runtime validation:
the compact constructor of PartitionRetirementAuthorityModel.Snapshot
exceeded Checkstyle Cyclomatic Complexity (19 > 16) and NPath
Complexity (3456 > 500). The downstream Normalize GA Evidence Seal was
correctly BLOCKED; this was not an independent release correctness failure.

The constructor still performs the exact same fail-closed validations,
but delegates to four private helpers: domain, initial-state consistency,
leader/terminal-state consistency, and persisted watermark bounds. No
exceptions are downgraded, no guard is removed and no Checkstyle or GA
threshold is relaxed.

Four additional required JUnit witnesses cover unknown initial authority
offsets, terminal deletion before first election, preservation of
watermarks in terminal snapshots, and malformed negative domains.
The mandatory anti-skip list now requires **80** named methods across
storage and controller metadata tests, with zero skipped/failed.

The corrected guard is deliberately non-emitting. KRaft authoritative
transition records, controller-commit fencing, mixed-version rollout,
durable COMMITTED reference retirement and physical MinIO deletion remain
separate hard requirements. This checkpoint does not certify any of them.


## Batch 25: offline versioned authority envelope and WAL barrier evidence

### Non-emitting, strict versioned authority snapshot envelope

PartitionRetirementAuthoritySnapshotCodec is an **offline, non-production**
binary envelope for a hypothetical controller-authenticated retirement
snapshot. It defines a fixed **56-byte, big-endian v1** representation:

- magic (4 bytes), version (2), flags (2)
- immutable topic ID high/low (16), partition (4)
- last accepted authority transition offset (8)
- maximum source KRaft epoch (4), leader broker ID (4)
- explicitly present log-start watermark or canonical absent zero (8)
- CRC32C covering the first 52 bytes (4)

Unknown versions, reserved flags, noncanonical missing values, negative or
invalid snapshot state, truncation, extension, wrong topic incarnation,
uncommitted initial snapshots, corrupt CRC, and checkpoints behind the
caller-supplied committed authority horizon are all rejected. The required
Java 25 anti-skip contract adds **20 strict codec/replay tests**, taking
the required leadership, controller and snapshot coverage from 80 to 100
named methods.

Crucially, a CRC32C is **not cryptographic authentication**. This envelope
can be forged by an adversary and cannot establish a committed KRaft record,
source-leader truth or a fresh controller generation. Neither the encoded
bytes nor decoded Snapshot authorizes watermark emission, reference
retirement or MinIO deletion. The horizon is caller-supplied and MUST
ultimately be independently proven against committed controller state.
No KRaft MetadataRecord type, controller RPC or disk snapshot publisher is
introduced here. Compatibility/capability rollout and actual KRaft state
machine integration are still hard blockers.

### Batch 24 performance failure is WAL force dominated, not yet fixed

The unchanged performance test source blob
35ca6a384979227ba27d0fe04099f93d4b59b457 produced three
different measured distributions on GitHub's ephemeral runners:

| Batch | LZ4 produce ratio samples | Median | Raw produce ratio |
| --- | --- | --- | --- |
| 22 | 0.9949, 0.9043, 0.9075, 1.2346 | 0.9512 | 0.4567 |
| 23 | 0.8861, 0.3917, 0.7601, 0.7702 | 0.7652 | 0.4093 |
| 24 | 0.3614, 0.0895, 0.3160, 1.0641 | 0.3387 | 0.2463 |

In Batch 24 repetition 2, shared produce took 226.898 ms and all three
brokers' **aggregated** WAL durability barriers accumulated 243.730 ms
(116.783 ms data-force plus 126.166 ms checkpoint-force). These broker
measurements are cumulative/concurrent, so they must NOT be interpreted
as the single request's exclusive critical path. Nevertheless, the
barrier share of 1.0742 and WAL average barrier 3532 microseconds
identify storage-forcing variance as the main *observed* candidate.
The four hot-read ratios remain near 1, and the compared benchmark
source has not changed between these runs.

This is **not a claim that a performance regression is fixed**, nor
proof that hosted runner variance alone explains it. The required
minimum median produce ratio remains **0.60**, the consume ratio
remains **0.50**, both sample counts and all acks=all/fsync semantics
remain unchanged. Batch 24 therefore correctly produced a FAIL,
with a downstream BLOCKED GA Manifest.

Next performance investigations must isolate the underlying filesystem
force latency, compare cold/hot runner evidence and the two ordered
checkpoint barriers, and test any proposed batching improvement against
WAL crash windows, reopen, head reclamation and MinIO durability. A
faster but non-durable acknowledgement or a retry-to-green cannot be
a substitute for verified production throughput.


## Batch 26: bounded v1 decoder verification and CRC forgery witnesses

The previous Batch 25 Java 25 workflow was blocked by a **static analysis**
error, not a failing persisted-record semantic test:
PartitionRetirementAuthoritySnapshotCodec.decode() exceeded Checkstyle
NPath Complexity (1536 > 500). This checkpoint refactors the single decode
method into six small, independently reviewable checks: input/length,
checksum plus header, flags, immutable partition identity, minimum
authority offset, and canonical optional watermark. All original
validation and exceptions are retained. No complexity limit is relaxed.

Six additional negative tests cover tampered topic identity, malformed
negative partition, impossible broker ID, unknown nonterminal leader
epoch and, importantly, **forged CRC-valid high authority offset** and
**forged CRC-valid deletion-bit removal**. The latter two tests intentionally
demonstrate that the offline codec can accept a syntactically valid
but *unauthenticated* state when an adversary can rewrite the bytes.
CRC32C cannot establish a committed controller version or Topic Delete
authority. No consumer of this byte format may interpret successful
decode() as permission to advance a log-start watermark, retire a
COMMITTED reference, or delete a physical MinIO object.

The Java 25 mandatory JUnit evidence checker now requires **106 named
controller/epoch/metadata recovery tests**, with missing/skipped
tests remaining hard failures.

### Batch 25 measured performance (unchanged threshold)

The Batch 25 performance workflow passed with four LZ4 produce ratios
of **1.0042, 0.8587, 0.7561, and 1.1160**, a median of about **0.9315**
against the unchanged 0.60 gate. Hot-read ratios were **0.9913,
1.0132, 0.9638, and 1.0029**, with median about **0.9971**
against the unchanged 0.50 gate. The uncompressed diagnostic ratio
was **0.4906** and is *not* part of the LZ4 release threshold.

The measured WAL mean durability barrier for each LZ4 repetition was
about **460, 406, 387, and 374 microseconds**, versus **3532
microseconds** for the Batch 24 worst repetition. These are
per-batch averages aggregated across brokers, not exclusive request
latencies. The unchanged benchmark source and pronounced fsync variance
strengthen the runner/device latency hypothesis but do not establish
that performance is reliably fixed. Durability barriers remain enabled,
and no performance gate is bypassed.

**Still blocked:** actual versioned KRaft MetadataRecord deployment,
controller-image snapshot/replay integration, authoritative
commit-time broker/epoch validation, mixed-version rollout, durable
COMMITTED reference retirement, and physical MinIO lifecycle GC.


## Batch 27: isolate metadata-quorum unavailability from acks=1 and harden crash leader readiness

### Batch 26 failure evidence

The full Java 25 Shared Storage suite, real MinIO, performance baseline and
controller safety tests were green. Two external-JVM gates were not:

1. **RF3 leader-only acks=1**: user partition data-plane readiness was
   confirmed, but the test then sought offsets in the separately replicated
   internal __shared_storage_metadata topic with only one broker alive. Its
   KafkaConsumer.seekToBeginning failed after 60 seconds. This cannot
   establish loss of user records or a defect in user-partition WAL durability.
   Internal control-plane metadata has an independent availability boundary.
2. **AFTER_PREPARE upload crash**: BEFORE arming the crash barrier or killing
   a broker, the 20-record idempotent warmup timed out. Broker 3 repeatedly
   rejected producer 1001 sequence zero when current end sequence was 19,
   at log offset 16. The OutOfOrderSequenceException is a real unresolved
   idempotent append/duplicate detection signal, not an ordinary timeout.
   It must be treated as a correctness risk until explained and fixed.

### Changes with unchanged gate semantics

In the acks=1 RF3/RF2/RF1 matrix, leader-only record 0 remains acknowledged
with no followers. The old requirement to inspect the internal metadata
topic *while its RF3 replica set is intentionally unavailable* is moved
past the return of the broker quorum. After all brokers and both data planes
have recovered, the test MUST verify that no COMMITTED coverage exists for
record [0,1). Its read_committed metadata scan must also reach the captured
last stable offset (LSO); early deadline expiry or incomplete replay is
UNKNOWN, never proof of absence. Only then may it report
ACKS1_LEADER_ONLY_REMOTE_UNCOMMITTED (verifiedAfterFullQuorum=true).
The original leader WAL recovery, no automatic failover, exact record
contents, and asynchronously replicated survivor evidence remain mandatory.

The real-MinIO AFTER_PREPARE/AFTER_PUT/AFTER_COMMIT crash test now requires
a read-only ListOffsets response from the elected user-partition leader
before idempotent warmup sends, and again from the replacement leader before
post-SIGKILL sends. CI demands two distinct
UPLOAD_CRASH_DATA_PLANE_READY markers before success. This is a targeted
startup/failover race preflight, NOT proof that the prior out-of-order
sequence defect was solved. All 120-second producer delivery semantics,
acks=all, idempotence, crash barriers and MinIO safety assertions remain
unchanged.

### Escalation if idempotent warmup still fails

Examine UnifiedLog.appendAsLeader, SharedLogSegment.append, producer
duplicate-batch history after remote recovery, and the WAL GROUP_COMMIT
completion boundary. Record the received producer epoch/base sequence,
retained last five batch sequence intervals, local LEO and durable WAL
batch layout. A repeat of producer 1001 seq=0 versus endSeq=19 at offset
16 must NOT be fixed by disabling idempotence, widening timeouts or hiding
a repeated batch.

This batch contains no authoritative KRaft watermark write, COMMITTED
reference retirement or physical MinIO lifecycle deletion. Full lifecycle
GC remains outside the historical GA manifest.


## Batch 28: generated KRaft metadata type and read-only replay

This checkpoint moves beyond offline authority reference code to **Kafka's
actual generated KRaft metadata record, metadata images and controller
replay timeline**, without enabling production emission or deletion.

### Record, snapshot, controller image

- New PartitionRetirementAuthorityRecord.json reserves metadata API key **29**
  (flexible v0), holding immutable Topic ID and partition, last accepted
  authority offset, highest source leader epoch, active broker ID, an
  explicit/missing (-1) log start, and terminal Topic Delete bit.
- PartitionRetirementAuthorityState rejects malformed records, decreasing
  authority offsets, decreasing source epochs/watermarks, same-epoch
  broker switches or re-promotion following demotion, and revivals after
  terminal Topic Delete.
- MetadataDelta and MetadataImage now replay the generated record into an
  immutable PartitionRetirementAuthorityImage. Its isolated v0 snapshot
  serializer includes all known states, including deleted Topic ID
  tombstones. Production MetadataImage.write **fails closed** for any
  nonempty authority image until an explicit cluster capability gate
  and compatible mixed-version rollout have been implemented.
- QuorumController now routes replay of that record to a TimelineHashMap
  backed by the actual SnapshotRegistry, allowing in-memory controller
  rollback/replay without an authority writer or new RPC.
- Ten record/state, seven image/snapshot and five controller timeline
  regression cases are added. The Java 25 anti-skip evidence requirement
  increases from **106 to 128 methods**. The targeted metadata test job
  also executes its main/test Checkstyle tasks.

### Compatibility and safety boundary

**Never emit the new API key 29 in production yet.** Existing older Kafka
controllers and brokers cannot decode an unknown KRaft record type. A
code-generated message type is not itself a rolling upgrade feature
gate. This stage provides no controller write event, Broker API, CLI,
timer, or metadata record producer for the new type.

A future implementation must explicitly gate ALL writer and snapshot
paths on cluster feature negotiation and coordinated versions across
controllers and brokers, including rollback. Until that negotiation is
proven, the only safe production image is an empty retirement image.
Even after replay, the authorityOffset field is supplied by the
record, not authenticated at a KRaft write boundary.

A Topic Delete record does not currently emit a corresponding
terminal retirement authority record. A future controller must
serialize deletion and retirement fencing together, preserving
immutable Topic-ID tombstones through snapshots and compaction.
Missing or incomplete images MUST NOT be used as proof that
COMMITTED object references are unreachable.

The following remain hard GA blockers for full lifecycle GC:
atomic controller event queue / KRaft authority validation and
commit, proven broker/epoch identity, metadata-version capability
gating, quorum failover and replay under mixed versions, durable
COMMITTED reference retirement, reader/upload quiescence and
real MinIO physical deletion with crash/retry proof.

Existing 19/19 GA evidence applies only to previously enabled
Shared Storage functionality. Batch 28 adds no physical GC.


## Batch 29: negotiated KRaft authority feature and replay-offset provenance

The generated retirement KRaft record from Batch 28 remains **non-emitting**
in all production controller and broker operations. This batch adds an
explicit, experimental Kafka cluster capability and refuses out-of-order
replay and unsupported metadata snapshot emission.

### Feature negotiation: production defaults to disabled

The standard Kafka Feature enum now includes
partition.retirement.authority.version with these levels:

- v0: no authority records are permitted; this is the latest production
  version and the default through metadata.version=4.3-IV0.
- v1: experimental, requiring metadata.version >= 4.4-IV0 (currently
  unstable), registered through Kafka's normal broker/controller
  supported-feature advertisements and finalized FeatureLevelRecord.
  FeatureControlManager can negotiate the level only when the full
  quorum supports it. Installing a new binary alone cannot activate it.

Both QuorumController and broker MetadataDelta reject API key 29 unless
feature v1 is finalized **and** the replayed metadata.version supports
4.4-IV0. This includes KRaft snapshot replay; the snapshot must place
metadata.version and feature records before the retirement records.

MetadataImage.write also requires negotiated v1 in the source image and
a snapshot target metadata.version >= 4.4-IV0. Writing a nonempty
authority image to a 4.3 or otherwise unnegotiated image throws
**before writing any bytes**. A negotiated test-only 4.4 image writes
the complete tombstone set; its full KRaft MetadataDelta replay must
recover both the feature level and terminal deletion state. Empty
legacy images remain writable without new records.

The v1 enum has LATEST_PRODUCTION=v0. The test matrix covers default
v0, unstable level rejection, dependency validation, wrong record
ordering, unsupported 4.3 replay, negotiated v1 snapshot round trip,
and fail-closed snapshot downgrade.

### Bind version provenance to actual KRaft log position

A new replayFromKRaft entry point in PartitionRetirementControlManager
requires the historical authority transition offset to match the
**actual KRaft append log offset** for ordinary record replay. For a
KRaft snapshot, it must be strictly less than the snapshot's
next-offset boundary; a valid historical authority may precede
unrelated KRaft records. A mismatch throws before mutating the
controller's SnapshotRegistry/TimelineHashMap-backed state.
This check cannot authenticate a fabricated or corrupted KRaft
snapshot independently, but no unverified arbitrary offset is accepted
on the ordinary controller replay path.

### Evidence and remaining release blockers

The dedicated Java 25 Shared Storage job now runs Metadata module
targeted Checkstyle + replay tests and Server Common feature-version
tests and four actual FeatureControlManager/QuorumFeatures contract
tests; it checks JUnit XML from all three modules with no missing,
failed or skipped methods. Total mandatory safety methods:
**147**, increased from 128.

There is still NO public controller API or producer for this record.
Even with feature v1 negotiated, there is no authorized watermark
advance, trusted source-log-start proof, per-broker incarnation grant,
or atomic controller event to emit API key 29. It must remain disabled
for production until the upgrade/rollback protocol, true
controller-serialized checks and quorum failover tests are complete.
No COMMITTED reference retirement or physical MinIO deletion is
enabled, and historical GA 19/19 does not certify either capability.

The replay-side authority-version check is tied to KRaft's real record
position for ordinary log replay, not to a caller's invented offset.
Controller tests also reject forged high-offset claims and confirm
that the previous state is unchanged on rejection. Experimental
FeatureLevelRecord replay can precede metadata.version=4.4 only as
an in-memory record; the effective authority feature stays disabled
until BOTH required levels are present. A production controller
rejects the unsupported experimental feature record outright.


## Batch 30: durable broker incarnation fencing and serialized CAS preflight

This checkpoint closes a protocol field omission in the experimental API key
29 (PartitionRetirementAuthorityRecord) and grounds candidate validation in
the real Kafka controller write-event queue. It **does not** enable the
record's production emission or COMMITTED physical-object retirement.

### Record 29 now persists full registered-broker identity

Two required v0 fields are added to the still-dormant KRaft record:

- BrokerEpoch: the controller-issued broker registration epoch, or -1 when
  no authenticated registration proof exists.
- BrokerIncarnationId: the UUID from RegisterBrokerRecord, or ZERO_UUID when
  the identity is absent.

The immutable PartitionRetirementAuthorityState.Value carries both fields
through record encoding, timeline replay, broker image, and future snapshots.
A five-argument compatibility constructor remains for the existing
non-emitting reference tests, but its active broker identity is explicitly
**unbound**, not safe for production authority emission.

Canonical validation rejects half-populated epoch/UUID pairs, negative
broker epochs, and any broker identity retained by NO_LEADER or a
terminally deleted Topic ID. Once a value has a bound broker identity,
a later active value may not silently drop its proof. A different
incarnation/registration epoch cannot take over at the same source
partition LeaderEpoch, even if it reuses the same numeric BrokerId.
A new source LeaderEpoch may carry a different registered incarnation
after appropriate controller validation.

**Compatibility note:** the experimental record's v0 binary wire layout
is deliberately expanded before first production use. The older
Batch 28/29 v0 schema has not been emitted to KRaft by production code.
If any external test cluster was manually seeded with the experimental
old v0 bytes, it must be reset or migrated in a separately validated
pre-release protocol; do not silently treat incompatible old bytes as
equivalent.

### Controller candidate precheck runs in serialized write queue

QuorumController.preflightPartitionRetirement posts a **record-free**
ControllerWriteEvent, so its validation is ordered with current KRaft
controller state transitions rather than an unsequenced external read.
The checker compares:

1. Negotiated experimental cluster feature (production still disabled).
2. The actual next KRaft write position versus requested authority offset.
3. Compare-and-swap of expected historical authority offset with
   PartitionRetirementControlManager's replayed current generation.
4. Immutable Topic ID, current recovered leader/epoch, ISR membership,
   and prior irrevocable deletion tombstone.
5. Live ClusterControlManager registration: unfenced broker, not in
   controlled shutdown, exact broker epoch and incarnation UUID.
6. Monotonic source LeaderEpoch and accepted log-start watermark;
   no same-epoch resurrection or broker replacement.

A matching candidate is **not** granted a write token. It returns
SOURCE_LOG_START_NOT_VERIFIED, and the event emits zero KRaft records.
This is intentional: Kafka Controller does not yet have a trusted
source-partition LogStart attestation path. A broker's claimed
log-start, even with valid registration, is NOT sufficient evidence
for authoritative watermark advancement. The CAS currently proves
only preconditions at one point in the controller queue; it does
not reserve an offset for a later writer.

The targeted Java 25 gate gains seven persisted identity invariants
and sixteen real-controller-metadata candidate tests, raising required
cross-module JUnit coverage from **147 to 170 named methods**.
A missing report, any skip or any failed case remains a release blocker.

### Remaining hard blockers before true authority commit

1. Design and verify an authenticated source-log-start attestation,
   bound to Topic ID, partition, BrokerEpoch, BrokerIncarnationId,
   LeaderEpoch, committed WAL horizon, and replay/catch-up evidence.
   Reject old acknowledgments, restarted brokers and speculative
   log-start values.
2. Implement one atomic controller write event with a **fresh** full
   recheck, exact KRaft assigned record offset, Feature v1 eligibility
   and durable post-commit result. Never convert the preflight result
   into a reusable write authorization.
3. Serialize Topic Delete with terminal retirement fencing so that
   a deleted Topic ID cannot lose its tombstone through KRaft snapshot
   replacement, controller failover or metadata compaction.
4. Test Controller failover, mixed-version feature enablement/rollback,
   source-log-loss and broker re-registration on real KRaft batches.
   No old reader may encounter an unsupported metadata API key.
5. Only then design and test multi-partition COMMITTED reference
   retirement, reader/upload quiescence and actual MinIO physical GC.

Current 19/19 historical GA PASS continues to cover existing Shared
Storage runtime only. This batch neither activates Watermark Writer nor
authorizes physical deletion.


## Batch 31: restore CI compilation and pin CAS evidence to one controller state

### Batch 30 failure classification

Batch 30's Java 25 Shared Storage and MinIO workflow jobs both failed
BEFORE meaningful runtime testing, while compiling
PartitionRetirementCandidatePrecheckTest. Four local variable declarations
used the package-private nested type `Candidate` without importing
`PartitionRetirementCandidatePrecheck.Candidate`. The downstream
Normalize GA Evidence Seal was BLOCKED. This was one deterministic
test compilation failure observed in two workflows, not evidence
that the replicated WAL, remote upload or MinIO correctness regressed.

Batch 31 adds the missing import without relaxing any Java compiler,
Checkstyle, CI or GA gate. Both affected workflows must actually run
on the new canonical HEAD before this repair is considered verified.

### CAS and broker incarnation review fixes

- Controller candidate preflight now reads the previously replayed
  authoritative Value only **once** per assessment, then uses the same
  immutable value for both expected-authority-offset CAS and monotonic
  epoch/watermark/broker continuity validation. This prevents a
  potential mixed-observation classification if the checker is ever
  called outside the serialized ControllerWriteEvent context. In the
  existing event-queue call path, updates are already serialized.
- A KRaft broker registration without an incarnation UUID is an
  explicit BROKER_INCARNATION_MISMATCH rather than a NullPointerException.
  The candidate must still present a nonzero incarnation, broker epoch
  and unfenced, non-shutting-down registration.
- A preflight finding is NEVER a reusable authorization token.
  Deterministic tests first observe
  SOURCE_LOG_START_NOT_VERIFIED, then mutate authoritative CAS state or
  elect a different leader, and require the original candidate to be
  rejected on a fresh controller assessment.

### Broker identity persistence and fail-closed metadata replay

Three mandatory MetadataImage tests now verify:

1. BrokerEpoch and BrokerIncarnationId survive an actual negotiated
   KRaft MetadataImage snapshot serialization and full MetadataDelta
   replay without loss or substitution.
2. A record with only one of the two broker-identity fields cannot
   enter the Broker metadata image.
3. Replacing an authenticated broker incarnation without incrementing
   source partition LeaderEpoch is rejected during MetadataDelta replay.

These complement four new Controller candidate regression methods.
The mandatory Java 25 evidence checker now requires **177 named methods**
(up from 170), including real Storage, Metadata and Server Common
JUnit XML; missing, skipped or failed witnesses block the release.

### Remaining unimplemented authority proof

The current broker SharedCommitProgress is a **process-local cache** of
Kafka log-start, HW and replica role, not an authenticated, durable
Controller attestation. Neither a caller-provided watermark nor a
CRC32C-valid offline snapshot is proof of authoritative Kafka source
state. The Controller deliberately returns
SOURCE_LOG_START_NOT_VERIFIED even after every CAS/registration check.

Next work must bind an independently verified source log-start with
KRaft Topic ID, BrokerEpoch, BrokerIncarnationId, LeaderEpoch,
committed WAL/replica horizon and read-committed metadata catch-up;
it must be revalidated inside one actual controller write event that
atomically emits the version-gated authority record at its assigned
Raft append offset. No reusable preflight receipt is acceptable.

COMMITTED reference retirement, physical deletion, reader quiescence
and object-store garbage collection remain disabled. Existing
historical GA evidence does not certify these missing capabilities.


## Batch 32: live native source LogStart visibility and upload-role fencing

### Discovered missing runtime observation

Before Batch 32, SharedUnifiedLogFactory called
SharedCommitProgress.onLogLoaded() only when LogLoader returned offsets.
The SharedUploadScheduler then used the cached logStartOffset in its
segment selection and revalidation. Native Kafka UnifiedLog can advance
the real log start later via DeleteRecords or retention, and can
lower it during replica divergence truncation and restart. Merely
observing the initial value therefore risks scheduling already-deleted
source records for remote upload or retaining an obsolete cached start.
It is **not** a trusted LogStart attestation for KRaft.

### Actual Kafka mutation hooks rather than candidate-supplied watermarks

1. SharedUnifiedLog now binds the SharedCommitProgress tracker **after
   native UnifiedLog construction**, taking the initial logical
   logStartOffset and high watermark from the actual initialized log,
   not the pre-constructor LogLoader candidate.
2. A successful override of maybeIncrementLogStartOffset publishes
   the new actual Kafka log start. The override MUST NOT acquire the
   shared remote-recovery lock: Kafka retention code may invoke it
   with the native log monitor already held, reversing the normal
   recovery-fence -> Kafka-log lock order and risking a deadlock.
   Instead, SourceLogStartCallbackFence captures a local generation
   before the native mutation and serializes callback publication
   only if that generation remains current. The bridge is strictly
   in-memory; failed/no-op Kafka updates publish nothing.
3. Both truncateTo and truncateFullyAndStartAt publish an exact
   post-operation native log-start rebase under the remote recovery
   write fence; a lower value is legal following Kafka truncation.
   The generation increments at BOTH truncation entry and completion.
   Old callbacks captured before or during truncation cannot republish
   stale higher values afterward, even if they complete out of order.
   onLogRebased ignores already-removed partitions, so late callbacks
   cannot re-create retired upload state. The native high-watermark
   callback remains responsible for the high-watermark value.
4. SharedPartitionRoleListener now checks its local epoch fence
   before publishing a LEADER role to SharedCommitProgress. A
   delayed LEADER(E) following FOLLOWER(E), a lower-epoch leader,
   or an unversioned leader callback after an observed epoch can no
   longer re-enable uploads when the local fence rejected that
   leadership. Genuine new LEADER(E+1) still re-enables uploads.

These are **production runtime safety/visibility fixes** for the
existing shared upload scheduler. All callbacks stay in-memory,
non-blocking and free of remote I/O under Kafka log/role locks.
The native replica HW and acknowledgement semantics are unchanged,
as are the MinIO upload, remote metadata and WAL formats.

### Fail-closed tests and release evidence

The Batch 32 Java 25 Shared Storage workflow MUST execute:
- all 12 SharedCommitProgressTest methods, including six new
  deterministic DeleteRecords/truncation/removal callbacks
- all 14 SharedPartitionRoleListenerTest methods, including three new
  stale-leader upload-fencing regressions
- all 6 SourceLogStartCallbackFenceTest methods, including generation
  capture before/during/after truncation, nested truncation callbacks,
  and callbacks arriving after partition removal

The strict anti-skip evidence checker therefore expands from 177 to
**198 named methods**. Skipped, missing and failed methods remain
hard blockers. Existing Shared Storage and real-MinIO E2E continue
to run as before and GA source fingerprints include the updated
production Java files.

### What this does NOT prove

A Kafka source log's local logical LogStart and high watermark are
observations from one broker, not a KRaft-committed, replicated
certificate. A broker may crash or be fenced after the observation,
the WAL durability/replay horizon may be behind, and local upload
state can change without a controller event.

No code in this batch hands SharedCommitProgress observations to a
KRaft authority writer. Controller preflight continues to return
SOURCE_LOG_START_NOT_VERIFIED and emits zero metadata records.
The remaining GA blockers for COMMITTED lifecycle GC are a
trustworthy source LogStart certificate bound to topic ID, leader and
broker incarnation, durable log/WAL/remote metadata catch-up,
a revalidated atomic KRaft controller write, and durable
COMMITTED-reference retirement with reader/upload fences before
physical MinIO deletion.


## Batch 33: preserve removed-partition epoch fences and capture native source windows

### Production safety bug: removal forgot the maximum source leader epoch

Batch 32 correctly rejected delayed LEADER(E) after FOLLOWER(E),
but LocalRetirementLeadershipFence.onRemoved removed the entire
partition entry. A later delayed LEADER(E) callback could then look
like the first-ever assignment, obtain a new valid local ticket, and
set SharedCommitProgress's upload role to LEADER on a removed partition.
The scheduler might eventually upload obsolete data if old WAL progress
or callbacks repopulated the commit window.

Batch 33 keeps an explicit, fail-closed removed-partition tombstone
holding the highest already-seen source LeaderEpoch and a new
local generation. A removed partition is only readmitted after a
strictly higher, **explicit** Kafka epoch notification. Legacy
epoch-less callbacks are not sufficient for reassignment because
they cannot distinguish a new assignment from a delayed old one.
Repeated removal, delayed same/lower epoch, and later follower
notifications do not erase this tombstone; an immutable new Topic ID
starts independently. Existing initial legacy callbacks remain
supported before a partition has ever been removed.

SharedPartitionRoleListener serializes its removal and role callbacks
using one strictly in-memory monitor, then checks the removed
tombstone before creating any SharedCommitProgress entry. It removes
stale progress instead of turning rejected leader/follower callbacks
into synthetic new partitions with default offsets. The monitor
never performs Kafka, MinIO or metadata I/O.

**Lifecycle trade-off:** removed-epoch tombstones are retained for the
lifetime of the broker process rather than evicted on a timer, because
removing one without KRaft evidence could re-enable an old callback.
The future authoritative broker fencing protocol needs a validated
reclamation policy with bounded memory, not a best-effort TTL.
Until then these process-local tombstones are not durable KRaft state.

### Real native source window, not a self-reported watermark

SharedUnifiedLog.captureNativeSourceWindow reads actual Kafka
logStartOffset, highWatermark and exclusive logEndOffset under
the established **remote-recovery read lock -> Kafka native log
monitor** order. Invalid offset relations (negative start, HW below
start, LEO below HW) return UNKNOWN rather than inventing a result.
No user-facing Kafka log mutation or I/O is performed by the reader.

SourceLogStartObservation.capture additionally requires:

- matching immutable Topic ID and partition between the requested
  SharedPartitionId and the actual live UnifiedLog
- a Kafka-epoch-aware local leadership ticket
- a valid native source offset window
- that the same ticket is still the local leader after the native
  snapshot has been read

A leadership change during observation fails closed. A stale topic
name, different partition, removed partition, unknown epoch or
inconsistent source offsets cannot produce an observation.

This is strictly broker-local **read-only evidence**. A local ticket,
log bounds and normal log lock are not a signed broker-incarnation
attestation, durable WAL checksum/commit horizon, quorum-wide
source progress, or a KRaft-authorized Controller CAS. No producer,
controller RPC, watermark writer or deletion authority is introduced.

### Regression and GA scope

Five LocalRetirementLeadershipFenceTest methods and four
SharedPartitionRoleListenerTest methods cover removed partitions,
reassignment epochs, recreated topics and prevention of phantom
upload progress. Ten SourceLogStartObservationTest methods cover
native bounds, topic/partition binding, legacy epochs, demotion
during capture and re-election after removal.

The Java 25 Shared Storage anti-skip JUnit gate grows from
**198 to 217 named safety cases**, alongside existing real MinIO,
crash/failover, rolling upgrade and performance workflows.
No other job duplicates these storage unit tests.

Open P0 blockers before enabling full COMMITTED lifecycle GC remain:
authenticated, durable BrokerEpoch/Incarnation/LeaderEpoch/LogStart
evidence with WAL and metadata replay catch-up; one atomic,
version-gated KRaft Controller commit event with exact assigned
offset; Topic Delete tombstone survival through snapshots; durable
reference retirement; reader/upload quiescence; and retryable
physical MinIO deletion with crash and restart proofs.


## Batch 34: bounded removed-partition identities with permanent fail-closed quarantine

### Why a simple TTL or LRU eviction is unsafe

Batch 33 correctly retained the maximum Kafka LeaderEpoch for a removed
immutable Topic ID and partition so a delayed LEADER(E) could not
re-create SharedCommitProgress upload eligibility. However those
removed-partition entries accumulated for the broker process lifetime.
A topic create/delete or partition reassignment workload could grow
the role identity map without bound.

Evicting the oldest tombstone via LRU, time-based TTL, or a fixed age
is **not safe**: callbacks carry no signed expiry proof, and eviction
makes a delayed old LeaderEpoch indistinguishable from first assignment.
This would trade a memory bound for possible reuse of stale upload
leadership.

### Bounded, fail-closed operational behavior

LocalRetirementLeadershipFence now has a fixed per-process limit of
**131072 distinct tracked immutable partition identities** (including
active roles and removed-topic tombstones). This is a conservative
bound for the in-memory local role evidence, not a bound for Kafka's
registered partitions or cluster metadata. Internal tests use a
smaller injected limit to exercise saturation deterministically.

On the first additional previously unseen identity when full:

1. The fence permanently latches QUARANTINED for this broker process,
   clears all its previously tracked roles and tombstones, and refuses
   **every** new leader ticket, including a newer LeaderEpoch. It
   never silently evicts a tombstone and remains unable to grant local
   upload eligibility until a full broker-process restart.
2. SharedPartitionRoleListener serializes removal/reassignment
   callbacks through its existing in-memory callback lock and, on
   quarantine, issues one ERROR-level operational message explaining
   the restart requirement.
3. SharedCommitProgress latches DISABLED, clears all previously
   selected leader/HW/log-start commit windows, and rejects later
   onLogLoaded, HW, native LogStart, rebase and role callbacks. The
   upload scheduler observes an empty partition snapshot regardless
   of concurrent callback timing.
4. SharedUploadScheduler rejects new scheduling before reserving
   upload slots and rechecks the latch before accepting a selected
   WAL candidate. It does not allocate new object IDs or start a new
   object PUT from a quarantined selection.

This is a deliberate **availability-for-safety** choice. A broker
that exhausts its local role identity memory budget may stop making
progress on shared uploads until restart. The limit is not auto-reset
during partial component reload, and there is no unsafe time-based
quarantine release.

Already-started asynchronous S3 PUT/metadata futures are not forcibly
cancelled; they follow the existing bounded completion and shutdown
protocol. Quarantine prevents **new upload scheduling** and fences
new local leadership, but it is not a distributed object publication
revocation or cross-broker GC permit. Kafka's regular source-log and
replication processing remain unchanged.

### Regression evidence and remaining Controller requirements

Mandatory Java 25 safety coverage expands from **217 to 229 named
tests**. Four LocalRetirementLeadershipFenceTest methods cover
capacity exhaustion, old-ticket revocation, non-eviction of
previously seen identities and bad capacity settings. Three
SharedCommitProgressTest methods verify permanent callback shutdown.
Three SharedPartitionRoleListenerTest methods verify revocation of
all existing progress windows on leader- or removal-triggered
quarantine, and two SharedUploadSchedulerTest methods verify both
pre-scheduling and selection-to-PUT races. The strict anti-skip
checker still rejects missing, skipped or failed JUnit XML evidence.

Next, cross-broker durable controller authority MUST NOT depend on
these ephemeral process-local latches. Safe tombstone retirement
requires a separate KRaft state-version/reader horizon proof. A
fully trusted LogStart certificate still needs broker incarnation,
source LeaderEpoch, exact Kafka LogStart/HW/LEO, durable WAL and
remote metadata catch-up, and atomic feature-gated Controller CAS
record emission at the genuine assigned KRaft offset. None of that
is bypassed or enabled by this batch. Physical MinIO deletion and
COMMITTED-reference retirement remain disabled.


## Batch 35: repair shared storage test compilation and preflight required Java sources

### CI evidence and exact failure classification

Batch 34 HEAD `8dd3e264`, normalized to `668155ff`, launched
20 GitHub Actions workflows. Fourteen completed successfully.
Six failed: Main Shared Storage, Object Format Correctness,
Ring WAL Correctness, WAL Crash Windows, Local State Loss Recovery,
and the dependent Normalize GA Evidence Seal.

The first five failures all trace to exactly one common Java
compilation error in
`storage/src/test/java/org/apache/kafka/storage/internals/shared/kafka/SharedCommitProgressTest.java`:
the new permanent-quarantine tests call JUnit's `assertTrue` without
`import static org.junit.jupiter.api.Assertions.assertTrue;`.
Gradle fails at `:storage:compileTestJava`; the dependent seal
cannot PASS because the source-SHA evidence is incomplete.

There is **no evidence from those failed jobs** of an independent WAL,
MinIO or Kafka correctness regression, because their affected tests
did not reach actual runtime execution. Batch 35 fixes the missing
test import without changing the production quarantine contract,
the mandatory count of **229 JUnit methods**, or any Checkstyle rule.

### Cheap negative-tested source preflight

A new standard-library-only Python checker
`tools/check_shared_storage_epoch_test_sources.py` examines the exact
mandatory test suites in the existing Java 25 JUnit evidence manifest:

- Maps each fully qualified mandatory test class to its actual
  Storage, Metadata or Server Common Java test source.
- Rejects any missing required Java file or named test method.
- Detects unqualified JUnit assertion calls (`assertTrue`,
  `assertFalse`, `assertThrows`, etc.) without a matching
  `org.junit.jupiter.api.Assertions` static import.
- Supports explicit assertions, JUnit wildcard static imports and
  fully qualified assertion calls without requiring a static import.

The checker has dedicated fast regression tests for the exact missing
`assertTrue` scenario, a wrong-framework static import, wildcard and
fully qualified legal forms, missing test file and missing method.
The Shared Storage Java 25 workflow executes these cheap tests and the
source preflight **before Gradle setup or expensive Java test startup**.
This is a compiler-error preflight, not a substitute for javac,
JUnit XML anti-skip gating, or runtime MinIO tests; it does not
duplicate the mandatory 229 JUnit tests.

### Scope and next blockers

Batch 35 is deliberately a targeted CI repair with fail-fast
validation. Its prior evidence is not GA PASS until Java 25 and the
complete refreshed source-SHA GA matrix rerun on the new canonical
commit. The production hard blockers for COMMITTED object retirement
are unchanged: durable authenticated source LogStart evidence tied to
the immutable Topic ID, BrokerEpoch, Incarnation ID and LeaderEpoch,
KRaft controller atomic version-gated emission, Topic Delete
tombstone durability, WAL/remote metadata replay catch-up and
reader/upload quiescence ahead of any MinIO physical deletion.


### Batch 35 fast preflight bootstrap correction

The initial Batch 35 push reached the new fast Python preflight.
All seven source-preflight unit tests succeeded, but the subsequent
script-path invocation failed before scanning Java sources with
`ModuleNotFoundError: No module named 'tools'`. The cause was the
Python startup search path for `python3 tools/script.py`, which
starts at the tools/ directory rather than the repository root.
The checker now bootstraps the repository root into `sys.path` when
run as a standalone script, while retaining the package mode
`python3 -m tools.check_shared_storage_epoch_test_sources`.
An eighth negative-tested subprocess regression runs the script
from an unrelated directory and verifies that it resolves the
package and reports missing required Java sources normally.

This was a new preflight invocation defect, not a Java or object
storage runtime regression. The compilation repair and original 229
mandatory Java tests remain unchanged. The canonical-head CI rerun
must still pass all release gates before declaring Batch 35 PASS.


## Batch 36: last synchronous upload quarantine fence and recovery GA evidence ownership

### Source-SHA GA evidence classification after Batch 35

The Batch 35 Java 25 Shared Storage tests and three-broker real MinIO
integration job both PASS on the repaired source. Normalize Author also
PASS; the final GA Evidence Seal remains BLOCKED on **one of 19 gates**:
Shared Storage Local State Loss Recovery. Its latest matching run was
a Batch 34 test-compilation failure caused by the missing assertTrue
static import, not a local-loss runtime correctness failure.

The recovery workflow's push.paths did not include
SharedCommitProgressTest.java, although the workflow executes
:storage:test and therefore recompiles it. The repaired test source
alone could not invalidate this recovery gate's manifest contract
or schedule new evidence; the seal correctly rejected the stale
failure rather than interpreting other successful workflows as proof.

The Local State Loss Recovery workflow now watches both
SharedCommitProgressTest.java and SharedUploadSchedulerTest.java for
push and pull-request events. Its GA workflow contract fingerprint
therefore includes these compiled quarantine-safety test inputs.
A separate GA workflow consistency regression verifies these
dependencies in both event filters. Changes to either Java test
now trigger genuine recovery validation instead of relying on a
failed older source tree.

This adds **no duplicate runtime JUnit tasks** to the recovery job:
it still runs its existing focused storage recovery, fetcher,
three-broker local-state loss and remote checkpoint-loss tests,
using MinIO and Java 25. New source-file triggers merely keep the
job's evidence fresh.

### Narrow quarantine race during object ID allocation

Before this change, SharedUploadScheduler.selectionStillCurrent()
correctly rejected a partition quarantined during selection, but
startUpload() used a pluggable object ID supplier and then immediately
called SharedObjectUploader.upload(). An allocator could synchronously
trigger quarantine **after** the last selection check and still
cause a new object PUT, despite disabled local upload eligibility.

startUpload() now rechecks the quarantine latch **after** obtaining
a valid object ID and **before** invoking the asynchronous uploader.
If disabled, it releases candidate reservations and the acquired
upload slot, returns an empty result, does not PUT, and does not
record a spurious upload failure. Consuming an otherwise unused
monotonic object ID is safe; object IDs must not be recycled.

A deterministic SharedUploadSchedulerTest makes the allocator
latch quarantine during that precise gap. It verifies zero PUT,
zero reservations, zero in-flight slots, no remote coverage,
no false failure diagnostic and no later allocator invocation.

**Concurrency limit:** a fresh quarantine from another thread may
still race in the nanoseconds after this final volatile check and
before asynchronous upload initiation. This change closes the
allocator-triggered synchronous gap but does NOT claim atomic
distributed revocation or cancellation of already-started uploads.
A future true authorization fence must linearize producer admission,
persist WAL and metadata durability, and revoke the right to
publish before COMMITTED reference retirement; that protocol is
still unimplemented.

### Evidence and release impact

Mandatory retirement/source epoch JUnit witnesses increase from
**229 to 230** with the new allocator-quarantine regression. The
strict anti-skip source and JUnit XML checks remain mandatory.
The existing Java 25, MinIO, WAL crash and recovery workflows must
revalidate the new commit before GA PASS can be claimed. This batch
does not activate the KRaft authority producer, COMMITTED object
reference GC, or physical MinIO deletion.


## Batch 37: CAS-linearized upload admission versus local retirement quarantine

### Batch 36 evidence classification

Batch 36 canonical HEAD was normalized to
`d2b47cdc59e80ded514829df8ad1d6baaf721038`.
The Java 25 main Shared Storage test job and the real MinIO
three-broker KRaft E2E job succeeded, along with several independent
GA gates. The Ring WAL Correctness job failed before Java source
compilation while Gradle resolved buildscript artifacts: unrelated
grgit, Log4j, Kotlin and other dependencies were temporarily
unavailable from configured artifact repositories. No Ring WAL test
ran in that attempt. The single failed GitHub Actions job was
re-run without changing dependencies or starting a new complete CI
matrix. A failed or nonterminal Ring WAL gate still blocks GA.

### Previous last-check/PUT concurrency gap

The Batch 36 upload scheduler rechecked
`isDisabledForRetirementQuarantine()` after allocating an object ID
and before calling the object uploader, fixing the deterministic case
where that allocator synchronously triggers quarantine.
However, an independent callback thread could set quarantine after
the final volatile boolean read but before upload initialization.

This is a local admission race, **not** a cross-broker Controller
LogStart or COMMITTED reference-retirement problem. Closing it must
not hold Kafka log monitors, upload metadata locks or any remote
I/O lock while performing PUT/COMMIT.

### Atomic process-local upload admission

SharedCommitProgress now owns one `AtomicLong` admission state:

- High bit is the irreversible CLOSED latch for the current broker
  process, initially clear.
- Low 63 bits count already-admitted object uploads, initially zero.
- An upload `tryAcquireUploadAdmission()` uses CAS to increment that
  count, but always rejects after the high bit is set.
- `disableForRetirementQuarantine()` sets CLOSED atomically
  **before** marking progress disabled and clearing Kafka offset
  windows. CAS provides a single order between an admission and
  quarantine, including when both run on different threads.
- `releaseUploadAdmission()` CAS-decrements the count while
  preserving CLOSED, detects underflow, and cannot reopen the latch.

The scheduler performs this admission **after** object ID allocation
and **before** invoking `SharedObjectUploader.upload()`. A denied
admission releases selected WAL reservations and the upload slot
without performing PUT or recording a spurious candidate failure.
An accepted upload is now logically **in-flight at its CAS
linearization point**, and may complete the existing durable remote
PUT + metadata COMMIT protocol even if quarantine follows later.
Both synchronous launch exceptions and asynchronous success/failure
release the admission count exactly once.

This defines the precise local boundary. An accepted upload may
physically start PUT after the closed bit is set if preparation was
already under way when quarantine arrived; the authorization
preceded closure. The mechanism does **not** forcibly cancel
already-admitted work or provide a controller/cluster-wide
publication revocation. Reader quiescence and persistent GC
must not use this local admission bit as retirement authority.

### Focused deterministic and concurrent evidence

Four SharedCommitProgressTest regressions check quarantine ordering,
multiple in-flight admissions, double release/underflow, and a
32-iteration two-thread CAS-vs-quarantine race. Two
SharedUploadSchedulerTest regressions hold an actual asynchronous
ObjectStore PUT across quarantine and force a PUT failure that
itself triggers quarantine, verifying zero leaked admissions,
reservations or upload slots, and that new work stays blocked.

Mandatory Java 25 Storage/Metadata/Server Common JUnit evidence
increases from **230 to 236 named methods**; a missing or skipped
test remains a hard blocker. The main Java 25/real MinIO workflow,
Ring WAL failure recovery, GA normalization and all other required
source-SHA gates must succeed before this batch can be treated
as GA evidence.

### Remaining P0 work

This local upload admission CAS is not the missing KRaft
authoritative source LogStart transaction. COMMITTED object
retirement still requires authenticated BrokerEpoch/Incarnation
and source LeaderEpoch identity, durable Kafka and WAL source
horizons, verified metadata-consumer replay catch-up, an atomic
version-gated Controller record at its assigned Raft offset,
Topic Delete tombstone survival across failover/snapshot and a
reference-retirement grace barrier before retryable MinIO
physical deletion. Production authority emission and physical
COMMITTED GC remain off.

## Batch 38: fence selected uploads across Kafka leader ABA before local admission

### Source selection and admission had different leadership moments

The Batch 37 CAS gate atomically separated quarantine from new S3 upload
admission. It did not bind admission to the Kafka role under which a WAL
candidate was selected. A leader could select a batch, lose leadership,
then regain leadership before object ID allocation. With identical
logStart/HW and unchanged remote/WAL revisions, the old selection passed
revalidation and the new CAS accepted it. This is a local role-ABA race
and does not depend on KRaft source certificate or object-store failure.

### Non-blocking object admission with a role revision

SharedCommitProgress now combines role revision, existing in-flight count,
and the irreversible quarantine flag in one CAS state. Local leader,
follower and removal callbacks serialize only their own in-memory
mutation and publish an odd revision before changing cached role state,
then an even revision after completion. An overflow closes admission
permanently rather than wrapping to an old version. Neither callback
does remote I/O, waits for PUT or takes Kafka's native log monitor.

The scheduler captures the role revision when selecting a candidate
batch. Before invoking the object uploader, a CAS admission verifies
that the revision is unchanged, even, and not quarantined. This rejects
old selections across LEADER -> FOLLOWER -> LEADER and partition removal
even if Kafka offsets and remote/WAL revisions appear identical.
The old reservation and upload slot are released without a PUT or
a misleading candidate failure. A fresh scan is eligible immediately
after a valid new leader callback.

An upload admitted before role mutation remains in flight and follows
the prior durable PUT/COMMIT protocol. Local admission is NOT a KRaft
lease, a COMMITTED reference retirement certificate, a forced S3 PUT
cancellation, or a cross-broker garbage-collection authorization.

### Deterministic evidence and GA scope

Two SharedCommitProgressTest cases validate stale-version rejection,
in-flight permit accounting across ABA and 32 CAS-vs-demotion races.
One SharedUploadSchedulerTest triggers full follower/leader ABA inside
object-ID allocation, verifies zero stale PUT, leaked slot, reservation
or spurious failure, then proves fresh rescheduling can succeed.

The Java 25 mandatory storage/metadata/server-common JUnit evidence
increases from 236 to **239 named methods**, with strict no-skip
verification. These source changes invalidate the exact-candidate GA
fingerprint and require a fresh normalized MinIO GA Seal to requalify
the new HEAD. The prior 32ffa9a6... tree retains its historical
19/19 MinIO GA PASS; this batch makes no claim about native AWS S3.

The trusted BrokerEpoch/Incarnation and LeaderEpoch source certificate,
WAL/metadata replay horizon, Controller atomic append, durable reference
retirement, reader/upload quiescence and physical object GC remain
unimplemented and disabled.

## Batch 39: revalidate native source windows after upload-ID allocation

Batch 38 CAS upload admission rejects leader-role ABA, but admission
only authenticates local role revision and quarantine. Source LogStart,
high watermark, WAL truncation and remote coverage can change during a
pluggable object ID allocation without any role callback. Previously the
scheduler only evaluated those candidate invariants before allocation,
allowing stale ranges to enter a new PUT on unchanged leadership.

After winning the Batch 38 CAS admission, the scheduler now performs a
second in-memory candidate/window/revision/witness verification before
calling SharedObjectUploader. It rejects and releases a stale selection
without object PUT, remote publication, a spurious failure diagnostic,
leaked admission, WAL reservation or upload slot. Monotonically assigned
but unused object IDs are never recycled. A fresh candidate selection may
run on subsequent scheduler ticks.

Two deterministic SharedUploadSchedulerTest cases mutate the actual
cached native HW and LogStart during object ID allocation. They require
zero stale PUTs and show that restoring a valid HW permits a fresh
selection/upload. The mandatory Java 25 no-skip JUnit evidence rises
from **239 to 241 methods**. As before, MinIO integration and all 19 GA
workflows must pass on the new normalized source tree.

This is a targeted final-validation hardening, NOT a distributed or
fully atomic source-log lease. A Kafka source-window change after the
final validation can still overlap an already-admitted upload. Durable
source attestation, KRaft controller CAS, reader quiescence and physical
COMMITTED GC remain disabled. A future certificate must supply the
cross-broker durability/identity barrier; a local last-check cannot do so.

## Batch 40: fence scheduler stop during object-ID allocation

The Batch 39 post-admission source-window revalidation did not include
the scheduler lifecycle flag. A stop() that returned during a pluggable
object-ID allocation could still be followed by a new remote PUT despite
the shutdown having already begun.

A final local closed-state check, combined with the existing source
and role revision revalidation, now discards that not-yet-PUT batch.
The scheduler releases its newly acquired upload admission, WAL
reservations, and upload slot, without reporting a candidate failure.
The already-admitted and already-running asynchronous PUT drain contract
is unchanged; stop() does not cancel those operations.

A deterministic scheduler test closes the scheduler inside the object
ID supplier and verifies zero PUT, zero coverage, zero leaks, and no
further scheduling. The mandatory Java 25 JUnit evidence increases to
**242 named tests**. This remains local shutdown hardening, not an
atomic cluster-wide retirement or GC authorization; race windows
following the final local check require the future source authority
protocol rather than another speculative lock around remote I/O.

## Batch 41: join real native Kafka source evidence to read-only metadata replay preflight

The older PartitionRetirementEpochPrecheck assessment accepted an observed
leader epoch and observed LogStart as freely supplied numbers. This is useful
as a value-domain reference but cannot prove that the caller read the actual
Kafka source log for the intended immutable Topic ID.

A second, package-local assessNativeSource path now starts from the actual
SharedUnifiedLog and SharedPartitionRoleListener. It captures a
SourceLogStartObservation: matching immutable Kafka Topic ID and partition,
epoch-aware current local leadership and source LogStart/HW/LEO read under
Kafka's native log monitor. Legacy/unknown roles, identity mismatches,
inconsistent windows, or a demotion during native observation yield
NATIVE_SOURCE_WINDOW_UNKNOWN with no value finding.

When a native local observation exists, PartitionLogStartAdvancePrecheck
still demands replay to the required metadata horizon, never allows a
proposed LogStart above the actual source value, and preserves explicit
initial-zero/monotonic watermark semantics. After metadata lookup, the
preflight checks the exact local leader epoch + generation again; a
demotion/reassignment during that window discards the result. No Kafka
native log lock is held across metadata lookups.

Eight Java 25 regression methods test the real source coordinate binding,
legacy/unknown source, leader demotion in either stage, metadata lag,
native bounds, and fail-closed metadata replay. Mandatory anti-skip JUnit
evidence increases from 242 to **250 named methods**.

Every finding remains **advisory**, including LOCAL_EPOCH_OBSERVATION_MATCH.
The native read cannot authenticate BrokerEpoch or BrokerIncarnationId,
prove a durable WAL replay horizon, validate a read-committed metadata
consumer catch-up under the controller's timeline, or reserve a KRaft
append offset. The result is not a serializable certificate or a
controller-writer permit. The controller still returns
SOURCE_LOG_START_NOT_VERIFIED and no physical COMMITTED object GC
or post-retention S3 DELETE is enabled.

## Batch 42: retire stale advisory evidence after metadata-replay demotion

The legacy PartitionRetirementEpochPrecheck.assess path previously checked
its caller-supplied local leader ticket only before reading the
SharedMetadataImage. A demotion during that read could return
LOCAL_EPOCH_OBSERVATION_MATCH even after local leadership was lost.

After calculating the advisory value classification, it now rechecks
that exact ticket and discards the finding with LOCAL_TICKET_STALE if
the generation changed. This matches Batch 41's actual-native path
without changing the public method signature, inventing source
authenticity, or adding Kafka/MinIO I/O under callback locks.
A Mockito-triggered demotion during metadata lookup is covered by a
deterministic regression. The mandatory Java 25 safety count reaches
**251 tests**; absent/skipped methods remain release blockers.

Both paths remain read-only and non-authorizing. Production KRaft
controller emission, durable reference GC, and S3 DELETE stay disabled.

## Batch 43: fence source identity mutation during native offset capture

SourceLogStartObservation.capture() previously checked the immutable Topic
ID and partition only before reading the native source window. A source
log identity change between those operations could misassociate offsets
with an earlier source even though the local leader ticket remained valid.

The capture now rechecks both Topic ID and partition after reading actual
LogStart/HW/LEO, before returning any read-only observation. Two
deterministic tests alter the mocked log identity between checks and
require that no observation is returned. Mandatory Java 25 anti-skip
evidence increases from 251 to **253 named methods**.

Neither identity check is a cluster-wide linearizable lease. Controller
authority emission and COMMITTED physical deletion stay disabled.

## Batch 44: reject changing Kafka source LogStart across metadata replay checks

Batch 41 captured real Kafka LogStart/HW/LEO before querying replayed
shared-storage metadata, and Batch 42 rechecked local role generation.
Kafka can also change the actual source LogStart during that metadata
lookup without changing leadership: retention, DeleteRecords,
truncation and recovery are not all role transitions. A preflight that
only checks role identity could report a value finding computed against
a stale native source start.

After metadata lookup and the role-generation recheck, the read-only
preflight now captures native source identity and offsets a second time.
If identity/window disappears or the inclusive Kafka LogStart differs
in either direction, it discards the finding with
NATIVE_SOURCE_WINDOW_STALE. A different epoch/generation also discards
the result. HighWatermark/LEO may legitimately change without a
LogStart change; they are not frozen by this advisory preflight.

Four deterministic tests force source LogStart regression, advancement,
window disappearance and Topic ID replacement between the first source
read and metadata lookup. They verify that stale evidence contains no
value finding. Mandatory Java 25 anti-skip evidence grows from 253
to **257 named tests**.

This double observation is deliberately NOT a linearizable attestation
or an ABA-proof source mutation generation. A source can change again
after the check, and no broker-incarnation binding, durable WAL proof,
controller CAS or physical COMMITTED deletion authorization exists.

## Batch 45: make source stability checks precise without freezing normal HW progress

The Batch 44 double-observation fence intentionally compares the Kafka
source LogStart, not the entire native offset window. A producer can
advance HW/LEO while the read-only metadata classification executes;
those advances do not invalidate an otherwise stable retirement
LogStart value.

A deterministic Java 25 test requires the preflight to retain its
advisory finding across an HW/LEO-only advance while still rejecting
LogStart or Topic ID changes. The native-identity mutation test also
uses a guaranteed-distinct Topic ID rather than an extremely unlikely
random collision. Mandatory anti-skip evidence increases to
**258 named methods**.

No broker incarnation proof, durable WAL/metadata catch-up,
controller writer permission or COMMITTED S3 deletion is introduced.

## Batch 46: real broker registration identity in Storage Extension context

BrokerLifecycleManager now exposes its immutable process-incarnation UUID,
already used in controller RegisterBroker requests. BrokerServer passes
the controller-issued BrokerEpoch and same UUID to the broker-ready
StorageExtensionBrokerContext. Unknown broker epoch means both values
are unbound (-1, ZERO_UUID); the five-argument legacy constructor
remains supported. Malformed half-populated identity pairs are rejected.

This is only an immutable local registration snapshot. A future source
attestation must independently verify broker identity, epoch, current
KRaft partition leader, WAL durability and metadata consumer horizon.
It does not grant a reader, Controller write, lifecycle retirement or
MinIO physical-delete permission.

Broker lifecycle focused tests verify request identity consistency across
controller re-registration and a new incarnation after process restart.
Storage context tests cover legacy behavior, complete binding and
partial-identity rejection.

## Batch 47: require broker identity through every GA source contract

The main Java 25 job runs the two selected Core broker lifecycle Scala
tests alongside its existing LogManager regression in one core:test task,
plus all five StorageExtensionBrokerContext Java tests in one storage:test
task. The four-module JUnit evidence gate now checks Core as well as
Storage, Metadata and Server-Common; source preflight handles the
required Scala method declarations. Total mandatory no-skip methods:
**265** (previously 258).

The broker lifecycle Java file is now explicitly included in the
production fingerprint and all 19 required source-workflow
push/pull_request patterns, so later registration changes invalidate
the complete GA evidence set. The workflow consistency checker audits
server production paths. This checkpoint adds neither the KRaft
authority writer nor COMMITTED object physical GC.

## Batch 50: live read-committed metadata consumer replay horizon (non-authorizing)

The existing SharedMetadataImage tracks the offset of its last *applied
record*, not KafkaConsumer's next consumed position. On the compacted
`__shared_storage_metadata` topic, records may have offset gaps. Thus
the last applied record offset can lag a fully caught-up read-committed
consumer cursor, even though all visible data through the captured Last
Stable Offset (LSO) has been processed. The two numbers are not
interchangeable, and neither is durable Controller authority.

KafkaObjectMetadataStore now exposes a read-only asynchronous
`captureReadCommittedReplayHorizon()` request. It is served only by the
already-running, manually assigned metadata Consumer thread, which
uses the previously enforced `read_committed` isolation level. The
thread captures its current Kafka `endOffsets` LSO-exclusive boundary
and returns a `ReadCommittedReplayHorizon` only **after** applying
polled records and observing `consumer.position >= capturedLSO`.
Polls may cross compacted offset gaps or start with an empty topic.
There is no second Kafka Consumer, no extra producer, no periodic
endOffsets network calls without a request, and no metadata writer.

The request queue is bounded to at most 16 inspections per poll
iteration; calls are still asynchronous. Shutdown, unavailable metadata
image and terminal KafkaConsumer failure fail outstanding requests.
A late position callback cannot reopen a failed future. A returned
snapshot reports *historical catch-up to the sampled LSO*, not that
the metadata topic remains at that end offset or that another Broker
has committed its state. No logical or physical COMMITTED retirement
is enabled by this read-only API.

## Batch 51–53: deterministic replay-cursor and strict S3 evidence checks

Five KafkaObjectMetadataStoreTest methods verify an LSO-exclusive
boundary with compacted gaps, empty metadata topic, position beyond a
sampled horizon, rejected negative/missing inputs, and terminal
failure fencing. The existing full S3 module test task executes them;
its JUnit XML is checked immediately afterward by reusing the MinIO
test evidence checker with a separate `--metadata-horizon` scope.
Five required tests must appear exactly once, without skips/errors or
failures. This mode does **not** modify the real-MinIO eight-case
evidence contract or add another Gradle test invocation. The existing
265-mandatory Java/Scala controller/storage/Core gate is unchanged;
the separate S3-runtime strict gate adds 5 independent methods.

This is a necessary read-committed replay component, not a complete
Source Attestation. Before any Controller authority write, the
captured broker lifecycle identity must be revalidated against the
current KRaft registration and source leader epoch, WAL/replica
durability must be proven, and the actual KRaft writer must serialize
monotonic retirement with its authoritative append. Until those
proofs, read/upload quiescence and durable reference retirement are
established, physical COMMITTED MinIO DELETE remains forbidden.

## Batch 54: bound read-committed replay request lifetime

The optional replay-horizon diagnostic may be queried concurrently.
An unbounded pending-request queue would permit memory growth while
KafkaConsumer is blocked in endOffsets or network recovery. The
mailbox now grants at most 32 active permits, rejects excess requests
immediately, and removes completed, canceled, or failed requests
before returning capacity. Broker close and consumer failure fail
all outstanding futures, even if a network request cannot finish.
Only 16 queued candidates are examined per consumer poll, so client
work remains bounded.

Two additional deterministic S3 JUnit witnesses cover capacity
rejection, request cancellation, failure release and close draining.
The independent strict horizon evidence increases from 5 to
**7 required non-skipped methods** without changing the existing
265-method cross-module source-identity gate or real-MinIO E2E
evidence. This is still read-only infrastructure, not COMMITTED GC.

## Batch 55: isolate optional replay probe network errors from COMMIT replay

Sampling a read-committed LSO uses KafkaConsumer.endOffsets and position
on its single normal metadata consumer thread. That optional probe
must NOT reuse Kafka's long default API timeout or promote a temporary
endOffsets timeout into global metadata-image failure. The diagnostic
now uses explicit two-second Kafka client timeouts for both methods.
A WakeupException from real broker close continues to the existing
consumer shutdown path, but other probe-specific runtime errors
fail only that waiting horizon future. The next normal metadata poll
may continue applying COMMIT/cleanup records or independently fail
for genuine Kafka consumer errors.

An additional deterministic S3 test checks that a failed request
releases its mailbox slot and does not revoke an independent waiting
request. The strict S3 horizon anti-skip gate rises to **8 methods**.
This is read-only diagnostic isolation, not a change to metadata
authoritative writes, durability ACK or COMMITTED GC.

## Batch 56: narrow optional metadata probe failure isolation and close cancellation gap

An optional read-committed replay probe can fail with Kafka TimeoutException
while ordinary metadata polling is otherwise healthy. Only that specific
client timeout now fails the individual diagnostic future. WakeupException,
authentication/authorization failures and other unexpected Kafka runtime
errors still propagate to the normal image-failure path; an unauthenticated
or corrupted metadata consumer is never treated as merely a failed probe.

The bounded mailbox now enqueues before registering its completion callback,
and rejects duplicate in-flight request identities. Canceling a request
between the enqueue and callback steps thus releases the permit, instead
of leaving an orphaned canceled queue element. Two deterministic tests
verify already-completed requests, duplicate admission rejection, capacity
return after cancellation and absence of leaked queue members. The
independent S3 replay-horizon evidence list grows from 8 to **10** tests.

This is non-authorizing runtime maintenance. It adds no KRaft authority
producer, COMMITTED reference retirement or S3 physical lifecycle GC.

## Batch 57: monotonic expiration for stalled replay-horizon diagnostic futures

Bounding Kafka endOffsets()/position() calls does not guarantee
completion if the metadata consumer never advances to the sampled
read-committed LSO (or if other diagnostic requests fill the queue).
Each request now has a ten-second monotonic age bound, evaluated by
the same consumer thread both before and after a Kafka client lookup.
Expiration completes the future exceptionally and automatically
releases the bounded mailbox permit. This does not add a timer
thread or an external broker API call. An indefinitely blocked Kafka
client still requires its own normal consumer failure/shutdown handling;
the age check can only run when the consumer thread regains control.

Two deterministic tests verify incomplete replay after a deadline,
capacity release, later cursor movement not reopening evidence,
monotonic wrap-around and preservation of already-completed evidence.
The strict S3 horizon anti-skip count reaches **12 methods**. No
retirement authority writer or physical COMMITTED GC is enabled.

## Batch 58: mandatory evidence for timeout-only probe isolation

The optional read-committed replay probe must never launder a fatal
Kafka client exception into an innocuous failed diagnostic future.
Its catch path delegates to a small fail-closed classifier that
is independently tested: only Kafka TimeoutException completes the
single probe future, while unexpected IllegalStateException or Kafka
WakeupException retains its original object and propagates to the
existing metadata consumer failure or shutdown path.

Three deterministic JUnit tests lock that boundary. The S3-only
no-skip replay horizon evidence grows from 12 to **15 methods**; the
265-method Java/Scala identity gate and real MinIO runtime suites
continue unchanged. These additions issue no KRaft writer command,
no metadata watermark record and no physical COMMITTED S3 DELETE.

## Batch 59: compacted metadata read-committed cursor preflight (read-only)

The legacy PartitionLogStartAdvancePrecheck checks lastConsumedMetadataOffset
against a requested record offset. That offset is not the Kafka consumer's
next-position: a compacted __shared_storage_metadata log may have record
offset gaps, so the final applied record can be behind the previously
captured Last Stable Offset even when the actual read-committed consumer
has already replayed through it.

A new assessReadCommitted overload keeps the same source-log bounds,
per-topic immutable ID separation, explicit initial zero and monotonic
watermark logic, but checks captured LSO-exclusive against the actual
post-apply read-committed Consumer position. The old precheck remains
untouched for compatibility and retains its conservative behavior.
Neither pathway can write PARTITION_LOG_START or authorize deletion.

Six deterministic Java 25 tests prove gap-safe classification, an
empty metadata topic, lagging consumer rejection, later progress,
topic-recreation separation and strict invalid/unready rejection.
The mandatory anti-skip Storage/Metadata/Core/Server-Common test
matrix grows from 265 to **271 named methods**.

## Batch 60: bind live read-committed replay evidence to the metadata value preflight

KafkaObjectMetadataStore now exposes the read-only
previewReadCommittedPartitionLogStart(partition, requestedLogStart,
observedKafkaLogStart) workflow. It first obtains a real LSO/consumer
cursor witness from its own running metadata consumer and then reads
the partition-scoped SharedMetadataImage to classify only a possible
log-start value. The response retains the immutable Topic ID and
partition, captured replay cursor, and value finding as diagnostics.
Canceling the dependent result cancels its queued LSO request and
releases bounded mailbox capacity.

This combines the real compacted-log cursor semantics of Batch 59
with the already-running S3 metadata consumer; it is not a fabricated
caller-supplied last-applied offset. Source LogStart remains
caller-supplied at this layer and may go stale; it must be observed
and rechecked against the Kafka UnifiedLog and current broker
registration before an eventual Controller writer can decide.

Three S3 tests enforce genuine metadata gaps, topic incarnation
separation, and fail-closed image/source bounds. The strict S3
horizon anti-skip requirement grows from 15 to **18 tests**.
No normal COMMIT or orphan-cleanup flow is changed. No KRaft
PartitionRetirementAuthorityRecord or physical COMMITTED DELETE
is emitted.

## Batch 61: native Kafka source and compacted metadata replay in one advisory preflight

PartitionRetirementEpochPrecheck now provides
assessNativeSourceReadCommitted(), which combines two actual
SourceLogStartObservation reads (immutable Topic ID and partition,
epoch-aware local leader generation and Kafka native LogStart/HW/LEO)
with the gap-safe LSO/consumer-position value-domain classifier.
Both old last-record-offset and new read-committed entrypoints reuse
the exact same before/after Kafka role/source revalidation to prevent
drift between compatibility paths.

Four Java 25 tests cover a compacted metadata offset gap, a genuinely
behind consumer position, demotion during metadata lookup and Kafka
LogStart change during the replay check. Mandatory anti-skip evidence
increases from 271 to **275 named Java/Scala tests**.

This operation is still read-only and relies on caller-supplied
LSO/position values. It must be invoked with the real metadata store
consumer's future evidence and the actual SharedUnifiedLog, plus a
fresh controller broker-epoch/incarnation check, before an eventual
KRaft writer can authorize durable retirement. Physical GC remains
completely disabled.

## Batch 62: prevent dangling JavaDoc from obscuring all GA correctness results

Batch 59-61 compilation was blocked in KafkaObjectMetadataStore
because two JavaDoc comments were adjacent before one record declaration.
Java 25's dangling-doc-comments warning was promoted to a compilation
failure by -Werror. This was not a failed Kafka/S3 correctness assertion,
but did prevent every dependent runtime suite from executing.

Attach the preview comment only to the preview record and move the
bounded-mailbox comment immediately before its actual class. Extend
the already-executed pre-Gradle source checker to reject consecutive
JavaDoc blocks in the Shared Storage Java production trees. Two small
unit tests verify negative and positive cases. Do not disable -Werror,
change dependencies, relax CI, or grant any retirement/GC authority.

## Batch 63: fail closed across asynchronous read-committed replay preview completion

The S3 metadata preview now delegates the LSO future and image
classification to a package-visible, deterministic asynchronous
composition helper. This does not expose a new producer or authorize
a retirement: it allows the cancellation and failure ordering to be
verified before the MetadataConsumer network call completes.

Four S3 regression witnesses require (1) downstream cancellation to
cancel and release the upstream replay-horizon request, (2) a failed
LSO query to propagate its original failure, (3) an image poisoned
after the LSO request but before completion to reject the finding,
and (4) a successful empty-image preview to preserve the immutable
topic partition and avoid metadata mutation. The strict S3 replay
horizon evidence list increases from 18 to **22** methods.

No Broker incarnation, KRaft Controller writer, WAL durable
certificate or physical COMMITTED GC permission is claimed here.

## Batch 64: reject invalid BrokerId in both storage broker-context constructors

StorageExtensionBrokerContext previously enforced a paired BrokerEpoch
and IncarnationId, but allowed a negative BrokerId in a supposedly
registered snapshot. A Kafka BrokerId is non-negative. Rejecting the
invalid local identity at construction prevents downstream observers
from treating an impossible BrokerId as a populated registration.

Both the original five-parameter constructor and the seven-parameter
registration snapshot now share the same validation. Two Java 25
tests require negative IDs to fail closed in both paths while
preserving legitimate BrokerId zero and explicitly unregistered
legacy contexts. Mandatory source-identity JUnit evidence increases
from 275 to **277 named methods**. This does not authenticate a
controller registration or grant deletion privileges.

## Batch 65: bind native Kafka source re-observation to local WAL index mutation generation

SharedStorageEngine already exposes walMutationRevision(partition).
This generation advances when surviving indexed WAL ranges are
destructively replaced, truncated or reclaimed. Add an optional
read-only assessment variant that samples this revision before the
real Kafka leader/source LogStart and replay metadata checks and
revalidates it afterward. A changed or unavailable revision removes
any advisory value finding. Normal non-destructive appends do not
advance that revision and therefore need not starve a preview.

Four deterministic Java 25 tests prove stable revision preservation,
destructive mutation rejection, unavailable-revision fail-closed
behavior and independence from the existing source-LogStart drift
fence. The required Java/Scala anti-skip matrix rises from 277 to
**281 methods**.

This is a *local mutation observation only*. The WAL append future
crossing its durability barrier is a separate condition; this revision
does not prove fsync, replicated ACK, Controller leadership, broker
registration validity, committed reference retirement or any
permission to delete COMMITTED S3 objects.


## Batch 66: revalidate broker registration record identity on the controller preflight

The Controller preflight already looks up the current broker ID, epoch,
incarnation, fenced state and controlled-shutdown state. It now also checks
that the returned BrokerRegistration record's own ID equals the candidate
BrokerId. This fail-closed guard prevents a malformed or mismatched value
from being treated as current registration evidence, even when epoch and
incarnation happen to match. A deterministic controller test holds the
registration lookup result at the wrong ID and asserts no authority write.
The strict source-identity gate increases from 281 to **282 methods**.

## Batch 67: validate WAL-fenced read-committed inputs before probing local revision

The advisory WAL-mutation-fenced source preflight now checks its metadata
image and read-committed offsets before consulting walMutationRevision().
Otherwise an unavailable WAL revision could mask a negative LogStart, LSO or
consumer cursor, or a null image, as an ordinary unavailable-fence finding.
Two deterministic tests ensure malformed input never reaches the storage
probe. The strict identity gate rises from 282 to **284 methods**.

## Batch 68: reject malformed WAL group durability results before index publication

SharedStorageEngine's successful append group already applies the WAL index
only after its SharedWal appendBatch future succeeds. It now copies and
validates the complete result list before applying its first index entry.
An incomplete group or a null result therefore cannot partially publish a
local Kafka offset range even if a faulty backend signals success.
Four deterministic tests cover pending, exceptionally completed, incomplete
and null-result WAL futures; strict source-identity evidence reaches
**288 methods**. This validates the local future-to-index ordering only.
It is NOT a new fsync certificate, a cross-broker durability attestation, a
Controller-authorized CAS write or permission to physically delete any
COMMITTED remote object. Those remain disabled.


## Batch 69: restore the missing-incarnation Controller regression after BrokerId validation

The Batch 66 Controller regression exposed an accidental Mockito test fixture:
the existing missing-incarnation case used an unstubbed integer BrokerId,
which defaults to zero. It now explicitly supplies the registered BrokerId
one, preserving the BROKER_INCARNATION_MISMATCH negative assertion rather
than weakening the production BROKER_ID_MISMATCH validation.

## Batch 70: two-phase per-partition WAL index mutation fence

The old PartitionWalIndex revision advanced after data replacement or
reclamation and before truncate/clear completed. A source preflight could
therefore read equal revisions while a destructive mutation was still in
progress. Per-partition MutationGate now exposes an odd sequence as -1,
a new stable generation only after completion, and finalizes the sequence
even when a mutation operation fails. Tail append observations remain stable.
A concurrent probe is read-only and never acquires the mutation writer lock.

## Batch 71: apply the mutation fence to reclaim, clear, and empty truncate

Selective index reclaim and whole-index replay reconstruction now use the
same per-partition mutation boundaries as replacement and truncate. Reclaim
does not bump a partition with no removed WAL locations. An explicit
TRUNCATE remains a generation fence even if the partition has no local
index. Five deterministic tests cover a blocked writer, failed writer,
selective reclaim, recovery clear and empty truncate. These remain
broker-local observations; this is not a WAL fsync, source durability
certificate, Controller claim or physical garbage collection permit.

## Batch 72: fail an inconsistent durable WAL group before publishing any offsets

SharedStorageEngine already prevalidates group result cardinality and nulls
before index publication. The group must also return strictly increasing
logical WAL addresses. Duplicate and reversed WAL locations now fail
the complete group future without publishing any indexed Kafka batch.
Two deterministic asynchronous regression tests cover both cases. The
Java/Scala mandatory non-skipped regression inventory now contains
**295 named methods**. The KRaft authority writer and physical COMMITTED
object GC remain disabled.


## Batch 73: prevalidate the full durable WAL group before index publication

The returned WAL append-group future only reports success after the backend's
local durability barrier, but its result list remains an untrusted contract
boundary. Prior to this batch, a malformed later result length could leave a
durable group partially visible in PartitionWalIndex before WalLocation
validated its payload length. Construct and validate every returned
WalLocation first, then apply the index. Two deterministic tests cover
first- and later-record length mismatch without index publication.

## Batch 74: reject overlapping and overflowing logical WAL result spans

Strictly increasing WAL *start* addresses alone can accept two records that
overlap in their encoded byte spans. A group now checks the prior exclusive
end address and uses checked long addition for the complete record span
before publishing any indexed batch. Physical allocation gaps remain
valid, including ring and rotating-file boundaries. Three deterministic
tests cover overlapping addresses, address overflow and permitted gaps.

## Batch 75: validate single-record WAL durability results before publication

Copied and owned DATA append paths previously updated the WAL index ahead
of constructing their validated returned WalLocation. Both paths now use
the same validated-location helper as group appends, including overflow
rejection, before applying any index change. Three deterministic tests
prove incorrect encoded length or address overflow cannot expose
an indexed Kafka batch.

## Batch 76: reject an invalid durable TRUNCATE result before altering its index

A completed TRUNCATE future previously removed indexed batches without
validating that its returned WAL address and header-only encoded length
represented the requested control record. The preflight now validates the
zero-payload TRUNCATE record before invoking the index mutation. A
deterministic test retains a prior DATA range and its mutation revision
when the TRUNCATE backend reports an invalid encoded length.

The mandatory Java/Scala source-identity and durability test registry
increases from 295 to **304 named methods**. All these changes enforce local
WAL result-to-index publication invariants only. They do not authenticate
a cross-broker WAL durability certificate, KRaft Controller writer CAS,
reader/upload quiescence, or physical COMMITTED S3 garbage collection;
all production COMMITTED retirement writes and deletes remain disabled.


## Batch 77: preserve WAL logical-address overflow defense and satisfy SpotBugs

The Batch 73-76 source-SHA GA Seal was blocked by one precise
`RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT` warning in
`SharedStorageEngine.validatedLocation`. Ignoring the return of
`Math.addExact` was not accepted by SpotBugs. Use an explicit checked
logical-address upper-bound condition, retaining the previous fail-closed
behavior for invalid WAL offsets without suppressing static analysis.
Previously added single and grouped WAL overflow tests remain mandatory.

## Batch 78: fail closed on invalid WAL replay payloads or internal commit markers

The WAL backends consume GROUP_COMMIT markers internally and replay
only DATA and TRUNCATE records to the partition index. Index replay
now rejects an unexpected GROUP_COMMIT and checks DATA append-result
length against its decoded payload before touching any index entry.
Two replay-length tests and one internal-marker test protect the
recovery path independently of SharedStorageEngine's asynchronous
append-result validation. Old synthetic test fixtures now use the
true one-byte DATA record encoding length, 60 + 1 bytes.

## Batch 79: canonicalize overlapping WAL index ranges before publication

A replacement DATA range can intersect a prior RecordBatch and
multiple following batches. Merely inserting a new first-offset key
left overlapped, stale successors accessible through floor lookups.
The destructive mutation gate now removes any overlapped predecessor
and every overlapping successor *before* publishing the replacement.
RecordBatch ranges are indivisible; partially overlapped old batches
are removed in full. A non-overlapping later tail remains readable.
Tests cover predecessor plus successor overlap, multiple successors,
and a disjoint gap insertion. The mutation revision changes for any
destructive or out-of-order insertion but not ordinary tail appends.

## Batch 80: reject overlapping or reversed Kafka record batches before WAL admission

One Kafka MemoryRecords append maps to a single atomic WAL batch
group. Batches within that group must be ordered and must not
overlap by Kafka offsets; accepting duplicate, intersecting or
reversed offset windows would create misleading index state.
`appendOwnedBatchGroup` now rejects malformed groups before
`SharedWal.appendBatch` is invoked. A gap between batches remains
legal, as does a gap in the physical WAL address space. Four
deterministic tests check invalid requests cause zero WAL group
admissions and a valid gapped request still succeeds.

This iteration adds **10 mandatory Java/Scala tests** (304 -> 314)
without changing Kafka metadata record encodings or remote object
semantics. Local mutation generations, local WAL append futures and
Controller read-only preflights still do NOT form a signed durability
certificate or an atomic KRaft CAS; all production COMMITTED-object
physical retirement remains disabled.


## Batch 81: centralize logical WAL location address-span validation

Single DATA, grouped DATA and WAL cold replay can all construct WalLocation.
The overflow check now lives in the canonical WalLocation constructor rather
than being duplicated in SharedStorageEngine. A location whose encoded
length extends beyond Long.MAX_VALUE fails closed; an exact end at the
limit remains representable. Existing asynchronous append overflow tests
continue to cover the same safety boundary.

## Batch 82: validate replayed TRUNCATE before any index mutation

Unlike the live append future, cold replay flows directly through
PartitionWalIndex.apply. A malformed header-only TRUNCATE append result
previously bypassed payload-length and address-span validation and could
erase live indexed DATA. DATA and TRUNCATE now both construct a validated
WalLocation before mutating a partition index or its revision. Replay tests
cover incorrect TRUNCATE length, overflowing TRUNCATE and DATA addresses,
and unchanged indexed evidence on failure.

## Batch 83: reject malformed single-record WAL append-group results

SharedWal.append delegates to appendBatch for a one-record group.
The default adapter previously selected the first result without checking
cardinality, allowing a faulty backend to report a successful empty or
multi-record group. It now requires exactly one non-null WalAppendResult.
Deterministic tests cover a null list, empty list, extra result, null
member, and one valid append. No new assumptions about fsync or group
durability are introduced.

## Batch 84: preserve lock-order safety and diagnose performance evidence

File and Ring WAL writer callbacks can complete append futures while their
backend I/O locks are held. SharedStorageEngine WAL reclaim enters backend
I/O while already holding walMaintenanceLock. Avoid acquiring that
maintenance lock from append completion callbacks, which would introduce
a lock-order deadlock. Document this invariant without broadening the
locking surface or changing the runtime fast path.

The previous Batch 77-80 source-SHA run passed 17/20 workflows but failed
the WAL crash gate on Maven Central HTTP 429 before actual test execution;
a separate performance sample reported a 0.5573 median produce ratio
against the unchanged 0.60 threshold. Prior passing comparable runs had
median ratios 0.8523 and 0.8140. The failed run's slow samples coincided
with high WAL durability force latency. Only the two failed jobs were rerun. The WAL crash-window job passed on
attempt 2, and the performance job passed on attempt 2 with median produce
ratio 0.7573 and median consume ratio 0.9991. The 0.60 release guard was
retained without re-running the whole matrix. These two outcomes support
a transient dependency/host-I/O cause, but do not establish a root-cause
proof or make the earlier failed GA Seal equivalent to PASS. A new
normalized Seal still has to succeed on the exact evidence tree.

Batch 81-84 adds **10 named mandatory tests** (314 to 324); the KRaft
Controller authority writer and physical COMMITTED object GC remain
disabled. WAL index generations and local append success do not constitute
cross-broker durability or deletion authorization.


## Batch 85: close WAL when engine initialization fails during replay

SharedStorageEngine acquires its WAL dependency before replaying surviving
DATA and TRUNCATE records. Previously an exception from replay or local
checkpoint restore could abort construction without leaving an
AutoCloseable owner, leaking WAL handles and writer threads. Construction
now closes its WAL if initialization fails, rethrowing the original
IOException or RuntimeException. A deterministic test validates cleanup.

## Batch 86: preserve replay failure when WAL close itself fails

If WAL shutdown fails while unwinding initialization, the replay error
remains primary, and the close failure is suppressed for diagnosis.
Deterministic tests cover separate failures, the same Throwable reused
by both replay and close (which must never self-suppress), and successful
ownership transfer through engine.close().
This is local lifecycle safety, not a new durable persistence protocol.

## Batch 87: reject stale overlapping WAL address replacements

File, Ring and Rotating WAL backends allocate increasing logical WAL
addresses. A replayed DATA record whose Kafka offset interval overlaps
newer local DATA must not evict that latest indexed range if its physical
logical WAL address is older or equal. PartitionWalIndex checks every
displaced predecessor/successor WAL address before modifying the index.
Ordinary tail appends retain their fast path. Three deterministic tests
exercise stale predecessors, duplicated addresses and newer successors,
preserving the earlier index and mutation revision on rejection.

## Batch 88: tighten the non-emitting Controller CAS input boundary

The controller-side Candidate constructor now rejects the reserved
ZERO_UUID Topic ID and a proposed KRaft append position not strictly
after its expected previous authority version. These reject malformed
claims before interacting with KRaft timelines and registrations.
Three deterministic regression tests cover duplicate/reversed offsets
and reserved topic identity. The Controller still cannot emit authority
records or grant COMMITTED object deletion: authenticated Kafka source
LogStart evidence, a Controller event-queue atomic writer and external
reader/upload quiescence fences remain unimplemented.

The mandatory Java/Scala test manifest increases from **324 to 334
methods**. Batch 81-84 remains the latest fully validated GA baseline
until this new source tree completes all release workflows and GA Seal.


## Batch 89: preflight the full durable WAL group before indexing any member

SharedStorageEngine previously validated every returned WAL append result
for shape and address ordering, then called PartitionWalIndex.apply for each
DATA record. The later call could reject a stale overlapping record after
earlier group members were already published to the index. The new
PartitionWalIndex.applyDataGroup validates every member against one
partition lock, including the existing overlapping DATA addresses, before
writing a single index entry. A stale or duplicate later member now
rejects the entire index publication. Two deterministic regression cases
verify that no valid prefix becomes indexed on these failures.

## Batch 90: one mutation-generation boundary for a replacing DATA group

When at least one member is an index replacement or out-of-order
insertion, the entire validated group is applied under one seqlock-style
destructive mutation generation. Kafka read-committed source probes
observing an in-progress destructive group see an unavailable revision.
A successful replacement group advances the broker-local mutation
generation once, not once per overlapping member. This does not
make the entire concurrent read API transactional; the WAL record group
was already made durable by the backend before the index update.

## Batch 91: reject malformed direct DATA group input before mutation

Direct index group callers must supply one partition's DATA records,
strictly increasing Kafka batch offsets, and non-overlapping increasing
logical WAL byte ranges. The index rejects a group crossing partitions,
containing TRUNCATE, reversing Kafka offsets, or returning an incorrect
encoded length before any previously indexed DATA is removed.
Four deterministic tests cover these malformed input classes.

## Batch 92: freeze submitted WAL groups and preserve tail fast path

SharedStorageEngine now passes an immutable record-list snapshot to
the asynchronous SharedWal.appendBatch backend so it cannot alter group
membership while the durable future is pending. The index fast path
avoids per-record tree-search preflights for normal Kafka tail appends,
which do not invalidate the mutation generation. Two deterministic
tests verify immutable admission and unchanged tail revision.
Release performance gating retains the existing thresholds.

The mandatory Java/Scala named test inventory is increased from
**334 to 343**. Batch 85-88 is the last complete normalized GA PASS
until the new CI matrix and seal finish. These changes validate
broker-local WAL index publication only. They are neither cross-broker
WAL durability certificates nor Controller-authorized KRaft CAS records.
COMMITTED remote physical deletion remains disabled.


## Batch 93: satisfy WAL group Checkstyle complexity without weakening gates

The Batch 89-92 Java 25 job failed at `storage:checkstyleMain` because
PartitionWalIndex.applyDataGroup had NPath Complexity 990 against a hard
limit of 500. All other 18 source-SHA workflows completed successfully,
including real MinIO/KRaft, Ring WAL and the performance baseline, but
the normalized GA Seal remained BLOCKED. Split group cardinality, candidate
validation, current-index overlap checks and publication into focused
private methods. Preserve one per-partition gate, all-or-nothing
validation before publication, and the existing NPath limit. Do not
suppress the warning.

## Batch 94: fail closed on malformed group result cardinality and nulls

Direct index-group callers must provide at least one DATA record and
exactly one non-null result for each member. An empty list, mismatched
sizes or a null later result cannot publish an earlier Kafka prefix.
Two deterministic tests assert that no partition index appears on
those errors.

## Batch 95: reject overlapping logical WAL byte spans before publication

When Kafka ranges are disjoint but backend WAL byte spans overlap, the
full group must fail before writing any index entry. The validation
loop keeps the prior exclusive WAL end, including encoded header bytes.
One deterministic direct-index test prevents publishing a valid prefix
when a later WAL span overlaps.

## Batch 96: preserve gap insertion and multiple-replacement semantics

An out-of-order Kafka offset insertion into an index gap is an index
mutation fence even without overlap; replacing several existing
RecordBatches in a single durable group increments the broker-local
mutation revision exactly once. Two tests cover non-overlapping gaps,
multiple replacements, retained tail ranges and one generation advance.
The ordinary tail-only path still avoids tree searches.

The strict mandatory Java/Scala named test inventory grows from
**343 to 348 methods**. This iteration does not alter the shared WAL
fsync contract, KRaft metadata encodings, controller writer or remote
retirement protocol. The most recent fully validated GA baseline is
Batch 85-88 until a new source-SHA run passes the complete normalized
Seal. COMMITTED remote-object physical deletion remains disabled.

## Batches 97–100: separate MinIO GA freeze from non-emitting KRaft identity hardening

**Release split.** The canonical 2026-10-13 MinIO GA evidence checkpoint is
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`, validated by the
normalized 20/20-workflow SUCCESS evidence
[run 38354435176](https://github.com/funky-eyes/kafka/actions/runs/38354435176).
The immutable release-candidate branch is
`release/shared-storage-minio-ga-20261013`. Actual release dispatch,
artifact QA, tags and canary remain release-operator actions, not implied
by source-SHA evidence success.

**Implementation scope.** The follow-up branch
`feat/committed-lifecycle-authority` does not alter the sealed release
candidate. `PartitionRetirementAuthorityState.Key` now rejects
`Uuid.ZERO_UUID` as a reserved immutable Topic ID, aligning its persisted
KRaft decoder with the earlier non-emitting Controller Candidate guard.
Direct construction, generated-record decode, Controller timeline replay,
KRaft log/snapshot replay and negotiated MetadataImage replay all have
negative regression tests. Rejection must preserve existing authority
state; malformed input must never create an alternative Topic incarnation.

**Test ownership.** Six new named Java/JUnit methods increase the strict
Java/Scala source-identity inventory from 348 to 354. All are non-emitting:
the KRaft record writer, certified broker source LogStart, durable
COMMITTED reference retirement, reader/upload quiescence and physical
object DELETE remain OFF. The next P0 code boundary is a fresh,
controller-verifiable native-source and WAL horizon witness, not another
caller-supplied advisory preflight receipt.

See [MinIO GA freeze](shared-storage-minio-ga-freeze-2026-10-13.md)
and [next implementation boundary](committed-lifecycle-gc-next-boundary.md).

## Batches 101–104: native-source and metadata-replay horizon double-checks

This development-only checkpoint addresses two gaps in the advisory P0
broker-side observation. After two native `SharedUnifiedLog` observations,
matching Topic ID, partition, local role generation and LogStart alone are
insufficient: an in-place truncate can keep LogStart constant while reducing
High Watermark or LogEndOffset. The second sample must not regress either
HW or LEO from the first. Ordinary monotonic HW/LEO advance remains legal.

The metadata classifier reads `SharedMetadataImage`, which can replay new
records between the first native source sample and the final source sample.
The precheck now snapshots the target partition's explicit LogStart and
the image's last consumed metadata offset before classification, then
compares both after source revalidation. Even replay of an unrelated
metadata record invalidates the old observation, rather than returning
an advisory finding derived from an obsolete image generation.

Five regression cases cover HW regression, LEO regression, consumed metadata
position advance, changed LogStart at unchanged local replay position and
the fact that a stable local WAL mutation revision cannot override a
concurrently changed metadata image. The named Java/Scala manifest grows
from 354 to **359**.

**Safety boundary:** All results remain advisory, including a stable
pair of native source and metadata snapshots. They are not durable WAL
certificates, authenticated BrokerEpoch/Incarnation witnesses, a current
Controller KRaft CAS, or a remote-object deletion permit. No Controller
record writer and no COMMITTED physical GC are enabled.

## Batch 105: make pending durable WAL writes visible to source preflight

The broker-local `walMutationRevision` deliberately ignores ordinary
tail appends, so a stable destructive generation alone cannot show the
absence of uncompleted WAL Futures. `PartitionWalIndex` now maintains a
second, independently observed WAL admission generation. Each admission
and completion atomically changes a *single* packed 64-bit state containing
the generation and in-flight count. A sample returns -1 while any
write is outstanding. This avoids a stale "pending = 0" read caused by
two independent atomic counters. Capacity exhaustion fails closed.

## Batch 106: cover copied/owned DATA, groups and TRUNCATE through publication

`SharedStorageEngine` now tracks the full Future including its index
publication continuation. Copied DATA, owned DATA, durable group append and
TRUNCATE all enter the admission fence **before** calling `SharedWal`.
Success, backend failure, invalid durable result and synchronous backend
exceptions must release exactly one admission. Backend callbacks only
perform atomic state updates and never acquire WAL maintenance or Kafka
native log locks.

## Batch 107: compare admission generations around native source classification

The native source/read-committed metadata precheck now checks both
`walMutationRevision` and `walAdmissionRevision` around its read-only
classification. If an append was pending before capture it returns
`LOCAL_WAL_ADMISSION_IN_FLIGHT`; if a new admission or completion occurs
across capture it returns `LOCAL_WAL_ADMISSION_CHANGED`, without any
value finding. Destructive WAL mutation checks retain priority.

## Batch 108: deterministic ordering, failure and independence regressions

Thirteen named Java tests cover WAL Future admission/completion of copied
and owned data, group and TRUNCATE, synchronous and asynchronous failure,
independent partition fences, multiple concurrent pending futures, null
backend results, and the read-only precheck's before/after generation
behavior. The strict test-name manifest grows from **359 to 372**.

**Scope:** This is a local, short-lived admission activity observation,
not a durable-WAL replay horizon, fsync attestation, broker-incarnation
authentication, signed cross-broker certificate, KRaft authorization,
or permission to delete a COMMITTED object. Actual full-GC milestones
remain P0 trusted source provenance, P1 Controller atomic CAS, P2 durable
reference retirement and P3 safe physical MinIO deletion.

## Batch 109: capture backend-originated startup WAL replay inventory

A successful `SharedStorageEngine` construction now records immutable
`StartupWalReplayEvidence` from the very same `SharedWal.replay` callback
that reconstructs the local partition index. It reports counts of DATA
and TRUNCATE records by Topic ID/partition and the highest observed logical
WAL record end. It reports what actually survived startup recovery, not
what a producer or preflight caller claims to have persisted. The
snapshot is deliberately frozen after construction; it is not advanced
by live append acknowledgments.

## Batch 110: keep recovery provenance private and fail on broken ordering

The new inventory has no public constructor or public mutator; an internal
Builder consumes only actual replayed user records. The Builder rejects
GROUP_COMMIT markers presented as user data and rejects overlapping or
regressed logical WAL addresses, including cross-partition interleaving.
An invalid backend replay aborts engine construction and closes the WAL.
The returned partition map is an immutable defensive copy.

## Batch 111: File and Ring durable recovery evidence tests

Two actual-backend tests close and reopen File WAL and Ring WAL with a
durable two-partition DATA group followed by a TRUNCATE. The recovered
inventory contains only user records made visible by the backend's
group-commit recovery or ring superblock checkpoint, including TRUNCATE.
The local index independently verifies that the truncated Kafka range
is not readable. Neither record counts nor the maximum logical WAL
address implies contiguous live Kafka coverage.

## Batch 112: crash-incomplete group and post-start mutation boundaries

A File WAL test removes the GROUP_COMMIT segment after a successful write
and validates that recovery produces EMPTY inventory and no indexed DATA.
Other tests cover empty WAL, malicious out-of-order replay, immutable
summary copies and the fact that only a subsequent process restart may
include live appends in the startup inventory. Seven new mandatory
Java/JUnit tests grow the named strict source inventory from **372 to 379**.

**Authorization boundary:** This inventory is incomplete when physical
WAL prefixes were reclaimed, is not bound to current BrokerEpoch /
BrokerIncarnationId, and is neither a signed record nor a durable
WAL-to-Source-LogStart continuity certificate. It does not establish
cross-broker replication, reader/upload quiescence, controller commit or
safe COMMITTED deletion. KRaft authority writers and physical GC remain
disabled. P0 still requires an authenticated broker-originated durability
and replay horizon revalidated at the Controller's serialized CAS event.

## Batch 113: restore the Java 25 -Werror startup evidence integration gate

The first Batch 109-112 source-SHA run failed in `storage:compileJava`
with the Java 25 `dangling-doc-comments` warning (treated as an error)
because two method JavaDoc blocks became adjacent in
`SharedStorageEngine`. The shared Java preflight caught the same
incorrect doc attachment. Every other workflow stopped at the common
preflight or compile barrier. Separate and attach JavaDoc to the
correct startup replay getter and WAL admission revision method. Do
not relax `-Werror`, Checkstyle, or the release/PR test contracts.

## Batch 114: join real Kafka broker-ready identity to startup WAL replay

`SharedStorageExtension` receives Kafka's actual
`StorageExtensionBrokerContext` after the broker starts its listeners.
Only when the callback contains a complete BrokerEpoch and
BrokerIncarnationId and matches the extension's startup BrokerId may
it pair that local registration snapshot with the immutable,
backend-originated `StartupWalReplayEvidence`. This new
`BrokerStartupReplayObservation` has no public constructor or
authority writer. An unbound legacy context leaves it absent, without
breaking Kafka request processing.

## Batch 115: revoke replay pairing across incarnation/epoch/cluster ABA

The extension permits only one initial broker-ready pairing per WAL
startup lifecycle. A repeated identical callback is idempotent; a
BrokerId, BrokerEpoch, IncarnationId or ClusterId mismatch invalidates
the pairing. The old pairing never reappears on a later ABA callback
within the same extension lifetime. An unbound first callback cannot
be upgraded without a complete new extension startup and WAL replay.
Closing the extension invalidates the local pairing.

## Batch 116: real Ring WAL reopen + broker identity regressions

A real broker-extension runtime test writes to the Ring WAL, closes
the original extension, reopens the same WAL from a new extension and
pairs only the newly replayed records with a new BrokerEpoch and
IncarnationId on broker-ready. Further tests cover no ready callback,
unbound identity, wrong broker ID, changed ClusterId/IncarnationId,
repeated callbacks, revocation at close and callback-before-start.
The previously existing two WAL runtime tests are also registered as
strict named mandatory evidence. The mandatory source-identity list
grows from **379 to 391 methods** across 25 suites.

**Still NOT an authority certificate:** The broker-ready values are
Kafka lifecycle snapshots, not rechecked current KRaft registration or
authenticated, durable source-log provenance. There is no guarantee
the recovered WAL byte inventory covers an entire Kafka LogStart
window or the other brokers. No Controller KRaft writer, durable
COMMITTED-reference retirement or physical object DELETE is enabled.

## Batch 117: compare recovered DATA by complete Kafka RecordBatch intervals

A per-partition startup inventory of WAL record *counts* cannot establish
that any actual Kafka Source LogStart..HW window is covered. The new
read-only `StartupWalReplayCoveragePrecheck` compares the broker's
currently indexed `WalLocation` ranges against the immutable per-partition
startup replay high-water byte position. Its positive result requires
exactly aligned Kafka batch boundaries and no offset gap/overlap across
the source window. It does not require physically contiguous WAL addresses
when another partition was interleaved into the shared WAL.

## Batch 118: prevent post-start and incomplete records from filling recovery gaps

Missing startup replay, reclaimed/trimmed data, partial first batches,
HW splitting a batch, historical TRUNCATE, missing tail records and
post-start WAL appends all produce non-success findings. A location that
extends past the partition's highest *replayed* logical WAL end is never
credited to startup recovery. Even an extra unexpected index entry past
HW must be checked instead of accepting an early successful prefix.
Thirteen deterministic tests cover these invariants and bounds.

## Batch 119: read-only native Source / Broker identity / WAL replay join

`BrokerLocalStartupReplayPrecheck` joins the current extension's
Broker-ready identity pairing, epoch-aware local leader ticket, two
actual native Kafka LogStart/HW/LEO observations and the indexed data
actually recovered at startup. The WAL source's destructive mutation
revision and live-write admission revision are sampled around the whole
classification. Incomplete identity, a changed Kafka window/leader,
or WAL activity suppresses every finding. It returns only local
diagnostics, not a transferable proof or API grant.

## Batch 120: revoke diagnostics after registration ABA and runtime changes

Real Ring WAL close/reopen regression coverage compares an actual
recovered Kafka batch with the native source sample and valid broker-ready
snapshot. Other tests ensure empty recovery, post-start appends, Topic ID
recreation, native LogEnd/HW drift, local demotion, broker re-registration
ABA and a WAL append during source capture cannot masquerade as a stable
observation. Nine integration-style JUnit tests cover this join.

The strict Java/Scala mandatory test manifest grows from
**391 to 413 methods** across **27 suites**.

**P0 remains unfulfilled:** local startup completeness says nothing about
other broker replicas, a currently authoritative BrokerRegistration,
future WAL fsync, Kafka source-log provenance across LeaderEpoch or
a stable Controller event-queue CAS. These findings must not be sent as
KRaft retirement authorization. No metadata writer, logical reference
retirement or physical COMMITTED deletion is enabled.

## Batch 121: observe successful live WAL durability futures after index publication

The existing local WAL admission generation intentionally tracks *pending*
work rather than reporting successful completions or logical addresses.
`LocalWalDurabilityObservation` now separately records only successful,
backend-returned WAL logical locations after both the `SharedWal`
durability Future and its index-publication continuation have completed.
A per-partition immutable CAS snapshot reports DATA batch count, DATA
group count, TRUNCATE count and the highest observed successful WAL end.

## Batch 122: fail closed on failed groups and non-durable operations

The copied and owned DATA append path, owned batch-group path and
TRUNCATE path record observations only after valid WAL result length and
successful local index application. A pending Future, failed force,
invalid backend result or incomplete/partial group produces no positive
completion observation. Synchronous backend errors also never fabricate
one. Observations are per-partition and completion-ordered by a
monotonic logical maximum, not by a last-writer assignment.

## Batch 123: prove reboot resets live observations on real File/Ring WAL

Real File and Ring WAL append/close/reopen tests distinguish a successful
post-start durability Future from the next process's startup replay.
The new process starts with an empty *live* completion snapshot and may
only rebuild its immutable startup replay evidence from backend recovery.
No live observation is persisted, transferable or reusable after an
incarnation restart. Immutable earlier snapshots are not mutated.

## Batch 124: recheck live completion snapshots across native source capture

The existing read-only local Broker/source/replay classification now
compares the identity of its CAS-published live completion snapshot
before/after source and index observation. It invalidates the finding
if that snapshot was replaced, even when separately mocked WAL
admission/mutation generations remain constant. A regression case
simulates precisely this torn-view gap. Eleven deterministic tests
increase the strict named Java/Scala inventory from **413 to 424**
methods across **28 suites**.

**Trust limit:** a SharedWal successful Future is a backend's local
durability contract for this process, not proof that any range survives
another broker's failure or remains recoverable after local WAL
reclamation. Highest successful WAL end is NOT contiguous Kafka DATA
coverage, replicated HW, a stable native Source LogStart, KRaft
Controller registration, or a signed authority receipt. No KRaft
retirement authority writer or COMMITTED physical GC has been enabled.

## Batch 125: current locally durable Kafka RecordBatch coverage

The initial `StartupWalReplayCoveragePrecheck` deliberately rejected
records written after this process's startup, even if their WAL Future
successfully completed. A separate
`CurrentLocalWalCoveragePrecheck` now classifies exactly aligned,
gap-free Kafka RecordBatch ranges in the current `PartitionWalIndex`.
The `SharedStorageEngine.localDurableWindowCoverage` entrypoint obtains
its input from the actual WAL index rather than a caller-provided list.
Every indexed entry was admitted via successful WAL replay or a completed
backend durability Future followed by successful index publication.

A positive local result requires full batch-aligned coverage of
LogStart..HW, never just a maximum WAL logical address, a count of
acknowledgments, or an implicit assumption that gaps are covered.
A distinct EMPTY_SOURCE_WINDOW is not a positive DATA proof.

## Batch 126: mix startup replay and post-start durable WAL without duplicating indexes

Thirteen direct and real-backend regressions cover Kafka gaps,
overlaps, partial first/last batches, missing tails, unfinished/failed
durability Futures, cross-partition physical-WAL interleaving, true
File WAL close/restart plus newly durably appended data, Ring WAL
per-partition separation and durable TRUNCATE eviction.
The current index is reused: there is no second unbounded per-batch
provenance table.

## Batch 127: apply exactly the existing Broker/native Source/WAL fences

`BrokerLocalStartupReplayPrecheck` gains a separate
`assessCurrent` result type. Both startup-only and current-local
coverage share one generic read-only double-capture sequence over
Kafka's native LogStart/HW/LEO, role generation and LeaderEpoch.
It also rechecks the initial broker-ready incarnation pairing, WAL
destructive generation, in-flight admission generation and immutable
live-durability completion snapshot.

A current-local finding is not a startup-replay finding; neither is
a Controller-issued attestation or a usable retirement permit.

## Batch 128: isolate local ABA / stale Source / pending WAL cases

Seven integration-style tests cover recovery+live windows, exclusively
live durable windows, offset gaps, changed native Source samples,
re-registration/Incarnation ABA, append completion during capture
and missing broker-ready registration. A fence violation discards the
finding even when the WAL index would otherwise cover the window.
Strict required Java/JUnit inventory grows from **424 to 444 methods**
across **29 suites**.

**P0/GC still blocked:** all observations are process-local. This does
not prove which replicas must retain Kafka's committed bytes, freshness
of Controller BrokerRegistration, metadata quorum applied position,
replicated/fsynced recoverability after broker loss, or a serialized
KRaft authority CAS. Never use a local complete interval to authorize
COMMITTED physical-object GC. No Controller record emitter or physical
delete path was enabled; the frozen MinIO GA candidate remains separate.

## Batch 129: introduce an explicitly untrusted Controller source/WAL claim

A Controller-local `PartitionRetirementUntrustedSourceClaim` now
describes the proposed Topic ID, partition, BrokerId, BrokerEpoch,
BrokerIncarnationId, native Source LeaderEpoch, local role generation,
LogStart/HW/LEO and observed WAL mutation/admission generations.
Construction rejects impossible native Kafka offset ordering, missing
leader and broker identities, negative/unknown WAL generations and
a purported COMPLETE DATA window that is actually empty.

This is a **caller-reported claim**, not an attestation, signed
certificate or proof of KRaft BrokerRegistration ownership.
It cannot be used to authorize remote-object retirement.

## Batch 130: make untrusted source claims agree with live Controller CAS

`PartitionRetirementCandidatePrecheck.assessUntrustedSourceClaim`
first runs the existing NON-EMITTING current KRaft feature, write-position,
authority-timeline CAS, partition leadership and BrokerRegistration checks.
Only after those checks are valid does it compare the source claim
against the exact Candidate's topic identity, broker identity,
LeaderEpoch, claimed Source LogStart and local batch coverage flag.
Mismatches have explicit fail-closed findings.

**Even matching positive claims return SOURCE_LOG_START_NOT_VERIFIED.**
No caller-originated bytes, WAL Future count or boolean can switch that
finding to an authorization; this code never appends authority records.

## Batch 131: stale registrations and KRaft movement invalidate old claims

After a matching read-only preflight, a new Controller BrokerEpoch or
IncarnationId, different partition leader, non-current next KRaft
write offset, feature disablement or retirement authority timeline
advance is evaluated against the *current* manager state again.
Old claims cannot survive registration ABA, leadership movement,
KRaft commit or feature reconfiguration by being replayed unchanged.
A new authenticated Broker-to-Controller source verification transport
and serialized CAS event remain missing.

## Batch 132: deterministic untrusted source boundary verification

Eighteen named JUnit tests exercise positive-but-unverified claims,
missing and incomplete local coverage, immutable Topic/partition and
broker identity disagreement, leader epoch/LogStart mismatch,
impossible native HW/LEO and empty windows, in-flight WAL generations,
malformed local leadership, feature disablement, BrokerEpoch rotation,
KRaft leader change and authority CAS/replay changes. They also
assert that no KRaft authority record is emitted.

Strict named Java/Scala test inventory grows from **444 to 462**
methods across **29 suites**.

**GA limit:** This does not authenticate any source identity or durable
Kafka/WAL range and has no Broker-to-Controller RPC. A future
transport must establish the sender's current registered process
identity and validate a fresh actual source/WAL continuity proof
*inside* the serialized Controller KRaft event. Controller authority
writers, durable COMMITTED reference retirement and physical object
DELETE all remain OFF; the frozen MinIO GA branch is unchanged.

## Batches 133–136 — broker-local snapshot capture and Controller event-queue diagnostic

A broker-local `CurrentSourceWalClaim` is now populated from the same
immutable broker-ready WAL replay identity, exact native Kafka Source window
and currently indexed backend WAL batch coverage used by the read-only
`CURRENT_WINDOW_CLASSIFIED` classifier. The positive capture requires a
non-empty Kafka RecordBatch-complete window, unchanged source before/after
classification, stable WAL admission/mutation generations and an identical
live completion snapshot. It deliberately does not accept an injected
positive-coverage boolean. The payload includes ClusterId, TopicId,
PartitionId, BrokerId/Epoch/IncarnationId, LeaderEpoch, local role generation,
native LogStart/HW/LEO and WAL revision coordinates.

A new `QuorumController.preflightUntrustedPartitionRetirement` method
executes the matching Controller claim agreement gate in the serialized
Controller write-event queue, using the event's **current** next KRaft write
position, current BrokerRegistration/partition leader/feature gate and
replayed retirement authority. It always emits **zero records**, and a
matching claim still returns `SOURCE_LOG_START_NOT_VERIFIED`. Its
ControllerRequestContext deadline is honored; it is not exposed as a
public `Controller` API or RPC endpoint.

Nine deterministic broker-origin capture tests cover real WAL replay,
mixed replay/live durability, missing broker-ready identity, offset gaps,
empty windows, native source drift, BrokerIncarnation ABA, concurrent
backend WAL completion, and invalid reconstructed payloads. Two real
QuorumController tests verify default feature rejection, no KRaft emissions
and event ordering. Mandatory evidence inventory: **473 / 30 suites**.

**No authenticated broker transport exists at this checkpoint.** Both the
new local claim and Controller method are independently invocable; they
have not been connected by an authenticated Kafka-native RPC. They cannot
prevent forgery or replay of wire bytes, and current KRaft registration
does not prove the RPC's unique broker-process identity. Broker-to-Controller
authenticated ingress, freshness/non-replayability inside the serialized
commit event, quorum durability/cross-broker proof, an atomic KRaft authority
writer, COMMITTED retirement and physical MinIO deletion remain blocked.
The frozen MinIO GA branch is unchanged.

## Batches 137–140 — compile recovery and Controller-only probe security prerequisites

**CI root cause from prior checkpoint:** Batch 133–136 source-SHA CI ended
16/16 failed in common Java compilation because a new
`Objects.requireNonNull` invocation in `QuorumController` lacked its
`java.util.Objects` import. Batch 137 repairs that defect without
weakening compiler checks or changing the frozen MinIO GA branch.

A new immutable broker principal allowlist rejects missing, anonymous,
wildcard and shared principals. It performs an exact match on the trusted
`ControllerRequestContext.principal()` rather than on a self-reported
BrokerId. Crucially, the default runtime allowlist is EMPTY/DENY_ALL,
because Kafka's normal CLUSTER_ACTION privileges and a nonunique
interbroker principal are insufficient to prove individual broker
incarnation ownership.

A controller-volatile one-shot Source/WAL probe registry issues an opaque
Uuid nonce bound to the proposed TopicId/partition and exact Controller
candidate. Creation requires a uniquely bound principal. Its 5-second
TTL uses signed-origin-safe monotonic `nanoTime` subtraction; issuance
has a strict 128-entry outstanding cap and expires old pending sessions.
Consumption removes the nonce *before* any check so wrong-principal,
forged-envelope, expired, moved-epoch or changed-KRaft-position requests
cannot retry it later. A new Controller instance has no session; the
current one clears outstanding sessions on claim, re-epoch and renounce.

Two Controller-internal methods issue and consume probes in the
`appendWriteEvent` serialized queue, respect the request deadline,
and emit an EMPTY record list. Once consumed, the existing
`PartitionRetirementCandidatePrecheck.assessUntrustedSourceClaim` is
rerun against fresh feature negotiation, registration, partition
leadership, KRaft write position and retirement authority timeline.
A positive nonce remains **MATCHED_UNTRUSTED**, and even a matching
claim never becomes an authority writer or physical GC grant.

Mandatory regression inventory increases by 27 methods (9 principal
mapping, 16 nonce/lease and 2 Controller diagnostics) to
**500 named Java/Scala tests over 32 suites**. This is a test
inventory, not a CI success claim; this development SHA must pass its
own full Java 25/MinIO/WAL/Controller gates.

**Security and scope:** The application does not yet transport probes
over the network, inject trusted principal mappings or authenticate
actual Broker→Controller evidence. Possession of a nonce is not data
origin proof, nor is a successful local SharedWal Future an object
retirement certificate. Cross-replica persistency, Controller KRaft CAS
writer, COMMITTED logical-retirement transition and physical MinIO
DELETE remain forbidden. The frozen GA release remains unchanged.

## Batches 141–144 — challenge-bound native source read-only adapter

The Batch 137–140 HEAD passed 15/16 GitHub workflows. The only failure
was a Checkstyle BooleanExpressionComplexity violation (12 > allowed 5)
in the positive broker-local claim constructor, reached after actual
storage tests. Guard clauses now separately validate identity, broker
and source epochs, WAL admission/mutation revisions, and native Kafka
LogStart/HW/LEO; existing invalid domains still fail closed.

To avoid prematurely allocating a Kafka protocol API key or changing
production heartbeat semantics, the next handshake is represented by
a transport-neutral immutable `PartitionRetirementSourceProbe.Request`
and `Report` in the `clients` dependency shared by `metadata` and
`storage`. No wire endpoint, parser or sender has been activated.
Request preserves a one-time nonce, cluster, immutable topic,
partition, registered broker incarnation, leader epoch and expected
authority CAS; Report carries native Kafka offset coordinates, local
role generation, and stable WAL revision observations. Each record is
freely constructible and **must be considered untrusted**.

The broker-local adapter samples the existing real native
`SharedUnifiedLog` and current Indexed WAL coverage, double checks the
same source/role/identity/WAL generations, then compares the observed
coordinates to the request. It rejects wrong Topic ID, partition,
cluster, BrokerId, BrokerEpoch, BrokerIncarnationId, LeaderEpoch,
LogStart, incomplete batches or any transient WAL mutation. This is
a synchronous local diagnosis, **not** evidence that bytes are
durable across replicas or remain unchanged after capture.

The Controller nonce registry now round trips the immutable request
coordinates through the shared model; the report adapter converts
the caller's self-reported fields into the already non-authoritative
`PartitionRetirementUntrustedSourceClaim`. It invokes exactly one
`inspectRetirementSourceProbe` Controller queue event; the nonce is
burnt and the current KRaft registration, leader, metadata position
and feature negotiation are rechecked. The production principal
allowlist remains DENY_ALL; no KRaft record can be emitted and a
positive payload is always `SOURCE_LOG_START_NOT_VERIFIED`.

**New deterministic regressions:** 12 in the real broker-local source
capture suite, 3 nonce model round-trip/replay cases, and 2 in the
QuorumController suite. Required named test inventory is
**517 methods / 32 suites**. This count is not a CI success assertion
for the new SHA.

**Next:** design and implement an experimental Kafka-native
Controller-listener API with explicit version/feature negotiation,
dedicated per-broker network-authenticated credentials, bounded input
and no best-effort fallback to CLUSTER_ACTION-only auth. Do not add
the actual KRaft CAS writer, logical COMMITTED retirement or physical
DELETE before authenticated origin and cross-broker durability proofs
can be replayed and tested under crashes and leadership changes.

## Batches 146–148 — Java 11 DTO compile and mandatory Controller evidence

Batch 145's Java-11 class conversion was syntactically incorrect:
the generated equals methods referenced the cast variable `that` before
declaring it. Consequently all 16 workflows failed at a shared
`:clients:compileJava` gate. The correction makes each immutable
Request/Report `equals` method strictly Java 11: identity fast-path,
type check, explicit cast and short bounded comparisons. The same
source compiled under `javac --release 11 -Xlint:all -Werror` with
a stub matching the Kafka Uuid API used by the DTO. Its equality and
invalid-window smoke harness passed. The full repository Gradle compile
and JUnit verification remain separate CI requirements.

Eight deterministic `PartitionRetirementSourceProbeValueTest` tests
now verify equality/hash contracts under reconstructed source
requests/reports and negative identity, CAS, epoch, WAL and source
offset cases. Required tests grow **517 -> 525 methods, 32 -> 33 suites**.

Critically, the Shared Storage workflow's explicit metadata
`--tests` filter had not included the existing mandatory
`QuorumControllerTest`, principal policy, or source probe registry
suites. The filter now schedules all mandatory Controller test
methods, including the new value-contract suite, so the fail-closed
report verifier can inspect actually executed JUnit XML rather
than only test method names in source. `:clients:compileJava` runs
before the resource-intensive E2E steps. Workflow path filters
include relevant new clients and metadata Probe files.

**Scope:** No network RPC, producer or Consumer API, Java version
upgrade, principal trust shortcut, KRaft writer, reference retirement
or physical COMMITTED deletion. The next transport step must remain
separate until the corrected SHA establishes 16/16 CI success.

## Batches 149–152 — metadata layer boundary and experimental native heartbeat schema

The primary Shared Storage CI on `f229b4d5` encountered five
`ImportControl` errors because the new clients-layer
`PartitionRetirementSourceProbe` values were consumed by
`QuorumController`, `PartitionRetirementSourceProbeRegistry`
and their tests without an import-control declaration. The
dependency exception is now restricted to exactly those three
classes; global package access remains forbidden. Metadata
Checkstyle is executed in the early Java 11 compilation preflight
instead of after large storage and WAL test batches.

A new *unstable version 3* adds optional Kafka protocol tagged
`SourceWalProbeReport` to `BrokerHeartbeatRequest` and optional
`SourceWalProbeChallenge` to `BrokerHeartbeatResponse`.
The report carries immutable TopicId/partition, nonce, cluster,
broker incarnation, source LeaderEpoch/LogStart/HW/LEO, local
leadership and WAL mutation/admission revision, and authority
CAS coordinates. The challenge carries the current registered
BrokerId/Epoch/Incarnation, Source LeaderEpoch, TopicId/partition,
nonce and Controller authority coordinates. Legacy v0–v2 fields
retain their exact schema and tags. Only the request JSON declares
`latestVersionUnstable`, because the Kafka protocol generator
rejects that property on responses. Default production
`ApiVersions` must still cap heartbeat at v2.

Six clients-JUnit serialization tests are added as a mandatory early
gate: stable/unstable advertised versions; v0–v2 null defaults;
v3 Source/WAL report and Controller challenge round trips; and
deterministic rejection of non-null v3 tagged fields serialized
under v2. The existing Controller/Storage mandatory evidence count
remains **525 Java/Scala methods / 33 suites**; the six clients wire
tests are executed and XML-verified separately.

No Broker or Controller handler yet generates or consumes the new
wire tags. In particular, a self-reported Source/WAL report is not
an authenticated replica certificate. Unique per-broker credentials,
registration-incarnation binding, real replayable durable proofs,
CAS writer, reference retirement and physical COMMITTED GC are all
still pending. Production remains DENY_ALL; frozen MinIO GA remains
unchanged.

## Batches 153–156 — correct metadata ImportControl and reject untrusted v3 wire reports

The preceding experimental BrokerHeartbeat v3 checkpoint passed
**16 of 17 workflows**. Its single main Shared Storage failure was
`:metadata:checkstyleMain` rejecting two Source/WAL diagnostic imports
after an earlier patch updated the wrong ImportControl file. Metadata
compiles with the dedicated
`checkstyle/import-control-metadata.xml`; the new classes now have
explicit allow rules only under its `controller` subpackage.
The unnecessary change to the shared `import-control.xml` is
reverted. Workflow triggers track the correct rules file.

`PartitionRetirementHeartbeatReportGate` is a pure validation and
decoding boundary around the optional unstable v3
`BrokerHeartbeatRequestData.SourceWalProbeReport`.
An absent report takes the unchanged heartbeat path. Present reports
require protocol v3, a uniquely mapped authenticated Principal and
valid immutable Source/WAL coordinates; invalid/unknown fields are
rejected. A report with valid syntax and a matching Principal is
**still unverified** because the Controller has not correlated it to a
live nonce, current KRaft BrokerRegistration, native Kafka Source or
replica-durable WAL proof. It NEVER grants GC.

`QuorumController.processBrokerHeartbeat` invokes the gate **before**
its broker contact metric and lockless heartbeat tracking. Wrong
principal produces a cluster authorization failure; any other
non-absent finding produces an invalid request. These failures are
represented by failed futures so ControllerApis retains the existing
asynchronous heartbeat error handling. A rejected report cannot
trigger a KRaft event. The network sender has not been enabled,
and even an explicit experimental v3 API negotiation does not
change the production allowlist `DENY_ALL`.

Eleven gate/codec tests verify version/identity bounds, malformed
Source/WAL windows and revision fields, absence on v2, and that a
matching but caller-supplied report stays UNVERIFIED. Two real
QuorumController tests assert the wire report is refused before
KRaft records can be emitted. Mandatory named evidence now comprises
**540 methods in 34 suites**, but the new source SHA must independently
pass its CI gates; this count is not a success declaration.

Remaining blockers: real Broker-source sample sender, Controller
nonce challenge issued through the network, unique authenticated
principal-to-current-BrokerRegistration binding, anti-replay evidence
inside one serialized Controller event, cross-broker durable proof,
then explicit KRaft authority writer and COMMITTED retirement/physical
MinIO deletion. All write/delete paths remain OFF on the independent
feature branch; frozen MinIO GA remains unchanged.
 
A read-only broker-side `captureCurrentSourceWalHeartbeatReport`
adapter now serializes a fresh native Source/WAL observation into the
generated v3 tagged field without launching a heartbeat. Two additional
storage regressions cover native capture -> v3 bytes -> parsed report
and broker-incarnation mismatch rejection. Mandatory named evidence
is therefore **540 methods in 34 suites** rather than 538.

## Batches 157–160: observable BrokerHeartbeat v3 challenge with no response authority

The previous `e86a1d5c` HEAD passed 16 of 17 GitHub Actions, with
only the Shared Storage `:metadata:spotbugsMain` static analyzer
failing. The existing static-analysis artifact did not include
metadata's SpotBugs HTML or XML, preventing attribution to an actual
warning type. The primary workflow now uploads
`metadata/build/reports/spotbugs` and runs this uncompromised
SpotBugs gate **early** alongside Java 11 client compile and
metadata Checkstyle, so future failures are independently reviewable.

The broker's *real* heartbeat response event path now explicitly
examines an optional unstable v3 Controller Source/WAL challenge. The
read-only gate requires version 3, matching current
BrokerId/Epoch/IncarnationId/ClusterId, registered RUNNING broker
state, valid nonzero nonce, immutable TopicId, partition,
SourceLeaderEpoch, LogStart and authority CAS fields. An exact,
constructible challenge is classified `MATCHED_UNTRUSTED`, never
"authenticated", "authorized", or "durable". There is no sampling
callback or pending queue yet. The lifecycle handler intentionally
ignores the challenge and continues ordinary broker registration,
heartbeats, fencing/unfencing and controlled shutdown unchanged.

Ten new deterministic server-side tests cover the challenge's
wire-shape and identity checks plus the default non-authorizing
diagnostic. `:server:test` selects the exact suite, and a separate
fail-closed JUnit XML verifier checks all ten method names and
zero skips/failures. Existing mandatory storage/metadata Java/Scala
inventory remains **540 methods / 34 suites**, plus these ten
separately required server tests. No network Report is emitted.

Production Controller principal policy remains `DENY_ALL`.
Actual v3 Controller challenge signing/issuance, unique broker
principal configuration, authenticated transport, broker-native
source and WAL asynchronous sampling, KRaft authority CAS writing,
COMMITTED retirement and physical GC deletion are still blocked.
The frozen MinIO GA release is unchanged.

## Corrective evidence on Batch 157–160

The exact SpotBugs HTML, now retained by Shared Storage CI,
identified one failure only:
`DCN_NULLPOINTER_EXCEPTION` in the metadata heartbeat report gate.
Explicitly reject null nonce, ClusterId, TopicId or BrokerIncarnationId
instead of catching `NullPointerException`; continue to reject
illegal offsets via `IllegalArgumentException` only. A new
controller-side negative regression enforces the null-field contract,
taking mandatory Java/Scala evidence from 540 to **541 methods
across 34 suites**.

GA Workflow Consistency simultaneously identified a newly triggered
production source missing from the established release fingerprint.
The non-authorizing Broker v3 challenge parser is therefore nested
inside the already fingerprinted `BrokerLifecycleManager` source;
its independent 10-method JUnit suite remains and is still mandatory
in main CI. No fingerprint exception or generic code exclusion
was added. Frozen MinIO GA remains unchanged. The new corrective
HEAD still requires its own end-to-end CI pass.

## Batches 161–164 — independent offset namespaces and versioned diagnostic challenge transport

The Shared Storage `:metadata:test` on `4d5a6d46` failed two
tests while native source/WAL and other completed CI gates succeeded.

The constructor must compare **KRaft retirement authority** CAS
positions with each other, not with Kafka native Source LogStart
offsets. The regression now asserts true CAS non-advancement is
invalid, while Kafka LogStart greater than a numerically smaller
KRaft authority offset is valid. No runtime tightening based
on unrelated log coordinate systems has been added.

A separate test incorrectly counted KRaft controller bootstrap
feature/registration records as retirement mutations because it
captured `allRecords()` before startup had finished asynchronously.
All early retirement preflight tests now count only
`PartitionRetirementAuthorityRecord` for "no GC record" assertions.
The production writer remains OFF and tests still fail if one
retirement authority record is emitted.

`PartitionRetirementSourceProbeRegistry.Challenge` can now be
encoded to the existing unstable `BrokerHeartbeatResponseData`
v3 `SourceWalProbeChallenge` and reconstructed after decoding
without dropping nonce, BrokerId/Epoch/Incarnation, TopicId,
LeaderEpoch, source LogStart or CAS coordinates. Four tests prove
successful v3 round-trip, strict single-use untrusted nonce,
malformed/forged identity rejection and v2 downgrade rejection.
Named mandatory evidence is **545 tests in 34 Java/Scala suites**,
with separately verified clients/broker protocol suites unchanged.

**Not yet live:** This conversion alone does not issue a Controller
challenge, send it on BrokerHeartbeat, set a trusted Principal, or
enable retirement. The production feature gate remains disabled,
per-broker authentication remains DENY_ALL and physical GC remains
disabled. MinIO GA release stays frozen.

## Batches 165–168 — negotiated Controller diagnostic challenge delivery

The development branch's preceding `0f223df3` SHA passed all
**17/17 CI workflows**, including real MinIO and Controller HA.
This section extends the transport boundary but deliberately does
not change the frozen MinIO GA release or enable physical GC.

A `PartitionRetirementSourceProbeRegistry` challenge is a
Controller-volatile nonce, at most five seconds old. The new
read-only `pendingForHeartbeat` lookup checks BrokerId/Epoch and
the unique mapped authenticated Principal without minting,
refreshing, or consuming a nonce; Controller promotion/deactivation
still clears the registry. In the existing serialized Controller
heartbeat event, `QuorumController` may return a previously issued
pending Challenge only after **fresh, authoritative KRaft**
registration/leadership/feature/CAS revalidation. It sends nothing
if the heartbeat is non-v3, writes KRaft metadata, is behind
metadata, seeks fencing/shutdown or describes a fenced/shutting-down
broker. The original heartbeat records and atomicity remain intact.

`BrokerHeartbeatReply` now optionally carries an immutable typed
SourceProbe Request, while preserving its original four-argument
constructor. `ControllerApis` serializes it as an optional
`BrokerHeartbeatResponseData.SourceWalProbeChallenge` only when
the actual negotiated request version equals 3. The generated
stable v0–v2 wire envelope remains unchanged. The existing broker
handler rejects or ignores this diagnostic and does not schedule
source reads or send reports.

Four registry-JUnit tests validate read-only principal fencing,
BrokerEpoch ABA, TTL, one-use consume and leader turnover.
Three Scala ControllerApis tests validate non-v3 silence,
fenced/shutdown suppression and an exact v3 encode/decode.
The Shared Storage CI explicitly runs and XML-verifies these
three core tests and the existing strict Java/Scala contract
inventory is now **550 methods / 34 suites**.

No stable public API, new Kafka API key, production Principal
configuration, signed native Source/WAL proof, KRaft authority
writer or COMMITTED object DELETE is enabled. The full new
SHA must independently pass its 17 Workflow release tests
before this becomes a verified development checkpoint.
 
An additional Controller runtime test registers a Broker and
sends a v3 heartbeat under `DENY_ALL`, asserting neither a
Source/WAL challenge nor a retirement authority record is emitted.
The mandatory named inventory is 550 methods in 34 suites.

## Batches 169–172 — one in-flight asynchronous Source/WAL sampler, fail closed

Baseline `794f6d61` passed 17/17 Workflow gates. The new
Broker-side `SourceWalProbeSampler` is only an explicit,
default-null, transport-neutral asynchronous callback.
BrokerLifecycleManager samples only after successful receipt
of a Controller heartbeat v3 challenge with a registered RUNNING
Broker, exact ClusterId/BrokerId/BrokerEpoch/incarnation and valid
immutable Source/WAL CAS coordinates. A response indicating fencing,
shutdown or incomplete metadata catch-up cannot start sampling.

One pending capture, one staged result and one most-recent nonce
are held exclusively on the existing Broker lifecycle event queue.
Sampler Futures complete back into that queue, never directly
mutating Broker state on arbitrary executor threads. Identity
fencing prevents late callback ABA; null/error/empty results,
wrong nonce/source, stale BrokerEpoch or changed incarnation
fail closed. A hung provider cannot launch unlimited native I/O:
its active I/O fence survives provider replacement until the prior
callback finishes or synchronous scheduling fails. A valid result can only be
consumed once through a diagnostic-only mailbox and expires
at five seconds, with monotonic timer wrap handled by subtraction.
Registration, controlled shutdown, shutdown and sampler change
revoke pending or staged values without making the previous
nonce eligible again.

**No production adapter is installed and no Report is sent.**
This intentionally avoids source reads, transport credentials,
shared-storage mutations or KRaft writes in default deployments
until unique Broker authentication and durable source evidence are
implemented. All Controller denial, COMMITTED retirement and
physical object deletion controls remain unchanged and OFF.

Fourteen new `server:test` Java methods provide deterministic
coverage of these fences; the main Java 25 workflow verifies the
exact JUnit XML set with zero skipped/failing cases. The existing
550-method/34-suite metadata-storage inventory is unchanged.
The MinIO GA release branch remains frozen.

## Batches 173–176 — complexity gates and bounded replay-window design

On development SHA `71a23c6e`, 16/17 Actions succeeded.
Only the primary Shared Storage job failed, at
`:server:checkstyleMain`, with `NPathComplexity=972 > 500`
in the Controller Source/WAL challenge handler and
`BooleanExpressionComplexity=6 > 5` in the Broker capture
eligibility method. The previous Broker JUnit suite was
not executed; this was a static rule gate, not a JUnit
assertion or proven WAL correctness regression.

The heartbeat ingress is split into small helpers for
diagnostic eligibility, bounded off-thread scheduling and
serialized completion. Registration/RUNNING/timeout/
incarnation checks now use straightforward fail-fast guards;
neither Checkstyle nor SpotBugs rule limits are weakened.

The original Broker nonce fence remembered only the last
nonce. A challenged Broker could process nonce A, then B,
then accept replayed A while all were still fresh. The
replacement is a **64-entry maximum**, insertion-ordered,
Broker-local nonce history. It retains every nonce accepted
in the last five seconds, including revocations and provider
changes. Pruning is based on elapsed monotonic time with
signed overflow-safe subtraction. At full capacity, it
rejects new source sampling until older entries expire,
never evicting live replay barriers. Single in-flight
native I/O, late callback fencing, strict request equality
and one-shot local report consumption remain unchanged.

Four new deterministic server-side regressions cover
interleaved nonce replay, saturation without premature
eviction, reconfiguration and signed monotonic clock wrap.
The main CI requires **18 capture-state** and
**10 challenge-gate** test methods with zero skipped/failing
JUnit entries and retains the ordinary server static checks.
The strict metadata/storage manifest remains **550 / 34**.

The feature remains default-disabled: no storage adapter
is installed, no diagnostic report is transmitted,
Controller per-Broker Principal policy is `DENY_ALL`,
and neither KRaft authority CAS writing nor physical
COMMITTED garbage collection is enabled.
