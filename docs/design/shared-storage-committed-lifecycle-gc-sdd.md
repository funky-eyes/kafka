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
