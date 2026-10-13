# COMMITTED lifecycle GC — implementation boundary after MinIO GA freeze

**Base:** `9a28c11b6d97ecacafe1b8566cde5cc34d604889`.
**Feature branch:** `feat/committed-lifecycle-authority`.
**Initial production state:** no authority writer, no durable reference
retirement, no lifecycle-triggered physical DELETE.

This is a development order, not evidence that any unsafe capability has
been enabled. Keep each change small and run deterministic tests before
promoting it to the existing MinIO GA evidence branch.

## P0 — trusted Kafka source and authority input

1. Capture a real native `UnifiedLog` Topic ID / partition / source
   LeaderEpoch / LogStart / HW window from the current source broker;
   revalidate the same epoch and owner after metadata-consumer catch-up.
2. Bind BrokerId, BrokerEpoch and BrokerIncarnationId to the current
   KRaft registration. Bind the local WAL durable/replay horizon to the
   exact source range and prove no in-flight destructive mutation.
3. Treat a caller-provided offset, local revision, CRC, metadata-consumer
   position or old preflight result as *advisory*, never a grant.
   Unknown source ownership, incomplete replay, new leader, stale broker
   incarnation or uncertain WAL durability must fail closed.
4. Provide crash and broker-reregistration tests showing stale source
   evidence cannot be reused. Do not issue a reusable approval token.

## P1 — version-gated Controller append transaction

1. Maintain the reserved experimental authority metadata record and
   KRaft snapshot replay, including immutable Topic ID and terminal delete.
2. Add a Controller event-queue write path only with fresh registration,
   source-log proof and expected authority-version rechecks inside that
   **same** serialized event. Use the actual KRaft-assigned record offset.
3. On timeout or uncertain commit, re-read committed KRaft state;
   never infer success from an ACK, in-process cache or transaction ID.
4. Require negotiated feature support for every active and rollback-eligible
   broker/controller. Kafka 4.3.1 default metadata does not advertise
   the experimental v1 feature, so do not enable it by bypassing negotiation.

## P2 — durable logical reference retirement

1. Persist the Topic-ID terminal delete or monotonic partition log-start
   transition first. Replay it on every broker before changing reads.
2. Retire references to whole, unreachable RecordBatch ranges in a
   separate, recoverable state transition. A watermark inside a batch
   cannot retire that batch's physical bytes.
3. For a packed physical object, every referenced topic/partition range
   must be retired. No partial-object delete based on the first partition.
4. Fence admitted uploads and readers across partitions and epochs;
   process-local CAS quiescence alone is not a cluster-wide durability
   or deletion certificate.

## P3 — physical MinIO deletion and GA expansion

1. Physical DELETE only after durable reference retirement and verified
   absence of live reader/upload ownership.
2. Preserve retry evidence until DELETE succeeds; tombstone authoritative
   COMMITTED metadata **after** physical DELETE. Any uncertain result
   means retain rather than delete.
3. Test HEAD/ListObjectsV2 deletion vs protected packed ranges, late PUT,
   multi-broker failover, restart, Topic Delete + recreate, retention,
   DeleteRecords, crash between delete and tombstone, and mixed-version
   rolling upgrades. All must be mandatory and unskipped.
4. Create a **separate** lifecycle-GC GA contract; the frozen 19-gate
   MinIO GA seal alone does not certify the extended release scope.

## First code checkpoint: reject reserved KRaft identity end-to-end

The current Controller Candidate preflight rejects `Uuid.ZERO_UUID`
as an invalid Topic ID. Before this checkpoint, the persisted authority
`Key` decoder itself still admitted this reserved identity. Reject it
at the shared `PartitionRetirementAuthorityState.Key` boundary so a
malformed log record cannot enter the Controller TimelineHashMap or
MetadataImage, irrespective of which replay path is used. Add regression
coverage for direct construction, KRaft decoding, regular Controller replay,
KRaft log/snapshot replay and negotiated MetadataImage replay. No record
emitter or COMMITTED physical deletion is added.

**Next checkpoint after this PR:** design a fresh, controller-verifiable
source-horizon observation that survives broker restart and cannot be
forged or reused across LeaderEpoch/Incarnation changes. Prove the data
origin before implementing any KRaft write operation.

## Batches 101–104: source window / metadata replay generation hardening

The native source double-check now fails closed when HW or LEO regresses
even if LogStart, LeaderEpoch and the local role generation remain equal.
It also checks the metadata image's consumed offset **and** explicit
partition LogStart before and after read-committed classification.
This closes a stale advisory-result path after concurrent replay.

This is NOT the Controller-verifiable source/WAL durability certificate
needed for P0. In particular, local source samples, caller-provided
stable-end/applied-position counters and a local WAL mutation revision
remain unauthenticated. Do not implement a KRaft write or physical
COMMITTED DELETE based on these findings.

Next actual integration prerequisite: design and test an authenticated
current-BrokerEpoch/Incarnation source-horizon witness whose WAL
durability/replay evidence originates in the broker, not in user-supplied
offsets. The Controller must revalidate that origin inside its serialized
KRaft event before changing authority.

## Batches 105–108: broker-local WAL write admission activity

Index destructive-generation checks alone missed ordinary tail writes
still waiting for `SharedWal` durability and index publication.
The broker now observes every DATA/group/TRUNCATE Future via one atomic
per-partition admission state. Read-only native source classification
fails closed if any WAL append was pending or was admitted/completed
during the sample. Stable tail appends no longer silently pass a stale
source probe.

**Not an authenticated proof:** This does not establish that bytes
survive broker restart or replica loss. It has no broker-incarnation
binding and no Controller-side acceptance protocol. Never promote
a local admission revision, mutation revision, or metadata replay
position into an authority writer or COMMITTED delete permit.

Next integration work: define the durable/replay horizon with a
concrete backend-originated witness, bind it to the current registered
BrokerEpoch/IncarnationId, and recheck provenance inside a serialized
Controller KRaft event. Keep all authority writes disabled until that
end-to-end proof has fault-injection evidence.

## Batches 109–112: genuine backend replay inventory, still diagnostic

Startup `SharedStorageEngine` now receives an immutable per-partition
inventory from the real `SharedWal.replay` event stream. Tests reopen
File WAL and Ring WAL across the respective durable group-commit and
superblock checkpoint boundaries; a missing File GROUP_COMMIT cannot
invent a recovered DATA range. Invalid WAL address order aborts recovery.

The inventory is constructed privately in the storage implementation and
cannot be advanced by a live append or forged using a public constructor.
It is **not** sufficient to authorize LogStart advancement or GC:
reclaimed prefixes are absent, the highest recovered logical WAL address
does not mean complete Kafka offset coverage, and there is no
BrokerEpoch/Incarnation binding or Controller replay horizon.

Next milestone: formally bind a current registered broker identity to
backend-originated recovered/durable evidence and native Kafka source
observations, including failure and ABA scenarios. Only after current
Controller registration revalidation and an atomic feature-gated KRaft
write path may a durable retirement transition be considered.

## Batches 113–116: replay-to-broker-ready local identity pairing

The Java 25 startup replay JavaDoc error is fixed without relaxing
compiler warnings. The extension now records an optional local pairing
of its immutable recovered WAL inventory and the complete broker-ready
BrokerId/Epoch/IncarnationId/ClusterId snapshot supplied by Kafka.
Legacy unbound context, wrong broker ID or a changed registration
revokes or prevents this local pairing. Re-binding to a new incarnation
without re-opening/replaying the WAL is forbidden, including ABA.

An integration test reopens the real Ring WAL under a new broker
extension and validates that the new broker identity is paired only
with records the new extension actually recovered. This is still only
a **local diagnostic**. Broker-ready identity has not been freshly
authenticated against the Controller; it must never be treated as a
source durable horizon certificate or a COMMITTED GC permit.

Next: a Controller-verifiable protocol binding the true Kafka native
Source LogStart window, backend-recovered and new fsynced WAL evidence,
and current KRaft BrokerRegistration inside the serialized Controller
event. No authority write until crash, leader-change, registration
ABA, log-truncation and metadata-replay fault cases all fail closed.
