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

## Batches 117-120: recovered batch coverage against live Kafka source (advisory)

The extension can now ask whether its local startup replayed DATA
completely covers the exact native Kafka LogStart/HW RecordBatch window.
The check requires every Kafka batch boundary, detects missing/reclaimed
or post-start bytes, and compares a broker-ready incarnation pairing
plus two real native source samples with WAL mutation/admission revisions.
A registration ABA or any source/role/WAL change discards the result.

Positive local coverage is **not** a Controller-verifiable durability
certificate. The startup WAL is allowed to have lost reclaimed prefixes,
only its local persisted records are known, and this check does not
establish all required Kafka replicas, current broker registration,
KRaft applied position, authority CAS or any remote reader quiescence.

Next P0 development slice: backend durable/replay *live* horizon beyond
startup, and an authenticated controller revalidation protocol binding
real BrokerRegistration with native Kafka LogStart / LeaderEpoch. Reject
every unverifiable or stale witness; physical COMMITTED object deletion
remains disabled.

## Batches 121–124: local post-start durable-completion diagnostics

A per-partition, atomically published snapshot now distinguishes WAL
operations successfully acknowledged by the backend Future and applied
to the local index from operations still pending or rejected. Copied
DATA, owned DATA, full DATA groups and TRUNCATE are all counted. A
failed/partial Future is never positive. The highest logical byte end
is monotonic across asynchronous completion order, but it is not a
contiguous Kafka offset horizon.

The startup replay inventory and this post-start completion snapshot
are deliberately separate: real File/Ring WAL restarts reconstruct
replay evidence while the new process's live snapshot starts empty.
The read-only native Source/WAL precheck now also validates the
identity of the immutable CAS completion snapshot across the capture.
This strengthens local fail-closed behavior without changing any
native Kafka source offsets, KRaft state or object storage.

**Next actual P0 gate:** bind a backend-originated persistent/replay
range to native Kafka LeaderEpoch/LogStart and the *current* KRaft
BrokerRegistration in a Controller-serialized event, including crash,
leader-change and incarnation ABA tests. Until that contract exists,
a local completed Future is NOT a distributed certification or a
COMMITTED object-delete permission. Keep the frozen MinIO GA release
branch untouched and physical deletion disabled.

## Batches 125–128: current WAL DATA coverage without inventing a certificate

The broker may now inspect **current** index-resident DATA batches,
including genuine startup-replayed bytes and live writes that crossed
the backend's durable Future and index-publication continuation. The
window checker requires contiguous Kafka offset coverage with intact
RecordBatch boundaries; it does not confuse monotonic WAL addresses
with contiguous Kafka offsets. File/Ring, TRUNCATE, error, gap and
cross-partition regression cases are mandatory.

The broker-local native-source/identity precheck exposes separate
read-only STARTUP and CURRENT findings. Both require the same stable
broker-ready incarnation object, two native Kafka source snapshots,
unchanged local leadership, WAL mutation/admission generations and
the immutable live-durability snapshot.

A successful CURRENT finding is **not** an authenticated
distributed durability certificate. The next implementation milestone
must cross the current Controller BrokerRegistration, source-log-start
provenance and committed KRaft event boundary, with well-defined
attestation expiry and fail-closed restart/ABA semantics. All
retirement authority writes and physical COMMITTED deletes stay off.

## Batches 129–132: explicit untrusted Controller source claim contract

The metadata-side non-emitting Controller precheck now has an
untrusted source/WAL claim shape and deterministic consistency checks
for Topic ID, BrokerEpoch/IncarnationId, native Source LeaderEpoch,
Kafka LogStart/HW/LEO and stable local WAL generations.
The current KRaft BrokerRegistration, partition leader, feature gate,
authority state and next write offset are always checked first.
Re-registration, ABA, leader changes and stale authority versions
invalidate previously matching claims.

A forged affirmative coverage flag is still only an **untrusted
statement**. Even perfect field agreement always returns
`SOURCE_LOG_START_NOT_VERIFIED` and emits no authority record.
No durable and authenticated Broker-to-Controller transport has been
created by this checkpoint.

Next actual P0 implementation must select a Kafka-native broker
authentication/transport path, deliver non-replayable source and
backend WAL provenance tied to the current BrokerRegistration, and
revalidate it in the serialized Controller event before atomic
metadata publication. A local complete interval or completed Future
is insufficient. Do not enable physical COMMITTED deletion.

## Batches 133–136: real broker-local claim capture and serialized Controller preflight

- **133 — broker-origin local window fields.** The shared storage extension
  now exposes a read-only `CurrentSourceWalClaim` from the same actual
  Kafka-native Source, immutable broker-ready pairing and backend WAL indexed
  batch coverage used by the existing diagnostic. The claim contains the
  Cluster ID, Topic ID/partition, BrokerId/Epoch/IncarnationId,
  LeaderEpoch/local generation, LogStart/HW/LEO and observed per-partition
  WAL mutation/admission revisions. No caller-supplied coverage flag can
  turn an empty or incomplete window into a positive capture.
- **134 — fail-closed stable snapshot.** Capture only returns an envelope for
  `INDEXED_BATCH_WINDOW` after identical double source observations, the
  same startup pairing object and unchanged WAL generations/completion
  snapshot. Missing broker readiness, Kafka offset gaps, empty windows,
  source drift, incarnation ABA or WAL activity all return no claim.
- **135 — Controller event-queue recheck.** A *package-private*, non-network
  `QuorumController.preflightUntrustedPartitionRetirement` runs the existing
  current KRaft feature/registration/leader/authority CAS and untrusted-claim
  checks within `appendWriteEvent`, respects request deadlines and emits an
  empty KRaft record list. It is serialized against Controller state changes.
- **136 — deterministic regressions.** Nine broker-origin capture tests and
  two real Controller event-queue tests are mandatory; the strict named
  Java/Scala test inventory becomes **473 methods across 30 suites**.

**Security limitation:** `CurrentSourceWalClaim` is an UNSIGNED primitive
envelope. It can be copied, forged, delayed or replayed after leaving the
broker process. KRaft `BrokerRegistration` and Kafka `CLUSTER_ACTION` ACL
alone do not cryptographically bind an RPC sender to one BrokerId and
IncarnationId (deployments may share an inter-broker principal). This
checkpoint DOES NOT add a Broker-to-Controller RPC, authenticated transport,
freshness challenge, replicated/fsynced cross-broker certificate, authority
writer, COMMITTED reference retirement, or physical MinIO DELETE. Do not
mistake a positive local claim or queued Controller diagnostic for a GC grant.

**Next implementation:** add a feature-gated Kafka-native transport for the
broker-produced read-only data, establish per-broker authenticated channel
identity (not a shared principal or caller-declared BrokerId), and provide
single-use anti-replay/source-validation semantics *inside* the Controller
event before any KRaft write is allowed. Keep the frozen MinIO GA branch
at `9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

## Batches 137–140: CI compile fix and one-shot Controller probe boundary

The immediately preceding Batch 133–136 source-SHA validation ended
**16/16 FAILURE** due to a single shared `:metadata:compileJava` failure:
`QuorumController` referenced `Objects.requireNonNull` without importing
`java.util.Objects`. The new Batch 137 code explicitly fixes that import.
Those failures were build-stage failures, **not** proof that the 11 new
deterministic regressions or three-broker MinIO scenarios passed or failed.

- **137:** restore strict Java 25 compilation without loosening warnings.
- **138:** add a fail-closed immutable `PartitionRetirementBrokerPrincipalPolicy`
  requiring an exact, nonanonymous, nonwildcard, **unique** principal per
  BrokerId. Shared inter-broker credentials cannot impersonate a unique
  broker process. The production Controller's policy is `DENY_ALL`; there
  is **no runtime configuration or authenticated transport** at this stage.
- **139:** add a small, nonpersistent `PartitionRetirementSourceProbeRegistry`
  issuing unpredictable UUID correlation nonces only after the policy
  matches. Nonces are bound to ClusterId, the exact candidate's
  TopicId/partition, BrokerId/Epoch/Incarnation, Source LeaderEpoch and
  authority-generation coordinates. They are valid for **less than 5 s**,
  may be consumed **once**, have a **128-probe** outstanding cap, are burnt
  on malformed or cross-principal responses, and are lost on restart,
  controller epoch changes and leadership renouncement.
- **140:** make probe issuance/consumption two **package-private,
  non-network** `QuorumController` diagnostic events, serialized with
  registration/leader/authority events. A consumed nonce triggers a fresh
  Controller KRaft feature/leader/registration/CAS check. A match still
  resolves to `SOURCE_LOG_START_NOT_VERIFIED`, never a write grant;
  every event emits `List.of()` as its record list. Two Controller
  regressions and 25 policy/probe unit regressions are enrolled in the
  mandatory inventory: **473 → 500 methods across 32 suites**.

**Still blocked:** This is preparation for, **not** a functioning authenticated
Broker→Controller RPC. KafkaPrincipal equality by itself cannot prove the
authentication mechanism or physical broker origin. The deployment must
separately establish dedicated broker credentials and trusted principal
mapping; the current application intentionally offers no way to enable the
policy. A nonce also does not prove backend fsync, WAL replay continuity,
replica recoverability or native Source LogStart. No new metadata schema,
KRaft writer, COMMITTED reference retirement or MinIO physical DELETE is
enabled. Retain frozen MinIO GA `9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

The next actual transport milestone must wire a version-gated Kafka-native
broker RPC to network-authenticated principal identity with an explicit
unique broker credential contract, sample source/WAL from the actual
storage provider after the challenge, and revalidate the result inside
the serialized Controller event. Production GC remains fail closed.

## Batches 141–144: native-source challenge pairing, not a network certificate

The Batch 137–140 commit `704af53603dd1982efeb78143a7ee8875634ba65`
has **15/16 workflow SUCCESS**. The single failing job was
`:storage:checkstyleMain`: `BrokerLocalStartupReplayPrecheck`'s
`CurrentSourceWalClaim` compact constructor had BooleanExpressionComplexity
**12 > 5** at line 106. This is a common static-style gate, not a demonstrated
MinIO/WAL semantic failure. Batch 141 splits identity, generation and
Kafka offset bounds into small independently readable guards; the
Checkstyle threshold remains unchanged.

- **141 — compile/checkstyle recovery.** Remove the overly complex Boolean
  expression without weakening field validation or changing GC enablement.
- **142 — cross-module request/report DTOs.** Add immutable
  `PartitionRetirementSourceProbe.Request` and `Report` shapes in
  `clients`. Exact Topic ID/partition, Cluster ID, BrokerId/Epoch/
  Incarnation, LeaderEpoch, expected authority offset, nonce, proposed
  write position, native LogStart and current WAL revisions are preserved
  across Controller and broker modules. Their public constructors perform
  only shape validation: they are **not** an authenticating wire codec.
- **143 — actual broker-local challenge pairing.**
  `BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport` is
  read-only and generates a `Report` only after an actual double native
  Kafka Source observation, stable broker-ready identity and exact WAL
  indexed RecordBatch coverage. The returned local observation must
  match the Controller's challenge for immutable Topic ID/partition,
  cluster, BrokerId/Epoch/Incarnation, LeaderEpoch and native LogStart.
  Invalid/missing coverage, ABA, changed source or WAL admission during
  the capture yields no report. The echoed nonce is **not** a signature.
- **144 — consume inside existing Controller event.**
  `Challenge.toProtocolRequest` / `fromProtocolRequest` preserve the
  exact CAS coordinates. The package-private Controller
  `inspectRetirementSourceReport` adapts a transport-neutral report to
  its **existing** single queued nonce consume + live KRaft recheck; it
  does not enqueue a nested second event. Invalid and forged reports are
  never authorization. The event emits zero KRaft records.

**Regression delta:** 12 broker-source tests, 3 Controller nonce-protocol
mapping tests and 2 real Controller tests were added. Strict named
Java/Scala evidence is now **517 methods, 32 suites** (was 500/32).
All cases must run under Java 25 with zero skips before this SHA can be
considered passing.

**Remaining high-risk prerequisite:** This is a transport-**neutral**
model and in-process adapter, **NOT** yet a Kafka-native broker RPC.
No new API key or BrokerHeartbeat extension is advertised. Adding a new
Kafka API without feature negotiation and explicit sender identity would
prematurely expose an unusable endpoint. The next implementation needs
a dedicated version-gated Controller-listener request/response with a
trusted per-broker authenticated principal mapping, asynchronous
challenge round trip, actual storage-provider sampling after the
challenge, replay protection and fresh serialized Controller checks.
A Controller-matched report is still unauthenticated Source/WAL input.
A new KRaft authority writer, COMMITTED reference retirement, and
physical MinIO object DELETE remain forbidden. Frozen MinIO GA remains
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

## Batch 145 — repair the shared Kafka clients Java 11 source boundary

The Batch 141–144 SHA `8de1ebde1924614f8ae73bcf5937940ba387919c`
finished **16/16 workflow FAILURE**, all blocked before runtime testing
at `:clients:compileJava`: the newly introduced transport-neutral
`PartitionRetirementSourceProbe.Request/Report` used Java `record`
syntax, whereas Kafka's `clients` module compiles with `-source 11`
even in Java 25 CI. The successful older gates do not apply to this SHA.

Keep Kafka clients' Java 11 bytecode contract. Convert the two diagnostic
shapes to final immutable classes with final fields, exactly the same
read accessors, null/domain validation, defensive identity semantics,
and explicit `equals/hashCode`. Do **not** change the module's Java
source compatibility or API version advertisement to conceal the
failure. The existing Controller challenge equality tests exercise
value round trips. All GC mutation and COMMITTED physical DELETE paths
remain disabled.

This change is a focused compile-blocker repair checkpoint. The next
SHA must run the full 16-workflow CI again before a positive GA claim.

## Batches 146–148: restore Java 11 compilation and enforce actual Controller regression execution

Prior checkpoint `e8526444f20c0c2cf762f282dd94aaf038b90c36`
finished **16/16 Workflow FAILURE**, blocked in `:clients:compileJava`.
The new immutable `PartitionRetirementSourceProbe` did preserve the Java 11
language level, but its generated `equals()` compared `that.*` inside
`if (this == other)` *before declaring `that`*. Both nested value types
caused 18 identical undefined-variable compiler errors. No subsequent
MinIO/WAL execution on that SHA is evidence of correctness.

- **146 — verified Java 11 correction.** Reconstruct both `equals()`
  implementations in correct order: identity fast-path, Java 11
  `instanceof`, cast to the same immutable value type, then bounded
  field-equality guards. Keep matching `hashCode()` and all constructor
  validations; do not raise Kafka `clients` source/bytecode levels.
  Exact corrected source was compiled locally via
  `javac --release 11 -Xlint:all -Werror` against a minimal Kafka Uuid
  API stub, and an executable value/invalid-window harness passed.
  This is a focused local compile test, NOT a full Kafka Gradle build.
- **147 — deterministic value contracts.** Add eight Java/Scala-gate
  regression tests in `PartitionRetirementSourceProbeValueTest` for
  reflexive/symmetric equality, equivalent reconstructed instances,
  hash consistency, nonce, Topic ID, broker epoch/incarnation,
  leader epoch, KRaft position, Kafka LogStart, WAL mutation and
  admission revision changes, invalid requests and empty source windows.
- **148 — close CI evidence ownership.** The existing Shared Storage
  workflow selected a small, explicit metadata-test allowlist while
  `REQUIRED_TESTS` also demanded `QuorumControllerTest`,
  `PartitionRetirementBrokerPrincipalPolicyTest` and
  `PartitionRetirementSourceProbeRegistryTest`. This would have failed
  the final evidence checker even after compilation recovered.
  Add every required Controller test to the actual Gradle
  `--tests` command, explicitly select the six mandatory
  QuorumController methods, and add a fast `:clients:compileJava`
  gate before storage/MinIO tests. Trigger the workflow on shared
  Probe DTO and Controller Probe code/test changes. The strict
  inventory becomes **525 mandatory methods across 33 suites**.

No new wire API, privileged Controller builder setting, or authenticated
Broker→Controller sender has been introduced. Do not interpret the
transport-neutral request/response values as signed attestations or a GC
grant. Production Broker principal allowlist remains DENY_ALL; KRaft
authority writing, COMMITTED logical reference retirement and
physical MinIO deletion remain disabled. Frozen MinIO GA stays on
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.
