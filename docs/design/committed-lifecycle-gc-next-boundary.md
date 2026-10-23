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

## Batches 149–152 — exact ImportControl repair and unstable heartbeat wire reservation

The prior `f229b4d53a4f8a4615923ebe2e85110c3f537bc8` run reached
real Java 25 storage and MinIO tests, but the main Shared Storage job
failed during `:metadata:checkstyleMain` and `:metadata:checkstyleTest`:
five `ImportControl` violations for
`org.apache.kafka.common.replica.PartitionRetirementSourceProbe` and its
two nested values. This was a missing dependency-layer allowlist,
**not** evidence that MinIO, WAL or Controller safety assertions failed.

- **149 — exact ImportControl approval.** Allow only the outer
  `PartitionRetirementSourceProbe` plus its `Request` and `Report`
  nested value types from `common.replica` to metadata. Do not
  broadly enable that package or weaken Checkstyle rules.
- **150 — fail-fast CI ownership.** Move metadata main/test Checkstyle
  into the early `:clients:compileJava` preflight, before expensive
  `core:test`, `storage:test` and MinIO. Remove redundant Checkstyle
  invocation from the later metadata test command, keep its mandatory
  JUnit `--tests` selection, and add `import-control.xml` to path triggers.
- **151 — version-negotiated Kafka-native framing.** Reserve a single,
  **unstable** v3 `BrokerHeartbeat` tagged field in each direction:
  `SourceWalProbeReport` (request tag 2, old tags 0/1 unchanged) and
  `SourceWalProbeChallenge` (response tag 0). Both are optional/nullable.
  Version v3 is declared **unstable only on the Request specification**
  per Kafka `MessageSpec` generator contract. Ordinary controller
  ApiVersions continue to advertise v0–v2; an explicit unstable-features
  environment can opt into v3. The existing Kafka ApiKey 63 and
  Controller listener are reused, so there is no speculative new public
  ApiKey allocation. Legacy field order and semantics are unchanged.
- **152 — downgrade/roundtrip regression.** Add six focused
  `clients` tests covering production v2 max, unstable v3 opt-in,
  optional fields absent from v0–v2, exact v3 report/challenge
  serialization and explicit rejection of attempts to serialize
  non-null v3 evidence into a v2 message. Execute the suite as
  a fail-closed, early Java 25 workflow stage before MinIO.

**Critical limitation:** The v3 schema is a *transport reservation*,
not a functioning report processor or Controller challenge issuer.
No Broker sender, KafkaListener handler for the new fields, unique
network-authenticated per-broker identity or signed durable evidence
is implemented by this checkpoint. Existing heartbeat code ignores
these fields even when unstable API versions are enabled.
`CLUSTER_ACTION` alone is never sufficient provenance.
`PartitionRetirementBrokerPrincipalPolicy.DENY_ALL` remains active.
No KRaft authority writer, COMMITTED reference retirement, or physical
MinIO object deletion is enabled. The frozen MinIO GA release remains
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

**Next:** authenticate the network sender to a unique registered BrokerId
and incarnation, connect v3 challenge issuance and broker sampling behind
explicit feature negotiation, and burn the one-time nonce inside the
serialized Controller event before any KRaft mutation is even considered.
Then prove old-broker v2 fallback, controller failover and 3-broker
durability under real MinIO.

## Batches 153–156 — correct metadata import boundary and deny-only v3 ingress

The **previous** `70cd5293c11fac3b73b3d3b7d48f3e598d3ffabb`
completed **16/17 GitHub Actions SUCCESS**, with the sole Shared Storage
job blocked in early `:metadata:checkstyleMain`. The two remaining
`PartitionRetirementSourceProbe` imports were still rejected because
the preceding patch had edited `checkstyle/import-control.xml`,
whereas the metadata module actually uses
`checkstyle/import-control-metadata.xml`. This is a rules-location
mistake; the completed WAL, real MinIO and Controller HA checks were
otherwise successful.

- **153 — correct import ownership.** Restore generic
  `import-control.xml` to its earlier value and allow only the three
  Source/WAL diagnostic value classes inside the `controller` subpackage
  of `import-control-metadata.xml`. Update Shared Storage workflow
  path filters to include the *actual* metadata rules file. Do not
  allow all `common.replica`.
- **154 — parse the real v3 heartbeat transport.** Add a fail-closed
  `PartitionRetirementHeartbeatReportGate` that reconstructs the existing
  typed `PartitionRetirementSourceProbe.Request/Report` from an optional
  generated `BrokerHeartbeatRequestData.SourceWalProbeReport`.
  BrokerId/BrokerEpoch come from the outer heartbeat, while nonce,
  immutable TopicId/partition, incarnation, source epoch, CAS,
  LogStart/HW/LEO and WAL revisions are checked by the established
  immutable contract. Invalid data is rejected, not trusted.
- **155 — enforce the default-deny network ingress.** At the start of
  `QuorumController.processBrokerHeartbeat`, before contact-time
  metrics, lockless broker heartbeat tracking or a serialized metadata
  event, reject non-null v3 reports unless the protocol is v3 and its
  authenticated Principal matches the dedicated per-broker policy.
  Even a structurally valid report from a matching Principal yields
  **UNVERIFIED** and is refused: no probe or authority writer is
  invoked. An absent report retains the legacy heartbeat flow.
  The production allowlist remains `DENY_ALL`.
- **156 — deterministic security regressions.** Add eleven unit tests
  for absent v2 reports, dedicated-principal matching, version
  fencing, anonymous/foreign principals, malformed nonce, epoch,
  source window and WAL revisions, plus two actual QuorumController
  negative tests proving the rejected report cannot change KRaft
  records. Strict named mandatory inventory becomes **540 methods
  across 34 suites** (previously 525/33), with all new tests selected
  in the main `:metadata:test` CI command.


  Additionally, the broker-local adapter can now turn an exact
  challenge-matched `SharedUnifiedLog`/WAL capture into the generated
  v3 tagged `SourceWalProbeReport` structure. Two real storage
  regressions prove native capture survives a v3 encode/decode round
  trip and an old broker incarnation cannot emit a report. This
  adapter does not itself send network packets. The total mandatory
  inventory is **540 methods across 34 suites**.

**This is only a negative ingress, not a working end-to-end protocol.**
The broker has no integrated actual-WAL report sender, Controller does
not yet produce a challenge on real network heartbeats, and there is
still no configured per-broker authenticated transport. All positive
Source/WAL reports continue to be denied. To advance, implement a
controller-initiated, short-lived challenge bound to BrokerRegistration
and a dedicated authenticated principal, then request actual native
Kafka/WAL sampling through the Broker lifecycle without breaking
v2 fallback. KRaft authority CAS writing, COMMITTED reference retirement
and physical MinIO DELETE remain strictly disabled. The frozen MinIO
GA ref `9a28c11b6d97ecacafe1b8566cde5cc34d604889` is unchanged.

## Batches 157–160: SpotBugs evidence and real broker heartbeat challenge ingress

Prior checkpoint `e86a1d5cfcbc928632f8f7ef282fe0cd70336ea5`
finished **16/17 CI SUCCESS**. All Java 11/protocol, Checkstyle, storage,
MinIO, Ring WAL and Controller HA gates completed, but the main Shared
Storage job failed at `:metadata:spotbugsMain` after the storage suite
passed. Its GitHub artifact contained storage SpotBugs/metadata
Checkstyle but **not metadata SpotBugs**, so the precise rule cannot
yet be determined from that run. Do NOT disable SpotBugs or invent an
issue-specific fix without its report.

- **157 — early fail-closed static evidence.** Run
  `:metadata:spotbugsMain` together with the existing Java 11 clients
  compile and metadata Checkstyle preflight, before heavy core/storage/
  MinIO tests. Preserve `metadata/build/reports/spotbugs` in the
  `shared-storage-static-analysis` artifact even on failure. The next
  SHA must expose the actual SpotBugs finding and fix it, with the
  rule itself still enabled.
- **158 — broker-side challenge validation.** Add a read-only
  `BrokerHeartbeatSourceProbeChallengeGate`. An optional unstable
  v3 `SourceWalProbeChallenge` is rejected unless the response
  belongs to the **exact negotiated request version 3**, a broker that
  is **registered and RUNNING**, the current ClusterId,
  BrokerId/Epoch/IncarnationId, nonzero nonce/TopicId and valid
  Source LeaderEpoch/authority CAS/LogStart bounds. The immutable
  Java 11 Request reconstruction is available for diagnostics only,
  and positive shape agreement still yields `MATCHED_UNTRUSTED`.
- **159 — real lifecycle receive hook.** The standard
  `BrokerLifecycleManager.BrokerHeartbeatResponseEvent` inspects
  present v3 challenge tags on successful heartbeat responses on its
  serialized event thread. Invalid/legacy challenges are ignored.
  Even matching ones are **discarded**, not stored, sampled or sent
  to Controller. Normal v0-v2 readiness, offline-directory acknowledgement,
  shutdown and heartbeat cadence are unaffected. There is deliberately
  no privileged producer, feature bypass or controller response writer.
- **160 — regression/CI boundary.** Add ten deterministic JUnit tests
  under `:server:test` for v2/v3 compatibility, registration/fencing,
  BrokerId/Epoch/Incarnation ABA, wrong ClusterId, invalid UUIDs,
  malformed CAS/LogStart and the non-authorizing matched outcome.
  Require all ten named JUnit XML results with zero skips/failures
  in the main Java 25 CI and upload the server test reports. Keep the
  **540 methods / 34 suites** existing storage/metadata evidence
  unchanged; the ten server tests run as a separately hard-gated suite.

This is **not** an authenticated end-to-end Broker→Controller
Source/WAL exchange. The producer still lacks an installed asynchronous
source/WAL sampler, and Controller does not issue real v3 challenges.
A Controller's permission to use `CLUSTER_ACTION` does not bind it to
an independently authenticated Broker incarnation. Production
principal policy remains `DENY_ALL`; no KRaft authority writer,
COMMITTED reference retirement, or physical MinIO DELETE is enabled.

**Next:** use the new metadata SpotBugs artifact to repair the exact
failure; then introduce an explicitly configured per-broker mTLS/SASL
identity policy and asynchronous challenge→native Source/WAL capture→
response correlation. Revalidate and consume one-time nonce **inside
one Controller serialized event** before even considering an authority
writer. Prove Controller rotation and multi-broker durable recovery
using real MinIO. Frozen MinIO GA remains
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

## Batch 157–160 corrective CI checkpoint: verified SpotBugs and GA fingerprint

The first `a7a1df08` run exposed **two real build blockers**
while the other workflows were still executing:

1. `Shared Storage GA Workflow Consistency` rejected the new
   `server/src/main/java/.../BrokerHeartbeatSourceProbeChallengeGate.java`
   as outside the frozen production-fingerprint paths.
   Instead of weakening the manifest or adding a broad new Java prefix,
   keep the challenge validator as a **package-visible static nested
   class inside the already fingerprinted `BrokerLifecycleManager.java`**.
   Remove the new standalone Java production file and its explicit
   workflow trigger. The full Gate JUnit suite remains a distinct
   `:server:test` source and still runs under strict JUnit XML evidence.
2. The newly retained `metadata/build/reports/spotbugs/main.html`
   contained exactly **one medium-priority warning**, rule
   `DCN_NULLPOINTER_EXCEPTION` at
   `PartitionRetirementHeartbeatReportGate.inspect` line 67.
   Do not catch NullPointerException from untrusted wire fields.
   Explicitly validate nonce, cluster, TopicId and BrokerIncarnationId
   against `null` before constructing the immutable report and catch
   only `IllegalArgumentException` for impossible value ranges.
   One more negative Controller JUnit case covers all four missing
   required reference fields. Mandatory Java/Scala evidence is now
   **541 methods, 34 suites**, plus 10 separately gated Broker tests.

This patch fixes the **actual** static-analysis rule and the **actual**
release-manifest failure without broadening trust or changing the
frozen MinIO GA branch. It does not authorize reports, source/WAL
attestation, metadata retirement or COMMITTED physical object GC.

## Batches 161–164 — fix real metadata JUnit failures and bridge volatile challenge to heartbeat v3

On HEAD `4d5a6d4635d8b81482c17468dfcc0a08a347d22c`,
GA Workflow Consistency, Java 11, metadata Checkstyle/SpotBugs, clients wire
tests, dedicated broker challenge tests and storage JUnit passed. The
Shared Storage main job subsequently hit two **actual metadata JUnit
assertion failures**, unrelated to native MinIO/WAL:

- **161 — separate offset domains.** `PartitionRetirementSourceProbeValueTest`
  incorrectly expected `KRaft nextAuthorityOffset=0` and
  `Kafka nativeLogStartOffset=1` to be mutually invalid.
  They are positions in unrelated logs. Replace that assertion
  with an actually invalid CAS (`expectedPreviousAuthorityOffset=20`,
  `nextAuthorityOffset=20`) and explicitly assert the orthogonal
  native offset case is valid. Do not introduce a fabricated
  cross-log numeric inequality in production.
- **162 — isolate Controller records from bootstrap.** A
  `QuorumControllerTest` compared `clientEnv.allRecords().size()`
  immediately after `activeController()` against the count after
  its read-only preflight. Normal asynchronous KRaft bootstrap
  records caused expected `0`, actual `5`. All early Source/WAL
  no-write tests now compare **only**
  `PartitionRetirementAuthorityRecord` instances, so startup
  FeatureLevel/ControllerRegistration metadata cannot mimic
  a retirement write. Still assert zero retirement record emission.
- **163 — non-authorizing transport bridge.** Extend the existing,
  serialized Controller-only `PartitionRetirementSourceProbeRegistry.Challenge`
  with a `toHeartbeatChallenge` v3 tagged-field mapper and the
  reverse `fromHeartbeatChallenge` conversion. Preserve nonce,
  cluster, TopicId, partition, BrokerId/Epoch/Incarnation, source
  LeaderEpoch, expected/next KRaft CAS and Kafka LogStart without
  coercing unrelated offsets. Explicitly reject null identity
  fields before constructing the immutable typed request.
  **Encoding is not issuance.** The Controller keeps DENY_ALL and
  still has no production heartbeat reply challenge issuer.
- **164 — wire/anti-replay evidence.** Four deterministic registry
  JUnit tests exercise a genuine v3 encode/decode round trip that
  still results only in `MATCHED_UNTRUSTED`, the one-time nonce
  consumption, malformed wire identities, Broker incarnation
  alteration burning its nonce, and refusal to silently encode a
  populated v3 challenge under stable v2. Mandatory Java/Scala
  methods increase **541 → 545 across 34 suites**; separately
  hard-gated broker and clients wire regressions remain unchanged.

This feature branch deliberately leaves actual Controller challenge
issuance, Broker-side native WAL sampling **network send**, per-broker
mutually authenticated principal configuration, Controller nonce
recheck under the active KRaft epoch, and source/WAL durability
attestation for future checkpoints. There is no authority writer,
COMMITTED logical reference retirement, or physical MinIO DELETE.
The frozen MinIO GA ref remains
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

## Batches 165–168 — Controller-to-Broker v3 Challenge response plumbing (still default-deny)

**Baseline:** `0f223df37b7660c736284b4b5ec4819550826216`
completed **17/17 GitHub Actions SUCCESS**, including the Java 25,
metadata Checkstyle/SpotBugs/JUnit, real MinIO, 3-broker E2E, Ring WAL,
WAL crash and Controller HA gates. The independent MinIO GA release
branch remains frozen at `9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

- **165 — Controller-volatile pending source:** Add
  `PartitionRetirementSourceProbeRegistry.pendingForHeartbeat`.
  Only a nonce **already issued in the serialized Controller event**
  can be selected; no new nonce is minted or expiry extended.
  BrokerId/Epoch, unique authenticated principal and strictly
  positive remaining TTL must match. A lookup does not consume the
  one-use nonce. Controller leadership invalidation clears it.
- **166 — serial authority revalidation:** Inside
  `QuorumController.processBrokerHeartbeat`'s existing Controller
  serialized event, consider carrying a pending Challenge **only**
  when an exact unstable v3 heartbeat is negotiated, the broker
  is caught up and not fenced/shutting down, the request does not
  seek fencing or shutdown, and the normal heartbeat produces no
  KRaft records. The dedicated per-Broker principal mapping must
  match. The pending candidate must again pass the *current*
  finalized feature gate, BrokerRegistration, TopicId/partition
  leadership, broker incarnation and KRaft authority CAS checks.
  Otherwise the original heartbeat result is returned unchanged.
  A successful diagnostic reuses the existing KRaft result
  (preserving the original record list and atomicity) and adds
  only an **immutable** Source Probe Request to the reply.
- **167 — actual Controller response encoder:** Extend
  `BrokerHeartbeatReply` with a nullable immutable Challenge,
  preserving the original four-argument constructor. The
  `ControllerApis` response builder maps this to the optional
  `BrokerHeartbeatResponseData.SourceWalProbeChallenge` **only
  for version 3**, never for stable v0–v2 or broker fencing,
  controlled shutdown or shutdown. No API Key allocation,
  metadata writing or object deletion is involved.
- **168 — deterministic gates:** Four registry tests cover
  matched/wrong Principal, wrong BrokerId/Epoch, expiration,
  one-use consumption and Controller leadership revocation.
  Three ControllerApis tests cover stable version silence,
  refused fenced/shutdown replies and exact v3 serialization
  round-trip. Main CI now runs and XML-verifies the three Scala
  tests, and the strict Controller Java/Scala inventory increases
  **545 -> 550 named methods, 34 suites**. Trigger
  `BrokerHeartbeatReply.java` changes through the existing
  Shared Storage main workflow. Exact imports in the metadata
  module retain the restricted ImportControl boundary.

**Important security and GA boundaries:**
No production code configures a dedicated per-Broker Principal
(`DENY_ALL` remains the default), and the Kafka retirement feature
is not finalized in production. Thus this plumbing cannot create
live Challenge offers in default deployments. The broker receives
the optional field but its lifecycle handler still validates and
**discards** it: there is no asynchronous native Source/WAL sampler
bound to a broker-specific authenticated return channel. Even an
otherwise correct report is still rejected before normal Controller
heartbeat state or KRaft event handling, and no COMMITTED physical
GC can happen. Future work must finish authenticated Broker identity,
native WAL evidence, nonce consume under a single Controller event,
multi-replica durability and KRaft authority writer before enabling
reference retirement or MinIO DELETE.

  Additionally, one exact `QuorumControllerTest` runtime regression
  registers a Broker and sends a negotiated v3 heartbeat under
  the production `DENY_ALL` policy. It proves that no Source/WAL
  challenge is issued and no retirement authority record is emitted.
  The new test is explicitly selected by the main CI gate.

## Batches 169–172 — bounded asynchronous Broker-local Source/WAL capture (no wire Report)

The prior `794f6d61edbddde2778dd9d0a3aa78b32d250bf6` development
checkpoint finished **17/17 GitHub Actions SUCCESS**. The frozen
MinIO GA release `9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remains untouched.

- **169 — explicit disabled-by-default sampler seam.**
  `BrokerLifecycleManager` now declares a transport-neutral
  `SourceWalProbeSampler`, which must promptly return a
  `CompletionStage<Optional<PartitionRetirementSourceProbe.Report>>`.
  Production constructors do not install one. An explicit
  `setSourceWalProbeSampler` event replaces the provider on the
  existing Broker lifecycle event queue and fences all prior
  captures. The existing `storage` native Source/WAL reader is
  **not yet bound** to this seam.
- **170 — nonce/async completion fence.** On an actual v3
  Controller heartbeat challenge, the Broker first validates the
  request version, registered RUNNING status, ClusterId,
  BrokerId/Epoch/Incarnation, nonce and source CAS. Only then, and
  **only with an installed sampler**, can it initiate a local
  asynchronous diagnostic capture. At most **one provider Future**
  may be outstanding, even across provider replacement until the old
  callback completes; duplicate nonce offers do not start a
  second capture. Its result is returned to the serialized
  lifecycle queue and matches the original immutable challenge.
  A late completion from a replaced provider/nonce is discarded
  by **capture object identity**, not solely by nonce/epoch value.
- **171 — bounded result lifetime and lifecycle fence.**
  A captured Report is only staged in one in-process diagnostic
  slot; it is never put onto a heartbeat request. The single-use
  diagnostic mailbox validates registered RUNNING status, the
  precise BrokerId/Epoch/Incarnation and the original 5-second
  monotonic time window, with safe signed nanoTime wrap handling.
  Registration changes, controlled shutdown, broker shutdown and
  provider replacement revoke staged/in-flight diagnostic
  state. A repeated nonce remains blocked even after revocation.
  Fenced, shutdown or uncaught-up heartbeat responses trigger
  no sampling.
- **172 — deterministic asynchronous lifecycle tests.**
  Add **14** server-side JUnit regression cases for one-shot
  completed evidence, duplicate/replayed nonce, single-flight
  backpressure, late callback after revocation, wrong BrokerId,
  stale epoch/incarnation, fencing/unregistration, null/failed
  Future result, mismatched Request, exact TTL expiration,
  monotonic clock wrap and backwards clock. The primary
  `Shared Storage` CI executes and XML-verifies these
  14 named methods without skips, along with the prior 10
  Broker challenge tests. Existing metadata/storage evidence
  remains **550 named methods across 34 suites**; this is a
  separate 14-method Broker suite rather than an artificial
  increase in the metadata inventory.

**Not yet enabled or authorized:** A real
`SharedStorageExtension` + native Kafka Source adapter has
not been installed as a Broker sampler, and captured reports are
**not attached to `BrokerHeartbeatRequest`**. Production still
uses `PartitionRetirementBrokerPrincipalPolicy.DENY_ALL`.
Controller still refuses every inbound Source/WAL Report, no
nonce can authorize KRaft metadata, and no COMMITTED logical
reference retirement or physical MinIO DELETE is reachable.
Authenticating each Broker to its current BrokerRegistration,
connecting the actual native sampler and safely returning one
nonce-bound report remain follow-up work. Do not call this
an authenticated end-to-end GC implementation until the real
multi-Broker MinIO/Controller HA run proves those properties.

## Batches 173–176 — Checkstyle recovery and bounded multi-nonce replay fencing

The prior development HEAD `71a23c6e45efaf4afbe26df9f6af4baa48e06fa8`
completed **16/17 GitHub Actions SUCCESS**. The sole failed
`Shared Storage` main workflow stopped at **two exact Checkstyle
findings**, confirmed from the Java 25 job logs:

- `BrokerLifecycleManager.java:765`, `NPathComplexity` **972 > 500**.
- `BrokerLifecycleManager.java:1027`,
  `BooleanExpressionComplexity` **6 > 5**.

No correctness failure is inferred for the later Broker JUnit tests:
they did not run after `:server:checkstyleMain` failed. MinIO,
Controller HA, Ring WAL, WAL Crash, and the other 15 completed
workflows passed; the independent MinIO GA release remains frozen at
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.

- **173 — refactor the real BrokerHeartbeat ingress.**
  Split the monolithic `inspectSourceWalChallenge` into small
  `eligibleSourceWalChallenge`, `scheduleSourceWalCapture` and
  `enqueueSourceWalCapture` helpers, retaining the existing
  Controller v3/registered RUNNING Broker/nonce checks. Exceptions
  while initiating asynchronous capture release only the
  appropriate in-flight fence. Callback state changes still
  enqueue onto the Broker lifecycle event queue.
- **174 — fail-closed simplified eligibility.**
  Refactor `SourceWalProbeCaptureState.eligible` to reject
  unregistered/non-RUNNING Broker state and expired or
  backwards monotonic time before separately verifying
  BrokerId/Epoch/Incarnation. Retain the five-second strict TTL
  and `nanoTime` wrap semantics. Do not alter Checkstyle
  thresholds or add suppressions.
- **175 — bounded multi-nonce replay history.**
  Replace the unsafe one-element `mostRecentNonce` fence
  with a `LinkedHashMap<Uuid, Long>` tracking up to
  **64 distinct accepted nonces** for the diagnostic TTL.
  Previously, nonce A could be presented again after nonce B
  had been processed. Now all nonces observed within five seconds
  are blocked, including after provider reconfiguration or
  failed/empty captures. Expire entries by monotonic elapsed
  time only; if all 64 entries are still live, reject new
  challenges instead of evicting a still-valid replay fence.
  Retain the single active native I/O Future across
  provider changes. This history is Broker-local diagnostic
  state, not Controller authority.
- **176 — deterministic replay and mandatory CI.**
  Four new `BrokerHeartbeatSourceProbeCaptureStateTest`
  cases cover interleaved nonce replay, bounded saturation
  and expiry, revoked nonce persistence across changes and
  signed `nanoTime` wrap. The main CI selects and requires
  all **18** capture-state tests, as well as the existing
  10 Broker challenge tests, and explicitly retains
  `:server:checkstyleMain` and `:server:spotbugsMain`.
  The independent Java/Scala storage/metadata inventory
  remains **550 methods / 34 suites**.

**Safety:** There is still no production-installed native sampler
or network Source/WAL Report sender. Controller production
per-Broker Principal mapping is `DENY_ALL`; no report becomes
a KRaft retirement grant, no COMMITTED reference retirement
or physical MinIO DELETE is enabled. This corrective development
checkpoint requires its **own exact HEAD CI results** before
calling it validated. Following work must attach the real
native Source/WAL sampler behind an authenticated Broker identity
and consume Controller-issued nonces inside the serialized
Controller event, with MinIO and Controller HA evidence.

## Batches 177–180 — real native WAL capture adapter, deliberately no production activation

**Verified baseline:** `9f086c90204e49f3ef2e79994d6c6fbf1e77f584`
completed **17/17 GitHub Actions SUCCESS**. Frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remains unchanged. The sole objective of this checkpoint is
to bridge the Broker's real current Kafka log to the *existing*
strict Source/WAL evidence precheck, without pretending this
is an authenticated GC release.

- **177 — native-only extension adapter.** Extend the existing
  `SharedStorageExtension` with
  `captureSourceWalProbe(challenge, log)`. Reject classic/null
  logs, closed extensions, missing Broker-ready identity,
  wrong registered BrokerId, stale BrokerEpoch/incarnation,
  recreated TopicId or missing current WAL coverage. Accept
  only `SharedUnifiedLog`, build the partition identity from
  exact TopicId and partition coordinates, and call the existing
  `BrokerLocalStartupReplayPrecheck.captureCurrentSourceWalReport`.
  The precheck, not this adapter, remains the authority for the
  native Source offset/HW/LEO, actual role/leader epoch,
  broker-registration pairing, WAL index and mutation fences.
  This is an **unsigned read-only local diagnostic**.
- **178 — genuinely asynchronous factory.** Add
  `readOnlySourceWalSampler(logLookup, executor)` on the existing
  extension, returning a deferred `CompletableFuture`.
  Both TopicId lookup and native Source/WAL inspection run
  **only on the caller-supplied Executor**; the lifecycle event
  thread never performs this I/O. Caller must supply a bounded
  worker. Executor saturation fails fast and is *not* retried
  on the heartbeat thread; checked I/O failure completes the
  future exceptionally, so BrokerLifecycleManager discards it.
  No sampler is installed during ordinary extension startup.
- **179 — real Broker topic-to-log bridge.** `BrokerServer`
  has a package-restricted factory using the live metadata
  cache's `TopicId→topicName` lookup and LogManager's current
  `getLog(TopicPartition)` path (never future log/missing
  replica), delegating to the extension's asynchronous factory.
  It can be assembled with the actual BrokerServer fields, but
  is **not installed** on BrokerLifecycleManager until
  a unique authenticated Broker principal and negotiated
  feature gate exist. There is no new public protocol,
  credential bypass or background executor initialized by
  default.
- **180 — exact-source and compatibility regressions.**
  Seven new deterministic storage methods exercise actual
  recovered Ring WAL/native Source evidence, classic/no-log,
  unbound identity, stale epoch/recreated TopicId, delayed
  asynchronous execution, closed extension, missing lookup,
  rejected saturated executor and native source drift.
  Four new Scala `BrokerSourceWalProbeAdapterTest` methods
  verify BrokerServer's current TopicId→log resolution,
  missing/deleted TopicIds, removed local replicas and worker
  saturation without synchronous fallback. The main CI
  selects and checks those four Scala JUnit XML cases with no
  skips/failures. Storage/metadata mandatory named inventory
  increases **550→557 methods, still 34 suites**.

**Remaining release blockers:** The factory does not register
itself and never inserts a report into BrokerHeartbeatRequest.
The Controller continues rejecting incoming reports under
`PartitionRetirementBrokerPrincipalPolicy.DENY_ALL`.
The next gates are unique per-Broker authenticated identity,
single-use nonce consumption + fresh KRaft CAS revalidation
inside the serialized Controller event, cross-replica durable
Source/WAL provenance, real multi-Broker MinIO tests and
finally an independently gated authority writer. Logical
COMMITTED retirement and physical S3 DELETE remain **OFF**.
The feature PR stays Draft; new exact-HEAD CI must pass.

## Batches 181–184 — verified SpotBugs repair and post-capture log/identity fences

The previous `00ed9ee143588b4bdf3908a574557e2e5e08db40`
finished **16/17 GitHub Actions SUCCESS**. The only failing
job was main Shared Storage at `:storage:spotbugsMain`.
The uploaded, actual SpotBugs report
`storage/build/reports/spotbugs/main.html` identified exactly
one warning: **BC_VACUOUS_INSTANCEOF**,
`SharedStorageExtension.java:196`, a redundant
`partitionRoleListener instanceof SharedPartitionRoleListener`
where that exact concrete type is assigned by the extension.
No SpotBugs/Checkstyle suppression or rule change was made.

- **181 — type-safe, SpotBugs-compliant role reader.**
  Store the extension-owned listener as the concrete
  `SharedPartitionRoleListener` (the existing public SPI
  still returns `StoragePartitionRoleListener`), check it
  explicitly against null, and reuse the verified local
  native Source/WAL precheck. This removes the vacuous
  runtime type test without broadening accepted log sources.
- **182 — current-log double lookup.** The async sampler
  captures the current `UnifiedLog`, samples its native
  offsets and durable WAL revisions, then asks the current
  TopicId→TopicPartition resolver for the active log **again**.
  Report emission to its local diagnostic slot is denied if
  the source disappears or the second lookup returns a
  different log **object identity**, even when offsets,
  topic name and epoch still look the same. This closes
  a stale-source TOCTOU window during log unload/recreation.
- **183 — post-lookup broker identity + inline-executor
  fencing.** Capture the exact Broker-ready identity
  observation before WAL sampling and require the **same**
  immutable observation after the second log lookup.
  Revocation/re-registration during resolution causes a
  negative result, never an old report. Refuse inline
  executors: a supplied executor that invokes the
  `CompletableFuture` worker on the submitting heartbeat
  thread fails **before metadata lookup or WAL I/O**.
  A real bounded off-thread executor is still a caller
  responsibility; this check does not certify all possible
  executor implementations or credentials.
- **184 — exact regression and release evidence.**
  Add six deterministic storage JUnit tests for inline
  rejection, log replacement, replica removal, unchanged
  log acceptance, null lookup and Broker-ready identity
  revocation during the second lookup. Two new Scala
  `BrokerSourceWalProbeAdapterTest` methods prove
  inline-executor and null metadata rejection. Adjust the
  previous deferred-execution tests to run workers on
  actual separate test threads. The main CI now
  XML-verifies all **six** Scala adapter tests; mandatory
  metadata/storage Java/Scala inventory grows
  **557→563 methods across 34 suites**.

**No production authorization change:** the sampler factory
is still not installed by BrokerServer startup. Even a valid
local WAL sample remains a diagnostic; there is no
authenticated Broker→Controller report, no Controller
nonce acceptance with durable multi-replica proof, no
KRaft retirement authority writer, and no COMMITTED
reference retirement or MinIO physical DELETE.
`PartitionRetirementBrokerPrincipalPolicy.DENY_ALL`
remains the production default. The independently frozen
MinIO GA branch stays at
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.
The corrective HEAD requires its own 17/17 CI PASS.

## Batches 185–188 — verified core test recovery and serialized authenticated report rejection

The preceding `20b793192c2d297dd95509e31bd583b9bedfd277`
finished **16/17 workflows SUCCESS**. The sole failing
`Shared Storage` job passed Java 11, metadata/server/core
Checkstyle and SpotBugs, the 563-method Java/Scala source inventory
already executed in earlier stages, and real MinIO 3-broker E2E.
It failed on **two exact core Scala JUnit assertions**:
`BrokerSourceWalProbeAdapterTest.testRemovedLocalReplicaCannotSupplySourceWalEvidence`
still supplied `Runnable::run`, which correctly tripped the new
inline-executor guard; and
`testNullTopicMetadataResponseNeverFallsBackToLocalLog`
expected an exception instead of the existing, correct
**empty result** for a missing TopicId. These two tests,
not the production adapter, were corrected.

- **185 — match actual Broker adapter contract.** Run the
  missing-local-replica test on a real separate worker thread;
  the null metadata test now demands `Optional.empty` and
  verifies LogManager is never queried. Keep the inline-executor
  denial test intact; never weaken the native WAL scheduling
  or missing TopicId security gates.
- **186 — strictly internal Broker-specific principal map.**
  Allow `QuorumController.Builder` to receive an immutable,
  explicitly validated `PartitionRetirementBrokerPrincipalPolicy`
  **only from the Controller Java package**, to test unique
  broker principal ownership. Ordinary production builders
  retain `DENY_ALL`. This is **not** a new Kafka broker
  configuration, public authentication API, or authorization
  to use a shared inter-broker Principal.
- **187 — Controller event-bound report correlation, still rejected.**
  A BrokerHeartbeat v3 request reporting Source/WAL evidence is
  rejected before normal Broker contact-time and heartbeat
  bookkeeping unless its Principal matches the explicit
  per-Broker policy and its wire structure is valid.
  For that internal-only mapped case, reuse
  `inspectRetirementSourceReport`, whose **single serialized
  Controller event** consumes a one-use nonce, then checks
  current KRaft broker registration, source leader, TopicId
  and authority CAS. Even after a matching diagnostic
  report, the external RPC **always fails** with
  `InvalidRequestException`; no retirement authority
  KRaft records or MinIO operations are emitted. The
  source report remains unauthenticated as durable
  cross-replica bytes; Principal binding is not sufficient
  to authorize deletion.
- **188 — regression evidence and future-version denial.**
  Reject all report-bearing request API versions **other
  than explicitly negotiated v3**, including unknown v4.
  Add two real Controller JUnit tests: an explicitly mapped
  principal still cannot bypass the serialized event or
  invent an issued nonce, and another Broker's principal
  or future protocol version cannot piggyback on the mapping.
  Add one protocol gate test rejecting implicit v4 trust.
  The main Workflow explicitly selects the two new Controller
  tests; the mandatory evidence grows **563→566 methods
  across 34 suites**. Existing clients/standalone Broker
  protocol suites remain separately enforced.

**Release blockers remain:** No BrokerSourceWalProbeReport
is automatically transmitted by `BrokerLifecycleManager`,
no trusted Broker Principal policy is configured by
production startup, no cross-replica WAL/S3 durability
certificate exists, and no KRaft retirement authority
writer, COMMITTED reference retirement or physical MinIO
DELETE is reachable. The release-grade MinIO GA branch
remains frozen at
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.
Feature PR #2 remains Draft. The new HEAD must independently
pass all 17 workflows before being declared a stable
development checkpoint.

## Batches 189–192 — one-shot local v3 Report serialization and deterministic Controller offers

**Verified baseline:** `10da64ff84b1bcaa059063b532e8c63357e6c85c`
finished **17/17 GitHub Actions SUCCESS**, including the main
Shared Storage Java 25/JUnit/SpotBugs, real MinIO, WAL Crash,
Ring WAL and Controller HA gates. The independently frozen
MinIO GA release `9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remains untouched.

- **189 — complete Broker-side wire representation.** The existing
  `BrokerLifecycleManager.SourceWalProbeCaptureState` now has a
  **diagnostic-only** `takeWireOnce`, converting an already collected
  native Kafka Source/WAL `PartitionRetirementSourceProbe.Report`
  into the generated optional
  `BrokerHeartbeatRequestData.SourceWalProbeReport`. It preserves
  nonce, cluster, TopicId, partition, broker incarnation, source
  LeaderEpoch, expected/next KRaft CAS and native
  LogStart/HW/LEO, leadership generation and WAL revisions.
  BrokerId/Epoch remain in the **outer** heartbeat request.
  No new API key or wire version is introduced.
- **190 — shared one-shot fence across both representations.**
  `takeWireOnce` uses the same capture mailbox as
  `takeOnce`: the first accessor clears the staged bytes
  and a second typed/wire view cannot consume them again.
  The existing five-second TTL, exact BrokerId/Epoch/incarnation,
  registered RUNNING state, nonce replay history and
  single in-flight native I/O fence remain mandatory.
  Revocation, timeout, broker replacement and old epochs
  yield no wire diagnostic. Most importantly, **the normal
  `sendBrokerHeartbeat` does not invoke `takeWireOnce`**:
  default production traffic cannot accidentally send
  an unauthenticated report.
- **191 — deterministic Controller pending Challenge order.**
  Replace the Controller's volatile pending nonce map with
  insertion-ordered `LinkedHashMap`. Multiple valid
  Challenge offers for one authenticated Broker select the
  **oldest still-live** nonce for repeated heartbeats until
  it is consumed or expired; they cannot randomly alternate
  on changes to the hash table. Broker Principal/BrokerEpoch
  identity filters, the existing 128-entry global cap, five-second
  TTL, leadership invalidation, one-time consume and
  always-`MATCHED_UNTRUSTED` result remain unchanged.
- **192 — strict serialization, downgrade and replay evidence.**
  Six new deterministic Broker JUnit tests cover exact wire
  fields, a v3 encode/decode round-trip, forbidden v2
  downgrade, exclusive typed/wire consumption, changed
  BrokerEpoch/expiry and revoked registration. Three new
  Controller registry tests verify oldest-first offers,
  skipping expired entries and strict cross-Broker Principal
  isolation. Main Java 25 CI now XML-verifies **24 Broker
  capture methods**, plus the existing ten Broker challenge
  tests. The mandatory metadata/storage Java/Scala inventory
  increases from **566 to 569 methods in 34 suites**.
  These are tests of safety and framing, **not evidence of
  end-to-end authenticated GC execution**.

**Still intentionally disabled:** The production native sampler
is not installed, the normal BrokerHeartbeat sender does not
attach a `SourceWalProbeReport`, and the Controller's default
per-Broker Principal policy is `DENY_ALL`. Even a uniquely
authenticated Controller-side nonce correlation yields only
non-authoritative diagnostic findings, not trusted native
WAL attestation or a cross-replica durability certificate.
No KRaft retirement authority mutation, logical COMMITTED
reference retirement or physical MinIO DELETE is enabled.
PR #2 stays Draft pending actual per-Broker authenticated
round-trip and multi-node durability evidence. A new exact
SHA must pass its own 17 CI workflows before validation.

## Batches 194–196 — fail-closed, non-emitting report dispatch seam and independent WAL replay

The last fully validated parent `9e7b7fc73c5c038ee88a606c7f9c4834135a8ceb`
passed **17/17 CI** after Batch 193. These follow-up changes require their
own exact-HEAD Java 25, real MinIO, WAL Crash, Ring WAL and Controller HA
validation before they can be marked green.

- **194 — mutable heartbeat/report isolation.** Both ordinary and
  explicit v3 `BrokerHeartbeatRequest.Builder` instances snapshot
  generated protocol data, and each `build` returns an independent
  snapshot. A late caller mutation cannot inject a report into a
  stable v0-v2 request, nor can a diagnostic builder silently
  downgrade a non-null report. Five targeted protocol tests (including
  the two from Batch 193) are required in addition to the original
  six, bringing the protocol XML evidence to **11 methods**.
- **195 — dormant identity/version-gated preparation and strict Controller
  ingress.** The package-private
  `BrokerHeartbeatSourceWalReportDispatch` accepts a local one-shot
  Report only when a trusted caller explicitly confirms unique
  authenticated Broker-channel binding, Controller API versions
  support exact v3, and the Broker remains registered, RUNNING,
  unfenced, not shutting down, and free of offline log directories.
  Missing channel versions or v2 refuse to drain the staged report.
  An epoch mismatch burns old diagnostic evidence; neither ordinary
  heartbeats nor production startup invokes this helper.
  The Controller ingress independently denies report-bearing
  fencing, shutdown, or offline-log-dir heartbeats before normal
  Broker heartbeat bookkeeping, even with a uniquely mapped
  Principal. **Seven** new Broker tests and **two** Controller
  negative-path tests enforce these conditions.
- **196 — WAL persistence/replay isolation.** Real independent File
  WAL directories model two Broker instances: one replays a full
  Kafka RecordBatch source window while the other is missing its
  second batch, and the incomplete peer must still fail after close
  and restart. An actual Ring WAL reopen proves a durable TRUNCATE
  cannot restore obsolete indexed coverage. Both are deterministic
  backend tests, not forged self-reports. Combined metadata/storage
  mandatory inventory **569 → 573 methods**, across the existing
  **34 suites**. Broker capture CI additionally requires
  **31 tests**, including all seven dispatch regressions.

**Limitations:** A caller-provided Boolean is NOT authentication.
Neither an authenticated Broker-to-Controller production transport
nor current distributed WAL replica attestation is wired in.
These backend tests demonstrate independent close/restart replay,
not unclean process-crash durability or a quorum certificate; the
existing WAL Crash Workflow remains a separate required gate.
The Controller still consumes single-use nonces inside its serialized
event, verifies current BrokerRegistration/leader/CAS, and always
rejects Source/WAL Reports as non-authoritative. Source/WAL sampling
and Report sending remain disabled in normal Broker startup.
`PartitionRetirementBrokerPrincipalPolicy.DENY_ALL` remains the
production default. No KRaft retirement authority writer, COMMITTED
reference retirement or physical MinIO DELETE is enabled. Frozen
MinIO GA remains `9a28c11b6d97ecacafe1b8566cde5cc34d604889`;
PR #2 stays Draft.

## Batches 197–198 — GA fingerprint completeness for Broker v3 diagnostics

Base: `305b25e5eb95ffab875de94213d833e172b00641`.
Batch 194–196 has passed **16/17** exact-HEAD workflows. The only
failed workflow, `Shared Storage GA Workflow Consistency`, identified
an omitted production fingerprint input:
`server/src/main/java/org/apache/kafka/server/BrokerHeartbeatSourceWalReportDispatch.java`.
All Java 25/MinIO, Ring WAL, WAL Crash, and Controller HA runtime
workflows on that SHA were successful.

- **197 — repair real GA evidence contract.** Add the compiled v3
  report dispatch implementation to the hash-based production
  fingerprint. Do not remove the file from workflow triggers and
  do not relax the GA graph's strict source-coverage assertion.
- **198 — guard GC client-protocol prerequisites.** Explicitly hash
  both broker heartbeat v3 JSON protocol definitions, the
  transport-neutral `PartitionRetirementSourceProbe` and the
  `BrokerHeartbeatRequest` implementation. Teach workflow
  consistency analysis to recognize concrete
  `clients/src/main/` production paths. Deterministic Python
  regressions require coverage of all five inputs and prove that
  mutating the dispatch source changes the production fingerprint.

This intentionally changes the *development* evidence fingerprint:
old successful workflow runs cannot be recycled as evidence for the
new SHA. The frozen MinIO GA branch
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
is untouched. PR #2 remains Draft. This change adds no Broker
sender, no authenticated credential mapping, no KRaft authority
writer and no physical COMMITTED object deletion.

## Batches 199–202 — complete all GA trigger coverage and cross-replica Ring WAL replay negatives

The previous `ad1ff0ec1e99298e94a0e33fbdce118825000ab9` checkpoint
completed **16/17** workflows. The only failed job,
`Shared Storage GA Workflow Consistency`, reported that
eighteen independent GA evidence workflows lacked `push.paths`
coverage for five newly hashed Broker v3 diagnostic inputs.
That failure is a release evidence contract issue, not a
WAL correctness or Controller HA assertion failure.

- **199 — exact-scope workflow triggers:** Add the four
  Broker v3 Source/WAL client protocol inputs and
  `BrokerHeartbeatSourceWalReportDispatch.java` to all
  eighteen other eligible GA evidence workflows. Their
  existing `pull_request` events also gain matching paths.
  Do not use a broad `clients/**` wildcard or trigger
  unrelated Real S3 evidence.
- **200 — mandatory trigger regression:** Add a fast
  unit test to check each protected path belongs to the
  GA production fingerprint and is covered by `push`
  and any supported `pull_request` on every eligible
  evidence workflow. Keep the existing graph validator
  strict; do not relax its coverage assertion.
- **201 — independent Ring WAL source windows:** Use
  two physically separate real Ring WAL files with the
  same logical topic/partition. After both brokers close
  and reopen, a missing local Kafka batch remains missing
  even though the peer's Ring WAL contains it.
- **202 — independent Ring WAL TRUNCATE:** Reopen two
  physical Ring WAL backends after only one replica
  durably truncates its indexed source data. The revoked
  replica cannot borrow the healthy peer's complete window.
  Both WAL regressions are mandatory XML/JUnit evidence:
  the strict storage/metadata method inventory rises
  from **573 to 575**.

These two physical backend tests do **not** prove durable
cross-replica consensus, broker transport authentication or
current ControllerRegistration. No production report sender,
KRaft retirement authority writer, COMMITTED reference retirement
or MinIO physical DELETE is enabled. Production `DENY_ALL`
and the frozen MinIO GA source
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remain unchanged. PR #2 remains Draft. This checkpoint
requires its own exact-source-SHA 17/17 CI before validation.

## Batches 203–206 — live nonce selection and Controller heartbeat safety

Validated baseline: `4556b4319838771c0923ef8897e6a0c04e7db89b`,
**17/17** exact-HEAD workflows successful.

- **203 — oldest-live Challenge selection:** On the serialized
  Controller event the nonce registry evaluates the latest KRaft
  candidate predicate and skips stale oldest challenges, rather
  than allowing stale pending nonces to starve later valid ones.
  Neither a skip nor an offer consumes a nonce.
- **204 — authenticated Broker and epoch fencing:** Matching
  Broker Principal and BrokerEpoch are checked *before* reading
  live KRaft state for a challenge. An incarnation change cannot
  reuse the old challenge even under the same numeric epoch.
  Four deterministic registry regressions cover stale selection,
  non-consuming behavior, unauthorized identities and epoch ABA.
- **205 — offline-directory safety:** The v3 Challenge eligibility
  gate rejects Broker heartbeats with offline log directories,
  incomplete catchup, pending metadata records, fencing or
  shutdown signals. One Controller regression covers these
  independent negative conditions.
- **206 — Controller GA source contract:** Include
  `QuorumController`, all `PartitionRetirement*` Controller
  and image sources, BrokerHeartbeatReply and authority-state
  production classes in the GA fingerprint. Update the exact
  `push` and applicable `pull_request` trigger paths of
  every eligible evidence workflow, without broad unrelated
  wildcards. Add Python regressions to protect both the
  production-fingerprint and trigger-scope invariants.
  Mandatory metadata/storage JUnit methods increase **575 → 580**.

This checkpoint remains non-authorizing: no production Broker
Source/WAL sender or transport authentication is activated,
no KRaft retirement authority writer is enabled and no
COMMITTED physical object DELETE can be executed. Production
`DENY_ALL`, frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
and PR #2 Draft status are preserved. The new SHA must
pass its own exact-HEAD CI; earlier successful runs are
not reusable evidence for the changed source fingerprint.

## Batches 207–210 — recover Java 25 gate and pin diagnostic API identity

Base: `15c486d25ea5e1f7e28af6eb9ea6b58ccc067ae7`.
The preceding Batch 203–206 completed 16/17 workflows; the sole
`Shared Storage` failure was Checkstyle's
`BooleanExpressionComplexity = 7 > 5` in
`QuorumController.sourceProbeHeartbeatEligible`, not a WAL or
MinIO runtime failure.

- **207 — recover metadata Checkstyle:** Preserve the current
  fail-closed eligibility logic, splitting Broker metadata, shutdown,
  fencing and offline-log-directory checks into smaller independent
  conditions. No policy is relaxed.
- **208 — exact API and protocol binding:** Both v3 source report
  ingress and Controller Challenge egress now share one predicate that
  checks the actual `ApiKeys.BROKER_HEARTBEAT.id` and exactly protocol
  version 3. A version-three header belonging to a different Kafka
  API cannot enable an unstable diagnostic field.
- **209 — deterministic live-Controller rejection:** Two local
  protocol-regression tests and one real QuorumController event test
  reject foreign API headers even with a correctly mapped Broker
  Principal. The request is denied before nonce consumption, normal
  heartbeat contact refresh, or KRaft GC-authority emission.
- **210 — block premature production Report emission:** A fast
  Python regression asserts that the ordinary
  `BrokerLifecycleManager` still sends its original non-reporting
  heartbeat, rather than activating the currently dormant one-time
  v3 diagnostic dispatcher. Controller tests are explicit CI selectors,
  and the mandatory storage/metadata inventory becomes **583 methods**
  (from 580).

The above is preparatory security hardening, NOT delivery of a
genuine authenticated Broker-to-Controller production Report sender.
That sender requires proof of a unique Broker Principal bound to
the actual network channel, current BrokerRegistration and a
real Controller-issued nonce before enabling any traffic. Even a
one-time matched diagnostic cannot establish distributed WAL
durability or authorize GC. Production `DENY_ALL`, no KRaft
retirement authority writer, no COMMITTED physical DELETE,
frozen MinIO GA `9a28c11b6d97ecacafe1b8566cde5cc34d604889`
and PR #2 Draft remain intact. Exact-new-HEAD CI is mandatory.

## Batches 211–214 — per-Broker nonce quota, global bound, expiry and failover

Verified parent: `7a349ffffb2e094fc46d9ca5cea9a110f6b0884c`,
with all **17/17** exact-HEAD CI workflows successful.

- **211 — isolated authenticated diagnostic quota:** Pending Controller
  Source/WAL challenges now have both a global ceiling of **128** and
  a per-Broker ceiling of **16**. The second limit prevents one
  correctly mapped Broker Principal from monopolizing the global
  diagnostic nonce registry. Neither cap is a substitute for transport
  authentication or the existing Controller feature/CAS preflight.
- **212 — independent Broker admission:** Deterministic regression
  exhausts Broker 3's 16 challenges and verifies that Broker 4 can
  still issue, advertise and consume its own nonce. Another regression
  separately tests the global ceiling with 129 distinct Broker
  Principals and verifies that consuming a challenge frees capacity.
- **213 — bounded TTL and leadership revocation:** Real nonce-history
  regression verifies that pruning at the five-second expiry frees a
  Broker quota without allowing the old nonce to reappear; Controller
  epoch invalidation immediately frees the quota while preserving the
  old nonce's rejection.
- **214 — mandatory evidence:** The above four new JUnit methods
  are included in the existing strict metadata/storage JUnit XML gate.
  Its requirement rises **583 → 587**. No Gradle job is duplicated:
  the existing `PartitionRetirementSourceProbeRegistryTest` suite is
  already compiled, run and checked in the main Shared Storage CI.

The per-Broker cap protects in-memory, volatile *diagnostic*
resources. There is still no authenticated Broker-to-Controller
production report sender, no quorum durability certificate and no
authorization to advance COMMITTED GC. The default Broker Principal
policy is `DENY_ALL`; no KRaft GC authority writer, durable
reference retirement or physical MinIO DELETE is enabled. Frozen
MinIO GA remains `9a28c11b6d97ecacafe1b8566cde5cc34d604889`,
PR #2 remains Draft, and exact-new-HEAD 17/17 CI is required before
promoting this checkpoint.

## Batch 215–218 — bind diagnostics to real authenticated Controller request transport

Parent checkpoint: `dd45e345dd53f13a4dd2e8de11aa18c4289ccf44`.
The Controller's unique Broker Principal mapping is necessary but
insufficient proof of a direct authenticated transport: the old
`ControllerApis` handler constructed `ControllerRequestContext`
from a header and Principal alone, dropping the actual network
security protocol and `isForwarded` flag.

- **215 — preserve network authentication provenance:** BrokerHeartbeat
  handling now constructs `ControllerRequestContext` from Kafka's
  real `AuthorizableRequestContext`, carrying the connection's
  `SecurityProtocol` and the actual request forwarding state.
  Synthetic contexts built only from claimed header/Principal remain
  untrusted by default. Other normal Controller APIs and stable
  BrokerHeartbeat behavior are unchanged.
- **216 — transport-bound ingress:** Even a uniquely mapped Broker
  Principal cannot submit v3 Source/WAL diagnostics on PLAINTEXT,
  SASL_PLAINTEXT, forwarded requests, token-derived identities or
  synthetic contexts with unknown transport. Direct SASL_SSL or SSL
  carrying a *non-anonymous* Principal may reach **non-authorizing**
  nonce correlation, subject to the existing exact Broker mapping,
  version, source window, KRaft BrokerRegistration and CAS checks.
  SSL still requires correctly configured client certificate
  verification; this code does not inspect or invent a certificate.
- **217 — Controller issuance is equally gated:** Only a direct
  authenticated Controller request may issue a nonce (the existing
  feature and live KRaft preflight still applies) or receive a
  diagnostic Challenge. Bad transport reports reject before heartbeat
  contact-time mutation or nonce consumption.
- **218 — 4 JUnit regressions and release evidence:** Require three
  ingress tests for transport, SSL/nonanonymous, forwarded and
  token-authenticated conditions, plus one production
  QuorumController test asserting network rejects and zero GC
  authority records. Exact method inventory increases **587 → 591**.
  Include `ControllerRequestContext.java` and
  `ControllerApis.scala` in the GA production fingerprint and all
  eligible evidence workflow triggers. A Python source-contract test
  prevents regressing to the old header-only ControllerApis call.

**Important:** The TLS/SASL checks do not by themselves prove
current BrokerRegistration ownership, durable multi-replica WAL
state, or safe COMMITTED object deletion. The production Broker
still does not emit Source/WAL Reports; this is an authenticated
Controller ingress contract for future integration. The default
Controller policy remains `DENY_ALL`, KRaft GC authority writing
and physical MinIO DELETE remain disabled, frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
is untouched, and PR #2 remains Draft. A new exact-HEAD 17/17
CI run is required before this branch becomes a validated baseline.

## Batches 219–222 — authenticated one-shot nonce consumption

Verified previous checkpoint: `dd45e345dd53f13a4dd2e8de11aa18c4289ccf44` (17/17).
The preceding `028d02b032a3d38bf3640f4fa6c405a2fd6a303e`
revision finished **16/17** workflows: `Shared Storage` failed only
because `QuorumControllerTest.java` imported `SecurityProtocol`
twice, rejected by `metadata:checkstyleTest`.

- **219 — recover static checks:** Remove the duplicate import; keep
  all existing transport-identity tests and production checks intact.
- **220 — protect the destructive nonce consume:** Controller event
  handling now calls `consumeAuthenticated`, which requires a direct
  authenticated Kafka transport and a trusted unique Broker Principal
  mapping *before* any nonce removal. An unauthorized caller therefore
  cannot burn a legitimate Broker's one-shot token.
- **221 — protect cross-Broker nonce ownership:** Even an authenticated
  Broker with its own mapping cannot consume a different Broker's
  nonce by forging outer Broker fields: look up the pending nonce owner
  and compare to the authenticated channel principal first. An
  authenticated owner submitting a malformed envelope still burns
  its nonce, preventing replay.
- **222 — deterministic tests and CI evidence:** Three new registry
  tests cover PLAINTEXT, SASL_PLAINTEXT, forwarded, token and synthetic
  contexts, foreign Broker nonce interference, and legitimate
  one-shot/forged-envelope behavior. The strict metadata/storage
  JUnit method inventory increases **591 -> 594**. The existing
  `PartitionRetirementSourceProbeRegistryTest` selector already
  runs the entire class; no extra Gradle job or native build is added.

This is a defense-in-depth step toward authenticated report ingress,
not evidence of an enabled Broker-side report sender, cross-replica
WAL durability, or COMMITTED GC permission. Default `DENY_ALL`,
disabled KRaft authority writer and physical DELETE remain unchanged;
the MinIO GA branch
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remains frozen. PR #2 stays Draft until required exact-HEAD CI.

## Batch 223–226 — parsed v3 ControllerApis transport relay test evidence

Starting parent: `0cf762d3a1e86cd0d297b39bdaa0fc82fea7e64a`.
Production Broker Source/WAL sending and GC authority remain disabled.

- **223 — real Kafka protocol parse boundary:** The core ControllerApis
  suite now builds a v3 BrokerHeartbeatRequest containing an exact
  Source/WAL report, serializes it with a Kafka RequestHeader and
  parses it through RequestContext and RequestChannel.Request before
  invoking the Controller APIs handler. The mocked Controller receives
  an unchanged nonce, BrokerEpoch, Broker incarnation and WAL revisions.
- **224 — network-origin trust classification:** Direct SASL_SSL with
  a uniquely named nonanonymous Broker principal is propagated into
  ControllerRequestContext; PLAINTEXT, SASL_PLAINTEXT and a forwarded
  SASL_SSL request are all classified as ineligible. The forwarded
  case uses a RequestChannel.Request with a real enclosing request,
  not a Boolean value invented in Controller code.
- **225 — stable protocol compatibility:** Version-two BrokerHeartbeat
  without a Report still traverses the ordinary ControllerApis
  call path and is never classified as a v3 diagnostic.
- **226 — enforced executable evidence:** Three new
  `ControllerApisTest` methods are explicitly selected in the
  existing Java 25 Shared Storage core test step and required
  by its JUnit XML no-skips gate. No duplicate heavyweight workflow
  or Native Image build is introduced; the metadata/storage
  inventory remains at **594** since these are `core:test` methods.

This is an in-process parsed-RPC integration using a mocked
Controller, *not* evidence that a real SASL_SSL socket has completed
a TLS/SASL handshake or that a multi-node Broker has transmitted a
Report. It does not issue a Controller challenge, prove current
BrokerRegistration across a leader failover, or grant GC authority.
The existing authenticated Controller nonce and KRaft CAS checks
continue to reject unverified reports. The production sender,
KRaft authority writer and COMMITTED physical DELETE remain disabled;
production policy stays `DENY_ALL`, frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
is untouched and PR #2 remains Draft.

## Batches 227–230 — Broker re-registration nonce isolation

Validated baseline: `0cf762d3a1e86cd0d297b39bdaa0fc82fea7e64a`
(**17/17** exact-HEAD CI). The preceding parsed-request test change,
`87cdbffbb09d1f58d1364571a5d60e465830e35a`,
must complete its own CI before this checkpoint can inherit new evidence.

- **227 — reclaim superseded registration quotas:** On an authorized
  new diagnostic issue, Controller discards only live pending nonces
  issued to that Broker under a different BrokerEpoch or incarnation
  before checking the single-Broker/global capacity limits. This
  prevents a re-registered Broker from being starved for five seconds
  by the former instance's outstanding requests. Other Broker
  principals and challenges are unchanged.
- **228 — preserve same-registration concurrent CAS observations:**
  Challenges belonging to different source partitions/CAS
  observations under the SAME BrokerEpoch and incarnation are kept,
  preserving the original nonce ordering and one-shot semantics.
- **229 — require exact protocol at destructive consume:** In addition
  to the Controller network ingress guard, the authenticated registry
  consumption itself requires the exact `BROKER_HEARTBEAT` API key and
  version 3. A synthetic internal call using v2, a future version, or
  a foreign API key never burns another valid nonce. The
  package-private Controller begin path applies the same v3
  requirement before issuing a nonce.
- **230 — mandatory regressions:** Five new
  `PartitionRetirementSourceProbeRegistryTest` methods verify quota
  reclamation after BrokerEpoch changes, ABA at the same epoch with a
  new incarnation, preservation of independent CAS windows, explicit
  API/version replay refusal, and prevention of foreign-Broker quota
  eviction. The strict metadata/storage XML evidence requirement
  increases from **594 to 599** without another heavyweight CI job.

This remains diagnostic, with production `DENY_ALL`, no Broker
automatic Source/WAL emission, no KRaft GC-authority writer and no
physical MinIO DELETE. The frozen MinIO GA branch
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
is untouched; PR #2 remains Draft. Neither a mocked network channel
nor a one-time nonce proves real TLS authentication, multi-Broker WAL
durability or safe COMMITTED deletion. A fresh 17/17 exact-HEAD CI is
required.

## Batches 231–234 — isolated Broker v3 report transport preparation

Validated parent `7405b1dd9a7a787e1a2b547ea712bb17d567dc81`
has all **17/17** existing GitHub CI workflows successful.

- **231 — dedicated enqueue adapter:** An otherwise dormant,
  package-private `BrokerHeartbeatSourceWalReportDispatch.sendDiagnosticOnce`
  uses `NodeToControllerChannelManager` to enqueue a single logical,
  exact-version-v3 report built from the existing Broker one-shot
  local capture. It has NO ordinary `BrokerLifecycleManager` caller.
- **232 — no downgrade / no identity bypass:** Missing Controller
  API versions, max protocol below v3 and false identity precondition
  all refuse enqueue without consuming the ready capture.
  A Boolean precondition is NOT actual TLS/SASL authenticated Broker
  identity: production must not call this adapter until an independent
  channel-bound Broker Principal binding exists.
- **233 — response isolation and fail closed:** The diagnostic
  completion/timeout handler is deliberately separate from the normal
  `BrokerHeartbeatResponseHandler`, never changing liveness,
  registration or retirement authority. An enqueue exception
  consumes the staged local report without automatic retry.
- **234 — mandatory CI tests:** Four deterministic server tests check
  version and identity rejection, exact v3 request enqueuing, local
  one-shot behavior, callback isolation and failure/no-retry semantics.
  The existing `server:test` and strict JUnit XML gate require all
  four new methods; no extra native build or CI job. The separate
  metadata/storage evidence inventory correctly remains **599**.

This is a **dormant in-process adapter**, not real TLS/SASL
authentication, a multi-node diagnostic roundtrip or positive
Controller nonce acceptance. The Kafka 4.3.1 baseline does not meet
the `IBP_4_4_IV0` prerequisite for experimental authority v1;
do not bypass it. The Controller production Broker Principal policy
stays `DENY_ALL`, no KRaft GC writer exists, no COMMITTED object
physical deletion is enabled, and the MinIO GA branch
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remains frozen. PR #2 remains Draft.

## Batches 235–238 — revoke Broker diagnostic nonces on KRaft registration replay

Validated parent: `342110ded226745d8408173d3a2a3315103d066f`
(**17/17** exact-HEAD workflow success). This change does not activate
the independent v3 diagnostic sender or enable an experimental feature.

- **235 — durable registration replay fence:** Extend the volatile
  Controller Source/WAL nonce registry with a per-Broker revocation.
  Successful `RegisterBrokerRecord` and `UnregisterBrokerRecord`
  replay now immediately invalidate that Broker's pending nonces.
  This does not depend on requesting another Challenge or the
  five-second expiration timer.
- **236 — fencing and registration mutation coverage:** Apply the same
  isolated revocation immediately after successful replay of
  `FenceBrokerRecord`, `UnfenceBrokerRecord` and
  `BrokerRegistrationChangeRecord`. Normal heartbeats with no
  KRaft mutation do not revoke healthy diagnostics; other Broker
  IDs' nonces and quotas are unaffected.
- **237 — deterministic negative regressions:** Four JUnit registry
  tests verify per-Broker isolation, immediate reclamation of all 16
  per-Broker slots, revocation of multiple same-registration CAS
  windows, and absence of side effects from unrelated or invalid
  Broker IDs. Revoked nonces never become usable through an old
  report; reissued nonces are independent.
- **238 — evidence ownership:** Require these JUnit methods in the
  existing strict metadata/storage XML gate, increasing mandatory
  methods **599 -> 603**. A fast Python source-contract regression
  checks that all five KRaft Broker metadata replay events revoke
  the correct Broker's nonce **after** successful state replay.
  Existing GA production source fingerprint and workflow triggers
  already cover both affected production Java files; no extra
  workflow or Native Image build is introduced.

This is a Controller in-memory diagnostic safety change, not an
authenticated Broker TLS/SASL E2E or quorum WAL durability proof.
The v3 diagnostic dispatch remains dormant. Experimental GC
authority version 1 still requires `IBP_4_4_IV0`; no KRaft authority
record emitter, COMMITTED physical object DELETE, or authorization
relaxation has been added. The production policy stays `DENY_ALL`.
Frozen MinIO GA `9a28c11b6d97ecacafe1b8566cde5cc34d604889`
and Draft PR #2 remain unchanged. This new SHA requires fresh
exact-HEAD 17/17 CI evidence before promotion.

## Batches 239–242 — revoke source/WAL Challenges on topic and partition replay

Validated parent: `227b5a41afb678664f366042d0d095488eec0958`
(**17/17** exact-HEAD CI).

- **239 — partition/leader-epoch invalidation:** After successful
  `PartitionRecord` or `PartitionChangeRecord` replay, immediately
  invalidate every pending nonce for that exact Topic ID and partition.
  A changed leader epoch, ISR/replica assignment or other partition
  metadata mutation cannot leave a previously issued source-window
  Challenge alive until its five-second TTL.
- **240 — topic deletion/recreation fence:** After `TopicRecord`
  or `RemoveTopicRecord` replay, invalidate the matching immutable
  Topic ID. Deleting and recreating the same topic name cannot carry
  the old topic's nonce into the new Topic ID.
- **241 — strict isolation:** Four deterministic registry tests verify
  leader-epoch invalidation, whole-topic revocation, recreated Topic ID
  separation and non-eviction of unrelated partitions/topics. Other
  Brokers and unrelated source windows retain their pending challenges.
- **242 — mandatory evidence:** The strict metadata/storage JUnit
  XML inventory increases **603 -> 607**. A Python source-contract
  regression checks all four KRaft replay paths invalidate the matching
  diagnostic only after successful metadata-state application. The
  production source files are already in the GA fingerprint and all
  eligible workflow trigger paths, so no duplicate CI job is added.

The revocation registry remains volatile and diagnostic only. No
authority is inferred from a metadata replay, and this change adds no
Broker Report sender, no KRaft GC authority writer and no physical
COMMITTED object DELETE. Production `DENY_ALL`, the frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889` and Draft PR #2
remain unchanged. A fresh exact-HEAD 17/17 CI run is required.

## Batches 243–246 — close metadata-feature and KRaft authority diagnostic races

The parent `82816e471a2dfbbfbcb358c3eb2960df2b4c9135`
finished **16/17** CI. All runtime/MinIO/Controller/WAL checks
passed. The one failure was a Python source-contract assertion
matching a single-line `invalidatePartition` call against valid
two-line Java formatting, not a Java assertion.

- **243 — fix the evidence checker without weakening semantics:**
  Match whitespace-independent Java statements, preserving exact
  Topic ID/partition accessors and the mandatory
  successful-replay-before-invalidation ordering.
- **244 — feature-replay fence:** After successful
  `FeatureLevelRecord` replay, drop every volatile Controller
  Source/WAL Challenge. A metadata-version or authority-feature
  negotiation change cannot leave pending preflight tokens valid.
- **245 — authority-CAS replay fence:** After successful
  `PartitionRetirementAuthorityRecord` replay and its genuine
  KRaft-offset validation, revoke pending Challenges for the exact
  Topic ID/partition. Failed replay does NOT execute the revocation.
  Even when a feature is explicitly enabled in future test metadata,
  an old expected authority generation cannot be reused.
- **246 — evidence:** Four Java regressions cover global feature
  invalidation and quota release, cross-Broker partition CAS
  invalidation, and unrelated Topic-ID isolation. Mandatory JUnit
  XML evidence increases from **607 to 611**. The workflow
  source-contract adds ordering checks for both replay paths.
  No extra CI job or Native build is introduced.

This is a volatile diagnostic-only safety fence, not an authority
writer or a verified distributed source/WAL certificate. Production
`DENY_ALL`, the dormant Broker v3 report sender, disabled COMMITTED
physical DELETE, frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
and Draft PR #2 remain unchanged.

## Batch 247 — fail-closed source contracts for the dormant v3 sender

Base: `936fb07dce4262a6f5100242902cba96b6e06b9a`, confirmed
**17/17** GitHub Actions successful.

This batch does not enable a production sender. It makes the existing
security boundary executable in the GA consistency suite:

- Recursively scan all production Java sources under
  `server/src/main/java` and fail if any source other than the dormant
  adapter calls `sendDiagnosticOnce` or directly prepares a diagnostic
  report. This prevents a future helper from silently wiring the
  experimental path into ordinary Broker lifecycle.
- Scan all Controller production sources to ensure the
  `QuorumController` consumes source-probe reports through
  `consumeAuthenticated`, never the low-level destructive
  `consume` method.
- Assert that synthetic `ControllerRequestContext` constructors
  default to unknown transport, and that the direct-authentication
  predicate checks forwarding, anonymous/token principals, and actual
  SASL_SSL/SSL transport metadata.
- The tests live in `tools/test_check_shared_storage_ga_workflows.py`,
  which the existing `Shared Storage GA Workflow Consistency` workflow
  already executes and triggers on. No JUnit inventory bump, extra CI
  workflow or Native Image build is needed.

These are source-contract tests, not evidence of a TLS handshake or
multi-Broker report roundtrip. Production `DENY_ALL`, the dormant
diagnostic sender, no KRaft GC-authority writer, no COMMITTED physical
DELETE, the frozen MinIO GA branch and Draft PR #2 remain unchanged.

## Batches 248–251 — remove caller-Boolean identity bypass from dormant diagnostic sending

Validated parent: `54f35163cd9b4a78f99e78126e3b51d368ead401`;
17/17 CI workflows successful.

- **248 — unified Controller-channel attestation contract.**
  `NodeToControllerChannelManager.authenticatedBrokerDiagnosticVersions`
  returns an `Optional<NodeApiVersions>` only for a **single current,
  uniquely authenticated network session** bound to the specified Broker
  ID, BrokerEpoch and incarnation, with versions from that SAME active
  Controller session. The interface supplies `Optional.empty()` by
  default, and the existing production
  `NodeToControllerChannelManagerImpl` deliberately DOES NOT override
  it. Generic negotiated versions, listener TLS configuration or
  CLUSTER_ACTION are not identity proof.
- **249 — remove send-side caller-controlled Boolean.**
  `BrokerHeartbeatSourceWalReportDispatch.sendDiagnosticOnce` no longer
  accepts a `uniqueBrokerChannelIdentityVerified` parameter. It
  obtains exact Broker- and connection-bound versions from the channel;
  absence prevents enqueue WITHOUT draining the staged one-shot report.
  The package-private `prepareOnce` is still a non-network test/helper
  and remains strictly dormant in production.
- **250 — identity mismatch and version snapshot regressions.**
  Update the existing dedicated dispatcher tests and add three tests
  proving generic Controller API versions alone cannot send, a different
  BrokerEpoch/incarnation/ID attestation cannot be borrowed, and the
  authenticated version snapshot never queries a separate potentially
  changed Controller connection. Mock-supplied positive bindings only
  exercise local control flow; they are not proof of real TLS/SASL.
- **251 — GA evidence ownership.** Include the channel interface in
  the production fingerprint and all applicable push/PR workflow path
  triggers. Add the three JUnit methods to the existing `server:test`
  strict XML gate, plus a Python regression ensuring the production
  channel implementation still defaults to **DENY** and the sender
  cannot reintroduce the Boolean/independent-version bypass. No new
  workflow or Native Image build is added.

No real unique per-Broker authenticated Controller channel has yet been
implemented or tested. Consequently production cannot enqueue these
reports. Controller production policy `DENY_ALL` stays unchanged;
there is no source-WAL durability certificate, KRaft GC-authority
emitter or physical COMMITTED delete. MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remains frozen and PR #2 remains Draft.

## Batch 252 — replica-aware COMMITTED physical cleanup safety (AutoMQ-aligned)

The target architecture is **AutoMQ-style shared object storage with native
Kafka replication retained**: Kafka Leader/Follower, ISR, HW, LeaderEpoch and
BrokerEpoch remain responsible for the existing replica protocol. Broker
File/Ring WAL and S3 are storage tiers, not a substitute consensus engine.

AutoMQ's `S3ObjectControlManager` and `StreamControlManager` illustrate
Controller/KRaft-owned object lifecycle: persist MARK_DESTROYED after
node/stream epoch and ownership checks, then asynchronously delete physical
bytes after the retention period, then remove the metadata record. The
implementations are a reference, not a direct replacement for Kafka's
replicated partition and packed multi-partition object semantics.

**Correctness finding:** `RemoteObjectIndex` chooses the first equivalent
physical object published in its broker-local view. Different replicas can
publish duplicated COMMITTED copies in opposite orders, choosing different
physical read winners. Previously `OrphanObjectCleaner` could DELETE one
copy solely because its own local index did not reference it, even though
another replica's read path still used it. That is not safe COMMITTED GC.

**Change:** Disable local-index-driven physical COMMITTED deletion and
COMMITTED metadata tombstones. Preserve the four-argument constructor
for compatibility, but its `RemoteObjectIndex` is never authority.
PREPARED and durably CLAIMED PREPARED orphan cleanup continues to use its
existing serialized claim-before-delete fence and idempotent late-PUT
retry. Unit and real MinIO cases exercise two opposite replica read
winners and assert BOTH COMMITTED copies remain physically readable.
Extra object-storage consumption is deliberately preferred to
irrecoverable cross-replica data loss.

**Before enabling true COMMITTED reclamation:** persist Controller-owned
object retirement after validating every packed TopicId/partition
reference, current Kafka leader/partition/broker epochs, eligible
replicated coverage and reader/upload quiescence. Only a durable,
retryable retirement state, not a local index or broker WAL claim, may
authorize eventual physical DELETE. Keep unsure cases.

No new GC authority writer, COMMITTED DELETE, or broker authentication
protocol is enabled. PR #2 remains Draft and the frozen MinIO GA branch
at `9a28c11b6d97ecacafe1b8566cde5cc34d604889` is untouched.

## Batches 253–255 — repair MinIO evidence and enforce Controller-owned COMMITTED tombstones

**253 — Verified 16/17 CI, test-name evidence repair.** On commit
`e8688b1af7f4b5dcabe7a7557085d22869595efe`, all Java 25 storage
tests and the real MinIO S3 tests passed; the only failing workflow
was the main `Shared Storage` MinIO XML evidence checker still requiring
the prior redundant-COMMITTED-deletion test name. Require the
replacement `redundantCommittedPhysicalCopiesSurviveOppositeReplicaReadWinners`
instead, keeping the same strict executed/unskipped JUnit gate.

**254 — Close the unfenced metadata-delete bypass.** The old
`ObjectMetadataStore.delete(long)` operation is unsafe for Kafka's
replicated AutoMQ-style object storage. `KafkaObjectMetadataStore`
previously wrote a compacted object-key tombstone with no KRaft
lifecycle authorization, while `InMemoryObjectMetadataStore`
unconditionally removed metadata. Both now return a failed future with
`UnsupportedOperationException`, without producing or removing
metadata. Preserve the interface for source compatibility. PREPARED
orphan cleanup keeps its atomic claim and late-PUT retry. Existing
`SharedMetadataImage` replay continues accepting historic object
tombstones from the frozen MinIO GA to avoid breaking upgrades.

**255 — Prove the new boundary.** Add Java tests for rejected COMMITTED
metadata deletion and preservation of the claimed PREPARED cleanup
fence. Expand mandatory, unskipped XML evidence by five tests to
**616** methods, including the two-replica opposite-winner regression.
The GA consistency Python suite now asserts neither the orphan
cleaner nor either metadata-store implementation can bypass the
Controller-only COMMITTED retirement boundary.

Reference: AutoMQ `S3ObjectControlManager` emits durable KRaft object
state transitions (including MARK_DESTROYED), and its `ObjectCleaner`
deletes physical storage only after the lifecycle delay. Our system
retains native Kafka replication (ISR/HW/LeaderEpoch/BrokerEpoch), so
a Broker-local read index or a compacted-topic tombstone cannot
replace Controller-owned, replica-safe retirement evidence.

No physical COMMITTED delete, retirement CAS writer, or separate
consensus/authentication protocol is enabled. PR #2 remains Draft;
frozen MinIO GA `9a28c11b6d97ecacafe1b8566cde5cc34d604889`
is unchanged.

## Batches 256–259 — deterministic replica read selection after COMMITTED replay

Base: `758bd78584bd1903c4d8aea7755cd417c2dc2880`,
17/17 successful workflows. Keep Kafka's native Leader/Follower,
ISR, HW and LeaderEpoch replication unchanged. The compacted metadata
topic remains the COMMITTED-object source of truth.

- **256:** For fully equivalent logical Kafka batch ranges (identical
  offsets, leader epoch and content checksum), select the lower physical
  object ID after both COMMITTED copies are replayed. Apply the same
  deterministic, order-independent tie-break in `RemoteObjectIndex`
  and in the fsynced `LocalRemoteObjectCheckpoint`. This matches
  startup metadata-image object-ID ordering; it does NOT grant GC
  ownership. Conflicting logical ranges or same-object descriptors
  still fail closed.
- **257:** JUnit regression: two replicas ingest equivalent copies in
  reverse order, converge on the same read reference, and reflect a
  lower-ID replacement in the index revision. Checkpoint regression:
  fsync, reopen and source-metadata replay preserve the same descriptor.
- **258:** In-memory and real MinIO cleanup tests first observe
  divergent partial-replay copies, then the converged fully replayed
  view. BOTH physical COMMITTED objects must remain present/readable:
  a locally non-selected copy is not eligible for deletion.
- **259:** Add the two focused storage regression methods to strict
  JUnit execution evidence, from 616 to 618 required methods.
  Keep the existing workflow graph and the Native Image gate disabled.

This local read-reference tie-break is a project-specific recovery
detail, NOT a claim that AutoMQ uses minimum IDs as a distributed
ownership protocol. The transferable AutoMQ pattern is durable
Controller/KRaft-owned object lifecycle; this project still needs
authoritative retirement of all packed-range references and replica
reader/upload fences before any COMMITTED physical DELETE.
The Draft PR and frozen MinIO GA branch remain unchanged.

## Batches 260–263 — bounded COMMITTED physical-read fallback with native Kafka replication

Base: `9ed86168715ff826f9720c0310965bcbc1e3e204`,
17/17 workflow SUCCESS. The target remains AutoMQ-style immutable
remote objects **with native Kafka Leader/Follower, ISR and HW**,
not a new replicated WAL consensus algorithm.

- **260 — bounded replica-read alternatives.** `RemoteObjectIndex`
  validates every packed range and retains the two smallest known
  physical IDs per identical logical Kafka batch: one canonical
  local read target and at most one alternative. Index publication
  never treats a PREPARED upload as an alternative: its inputs are
  authoritative COMMITTED metadata replay or a post-COMMIT durable
  local checkpoint. Conflicting leader epochs, offsets or batch CRCs
  cannot enter the same fallback set.
- **261 — bounded recovery.** If the selected physical copy is
  missing, corrupt or suffers an object-store I/O failure,
  `SharedObjectReader` attempts one equivalent committed copy.
  The fallback is fully re-validated through the existing stream
  object index, DataBlock CRC and batch checksum (or the legacy
  direct-range CRC). Both failures remain visible when neither
  object can be read. Retry does not mutate Kafka ISR, HW,
  partition leadership or object metadata.
- **262 — real proof.** Unit tests cover missing primary,
  corrupted primary, both copies missing, deterministic alternate
  choice, and rejecting non-equivalent copies. A real MinIO test
  confirms that an absent primary can fall back to a separately
  COMMITTED physical object using the legacy checkpoint/direct CRC
  format, without deleting either COMMITTED metadata record.
- **263 — CI evidence.** Add four Java methods to strict
  unskipped JUnit enforcement (618 to 622 required methods), and
  require the new MinIO fallback test in the existing S3 XML gate.
  No additional CI workflow or Native Image build.

**Safety boundary:** the alternative is retained only in each broker's
local read index. This is a read-availability optimization, not a
cross-replica liveness, durability or garbage-collection certificate.
AutoMQ's transferable design remains KRaft-controlled immutable
object lifecycle. COMMITTED physical DELETE stays disabled until a
durable Controller retirement transition fences all packed-range
references and active replica readers.

## Batch 264 — staged immutable alternate publication; storage quality gate

On prior HEAD `775f96095cfc08f33b66078081a4a034009aa129`,
16/17 workflows passed. The remaining `Shared Storage` Java 25 job
failed at `RemoteObjectIndex.java:87`, Checkstyle `NPathComplexity`
1312 > 500. Storage compiled and the real MinIO three-broker
workflow passed. **Do not lower Checkstyle or weaken validation.**

Extract each packed reference's conflict checking and bounded-alternate
selection into separate helpers, leaving `addReferences` to stage
changes and publish only after full object validation. Preserve
the two-lowest-object-ID read fallback, no more than one alternate,
and no COMMITTED physical DELETE. Regression tests assert (a) a
later packed-partition conflict cannot leak a first-range fallback
into the live index; and (b) changing a previously admitted
alternate's immutable physical descriptor is rejected without
corrupting the previous reference. Enforce both new methods as
required unskipped JUnit evidence (622 -> 624).

Native Kafka replication (Leader/Follower, ISR, HW, epochs), PR Draft
status and the frozen MinIO GA branch remain unchanged. A successful
code commit is NOT a complete gate: final Java 25/Checkstyle, real
MinIO and all active workflows must pass before sealing this batch.

## Batches 265–266 — asynchronous COMMITTED read fallback safety

**Stable base:** Batch 264 at
`bf7dd9bc26b49b6b0c856e9c201073303e8a3f8a`:
17/17 GitHub Actions SUCCESS, including Java 25 Checkstyle,
native Kafka replication HA, real MinIO and Ring WAL.

- **265 — consistent read fallback snapshot.** Metadata COMMIT replay
  can publish an alternate and canonical physical reference in two
  distinct concurrent maps. Ordinary successful reads remain
  lock-free; only exceptional fallback lookup now takes the same
  short `RemoteObjectIndex` monitor as object publication to
  observe both indexes together. A deterministic concurrent test
  holds a metadata writer lock while a failed asynchronous read
  selects a physical backup, then verifies it uses the newly
  committed canonical object after publication.
- **266 — isolate shared I/O failures.** Multiple readers can share
  one failed `CompletableFuture` or cached transport exception.
  When both physical copies fail, report a new request-specific
  `IOException` with immutable primary cause and secondary
  suppressed failure, rather than mutating the shared original
  exception. Verify repeated calls do not accumulate suppressed
  exceptions on a reused transport failure. The source of either
  unreadable COMMITTED object remains discoverable; no reader
  behavior ever triggers deletion. Enforce both additional
  regression methods in the mandatory JUnit manifest (624 -> 626).

The authoritative lifecycle follows Kafka's existing native replica
semantics plus the AutoMQ-inspired Controller/KRaft object-state
pattern. These read-path changes are NOT a Controller retirement
certificate. COMMITTED physical DELETE stays disabled. PR remains
Draft; frozen MinIO GA release branch remains untouched.

## Batch 267 — cancellation is not a COMMITTED object read failure

Base `deff997e5c91135edd2225358c4340d1ac60e085`
had 16/17 successful CI workflows. The only failure was the
`:storage:checkstyleTest` unused `ExecutionException`
import in `RemoteObjectIndexTest`; remove that import without
suppressing Checkstyle.

The prior bounded read fallback retried a second COMMITTED physical
S3 object for every exceptional completion, including a cancelled
primary read. Cancellation is a caller or lifecycle signal, not a
corruption/availability signal. Short-circuit on the original
`CancellationException` before reading the alternate; retain
normal fallback for actual remote-object errors and CRC mismatches.
Add a regression with a cancelled primary future and a known
equivalent COMMITTED alternate; assert exactly one physical range
read and propagated cancellation. Enforce the new test in the
strict JUnit manifest (626 -> 627).

No COMMITTED deletion, new metadata writer, replication protocol
or additional hosted CI workflow is enabled. The Draft PR and
frozen MinIO GA release branch are unchanged.

## Batches 268–269 — fail-fast KSO2 object-index GET and replicated fallback

**Stable base:** Batch 267 at
`39f02dfd9be8e30f4176765d808f4d79d9e91bb0`, 17/17
GitHub Actions SUCCESS. Keep native Kafka Leader/Follower, ISR,
HW, LeaderEpoch and BrokerEpoch replication unchanged.

- **268 — prevent head-of-line blocking on a broken remote object.**
  KSO2 cold reads issue Header and Footer Range GETs concurrently.
  `CompletableFuture.thenCombine` can await both completions even when
  one has failed and the other remains outstanding. That delays
  physical-copy fallback indefinitely under an object-store partial
  outage. Fail the combined future as soon as *either* GET fails;
  preserve the existing two-GET parallel fast path and normal
  successful stream index CRC/footer validation. In-flight GETs
  remain owned by the storage transport and are not treated as
  deletion evidence.
- **269 — actual replica-read recovery proof.** A deterministic
  index reader test covers both directions: failed Header with
  pending Footer, and failed Footer with pending Header. A
  `SharedObjectReader` test verifies that an unavailable canonical
  Header still immediately falls back to a separate COMMITTED
  physical copy even when the first object's Footer GET remains
  pending. Neither case uses timers, retries, new consensus or
  COMMITTED physical delete. Enforce the two Java regression
  methods as required unskipped JUnit evidence (627 -> 629).

This is a targeted reliability correction to the AutoMQ-style
object-storage read path, not a claim that AutoMQ uses our exact
future-combination implementation. The planned eventual
Controller/KRaft `MARK_DESTROYED`-like object retirement remains
separately blocked on Kafka replica, packed-object and reader fences.
The frozen MinIO GA branch is untouched; PR #2 stays Draft.

## Batch 270 — COMMITTED inventory identity gate before object retirement

**Stable base:** Batch 268–269 at
`aff3959dc68e4ebac4e976962f7bdcb19e8191fc` passed all
17 GitHub Actions workflows, including native Kafka replicas,
Java 25, real MinIO and controller failover.

AutoMQ's `S3ObjectControlManager.markDestroyObjects` consults its
Controller-owned object metadata before creating a durable
`MARK_DESTROYED` transition. The equivalent safety invariant
here is not established merely by old partition LogStart values:
the exact immutable physical descriptor must still exist in the
COMMITTED metadata inventory. Otherwise a PREPARED object, a
compacted tombstone, or a forged/incomplete multi-partition object
could borrow old LogStarts and be misclassified as expired.

`CommittedObjectRetirementPrecheck.assess(object, image)` now
performs **one atomic image snapshot** of the requested
COMMITTED object ID, full immutable packed-object descriptor and
the *explicit* log-start watermarks for only its ranges. It rejects
missing/PREPARED/tombstoned objects with `OBJECT_NOT_COMMITTED`,
and mismatched descriptors with
`COMMITTED_DESCRIPTOR_MISMATCH`. The targeted snapshot avoids
copying the entire COMMITTED-object inventory and all unrelated
partition watermarks for each single-object assessment.

Existing positive precheck regressions now first replay an actual
COMMITTED metadata record. New negative tests cover an absent
or merely PREPARED object, a forged truncated packed descriptor,
a historical tombstone after an initially expired COMMIT, and an
immutable per-object snapshot across later watermark replay.
Enforce all four methods via strict storage JUnit evidence
(629 -> 633). The whole-inventory diagnostic classification
remains advisory as before.

**Still blocked:** Kafka leader/follower and ISR/HW remain
authoritative for replica semantics. A replayed metadata snapshot
is not a Controller/KRaft durable retirement record, replica
reader-drain certificate, latest read-committed horizon or
physical S3 DELETE authorization. The Draft PR remains Draft;
the frozen MinIO GA branch remains unchanged.

## Batch 271 — Kafka native replica-reassignment fence in KRaft retirement preflight

**Baseline:** Batch 270 at
`87b9da0ab38375a1f8c662bb78e4b67f89fb8597` completed
17/17 GitHub Actions SUCCESS, including real MinIO, native
leader/follower semantics, controller HA and Java 25.

Kafka's actual `PartitionRegistration.merge(PartitionChangeRecord)`
increments `partitionEpoch` on any partition change while
`leaderEpoch` only increments when the leader changes. In
particular, adding/removing replicas can keep the same leader
and ISR-visible source epoch. An AutoMQ-like shared object
lifecycle adapted to *replicated Kafka* must not treat unchanged
`leaderEpoch` as proof that the replica topology is stable.

The existing read-only Controller-event-queue preflight now
rejects any current KRaft partition registration with
`addingReplicas` or `removingReplicas`, reporting
`REPLICA_REASSIGNMENT_IN_PROGRESS`. This classification
propagates to `PartitionRetirementCandidatePrecheck` as
`LEADER_REGISTRATION_MISMATCH` without minting a durable
retirement decision. Tests use the actual `PartitionChangeRecord`
merge implementation to prove add/remove transitions increase
`partitionEpoch` without changing `leaderEpoch`, the candidate
is rejected, and completed reassignment only restores a
*read-only* leadership match. Require all four new tests as
unskipped JUnit evidence (633 -> 637).

This conservative preflight is **not sufficient** for object
GC authorization: a future trusted Controller writer also
needs the expected `partitionEpoch` or equivalent immutable
replica-configuration generation carried through issuance and
commit revalidation; native ISR/HW/reader quiescence must be
established for every packed object reference. Do not add
new ad-hoc Broker authentication or override Kafka's existing
replication protocol. COMMITTED physical DELETE is still
disabled, PR #2 remains Draft, and the frozen MinIO GA branch
is untouched.

## Batch 272 — retain replica-reassignment fence without weakening Checkstyle

The first Batch 271 CI at
`cfbfc44f16d1769390860ea928518494b27a8184`
finished 16/17 workflow SUCCESS. The only failure occurred in
`:metadata:checkstyleMain`: `PartitionRetirementControllerPrecheck`
`assessRegistration` NPath Complexity 864 exceeded 500.
Java compilation, MinIO three-broker E2E and all other workflows
completed successfully. Do not change the Checkstyle threshold.

Extract the already validated leader/recovery/epoch checks and
native replica-topology checks into separate small methods,
preserving existing diagnostic `Finding` precedence and the
`REPLICA_REASSIGNMENT_IN_PROGRESS` fail-closed result.
This is a structural repair only: it must not make a matching
KRaft leader registration a retirement grant. The four Batch 271
regressions remain required unskipped JUnit evidence (637 total);
there is no physical COMMITTED delete or new source-auth scheme.

## Batch 273 — native Kafka partitionEpoch-bound retirement candidates

**Baseline:** Batch 272 at
`3ed5132208f13717ccfe88aa16af392c6c0c6d49`
finished 17/17 workflow SUCCESS. This is an AutoMQ-style
storage-disaggregated system with **native replicated Kafka**:
Leader/Follower, ISR, HW and LeaderEpoch semantics are unchanged.

Kafka `PartitionRegistration.merge(PartitionChangeRecord)`
increments `partitionEpoch` for partition changes even when
`leaderEpoch` does not change. ISR-only shrink, replica directory
changes and post-reassignment topology are therefore not fenced by
leaderEpoch alone.

Add a required non-negative `sourcePartitionEpoch` to the
NON-EMITTING `PartitionRetirementCandidatePrecheck.Candidate`,
checked against the Controller's *current KRaft
PartitionRegistration.partitionEpoch* inside the serialized
Controller preflight. A stale value yields a distinct
`PARTITION_EPOCH_MISMATCH`; a perfect match still yields
`SOURCE_LOG_START_NOT_VERIFIED` because no trustworthy
cross-replica WAL/LogStart attestation is connected.

Carry that epoch through the existing Java 11 transport-neutral
`PartitionRetirementSourceProbe.Request` identity, equality
and hashCode, plus both unstable BrokerHeartbeat v3 tagged
Challenge and Report structures. Controller-issued one-time nonce
payloads, Broker decode/capture/echo and Controller report decode
must preserve the same epoch. Stable versions v0-v2 remain
unchanged and the v3 fields remain disabled for production
listeners.

New negative tests cover ISR shrink without leaderEpoch bump,
stale candidate rejection and fresh epoch re-preflight, one-use
nonce equality changes under the same LeaderEpoch, mandatory
typed Request validation, and v3 serialization of both sides.
Four new metadata JUnit methods are strict retirement
evidence (637 -> 641); the fifth, Java 11 BrokerHeartbeat
v3 protocol test, is enforced by the existing clients:test
XML evidence check in shared-storage.yml.

**Still blocked:** this remains read-only. The eventual
Controller/KRaft writer must revalidate partitionEpoch and
all replica ownership before atomically committing a durable
retirement transition. All packed object references, reader
quiescence, in-flight uploads and crash-safe physical deletion
must be handled separately. COMMITTED DELETE is disabled,
PR #2 stays Draft, and the frozen MinIO GA branch is unchanged.

## Batch 274 — repair storage test fixture after required partitionEpoch v3 migration

**Inherited HEAD** `2ebd05992dd0d1772abbfe800e9fceef42403d7c`
completed 12/17 GitHub Actions SUCCESS. The five other workflows
were blocked by the same `:storage:compileTestJava` errors in
`BrokerLocalStartupReplayPrecheckTest`: two callers still supplied
11 arguments to the new 12-argument
`PartitionRetirementSourceProbe.Request` constructor.

Update the explicit malformed-nonce fixture and its shared positive
probe helper to supply a concrete non-negative Kafka
`sourcePartitionEpoch` in the correct position. The test fixture
uses the existing source leader epoch 8 as its matching partition
generation. Also assert that a *valid* nonce/broker identity combined
with an unknown negative partition epoch is rejected. Do not
restore an old constructor or waive the required epoch field.

Only the test fixtures and release-design evidence change. The
actual v3 heartbeat, Controller/KRaft read-only CAS fencing, native
Kafka ISR/HW replication and all storage production code remain
untouched. No extra workflow is introduced. The complete 17/17
existing CI gate must run before considering Batch 274 verified.
COMMITTED physical DELETE stays disabled; PR #2 remains Draft,
and the frozen MinIO GA branch is unchanged.

## Batch 275 — immutable Kafka RecordBatch length parity across COMMITTED read and checkpoint

**Stable base:** Batch 274 at
`f72a2fc9ee094166b8fb81d77a8119765a8edb4b`
passed 17/17 workflows including native Kafka replicas, real
MinIO, Java 25, WAL crash and Controller HA.

`LocalRemoteObjectCheckpoint.sameLogicalRange` requires equal
Kafka offset range, LeaderEpoch, CRC and byte length.
`RemoteObjectIndex.sameLogicalRange` previously omitted byte
length. This could admit a different-size physical COMMITTED
copy with the same declared CRC, or expose an unsafe bounded
read fallback after checkpoint restore.

Require the RecordBatch `objectLength` to agree in both
paths. Do not compare whole-object length/checksum or physical
`objectPosition`, because an identical Kafka batch can live
in differently packed S3 objects; the actual bytes are still
independently CRC-verified by the reader.

Regression witnesses cover a same-range/same-CRC length
mismatch, an object whose later packed partition conflicts
after a lower-ID reference was staged, and durability-time
checkpoint rejection of the same mismatch without changing
the atomically persisted image. These three methods join the
existing unskipped JUnit evidence contract (641 -> 644).
No new hosted CI workflow or native build is added.

This is an immutable data identity/read correctness rule,
not a GC or cross-replica durability certificate.
Kafka Leader/Follower, ISR, HW, LeaderEpoch, partitionEpoch,
and non-emitting Controller preflight are unchanged.
COMMITTED physical DELETE remains disabled, PR #2 stays Draft,
and frozen MinIO GA release HEAD
`9a28c11b6d97ecacafe1b8566cde5cc34d604889` is untouched.

## Batch 276 — actual KSO2 repacking and cross-partition failover after checkpoint restore

**Stable base:** Batch 275 at
`256f3cfdec965a4cd666d130f07ea45beb423ffd`
completed 17/17 GitHub Actions SUCCESS including real MinIO,
three-broker Kafka semantics, Controller HA and WAL crash.

Two independently COMMITTED physical copies can pack identical
Kafka RecordBatches with different additional partitions, DataBlock
positions and total S3 object size/CRC. Equal logical offsets,
LeaderEpoch, RecordBatch CRC and byte length must admit a read
fallback without confusing the other partition's bytes with the
requested Kafka range. The prior read-fallback tests packed two
objects with otherwise identical layouts.

Add a deterministic end-to-end Java test using the **real KSO2
SharedObjectPacker**: serialize a canonical two-batch object for
partition zero and a physically different backup that prepends a
different partition's batch. Assert identical immutable Kafka
logical identity but differing object positions and object sizes.
Commit both into a fsynced `LocalRemoteObjectCheckpoint`, reopen
it, then replay the alternative COMMITTED metadata into a restored
`RemoteObjectIndex`. The canonical copy is absent from S3:
verify that both Kafka batches and the unrelated partition are
read exclusively from the independently packed alternative, with
normal KSO2 index and CRC checks. The checkpoint-only stage
correctly lacks a backup until COMMITTED metadata replay.

Make this method required in the existing unskipped JUnit evidence
manifest (644 -> 645); no extra workflow or Native Image build.
Keep Kafka's native Leader/Follower, ISR, HW and epoch semantics;
no local checkpoint or replica index may authorize physical
COMMITTED deletion. Controller/KRaft retirement writer and
cross-replica reader fences remain separate unfinished work.
PR #2 remains Draft and the frozen MinIO GA release branch is
unchanged.

## Batch 277 — persist native Kafka partitionEpoch through KRaft authority replay

**Stable base:** Batch 276 at
`97ed93c815b6399abffe1cb7c93f7f542216eea6` passed
17/17 GitHub Actions SUCCESS (Java 25, native Kafka replicas,
real MinIO, Controller HA, WAL Crash and Ring WAL).

Batch 273 bound the diagnostic Controller preflight and v3
Source/WAL challenge to native Kafka `partitionEpoch`, but the
future KRaft `PartitionRetirementAuthorityRecord` and its
`PartitionRetirementAuthorityState.Value` omitted this version.
A future authenticated writer could otherwise append a value
that loses the generation when controller leadership or a KRaft
snapshot is replayed, even if ISR or replica membership changed
without advancing the source `leaderEpoch`.

Add optional v0 **flexible tagged field 0**
`SourcePartitionEpoch` (default -1) to the generated
KRaft metadata schema. Existing v0 records deserialize with
-1, which explicitly means no topology-generation proof;
legacy 5- and 7-argument Java `Value` constructors keep this
absence instead of inventing a version. Candidate proposals
carry the actual nonnegative partition epoch into the new
8-field authority value; no writer, external API or GC is
enabled. KRaft record encoding/decoding and the Controller
timeline retain the epoch on replay and snapshot rollback.

`PartitionRetirementAuthorityState.validateAdvance` fails closed:
once a nonnegative partition epoch has been durably established,
no later active version may erase or regress it. A later leader
epoch must also carry an advanced partition epoch. For unchanged
leaders, monotonic ISR/reassignment generations may increase
while the source LogStart watermark advances. A pre-existing
legacy value can be upgraded to a first real topology proof.
These replay defenses supplement, but never replace, actual
Controller-event-queue commit-time partition-epoch revalidation.

Eight new unskipped regression witnesses cover tagged v0 binary
roundtrip, legacy v0 absence, partition epoch rollback/erasure,
new-leader generation, ISR-only version advance, real KRaft
replay rejection, SnapshotRegistry rollback, and candidate
proposal roundtrip (645 -> 653 mandatory tests).

**Still prohibited:** No controller authority writer or trust
adapter is activated. A persisted KRaft partition epoch
alone does not prove replica durability, COMMITTED object
retirement safety, packed-object reader quiescence or
permission to invoke physical DELETE. Keep native Kafka
Leader/Follower, ISR and HW semantics. PR #2 stays Draft and
the frozen MinIO GA release branch remains untouched.

## Batch 278 — feature-gated full KRaft image snapshot preserves replica generation

**Stable base:** Batch 277 at
`350aaadf13eb1459a046676fdc426d24a598de72`
completed 17/17 GitHub Actions SUCCESS, including Java 25,
real MinIO three-broker E2E, Controller HA and WAL crash.

A single `PartitionRetirementAuthorityRecord` v0 tagged
`SourcePartitionEpoch` serialization test and Controller
timeline replay are not the complete KRaft failure-recovery path.
An actual `MetadataImage` snapshot also writes finalized
features, all authority records and topic-ID tombstones; the
new Controller constructs a fresh `MetadataDelta` and resumes
ordinary replay against the recovered immutable image.

Four deterministic feature-negotiated full-image regressions
verify that (1) a nonnegative native Kafka `partitionEpoch`
survives metadata snapshot writing/replay with broker incarnation,
(2) a legacy untagged record remains explicitly unknown (-1)
until a **later** KRaft authority offset upgrades it,
(3) a stale ISR-only version is rejected after restoration
despite unchanged LeaderEpoch and increased AuthorityOffset,
without replacing the previous state, and (4) a terminal
topic-ID tombstone retains its last generation and cannot be
revived after a fresh snapshot. Run the complete
`MetadataImage.write -> RecordListWriter -> MetadataDelta
.finishSnapshot -> apply` path at experimental metadata 4.4.
Require the four methods in the existing JUnit evidence contract
(653 -> 657); no extra workflow and no Native Image build.

The presence of a recorded `partitionEpoch` is **not**
authenticated native source/WAL evidence, a cross-replica
object-reference barrier or a GC certificate. Production
KRaft emitter and physical COMMITTED DELETE remain disabled;
Kafka Leader/Follower ISR/HW stays authoritative.
PR #2 stays Draft; frozen MinIO GA is unchanged.

## Batch 279 — secondary COMMITTED read cancellation remains cancellation

**Stable base:** Batch 278 at
`409414458f9bd925f39350c21058afc3f345b5c2`
passed 17/17 workflows, including Java 25, native Kafka replication and real MinIO.

If a primary COMMITTED physical object fails, the asynchronous reader may
fall back to one independently verified equivalent immutable copy. An
outstanding alternate transport may then be cancelled as part of request
or storage lifecycle. Previously the fallback's completion handler treated
every alternate exception as an object-read failure and wrapped cancellation
as "both COMMITTED physical copies unreadable" IOException.

Propagate the alternate's actual `CancellationException` instead.
The primary-cancel no-fallback behavior remains unchanged. True double
transport/corruption failures still retain both diagnoses in a fresh,
per-request IOException; no remote reference changes, third retry, metadata
tombstone or physical deletion is introduced.

A deterministic KSO2 regression injects primary physical I/O failure,
then cancels the still-pending alternate Header/Footer requests. It
requires the result to remain cancellation and forbids a spurious dual
object-loss IOException. Raise strict unskipped JUnit evidence
657 -> 658; retain existing CI and skip Native Image.

Native Kafka Leader/Follower, ISR, HW, epoch/partitionEpoch semantics and
Controller ownership remain unchanged. PR #2 remains Draft, COMMITTED
physical DELETE disabled and frozen MinIO GA HEAD
`9a28c11b6d97ecacafe1b8566cde5cc34d604889` untouched.

## Batch 280 — bound corrupted KSO2 batch counts by physical DataBlock size

**Stable base:** Batch 279 at
`1acfe7a2cbe9076804e141db1909e76ce62fc37b`
passed all 17 GitHub Actions, including Java 25, Kafka replicated
acks=all and real MinIO.

The immutable KSO2 decoder previously verified the DataBlock header
against its index and validated payload CRC before allocating
`new ArrayList<>(header.batchCount())`. A corrupted or malformed
Header+Index pair with matching (but impossible) batch counts could
request enormous allocation despite only a few bytes of block payload.
Such input must fail as invalid object data rather than risk a Java
out-of-memory failure.

Each serialized Kafka batch requires its fixed 24-byte entry header
and a nonempty payload. Validate
`batchCount <= dataLength / (BATCH_ENTRY_HEADER_BYTES + 1)`
after Header/Index checks and before list allocation. Division avoids
multiplication overflow. This adds no allocations or remote requests on
the normal read path.

Two deterministic regression methods use a real KSO2 packed block,
forge both its block header and expected index entry to claim
`Integer.MAX_VALUE` or just three batches, retain the valid existing
payload CRC, and require an early IOException, not allocation based on
the forged count. Strict unskipped Java JUnit manifest grows 658 -> 660
with unchanged hosted CI workflow and no Native Image gate.

This is read-path robustness only. Kafka native replication, KRaft
Controller authority, disabled COMMITTED physical DELETE, PR #2 Draft
and frozen MinIO GA at
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`
remain unchanged.


## Batch 281 — isolate KSO2 index-cache publication from remote-object I/O

**Stable base:** Batch 280 at
`c6ecdff0f0f3ec322bd83d376cc90639c022b1cc` passed
all 17 Java 25, real MinIO, Kafka replication and WAL/Controller
workflow gates.

The bounded `StreamObjectIndexCache` previously called
`StreamObjectIndexReader.load` **inside** its shared LRU monitor.
That loader invokes synchronous `ObjectStore.rangeRead` before it
returns a Future. A stalled physical Header GET could therefore
serialize unrelated cold reads from other objects/partitions and
delay a Kafka replica's remote fallback even if an independent
COMMITTED copy is healthy.

Install the per-descriptor `CompletableFuture` while holding only
the short LRU lock, then run the actual Header/Footer I/O outside
the monitor. Concurrent readers for the same immutable object
receive the same pending Future; distinct object keys can issue I/O
independently. A synchronous exception or asynchronous failure
completes the placeholder exceptionally and removes it using an
identity-checked cache entry, preserving retry safety after eviction
or replacement. Null loader results fail closed. Retain existing
cache capacity/LRU limits.

Two required deterministic regressions cover (1) a deliberately
blocked Header GET, concurrent same-object coalescing and unrelated
object progress, and (2) synchronous object-store failure followed
by a successful retry with the same key. Enforce them as unskipped
storage JUnit evidence (660 -> 662) in the existing workflow.

This change does not alter native Kafka Leader/Follower, ISR, HW,
S3 object identity, KRaft authority, Controller epoch fencing,
the inactive COMMITTED physical DELETE path, or the frozen MinIO GA
`9a28c11b6d97ecacafe1b8566cde5cc34d604889`.
Keep PR #2 Draft; do not trigger additional Native Image CI.
