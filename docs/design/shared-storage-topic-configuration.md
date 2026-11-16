# Topic-level shared storage

## Contract

`shared.storage.enable` is a Boolean Topic configuration, defaulting to `false`.
Installing the shared-storage extension does not enroll an ordinary Topic. With the
default settings, ordinary Topics use Kafka's original `UnifiedLog` implementation,
replication, ISR, high watermark and request handling. Internal Topics remain classic.

The storage mode is selected when the Topic is created. The Topic configuration is
stored in KRaft and is delivered through the ordinary `LogConfig` path. The shared
log factory, partition-role tracking and upload eligibility use the same successfully
created log identity, including Topic ID. Deleting and recreating a name creates a
new identity and does not inherit the old shared-storage mode.

This is the WAL/object-storage data plane, separate from Kafka tiered storage.
`shared.storage.enable=true` and `remote.storage.enable=true` cannot be combined.

## Create and inspect Topics

First deploy compatible broker/controller binaries and configure the storage
extension on every broker. The extension must start successfully before its broker
can advertise the shared-storage capability. Bucket, WAL, credentials and extension
installation remain broker settings; new Topic names do not belong in broker lists.

Activate the cluster compatibility boundary explicitly:

```sh
bin/kafka-features.sh --bootstrap-server localhost:9092 \
  upgrade --feature shared.storage.version=1
```

The feature defaults to zero even when formatting or upgrading to the latest
metadata version. Activation requires metadata version 3.7 or later and support
from every registered broker and controller. A broker without the configured
extension, or a broker still using legacy selector compatibility, cannot satisfy
this requirement. After activation, an old, extensionless or legacy-mode broker
cannot register, and downgrading the feature to zero is rejected, including an
unsafe-downgrade request.

Create a shared Topic with the normal Topic creation API or CLI:

```sh
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic shared-events --partitions 3 --replication-factor 3 \
  --config shared.storage.enable=true --config min.insync.replicas=2
```

Create an ordinary Topic by omitting the option:

```sh
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic ordinary-events --partitions 3 --replication-factor 3
```

At feature level one, the controller persists an explicit `false` for omitted
values on newly created Topics. At feature level zero,
explicit `true` and explicit `false` are both rejected: old brokers may ignore the
new key, so neither value can safely override their selectors. Creating an ordinary
Topic without the new key remains supported at feature level zero.

Inspect the effective value through Admin `describeConfigs` or:

```sh
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name shared-events --describe --all
```

Existing Topic mode changes are rejected before configuration records are written.
This includes `true` to `false`, classic to `true`, and removal of an existing true
value. Repeating an unchanged explicit value is harmless. At feature level one,
normalizing a classic Topic between explicit `false` and an absent key is allowed;
both resolve to the same classic implementation, and legacy brokers cannot join
that cluster. Unrelated supported configurations retain their usual update behavior.
`kafka-configs.sh --alter` is therefore not a data migration command.

## Upgrade an existing selector-based deployment

Old releases route by `shared.storage.topics` and `shared.storage.topic.pattern`.
If neither selector is configured, they route all non-internal Topics to shared storage.
Their empty Kafka payload files are not sufficient evidence of a classic log; the
payload may exist only in the broker WAL or remote objects. The new default must
not reinterpret that state.

Use the following binary upgrade phase:

1. Inventory the existing Topic IDs and effective classic/shared choices. Preserve
   the exact selector settings, WAL directories, object-store namespace and metadata
   Topic. Resolve selector differences across brokers before proceeding.
2. Freeze Topic creation, deletion/recreation and reassignment while old and new
   binaries coexist. The new Topic key cannot coordinate routing with old binaries.
3. On every upgraded broker, set
   `shared.storage.legacy.selectors.enabled=true` and retain its existing selectors.
   When the Topic key is absent, this explicit compatibility mode retains the old
   selector behavior, including the previous default-all case. An unmarked,
   nonempty old WAL is rejected without this compatibility opt-in.
4. Roll the binaries one broker at a time. Check ISR convergence and acknowledged
   record reads before moving to the next broker. The existing rolling-upgrade
   test covers an old/new/old rollback probe during this phase, before feature
   activation, with selectors unchanged.
5. Keep this legacy cluster at feature level zero. Compatibility-mode brokers do
   not advertise support for activating Topic mode, even when their binaries know
   the new configuration. This prevents a newly assigned broker with no old local
   marker from misinterpreting an absent Topic key as classic.
6. Resume only operations supported by the unchanged legacy deployment. Use the
   separate migration procedure below to move to Topic-managed storage. Do not
   remove the compatibility setting or selectors merely because all binaries are
   now new.

The freeze and selector inventory are operator requirements; this change does not
introduce a cluster maintenance lock or prove the correctness of an operator's
historical selector inventory. A passing legacy binary-roll test is not evidence
that arbitrary selector changes or mixed-version Topic operations are safe.

## Existing data and ending compatibility mode

This change preserves existing selector deployments through an explicit upgrade
mode. It does not implement online conversion or automatically adopt an existing
Topic by adding `shared.storage.enable`.

Keep compatibility enabled while existing Topics still depend on legacy selection.
For the first implementation, move application traffic to a separately initialized
cluster whose brokers use the default Topic mode and whose cluster feature is
explicitly activated. Create new Topics there with the desired mode, then use an
application-reviewed data-copy and cutover procedure. Verify copied records and the
new read/write path before retiring the old Topics and their storage state.

Reusing a cluster requires first retiring all legacy shared Topics and their
dependent state through a separately verified procedure; toggling a broker flag is
not sufficient. This change does not supply an automated in-place conversion tool.
A copy creates new Topic IDs and generally new offsets; it does not preserve
consumer group offsets or transaction history automatically. Do not change an old
selector or edit metadata files to simulate a migration.

Per-log durable mode markers reject reopening an initialized log through the wrong
physical implementation. An unsupported or missing extension must fail closed for
a shared Topic; it must not silently load an empty classic log. Markers and replayed
Topic IDs also prevent a restarted log from inheriting a different incarnation's
mode. Preserve these markers with the log directory during supported storage
operations. They are a guard against misrouting, not a copy or conversion mechanism.

## Evidence and limits

The focused CI coverage includes:

- Config parsing, mutual exclusion with tiered storage, and default false.
- Controller create/update/delete-config validation, explicit false persistence,
  mixed-feature activation rejection and downgrade rejection.
- Real Admin requests on a Kafka test cluster: an ordinary Topic without the
  extension, extension-present default-off routing, legacy default-all preserved
  with feature activation rejected, explicit shared creation,
  immutable mode, unrelated configuration updates, produce/consume, broker restart,
  and same-name recreation as a classic Topic with a new Topic ID. A separate
  three-broker case keeps one shared replica offline during deletion/recreation,
  restarts it and reassigns the new classic Topic to all three brokers.
- Runtime routing, mode-marker rejection, Topic-ID-bound partition-role tracking
  and legacy selector compatibility.
- The existing independent old/new distribution rolling test, with explicit legacy
  compatibility, plus the existing shared WAL/S3 Topic lifecycle tests.

The main CI gate requires executed JUnit results with zero skips. Production
fingerprints and workflow triggers include the new controller, feature, broker and
Topic configuration paths, so older GA evidence cannot be reused for this change.

CI results must be read from the exact candidate run. This document does not claim
that online migration, post-activation rollback, mixed-version Topic administration
or a production rollout has been validated.
