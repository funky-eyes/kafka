# Shared Storage Operations Runbook

This runbook defines the minimum production operating procedure for the Shared Storage extension. It is intentionally
written around observable states and fail-safe actions. Do not bypass WAL, metadata, or remote-checkpoint safety
barriers to recover capacity.

## Production invariants

Operators should treat the following as non-negotiable:

1. Kafka acknowledgement semantics remain authoritative. Shared Storage must not acknowledge data before the configured
   Kafka durability contract is satisfied.
2. The broker-local WAL is the write durability path while S3 publication is asynchronous.
3. `__shared_storage_metadata` is the authoritative remote object metadata plane.
4. The broker-local remote checkpoint is a recovery accelerator and the durability prerequisite for WAL reclamation;
   it is not the cluster source of truth.
5. WAL reclamation must never be forced past durable remote coverage.
6. Kafka internal topics remain on the classic log path.
7. Persistent object and metadata formats are release compatibility contracts. Never modify persisted bytes manually.

## Required monitoring

The extension publishes broker-scoped Yammer/JMX metrics under:

`kafka.server:type=SharedStorage,brokerId=<broker-id>`

At minimum collect and retain:

| Metric | Meaning | Initial operating action |
| --- | --- | --- |
| `WalUsedBytes` | Bytes currently retained in the shared WAL | Correlate with capacity and upload progress |
| `WalCapacityBytes` | Configured WAL capacity | Use for alert ratios and capacity planning |
| `WalUtilizationPercent` | WAL occupancy percentage | Warn at 70%, page at 85%, critical at 95% |
| `PendingRemoteCheckpoints` | Remote COMMITs waiting for local checkpoint durability | Persistent growth indicates checkpoint/reclaim trouble |
| `RemoteControlPlaneReady` | 1 after authoritative metadata bootstrap/reconciliation | Must normally remain 1 after broker startup |
| `MetadataBootstrapFailureCount` | Number of metadata bootstrap failures observed by this broker process | Any increase requires investigation |
| `UploadsInProgress` | Active object uploads | Compare with configured max inflight |
| `ReservedUploadCandidates` | WAL candidates currently reserved for upload | Persistent non-zero with no progress indicates a stuck upload |
| `UploadCandidateCount` | Eligible committed WAL candidates | Sustained growth indicates upload debt |
| `EligibleUploadBytes` | Bytes eligible for remote publication | Primary upload backlog signal |
| `UploadFailurePresent` | 1 while the last upload attempt remains failed | Page when persistent |
| `MaintenanceFailurePresent` | 1 while checkpoint/reclaim maintenance remains failed | Critical because WAL cannot reclaim safely |

The initial thresholds above are conservative release defaults. Replace them with measured SLO-derived thresholds after
the performance and soak gates establish a production baseline.

## WAL pressure

### Symptoms

Typical signals:

- `WalUtilizationPercent` rising continuously;
- `EligibleUploadBytes` rising;
- producer latency rising;
- eventual WAL-capacity backpressure.

### Diagnosis order

1. Check `RemoteControlPlaneReady`.
2. Check `UploadFailurePresent`.
3. Check `MaintenanceFailurePresent`.
4. Check S3 reachability, latency, throttling, and credentials.
5. Check `UploadsInProgress` versus `shared.storage.upload.max.inflight`.
6. Check whether `PendingRemoteCheckpoints` is growing.
7. Confirm the metadata topic has a healthy leader and ISR.

### Safe action

Restore the failed dependency and let upload/checkpoint/reclaim converge naturally.

Do **not**:

- delete or truncate the WAL manually;
- remove the local remote checkpoint to create space;
- delete committed S3 objects;
- lower Kafka ISR/minISR rules to make the backlog disappear.

A full WAL is an intentional fail-closed condition when remote durability cannot advance.

## S3 outage

During an S3 outage the broker continues using replicated WAL durability until WAL capacity applies backpressure.

Expected state:

- `RemoteControlPlaneReady` may remain 1 if the metadata plane is healthy;
- `UploadFailurePresent` becomes 1;
- `EligibleUploadBytes` and WAL utilization increase;
- acknowledged data remains governed by Kafka replication semantics.

Recovery procedure:

1. Restore endpoint/network/DNS/credentials.
2. Confirm uploads resume.
3. Confirm `UploadFailurePresent` returns to 0.
4. Confirm `EligibleUploadBytes` trends toward 0.
5. Confirm `PendingRemoteCheckpoints` converges.
6. Confirm WAL utilization falls after checkpointed reclaim.
7. Only then clear the incident.

## Remote metadata bootstrap failure

The broker starts its local WAL path before the remote metadata control plane is ready. Upload and reclaim remain
disabled until authoritative metadata replay succeeds.

Investigate when:

- `RemoteControlPlaneReady=0` persists after startup;
- `MetadataBootstrapFailureCount` increases.

Check:

- connectivity to the configured metadata listener;
- health and ISR of `__shared_storage_metadata`;
- broker authorization;
- replay/codec errors in broker logs;
- whether another broker can read the metadata topic.

Do not delete the metadata topic. Its contents are authoritative for remote object ownership and coverage.

## Maintenance failure

`MaintenanceFailurePresent=1` means local checkpoint or WAL reclaim maintenance failed.

Because reclamation is fail-closed, the immediate risk is capacity exhaustion rather than silent data loss.

Procedure:

1. inspect the underlying I/O exception;
2. verify the WAL/checkpoint filesystem is writable and has free space;
3. verify atomic rename and directory fsync semantics are supported;
4. restore the filesystem;
5. confirm the metric returns to 0;
6. confirm `PendingRemoteCheckpoints` and WAL utilization converge.

Never edit `remote-object-ranges.checkpoint` manually.

## Local WAL/checkpoint loss

The supported disaster-recovery source of truth is authoritative remote metadata plus immutable remote objects.

If a broker loses its local Shared Storage state:

1. stop the affected broker;
2. preserve any remaining files for diagnostics;
3. restore/replace the failed local volume;
4. restart the broker with the same cluster identity and supported configuration;
5. let metadata replay rebuild remote state;
6. confirm `RemoteControlPlaneReady=1`;
7. confirm the replica catches up and ISR returns to the expected size;
8. verify producer/consumer traffic before returning the broker to normal rotation.

Do not copy WAL files between brokers.

## WAL engine changes

`shared.storage.wal.engine` supports `ring` and the legacy `rotating-file` backend.

The implementation deliberately refuses unsafe backend switching when files from the other engine are still present.

Migration rule:

1. run the existing engine until all required data has remote coverage;
2. stop the broker cleanly;
3. retain a backup of the WAL directory;
4. follow a release-tested migration procedure;
5. never rename ring/rotating files to bypass the startup guard.

For GA, `ring` is the default and should be used unless a release note explicitly documents otherwise.

## Capacity planning

Key configuration:

- `shared.storage.wal.capacity.bytes`
- `shared.storage.object.target.bytes`
- `shared.storage.upload.interval.ms`
- `shared.storage.upload.max.linger.ms`
- `shared.storage.upload.wal.pressure.percent`
- `shared.storage.upload.max.inflight`
- `shared.storage.s3.io.threads`

Size WAL capacity from worst-case remote outage tolerance, not normal throughput.

A conservative planning model is:

`required WAL bytes >= peak acknowledged ingest bytes/sec * tolerated remote outage seconds * safety factor`

Use a safety factor above 1 to cover record framing, burstiness, replication timing, and delayed reclaim.

Do not increase `upload.max.inflight` or S3 I/O threads blindly. Validate changes with the performance gate and observe
S3 throttling, CPU, memory, and request latency.

## S3 configuration

Required/important settings include:

- `shared.storage.s3.bucket`
- `shared.storage.s3.key.prefix`
- `shared.storage.s3.region`
- `shared.storage.s3.endpoint`
- `shared.storage.s3.path.style`
- `shared.storage.s3.io.threads`
- `shared.storage.s3.connection.timeout.ms`
- `shared.storage.s3.socket.timeout.ms`
- `shared.storage.s3.api.call.attempt.timeout.ms`
- `shared.storage.s3.api.call.timeout.ms`
- `shared.storage.s3.max.attempts`

Production AWS deployments should prefer workload identity / instance or pod role credentials supported by the AWS SDK
default credential provider rather than static long-lived secrets.

## Topic selection

Shared Storage routing is controlled by:

- `shared.storage.topics`
- `shared.storage.topic.pattern`

Internal topics always remain classic.

Before GA deployment, explicitly document the intended topic selection. Avoid accidental cluster-wide enablement by
configuration omission; validate the effective selector during change review.

## Upgrade and rollback

A release is not rolling-upgrade compatible merely because its persistent codecs can read golden bytes.

Before production rollout:

1. run `Shared Storage Rolling Upgrade` against the previous supported release/RC;
2. verify the OLD -> NEW -> OLD rollback probe passes after NEW has written authoritative remote metadata;
3. verify each upgraded broker can become preferred leader and continue acks=all/idempotent traffic;
4. verify final full-NEW remote coverage;
5. retain the workflow artifact containing old/new refs and broker diagnostics.

If the rolling-upgrade gate is not green, do not perform a mixed-version production rollout.

## Release gate

Before declaring GA, run `Shared Storage GA Release Gate` on the exact release ref.

The manifest must report PASS for:

- the complete correctness suite;
- `Shared Storage Performance Baseline`;
- `Shared Storage Soak and Chaos`;
- `Shared Storage Rolling Upgrade`;
- real S3 compatibility when the release claims AWS S3 production support.

The release ref is not GA-ready while any required evidence is missing, pending, cancelled, or failed.
