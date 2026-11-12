# WAL replica durable acknowledgement model — design gate

Status: PROPOSAL ONLY. This PR changes no runtime acknowledgement, KRaft writer, physical GC, or MinIO GA behavior.

## Safety objective

A successful acks=all acknowledgement must be recoverable after one Broker is permanently lost, provided the configured ISR/min.insync.replicas durability contract was met. A replica diagnostic report is never by itself a durable write acknowledgement.

## Certificate identity and proof coordinates

- Cluster identity, TopicId, partition, leader epoch, partition epoch, WAL segment/object identity and manifest/checksum.
- Contiguous WAL offset range [start, end), durable flush/completion boundary, no holes or unverified offsets.
- For every independent replica: BrokerId, BrokerEpoch, IncarnationId, disk/storage generation, and authenticated ingress principal provenance.
- Controller epoch, KRaft committed record offset and replay provenance, round nonce and expiry.
- Replica assignment, ISR snapshot, and configured acknowledgement policy. Distinct Broker IDs alone do not prove independent physical failure domains.

## Fail-closed state machine

UNKNOWN -> OBSERVED -> DURABLE_CANDIDATE -> REPLAY_VERIFIED -> ACK_ELIGIBLE.

OBSERVED reports remain nonauthoritative. ACK_ELIGIBLE requires independently authenticated, durable, contiguous replica WAL coverage; KRaft-replayable evidence; current Controller and Broker identity; and Kafka ISR/HW policy verification.

Revoke on Controller leadership change until replay validation, Broker epoch/incarnation or disk generation change, leader/partition epoch drift, assignment changes, checksum mismatch, coverage hole, or stale nonce. No old proof can silently become a new proof.

## Implementation PR sequence

1. Immutable validation-only certificate types and adversarial tests, with no producer ACK wiring.
2. Controller KRaft persistence/replay and epoch fencing, including snapshot and failover tests.
3. Broker durable WAL confirmation, independent replica identity, crash/restart and permanent disk-loss recovery.
4. acks=all/HW integration and exact-HEAD three-Broker MinIO failure matrix; document acks=1 weaker guarantees.
5. KSO2 cross-partition reference closure, durable retirement, and separately gated COMMITTED physical GC review.

## Release gates

Keep COMMITTED physical GC disabled, production Broker principal policy DENY_ALL, and the frozen MinIO GA baseline untouched. Each implementation PR must pass exact-HEAD Java 25, named test inventory, Kafka HA, crash windows and real MinIO E2E before merge. This design does not claim zero data loss.
