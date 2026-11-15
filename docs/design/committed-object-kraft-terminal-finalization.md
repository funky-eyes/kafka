# COMMITTED KRaft terminal retirement: safe last edge

Status: **experimental / package-private**, **NOT production Topic Delete GC**.

This slice implements only a durable terminal bookkeeping transition:

`DELETE_RECORDED -> DELETED`

It does **not** produce `RETIRE_INTENT`, `DELETE_READY`, or
`DELETE_RECORDED`. No caller outside the Controller event queue can grant
physical S3 deletion, and no S3 object-delete worker is connected to this
method. The Kafka 4.3.1 production feature level remains disabled.

## Why this stage can be implemented without weakening safety

A live, version-negotiated Controller with an already replayed
`DELETE_RECORDED` record may seal the object ID permanently as `DELETED`.
It performs an exact expected-previous-KRaft-offset CAS against the current
timeline state within `appendWriteEvent`; no shared Java lock is introduced.

The KRaft record is built for the Controller's *next* write offset, and the
existing active-controller and standby replay independently require the
record's accepted offset to equal the actual appended offset. The result
future completes only after KRaft commit. A stale request, missing
`DELETE_RECORDED`, or unnegotiated feature returns false and emits
zero records. The terminal identity remains visible through KRaft snapshots
to prevent physical object-ID reuse.

This stage still depends on eventual production proof that the earlier
`DELETE_RECORDED` was legitimately written after physical removal. It
does NOT make an unauthenticated KRaft replay claim trustworthy.

## What remains blocked

1. A Controller-authenticated, immutable COMMITTED catalog seal and
   cross-log handoff (not sampled offsets).
2. Per-object generation fencing of late COMMIT, PUT and alternate reads.
3. Complete, durable all-Broker/Topic-ID/partition packed-range reference
   retirement and reader/writer quiescence evidence, including failover.
4. Version-gated KRaft `RETIRE_INTENT -> DELETE_READY` writes, and an
   authenticated `DELETE_RECORDED` acknowledgement only after S3 DELETE.
5. Actual production adapter to the existing `CommittedObjectGcWorker`,
   with three-broker Kafka + MinIO crash, compaction, Topic Delete/recreate
   and rolling-upgrade tests.

Do not relax the feature gate or accept a caller-provided safe epoch to
skip any missing proof. Heavyweight verification stays on GitHub Actions.
