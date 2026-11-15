# Shared-storage S3 Bucket lifecycle safety

Status: Guarded ingress and explicit missing-object read failures; **not** a
replacement for Kafka retention, KRaft retirement, or COMMITTED physical GC.
This guard is independent of the draft GC PRs (#62/#63/#64).

## Invariant

A COMMITTED physical KSO2 object must remain retrievable while **any**
Kafka-authoritative offset range (including packed ranges from other topics)
references it. Kafka topic `retention.ms`, local WAL retirement, and Bucket
lifecycle expiration operate on different clocks and different scopes.

Example: Topic `retention.ms=10800000` (3h) plus Bucket expiration after 1h:
once WAL, local or independently verified alternate copies are no longer
available, an externally expired remote COMMITTED object means lost bytes.
Topic retention does not resurrect data. AWS S3 lifecycle expiration can run
independently of Kafka and a Bucket policy Deny does not stop its lifecycle
engine. Versioning with a delete marker does not make a normal GET readable.

**Production rule: do not configure Bucket lifecycle object expiration for
any Kafka data key under `shared.storage.s3.key.prefix`.** Other paths, such
as `metrics/`, can have separate lifecycle policies. Incomplete multipart
abort rules do not expire completed objects and are allowed. Expired delete
marker-only cleanup is also allowed.

Even `Bucket TTL >= topic retention` is *not* a proof: objects can contain
ranges from multiple topics, timestamps are based on object creation, and
effective Topic retention may be unlimited or increased at runtime.
There is intentionally no bypass based on a guessed Topic retention value.

## Enforcement

Before the first object PUT, each Broker checks `GetBucketLifecycleConfiguration`.
The result is cached for up to 30 seconds between attempted uploads; each new
PUT on the next cache boundary revalidates it. Fail closed on active expiration
overlapping the Kafka data prefix, `AccessDenied`, unsupported lifecycle API,
or an ambiguous response. Only the explicit `NoSuchLifecycleConfiguration`
404 response means no configured lifecycle. This requires IAM permission
`s3:GetLifecycleConfiguration`; it does not grant permissions to edit rules.

The check applies to normal single PUT, list multipart and both streamed
PartSource PUT variants. Read operations remain available while operators
fix a conflicting policy. A 404 from a physically referenced GET becomes
`RemoteObjectMissingException`; the read path must attempt a verified
alternative where available and must never advance LogStartOffset or high
watermark to disguise missing data.

The guard does not stop an external administrator changing rules between
revalidations, an already-expired object, a bucket-wide delete, or provider
misbehavior. An independent control-plane monitor and an all-Broker
audit/health fence should be added before declaring strong S3 GA guarantees.
No automatically shortened Kafka retention or fake-success read is allowed.

## Operations

1. Use a dedicated data Bucket/prefix without active expiration rules for
   `objects/<objectId>` (or the configured key prefix).
2. If an active matching rule is detected, disable it or move the Kafka data
   to an isolated Bucket; preserve local/WAL copies and investigate already
   expired objects before restarting ingestion. Changing Topic retention to
   match the Bucket TTL **does not repair previously lost Kafka data**.
3. If GET returns 404 for a COMMITTED object, record its ID and affected
   partition/offset, check the independently validated local/alternate copy,
   and treat the primary absence as a data-integrity incident. Avoid GC and
   avoid silently moving log-start. Versioned objects may require a separate
   manual restoration procedure; this module does not access old version IDs.
4. Keep the KRaft COMMITTED physical GC worker disabled until the cross-log
   barrier and all BrokerEpoch retirement proof are independently verified.
