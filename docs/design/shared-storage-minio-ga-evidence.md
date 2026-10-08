# MinIO-backed Shared Storage GA evidence

## Release scope

Default Shared Storage GA proves the pinned MinIO-compatible object-store path
and the Kafka shared-storage correctness under the required 19 automatic gates.
It does **not** certify native AWS S3 interoperability, IAM/OIDC, TLS, or
virtual-hosted addressing. AWS compatibility is a separate optional extension.

## Test-design references

* AutoMQ: [component CI](https://github.com/AutoMQ/automq/blob/main/.github/workflows/build_automq.yml)
  runs dedicated S3/core unit tests; [E2E runner](https://github.com/AutoMQ/automq/blob/main/.github/workflows/e2e-run.yml)
  isolates workloads, captures results and cleans up Docker fixtures.
  The [archive-fetch](https://github.com/AutoMQ/automq/blob/main/tests/kafkatest/automq/archive_e2e_test.py)
  and [S3-leakage](https://github.com/AutoMQ/automq/blob/main/tests/kafkatest/automq/s3_leakage_test.py)
  suites exercise remote read and object-retention behavior.
* Aiven: [S3StorageTest](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/blob/main/storage/s3/src/integration-test/java/io/aiven/kafka/tieredstorage/storage/s3/S3StorageTest.java)
  uses Testcontainers LocalStack, whereas
  [S3MinioSingleBrokerTest](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/blob/main/e2e/src/integration-test/java/io/aiven/kafka/tieredstorage/e2e/S3MinioSingleBrokerTest.java)
  executes Kafka with MinIO.
  [SingleBrokerTest](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka/blob/main/e2e/src/integration-test/java/io/aiven/kafka/tieredstorage/e2e/SingleBrokerTest.java)
  checks remote copy, remote read, DeleteRecords, retention and topic deletion.

The projects have different storage architectures: Aiven uses Kafka's
tiered-storage SPI; our design owns a replicated WAL and remote-object metadata.
Reuse test principles, not implementation internals.

## GA evidence matrix

| Evidence | Mandatory assertion | Gate |
| --- | --- | --- |
| S3 API | PUT, Range GET, zero-length range, DELETE and 404 after deletion | Shared Storage MinIO |
| Multipart | 5 MiB boundary read, ordered source close, successful complete, abort and no partial object | Shared Storage MinIO |
| Physical orphan cleanup | PREPARED cleanup, late PUT reclaimed, COMMITTED read winner survives, redundant COMMITTED copy reclaimed, mixed-partition object protected, active upload fenced, physical HEAD/list checks | Shared Storage MinIO |
| Replicated Kafka | Three-broker KRaft, committed remote coverage and failover | Shared Storage MinIO |
| Semantics | Kafka producer/consumer parity, acks and offset ordering | Semantics and durability |
| Recovery | WAL crash, upload crash, local-state loss, restart and replay | Specialized recovery |
| Lifecycle | Topic delete/recreate, partition expansion, DeleteRecords | Topic lifecycle |
| Readiness | Rolling upgrades, performance and soak/chaos | Required hardening gates |

The MinIO job runs `S3ObjectStoreTest`, `S3MultipartObjectStoreTest` and
`S3OrphanObjectCleanerIntegrationTest` against its pinned fixture in a single
Gradle invocation. Its JUnit result guard rejects missing, failed, skipped or
duplicate mandatory cases. The orphan cleaner tests use per-test random
prefixes and assert MinIO's physical HEAD/list results after deletion. This
prevents an absent `SHARED_STORAGE_S3_ENDPOINT` from turning into a green
build through JUnit `assumeTrue`. These tests validate duplicate COMMITTED
objects from a leader race and protect multi-partition objects while any
logical range still references their physical bytes.

## Topic deletion, Retention, and DeleteRecords: physical-GC boundary

The current Topic Lifecycle E2E proves **logical** isolation after topic
delete/recreate, partition expansion, and retained offsets after DeleteRecords.
It does not yet prove physical reclamation of otherwise valid COMMITTED
objects due solely to topic deletion, Retention or DeleteRecords.

`RemoteObjectIndex` currently publishes and retains range references; it
does not expose an authoritative durable range-retirement operation. Thus
`OrphanObjectCleaner` must not infer that every object belonging to a
deleted topic or expired offset range is safe to remove. Packed objects may
span multiple topics/partitions, including ranges that remain live.

A future physical lifecycle GC must first persist a topic-ID-scoped deletion
or partition log-start/retention watermark and replay it on every broker,
then retire the affected logical references atomically with the durable
watermark. A physical object can be deleted only after **all** its ranges
are retired, there are no in-flight uploads, and no reader still depends on
its bytes. Physical DELETE must precede the authoritative metadata tombstone;
failures require retry without losing the original COMMIT evidence.

The follow-up end-to-end gate must check MinIO `HEAD` and `ListObjectsV2`
for eventually reclaimed objects, ensure retained/packed live ranges
remain readable, and repeat after broker restart. Until then, claim only
the physical cleanup scopes explicitly proved by the MinIO tests.

Static analysis remains owned by the storage-tests job. Runtime gates do not
rerun global Checkstyle or SpotBugs; this retains independent evidence without
unnecessary CI duplication.

## Release

Run `Shared Storage GA Release Gate` with the canonical candidate and
evidence branch. Default `require_real_s3=false` evaluates all 19 mandatory
MinIO-backed GA gates. Selecting `require_real_s3=true` optionally adds
strict native AWS S3 evidence for a separate interoperability claim.
