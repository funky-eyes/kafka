# Orphan GC: concurrency and throughput decision

Status: **experimental, not GA**. PR #74 v2 cleanup is opt-in and must not
be enabled on a mixed-version cluster. The current Controller has no authority
to retire COMMITTED physical objects.

## Fast path: independent object IDs

- Broker-local upload admission uses immutable per-object states in a
  `ConcurrentHashMap`. Reads use plain concurrent map lookups, not a
  process-global monitor. Writes use short per-key atomic `compute` (not
  strictly lock-free; a bin-level critical section remains). Physical I/O,
  broker RPCs, Kafka transactions and completion callbacks are all outside
  map computations. Terminal local fences remain to avoid ABA/reuse.
- The orphan cleaner keeps a lock-free in-flight object ID set. Each
  scheduled pass issues up to **8** concurrent CLAIM/DELETE futures,
  drains their batch, and only then starts another. This is an initial
  cap, not a measured optimal setting. Durable CLAIM is retained after
  DELETE because a previously dispatched PUT can become visible later.
- Before expensive `initTransactions()`, local v2 PREPARE/CLAIM performs a
  CAS single-flight reservation. Duplicate local contenders must not fence
  the winner. The 4-worker lifecycle executor has a bounded 256-task
  backlog with explicit rejection, not an unbounded memory queue.
- The Kafka transaction coordinator with a deterministic **per-object**
  `transactional.id` still decides cross-Broker PREPARE/COMMIT/CLAIM
  ordering. The cleaner fences the upload producer, captures a NEW
  read-committed metadata LSO, catches up, and only then issues CLAIM.
  Neither the local map nor a non-authoritative replica grants DELETE.

## Why not make everything lock-free?

A lock-free CAS on a Broker's heap cannot prevent another Broker from
publishing COMMIT or writing S3. Replacing synchronized blocks with CAS
would improve a local microbenchmark while leaving the distributed race.
The correct concurrency hierarchy is:

1. One durable, fenced authority for lifecycle transitions.
2. Per-object local admission, never one JVM-wide lock.
3. Immutable snapshot reads; reader references must be pinned until drain.
4. Bounded asynchronous I/O, with no blocking on the metadata consumer.
5. Idempotent retries after crash or partial delete; never erase a durable
   CLAIM without proving late PUTs impossible.

## Source design inspirations

- **AutoMQ** persists object and stream metadata in KRaft and separates
  PREPARED / COMMITTED / pending-destroy transitions; its Controller is the
  serialization point, and deletion benefits from batching.
  See https://www.automq.com/blog/insight-metadata-management-in-automq
- **Kafka** uses transaction coordinator producer epochs and fencing to
  prevent earlier producer generations from completing transactions.
  See https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala
- **Apache Pulsar** bounds concurrent ledger deletions, combines asynchronous
  callbacks with retry, and batches task handover to reduce executor overhead.
  See https://github.com/apache/pulsar/blob/master/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl/ManagedLedgerImpl.java
- **RocksDB** keeps read snapshots referenced while writers publish newer
  versions. For COMMITTED GC the analogous cross-replica reader fence is
  required *before* S3 DELETE, not just a local index absence check.

## Bottleneck still not solved

A `KafkaProducer` and `initTransactions()` **per physical object** are
expensive. Four lifecycle workers cap initialization concurrency, no matter
how many S3 delete requests are batched. Sharding transactional IDs by
object-ID hash is unsafe: a cleaner would fence unrelated active uploads
sharing the shard.

A future scalable version should use a Controller-serialized, versioned
**per-object compare-and-transition** in KRaft, batch multiple object
transitions in one request, and replace per-object producer initialization
without losing immutable object ID, fencing epoch, or compaction recovery
proof. It needs a real cross-log COMMIT handoff before rollout; do not enable
a hypothetical CAS from an advisory broker image.

For Topic Delete, KRaft may authorize COMMITTED retirement only after
checking every packed range/topic incarnation, reader and upload fencing,
catalog seal, and durable deletion intent. PREPARED orphan CLAIM does not
provide that authorization.

## CI verification

All tests run on GitHub Actions: Java 25 unit races, three Kafka Brokers and
MinIO, real log compaction, AFTER_PREPARE/AFTER_PUT/AFTER_COMMIT crashes,
restart and late PUT, and mixed-version fail-closed behavior. Benchmark
1K+ objects with several sizes, record total DELETE/s, CLAIM/s, Kafka
transaction initialization/s, p95/p99 latency, pending queue, S3 request
count, allocated heap and CPU. Compare sequential and bounded parallel
implementations; do not claim theoretical speedup as a measured result.
