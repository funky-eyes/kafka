# PREPARED orphan cleanup: durable arbitration requirement

Status: **safety mitigation; full arbitration and COMMITTED lifecycle GC not implemented**.

## Why production cleanup is disabled

The shared metadata topic uses a single compacted partition. Object COMMIT
and orphan CLAIM are written to different compacted keys by independent
brokers. Kafka gives an append order, but not an atomic conditional state
transition across the writers. Compaction can erase the evidence required
to reconstruct the winning transition. Producer acknowledgment or a local
replayed CLAIM is insufficient authority for physical S3 DELETE.

The replay image already fails closed on ambiguous COMMIT and CLAIM pairs.
Production now rejects new claims and old CLAIMED retries, and the extension
does not schedule physical orphan cleanup. Normal uploads, COMMIT publication,
replicated WAL durability and WAL reclaim are unchanged. PREPARED orphans
may accumulate in S3; monitor capacity rather than bypassing this guard.

## Mandatory completion criteria

1. One durable version-negotiated multi-broker arbiter must decide PREPARED
   to COMMITTED or PREPARED to CLAIMED. Mixed-version writers cannot bypass
   it. Do not replace CAS with local scans, offset snapshots or grace time.
2. A winning CLAIM must fence every delayed COMMIT and upload attempt.
   The identical outcome must replay after actual Kafka compaction and
   Broker crashes. Retry deletion idempotently without object-ID reuse.
3. Test both race orders, double CLAIM, duplicates, mixed versions, late PUT,
   producer fencing, actual compaction, restart, and replica failover with
   three real Kafka brokers and MinIO in GitHub Actions.
4. Only after these gates pass can the production S3 orphan cleaner run.
   No unsafe force-enable configuration is provided.
5. Topic Delete physical GC separately requires immutable Topic ID
   terminal state, a cross-log catalog handoff, authenticated KRaft
   retirement authority, complete packed-range/alternate-reference
   checks, reader/upload fences, durable DELETE_READY, and retryable
   terminal metadata updates. The existing GC worker has no production
   Controller authority.

Heavyweight tests must remain in GitHub Actions, never local downloads.
