# Experimental PREPARED v2 COMMIT / CLAIM / physical orphan cleanup

Status: **default OFF; GitHub CI validation required; NOT GA**.

When every Broker in a fresh test cluster explicitly enables
`shared.storage.orphan.fenced.lifecycle.enabled=true`:

1. A unique per-object transactional producer is initialized BEFORE
   PREPARE and reused for COMMIT, never reacquired by a fenced uploader.
2. The cleaner initializes the same transactional.id, fencing old
   COMMIT attempts. Only AFTER fencing does it capture a fresh read-
   committed metadata LSO and wait for the shared metadata image to
   apply it.
3. The cleaner validates the exact v2 PREPARED object/timestamp and
   publishes a CLAIM transaction under the SAME compacted object key.
   The write completes only after local authoritative replay confirms
   the durable claim. A committed object never qualifies.
4. Only the CLAIM winner may issue physical S3 DELETE. The CLAIM
   remains discoverable so future passes re-delete an arbitrarily
   late physical PUT. Historical v1 CLAIMED records are NOT retried.
5. The CI upload crash test (AFTER_PREPARE, AFTER_PUT, AFTER_COMMIT) uses
   three Brokers and real MinIO with v2 explicitly enabled.

Caveats: per-object producer creation can be expensive; throughput and
latency need benchmarking. Mixed-version rolling upgrade is NOT safe
to activate and there is no cluster-wide feature-level agreement yet.
The test config is NOT an operational recommendation. No COMMITTED
physical object GC, cross-topic reference retirement or Topic Delete
deletion authority is enabled. Keep all work Draft until CI,
process-restart recovery, producer-fencing races and review pass.
