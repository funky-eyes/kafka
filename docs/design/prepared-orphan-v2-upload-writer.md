# Experimental per-object fenced upload writer

This opt-in slice adds a Kafka transactional uploader session for each new
shared-storage object. It is **disabled by default** and DOES NOT re-enable
orphan deletion.

- Set `shared.storage.orphan.fenced.lifecycle.enabled=true` only in
  isolated all-upgraded test clusters. Old binaries cannot decode the v2
  PREPARED value and a mixed rollout is not yet supported.
- Before PREPARE, the session initializes a transactional.id derived from
  immutable cluster ID and object ID. PREPARE and COMMIT use this same
  producer, never a re-initialized session for a fenced ID.
- Both metadata records use the same compacted object key.
- Existing normal upload paths continue unchanged while the flag is false.
- Release the producer on terminal uploader completion, with no unsafe
  physical DELETE permissions.

Next: owner fencing by a separate Broker, actual post-fence read-committed
catch-up via the long-lived metadata image, an exact v2 CLAIM transaction,
a retriable DELETE worker and passing real Broker crash/compaction tests.
No code here authorizes physical deletion or Topic Delete COMMITTED GC.
