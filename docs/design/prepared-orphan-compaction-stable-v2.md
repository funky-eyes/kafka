# V2 object-key lifecycle: compaction-stable state encoding

Status: **parser and recovery tests only**. No production producer emits
v2 state. Physical orphan deletion remains disabled. The new value types
are intentionally not understood by old brokers; activation requires a
mixed-version rollout barrier.

Legacy PREPARE/COMMIT occupy the object key (type 1); legacy CLAIM occupies
a separate cleanup key (type 3), creating recovery ambiguity when Kafka
compacts both keys independently.

V2 fenced PREPARED (value type 8) and CLAIMED (type 9) occupy the **same
object key** as COMMITTED (type 2). A compacted image therefore preserves
one final state. Legacy cleanup cannot claim a fenced PREPARED object, and
a late COMMIT/prepare after fenced CLAIM fails closed.

**Encoding is not arbitration.** A future producer must initialize one
per-object transactional producer identity BEFORE PREPARE; never reinitialize
the identity after fencing; and use it for COMMIT. The cleaner must first
fence this producer, capture a read-committed horizon AFTER fencing, verify
the same fenced PREPARED generation, then transactionally write a same-key
CLAIM. Only a confirmed CLAIM winner may issue physical DELETE; delayed
S3 PUTs must be re-deleted. An older unfenced writer may not operate on
v2 object IDs.

Producer fencing, mixed-version activation and real Kafka/MinIO races are
not implemented here. Keep deletion off until those tests pass.
Topic Delete full reference retirement remains separate and disabled.
