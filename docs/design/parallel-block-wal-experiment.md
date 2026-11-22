# Parallel block WAL: experimental status

The optional `shared.storage.wal.engine=parallel-block` uses a recoverable block
ring with concurrent writes and an ordered commit frontier. It is experimental,
and `ring` remains the default. It does not include an online migration or fence
older binaries that ignore the ownership lock. Data and checkpoint persistence
also rely on the underlying storage's force and failure-unit guarantees.

The opt-in implementation was exercised with NIO and AIO through the actual
Broker factory, including selected force/ACK, transaction, failure, restart and
leader-reassignment cases. These tests do not establish power-loss safety,
multi-host failover or production readiness. In a same-partition concurrency
probe, six requests formed six singleton blocks with either backend. Consequently
there is no established same-partition batching improvement or comparable RF1
`acks=1` performance result.

`PartitionDurableOffsetTracker` under the test tree is a standalone experiment,
not an implementation of a Broker visibility barrier. It rejects gaps in offset
reservations, advances confirmation over a contiguous prefix only after both WAL
and Kafka-state signals complete, and fences outstanding reservations as soon as
either signal fails. It waits for actual outstanding WAL operations before a
generation reset. Tests use the real parallel block WAL on NIO and AIO, but the
Kafka-state signal is synthetic. They do not connect the experiment to
`UnifiedLog`, `Partition`, Broker ACKs, fetches or leader changes.

Before considering an asynchronous partition append path, account for the
speculative state held by Kafka's `UnifiedLog`. In particular, `Partition`
uses log-end metadata to advance the high watermark, includes log end and last
stable offset in Fetch responses, exposes offsets via ListOffsets, and consults
leader-epoch state. Restricting only Produce ACKs would not prevent early
visibility. A full design must also isolate validation errors, order duplicate
producer sequences and transactions, bind publication to the correct leader
epoch, fence failures and drain in-flight I/O before truncation/recovery. Test
those boundaries with real Broker requests before running the requested matched
RF1 `acks=1` NIO/AIO comparison against AutoMQ.
