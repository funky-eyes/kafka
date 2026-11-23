# Parallel block WAL: experimental status

The optional `shared.storage.wal.engine=parallel-block` uses a recoverable block
ring with concurrent writes and an ordered commit frontier. It is experimental,
and `ring` remains the default. It does not include an online migration or fence
older binaries that ignore the ownership lock. Data and checkpoint persistence
also rely on the underlying storage's force and failure-unit guarantees.

The encoder calculates append locations while constructing each block. Admission
and allocation use encoded lengths without recalculating record CRCs, and the
write path does not decode the freshly encoded block before dispatch. Each
record and block still carries its checksum; reads and recovery validate both.
This removes a pre-write self-check of encoder output. Format and location tests
are regression checks, not a per-write check against memory corruption.

Data write workers queue finished blocks for a shared force. A force covers
a snapshot of the finished queue. Blocks that finish writing after the snapshot
wait for the next force. The checkpoint thread can publish the full contiguous
forced prefix with a single checkpoint transition. Entries
for reservations and reads are looked up by logical position so that preceding
committed blocks do not lengthen those searches.

New files created by the Broker factory use checkpoint version 2. Data and
checkpoint have separate forces. The checkpoint alternates between banks of
three aligned copies, leaving the complete preceding bank untouched. Recovery
checks the newest valid generation against the complete committed window and
repairs only missing copies in that bank before admitting writes. A crash that
tears the entire bank being written leaves the preceding committed bank
available. Once a bank has been forced, any two independent slot losses leave
at least one valid copy of that generation. This fault model assumes failure
unit isolation, effective force, and intact data blocks. Existing version 1
files reopen in place and continue using their original checkpoint protocol.

The experimental `shared.storage.wal.async.partition.append=true` switch, off
by default and accepted only with `parallel-block`, pipelines RF1 `acks=1`
client appends without producer IDs or transaction/control batches. Native
Kafka validation and offset assignment remain serialized by the log monitor.
For a Produce request handled by a Kafka request thread, the leader lock is
released after admission; ordered WAL completion schedules a request-handler
callback that rechecks leadership and epoch before ACK. Eligible multi-partition
requests submit each shared partition before waiting and send one combined
response. An advisory preflight rejects mixed or otherwise ineligible requests
before any submission; a partition that becomes ineligible after preflight uses
the existing synchronous path once. Role changes drain the admitted prefix
before updating partition state. Other entry points retain the synchronous wait
with the leader read lock held. Ineligible appends use the ordinary path after draining tracked writes.
The implementation caps log-end visibility, fetches, offset snapshots
and high-watermark updates at the confirmed prefix, and drains before truncation,
remote recovery, close and synchronous fallback. Segment rolls wait for prior
writes before taking producer snapshots. An async failure fences the tracked
path; recovery may require restarting the Broker.

`shared.storage.wal.block.linger.ms` controls the maximum assembly wait for
the parallel-block engine (default 1 ms, allowed range 0–1000 ms). A longer
window can increase batching but adds latency; its effect must be measured for
the actual workload. It does not alter the synchronous ring engine.

The tracker rejects gaps and confirms only a contiguous prefix whose WAL and
Kafka-state signals have both completed. NIO/AIO experiments through the real
Broker factory observed six concurrent requests in four blocks, with controlled
force gates and test-only logging. Selected ACK/visibility, transaction,
idempotent retry, malformed-neighbor, capacity, restart and reassignment cases
passed. The block occupancy gauges report completed blocks and groups separately
from checkpoint cohorts; `WalNonBlockingRequestCount` counts accepted requests
using the callback path. These controlled tests are not a multi-host failover or
power-loss guarantee, and they do not establish production readiness or a
performance comparison. Broader failure windows and sustained-load behavior remain
to be verified. The implementation does not yet promise concurrent batching for
transactional, idempotent, follower or RF>1 appends.

The proposed `shared.storage.wal.experimental.early.acks.one` option is off by
default and requires the experimental partition pipeline and `parallel-block`.
When enabled, eligible non-transactional and non-idempotent client `acks=1`
requests, including fully eligible multi-partition requests, may return after physical WAL space is reserved and
before any WAL copy is durable. RF2 replicates after the leader's confirmed WAL frontier
advances; it does not make an early ACK safe against immediate permanent loss
of the leader. Other requests retain the existing durable path. The feature
must not be considered usable until real-Broker RF1/RF2 ACK, replica progress,
failure, restart and epoch-fencing tests pass. `WalEarlyAckAdmissionCount`
counts successfully reserved appends on this path, not successful network responses.
If physical reservation fails after Kafka has provisionally assigned offsets,
the request fails and the partition's log directory is fenced; automatic
rollback and continued operation under WAL capacity pressure are not yet
implemented.

The multi-partition request path has targeted mocked ReplicaManager coverage for independent completion failures, leadership change at completion, single response and preflight fallback. A real SharedUnifiedLog test exercises two partitions sharing one WAL block with NIO and AIO, force held and confirmed visibility fenced. These checks do not yet establish end-to-end Broker network behavior, multi-node failover, or throughput for this path.
