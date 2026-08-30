# WAL I/O Backend Design

## Goal

Evolve the shared WAL from a correctness-first FileChannel implementation toward an AutoMQ-style high-throughput WAL architecture without changing Kafka durability semantics.

## Current state

The current WAL implementation provides:

- single logical writer;
- sequential append;
- batched group commit;
- crash-atomic DATA + GROUP_COMMIT framing;
- explicit force durability barrier;
- replay recovery;
- bounded WAL capacity and safe reclaim.

The current physical backend is intentionally simple:

```
FileSharedWal
    |
    +-- FileChannel positional write
    +-- FileChannel.force(false)
```

This is the reference correctness backend.

## Why not directly use Java AIO

`AsynchronousFileChannel` should not be assumed to provide native asynchronous disk I/O. Depending on the JDK/platform implementation it may be implemented through worker threads executing blocking file operations.

The important optimization boundary is not the Java API name. The important properties are:

- sequential write aggregation;
- fewer durability barriers;
- preallocated storage layout;
- direct buffer ownership;
- reduced page-cache copy overhead when appropriate;
- explicit completion ordering.

## Proposed abstraction

Introduce:

```
WalIoBackend
```

Responsibilities:

- positional write;
- positional read;
- force durability;
- create/seal/reopen physical storage units;
- optional preallocation capability;
- optional direct-I/O capability.

It must not own:

- Kafka offsets;
- append groups;
- GROUP_COMMIT protocol;
- WAL reclaim policy;
- remote object metadata.

## Backends

### FileChannel backend

Default portable implementation.

```
WalStateMachine
        |
WalIoBackend
        |
FileChannelBackend
```

### Direct I/O backend

Future Linux production backend:

```
WalIoBackend
        |
DirectIoBackend
        |
O_DIRECT / aligned buffers
```

### io_uring backend

Future experimental backend:

```
WalIoBackend
        |
IoUringBackend
```

Requirements:

- explicit packaging strategy;
- runtime detection;
- fallback to FileChannel;
- crash durability benchmark.

## Circular WAL target

The final physical layout should move away from unlimited segment file creation.

Target:

```
+------------------------------------------------+
| fixed preallocated WAL region                  |
|                                                |
| slot0 slot1 slot2 ... slotN                    |
|                                                |
+------------------------------------------------+

head -> append position
tail -> reclaim position
```

Rules:

1. Physical slots are reused.
2. Logical segment/generation id is always increasing.
3. WalLocation includes enough generation identity to reject stale references.
4. A slot cannot be reused until all local recovery references are released.
5. Crash recovery reconstructs committed groups and current head/tail state.

## Performance priorities

Order:

1. Complete online reclaim.
2. Introduce WalIoBackend.
3. Add preallocation benchmark.
4. Add direct buffer path.
5. Add circular slot WAL.
6. Evaluate Direct I/O.
7. Evaluate io_uring.

Do not optimize the I/O backend before the logical WAL state machine is stable.
