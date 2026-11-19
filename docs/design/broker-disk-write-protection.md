# Broker disk write protection

`log.disk.protection.max.usage.percent` is a dynamically configurable broker setting. Its default, `100`, disables the guard. A value from `0.01` up to but excluding `100` rejects client produce requests for a partition when its local log filesystem reaches the configured percentage of total space, measured using usable space. Shared-storage partitions also check the broker shared WAL filesystem. A check is performed on first use and cached; registered filesystems are refreshed every five seconds. An unavailable filesystem statistic fails closed until a successful refresh. Threshold changes apply without restarting the broker. Configure this threshold as a finite numeric literal; Kafka config-provider expressions are not supported for this setting because the controller validates the stored value before committing it.

```sh
bin/kafka-configs.sh --bootstrap-server broker:9092 --entity-type brokers \
  --entity-name 1 --alter --add-config log.disk.protection.max.usage.percent=85
```

Set the value to `100` to disable the guard. This is a per-broker write-admission control, not a cluster-wide capacity quota: it checks the leader receiving the client write. It leaves reads, internal-topic appends, coordinator writes, and follower replication running so that consumers can commit offsets and recovery can proceed. These operations still consume disk space. Monitor every broker and filesystem, leave sufficient headroom for replica traffic, internal topics, in-flight requests and filesystem reserved space, and restore or expand capacity before it is exhausted. Kafka acknowledgement, ISR and min ISR rules continue to govern writes; this control does not establish a remote follower's remaining capacity.

The refresh interval creates a window in which writes can continue after a disk passes its threshold. Tune the threshold for the maximum expected write rate during this window and for concurrent writes. This protection is not a guarantee that a filesystem can never fill. Partition log-directory moves are evaluated on the destination log directory at request time. For shared storage, the WAL directory is fixed by the storage-extension startup configuration.

The optional `DiskPressureGuardIntegrationTest.sharedWalFilesystemPressureRejectsAndRecovers` test requires `KAFKA_TEST_WAL_PRESSURE_ROOT` to point to a dedicated, disposable memory-backed filesystem of at least 96 MiB. It fills up to 70 MiB there to bring only the WAL filesystem above the threshold, and removes its filler afterward. Never point this variable at a production filesystem. The test is skipped when this variable is unset.

## Broker monitoring

The broker exposes gauges under `kafka.server:type=DiskWriteProtection,brokerId=<id>`:

- `Enabled`: 1 when the threshold is below 100, otherwise 0.
- `ThresholdPercent`: the currently active threshold.
- `ObservedPaths`: number of local paths already checked during client writes; this is not an inventory of all broker filesystems.
- `BlockedPaths`: observed paths at or above the threshold, including paths whose filesystem statistics could not be read. It is 0 when the protection is disabled.
- `ReadFailures`: cumulative filesystem-stat read failures since broker startup.
- `RejectedAppends`: cumulative client partition appends rejected by this guard since broker startup.

These metrics reflect observed local paths, not remote replicas or unused log directories. Alert on rejected appends and read failures, and monitor filesystem free space separately for every broker and volume. A count of zero does not establish that an unobserved filesystem is healthy.
