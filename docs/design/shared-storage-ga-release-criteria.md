# Shared Storage GA release criteria

A Shared Storage build is eligible for GA only when the machine-generated GA manifest passes for the release
production tree.

The release gate deliberately separates two concerns:

* correctness gates prove durability, crash recovery, failover, Kafka semantics, object-format compatibility, and
  lifecycle behavior;
* GA hardening gates prove production readiness: performance regression bounds, long-running soak/chaos stability,
  and mixed-version rolling upgrade compatibility.

Run **Shared Storage GA Release Gate** with the candidate release ref. Evidence is matched using a production-tree
fingerprint rather than the raw commit SHA so author-normalization and test/workflow-only commits do not discard
otherwise equivalent evidence.

The GA gate currently requires these hardening workflows in addition to the correctness suite:

* `Shared Storage Performance Baseline`
* `Shared Storage Soak and Chaos`
* `Shared Storage Rolling Upgrade`

Until those workflows exist and pass for the candidate production tree, the GA manifest must remain **BLOCKED**.

Real AWS S3 compatibility is selectable with `require_real_s3=true`. Keep it optional for MinIO-only deployments;
enable it for a release that claims AWS S3 as a supported production object store.
