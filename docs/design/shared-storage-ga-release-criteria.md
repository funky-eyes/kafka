# Shared Storage GA release criteria

A Shared Storage build is eligible for GA only when the machine-generated GA manifest passes for the release
production tree.

The release gate deliberately separates two concerns:

* correctness gates prove durability, crash recovery, failover, Kafka semantics, object-format compatibility, and
  lifecycle behavior;
* GA hardening gates prove production readiness: performance regression bounds, long-running soak/chaos stability,
  and mixed-version rolling upgrade compatibility.

Run **Shared Storage GA Release Gate** with the candidate release ref. The release workflow resolves that ref once
after checkout and evaluates the exact checked-out commit SHA, so a moving branch cannot change the candidate during
the manifest run. Production code is matched using a production-tree fingerprint so author-normalization and unrelated
documentation/CI commits do not discard otherwise equivalent evidence. Automatic gates also have a contract fingerprint
made from their workflow file plus the files selected by that workflow's `push.paths`. All gate contracts include the
shared local `setup-gradle` composite action and the Gradle launcher/wrapper inputs they execute. Automatic evidence
workflows include those execution inputs in their `push.paths`, so a harness change both invalidates stale evidence
and produces replacement evidence. The production fingerprint also includes `gradle.properties` and
`gradle/dependencies.gradle`, because version and dependency changes alter the build even when Java/Scala source is
unchanged. The real-S3 gate additionally fingerprints the explicitly mapped `S3RealCompatibilityTest` source. Before the
workflow exists on the repository default branch, real AWS evidence can be triggered by pointing the dedicated
`<evidence_branch>-real-s3` branch at the exact candidate SHA; after the workflow lands on the default branch,
`workflow_dispatch` remains available. Workflow, build-harness, local-action, or selected-test changes therefore
require fresh evidence.

The GA gate currently requires these hardening workflows in addition to the correctness suite:

* `Shared Storage Performance Baseline`
* `Shared Storage Soak and Chaos`
* `Shared Storage Rolling Upgrade`

Until those workflows exist and pass for the candidate production tree, the GA manifest must remain **BLOCKED**.

Real AWS S3 compatibility is selectable with `require_real_s3=true`. Keep it optional for MinIO-only deployments;
enable it for a release that claims AWS S3 as a supported production object store.


## Evidence ownership

The `Shared Storage` workflow owns compile-time and static-analysis evidence for the Shared Storage production surface
and the shared Kafka server tests selected by its path filters. Specialized durability, failover, performance, soak,
and rolling-upgrade workflows are runtime correctness gates; their focused `:core:test` invocations deliberately
exclude the global Core Checkstyle and SpotBugs tasks.

This separation prevents an unrelated Core test/style violation from turning a durability gate red when that workflow
does not trigger on the offending file. It does not waive static analysis: the GA manifest still requires the main
`Shared Storage` workflow to pass for the same production-tree fingerprint.


### Real AWS S3 evidence

Releases that claim AWS S3 as a supported production object store must run `Shared Storage Real S3 Compatibility`
for the exact release production tree and enable `require_real_s3` in the GA release gate.

Configure a protected GitHub Environment named `shared-storage-aws-s3` with secret
`SHARED_STORAGE_AWS_ROLE_ARN`. For branch-triggered evidence, also configure environment variable
`SHARED_STORAGE_AWS_S3_BUCKET`; `SHARED_STORAGE_AWS_S3_REGION` is optional and defaults to `us-east-1`.
The role should use GitHub OIDC and receive only the bucket permissions needed for the dedicated compatibility bucket.
The workflow uses a random object prefix and deletes the objects it creates.

For pre-merge evidence, create or fast-forward `<evidence_branch>-real-s3` to the candidate SHA. The push runs the
branch-local workflow without requiring the workflow file to exist on the default branch. The GA manifest accepts that
run only from the same repository and dedicated evidence branch, and still requires matching production and gate-contract
fingerprints. Do not force the development branch or create a synthetic code commit merely to trigger AWS evidence.

The proof intentionally uses the AWS SDK default endpoint, TLS and virtual-hosted addressing. It verifies a normal PUT,
Range GET, native multipart completion across the five-MiB S3 part boundary, and DELETE. MinIO evidence does not
substitute for this workflow when AWS S3 support is claimed.


### Evidence branch isolation

Evidence is branch-scoped. The GA release workflow requires an `evidence_branch`, binds every required gate to its
expected workflow file path, and only accepts runs whose `head_branch` and `head_repository` match that branch in
this repository. Core correctness and GA hardening gates accept only automatic `push` runs; manual
`workflow_dispatch` runs cannot satisfy those gates because several workflows expose tunable workload or threshold
inputs. The optional real-AWS-S3 gate is the exception: it accepts either `workflow_dispatch` on the configured evidence
branch or a `push` run from the dedicated `<evidence_branch>-real-s3` branch. The push form reads bucket/region from
the protected `shared-storage-aws-s3` environment so pre-merge evidence does not depend on default-branch dispatch.

For every accepted run, both the Shared Storage production fingerprint and that workflow's gate-contract fingerprint
must match the candidate. This preserves author-normalization equivalence without allowing an old green run to survive
a workflow or selected-test change. A pull request, fork, different branch, same-named workflow at another path, or
partial recursive GitHub tree cannot satisfy a release gate. Recursive tree responses are rejected when GitHub marks
them truncated.
