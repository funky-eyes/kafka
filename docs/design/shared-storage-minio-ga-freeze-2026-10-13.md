# Shared Storage MinIO GA freeze — 2026-10-13

This is a **release handoff**, not a release tag, a published binary, or a
declaration that every future lifecycle feature is enabled.

## Frozen source and evidence

| Field | Value |
| --- | --- |
| Product scope | Kafka 4.3.1 Shared Storage, pinned MinIO-backed S3-compatible backend |
| Immutable candidate | `9a28c11b6d97ecacafe1b8566cde5cc34d604889` |
| Frozen branch | `release/shared-storage-minio-ga-20261013` |
| Original evidence branch | `shared-wal-s3-4.3.1` |
| Equivalent development tree | `5f94c4efcd79694d69d13ab36e1f6dce2f01eabe` |
| Final normalized GA Seal | [Actions #38354435176](https://github.com/funky-eyes/kafka/actions/runs/38354435176) — SUCCESS |
| Java 25 + real MinIO / three KRaft brokers | [Actions #38354435224](https://github.com/funky-eyes/kafka/actions/runs/38354435224) — SUCCESS |
| Automatic source-SHA workflows | 20/20 SUCCESS, including all 19 required correctness/hardening gates |

The original source-SHA evidence is bound to the **original evidence
branch**, production fingerprint and gate contracts. A release branch or
commit with identical source is not a substitute for those checks; its
acceptance must be established by the manifest at the exact release SHA.

## Explicit exclusions

* No claim of native AWS S3 interoperability (`require_real_s3=false`).
* No COMMITTED physical-object reclamation caused by Topic Delete,
  Retention or DeleteRecords. Existing orphan/duplicate COMMITTED cleanup
  is a separate, previously validated capability.
* No active KRaft retirement authority writer or verified source-log-start
  certification. Experimental retirement feature v1 requires the
  unshipped metadata-version feature gate.
* No AutoMQ performance-parity or native async/io_uring claim.

## Operator release steps — not performed by this handoff

1. Leave the frozen branch at the exact SHA above. Do not merge
   `feat/committed-lifecycle-authority` or later GC code into it.
2. Re-run the **Shared Storage GA Release Gate** manifest for the exact
   candidate with `evidence_branch=shared-wal-s3-4.3.1` and
   `require_real_s3=false`. As of this handoff the workflow file exists
   on the feature branch but **not** on the repository's default `trunk`,
   so GitHub UI `workflow_dispatch` may not offer it. Do not merge the
   unrelated histories merely to enable dispatch. An authorized release
   operator can instead run `tools/test_shared_storage_ga_manifest.py`
   and `tools/shared_storage_ga_manifest.py` from the frozen checkout
   against the GitHub API, with the same exact ref/branch arguments as
   `.github/workflows/shared-storage-ga-release.yml`; require a PASS
   output. Alternatively first arrange a safe, audited default-branch
   workflow installation and then dispatch it with those exact inputs.
3. Confirm distributions, checksums/signatures as required by the chosen
   release process, startup configuration, topic selector, runbook,
   release notes and old-new-old rollback results.
4. Publish a release/tag only after the release gate and artifact QA.
   Stage a small production canary before wider rollout.
5. If the production code, build harness or workflow contracts change,
   do **not** reuse this evidence. Rebuild and re-seal the new exact tree.

The GitHub connector used for this handoff cannot dispatch workflow inputs
or publish a signed release. The default `trunk` currently lacks this GA
workflow, so an additional release orchestration step is genuinely pending.
Neither action is represented as complete.

## Engineering continuation

GC-specific changes belong in
`feat/committed-lifecycle-authority` and are **not** part of this
frozen GA candidate. See
[GC implementation boundary](committed-lifecycle-gc-next-boundary.md)
and [full retirement SDD](shared-storage-committed-lifecycle-gc-sdd.md).
