#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Build a machine-verifiable Shared Storage GA evidence manifest.

Evidence is matched by the production-tree fingerprint rather than raw commit SHA so the
author-normalization workflow and test/workflow-only commits do not invalidate otherwise
identical production code.
"""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

CORE_REQUIRED = [
    "Shared Storage",
    "Shared Storage acks=1 Durability Matrix",
    "Shared Storage acks=all Durability Matrix",
    "Shared Storage WAL Crash Windows",
    "Shared Storage WAL Capacity",
    "Shared Storage S3 Outage",
    "Shared Storage Upload Crash Points",
    "Shared Storage Ring WAL Correctness",
    "Shared Storage Object Format Correctness",
    "Shared Storage Kafka Semantics and HA",
    "Shared Storage Kafka Client Failover",
    "Shared Storage KRaft Controller HA",
    "Shared Storage Local State Loss Recovery",
    "Shared Storage Inflight Idempotent Produce",
    "Shared Storage Kafka Multipart E2E",
    "Shared Storage Topic Lifecycle",
]

GA_HARDENING_REQUIRED = [
    "Shared Storage Performance Baseline",
    "Shared Storage Soak and Chaos",
    "Shared Storage Rolling Upgrade",
]

REAL_S3_REQUIRED = "Shared Storage Real S3 Compatibility"

EVIDENCE_EVENTS = {"push", "workflow_dispatch"}

PRODUCTION_PREFIXES = (
    "storage/src/main/java/org/apache/kafka/storage/internals/shared/",
    "storage/shared-storage-s3/src/main/",
)

PRODUCTION_PATHS = {
    "build.gradle",
    "settings.gradle",
    "storage/shared-storage-s3/build.gradle",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/LogOffsetsListener.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegmentFactory.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLogCreationContext.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/KafkaStorageExtension.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/StorageExtensionBrokerContext.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/StorageExtensionContext.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/StorageExtensionLoader.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/StoragePartitionRoleListener.java",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLogFactory.java",
    "core/src/main/scala/kafka/cluster/Partition.scala",
    "core/src/main/scala/kafka/log/LogManager.scala",
    "core/src/main/java/kafka/server/builders/LogManagerBuilder.java",
    "core/src/main/scala/kafka/server/BrokerServer.scala",
    "core/src/main/scala/kafka/server/ControllerServer.scala",
    "core/src/main/scala/kafka/server/ReplicaManager.scala",
    "core/src/main/scala/kafka/server/ReplicaFetcherManager.scala",
    "core/src/main/scala/kafka/server/ReplicaFetcherThread.scala",
    "core/src/main/scala/kafka/server/AbstractFetcherThread.scala",
}


class GitHub:
    def __init__(self, repo, token):
        self.repo = repo
        self.token = token
        self.cache = {}

    def get(self, path, params=None):
        url = "https://api.github.com/repos/" + self.repo + "/" + path.lstrip("/")
        if params:
            url += "?" + urllib.parse.urlencode(params)
        if url in self.cache:
            return self.cache[url]
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": "Bearer " + self.token,
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "shared-storage-ga-manifest",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                value = json.load(response)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", "replace")
            raise RuntimeError(f"GitHub API {exc.code} for {url}: {body}") from exc
        self.cache[url] = value
        return value

    def commit(self, ref):
        return self.get("commits/" + urllib.parse.quote(ref, safe=""))

    def production_fingerprint(self, ref):
        commit = self.get("git/commits/" + urllib.parse.quote(ref, safe=""))
        tree_sha = commit["tree"]["sha"]
        tree = self.get("git/trees/" + tree_sha, {"recursive": "1"})
        if tree.get("truncated"):
            raise RuntimeError(
                f"GitHub returned a truncated recursive tree for {ref} ({tree_sha}); "
                "refusing to compute an incomplete production fingerprint"
            )
        rows = []
        for entry in tree.get("tree", []):
            if entry.get("type") != "blob":
                continue
            path = entry["path"]
            if path in PRODUCTION_PATHS or path.startswith(PRODUCTION_PREFIXES):
                rows.append(path + "\0" + entry["sha"])
        rows.sort()
        digest = hashlib.sha256()
        for row in rows:
            digest.update(row.encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest(), len(rows)

    def recent_runs(self, branch, max_pages=10):
        runs = []
        for page in range(1, max_pages + 1):
            payload = self.get("actions/runs", {"branch": branch, "per_page": 100, "page": page})
            page_runs = payload.get("workflow_runs", [])
            runs.extend(page_runs)
            if len(page_runs) < 100:
                break
        return runs


def is_branch_evidence_run(run, repo, branch):
    head_repository = run.get("head_repository") or {}
    return (
        run.get("event") in EVIDENCE_EVENTS
        and head_repository.get("full_name") == repo
        and run.get("head_branch") == branch
    )


def run_url(repo, run):
    run_id = run.get("id")
    return f"https://github.com/{repo}/actions/runs/{run_id}" if run_id else ""


def render_row(repo, name, state, run, detail):
    if run:
        number = "#" + str(run.get("run_number", "?"))
        evidence = str(run.get("head_sha", ""))[:12]
        status = run.get("conclusion") or run.get("status") or "unknown"
        link = run_url(repo, run)
        run_cell = f"[{number}]({link})" if link else number
    else:
        run_cell = "-"
        evidence = "-"
        status = "missing"
    return f"| {name} | {state} | {run_cell} | {status} | {evidence} | {detail} |"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--ref", required=True)
    parser.add_argument("--evidence-branch", required=True)
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    parser.add_argument("--output", default="shared-storage-ga-manifest.md")
    parser.add_argument("--require-real-s3", action="store_true")
    args = parser.parse_args()

    if not args.token:
        parser.error("--token or GITHUB_TOKEN is required")

    github = GitHub(args.repo, args.token)
    target = github.commit(args.ref)
    target_sha = target["sha"]
    target_fingerprint, production_files = github.production_fingerprint(target_sha)

    required = CORE_REQUIRED + GA_HARDENING_REQUIRED
    if args.require_real_s3:
        required.append(REAL_S3_REQUIRED)

    runs = github.recent_runs(args.evidence_branch)
    by_name = {}
    for run in runs:
        if not is_branch_evidence_run(run, args.repo, args.evidence_branch):
            continue
        by_name.setdefault(run.get("name"), []).append(run)

    fingerprint_cache = {target_sha: target_fingerprint}
    rows = []
    failures = []

    for name in required:
        candidates = by_name.get(name, [])
        equivalent = []
        for run in candidates:
            sha = run.get("head_sha")
            if not sha:
                continue
            try:
                fingerprint = fingerprint_cache.get(sha)
                if fingerprint is None:
                    fingerprint, _ = github.production_fingerprint(sha)
                    fingerprint_cache[sha] = fingerprint
            except RuntimeError:
                continue
            if fingerprint == target_fingerprint:
                equivalent.append(run)

        equivalent.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        run = equivalent[0] if equivalent else None
        if run is None:
            state = "MISSING"
            detail = "no run covers this production tree"
            failures.append(name)
        elif run.get("status") != "completed":
            state = "PENDING"
            detail = "equivalent production tree is still running"
            failures.append(name)
        elif run.get("conclusion") != "success":
            state = "FAIL"
            detail = "latest equivalent production-tree evidence is not green"
            failures.append(name)
        else:
            state = "PASS"
            detail = "production-tree fingerprint matches"
        rows.append(render_row(args.repo, name, state, run, detail))

    lines = [
        "# Shared Storage GA Evidence Manifest",
        "",
        f"- Repository: `{args.repo}`",
        f"- Release ref: `{args.ref}`",
        f"- Release commit: `{target_sha}`",
        f"- Evidence branch: `{args.evidence_branch}`",
        f"- Production fingerprint: `{target_fingerprint}`",
        f"- Production files fingerprinted: {production_files}",
        f"- Result: **{'PASS' if not failures else 'BLOCKED'}**",
        "",
        "| Workflow | Gate | Run | Status | Evidence SHA | Evidence |",
        "| --- | --- | --- | --- | --- | --- |",
        *rows,
        "",
    ]
    if failures:
        lines.extend([
            "## Blocking gates",
            "",
            *[f"- {name}" for name in failures],
            "",
        ])

    with open(args.output, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))

    print("\n".join(lines))
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
