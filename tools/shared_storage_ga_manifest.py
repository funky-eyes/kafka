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

Evidence is matched by both the production-tree fingerprint and a per-gate contract
fingerprint. The production fingerprint tolerates author-normalization and unrelated
CI/documentation commits, while the gate contract prevents stale workflow/test evidence
from being reused after the gate itself changes.
"""

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from fnmatch import fnmatch

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

EVIDENCE_WORKFLOW_PATHS = {
    "Shared Storage": ".github/workflows/shared-storage.yml",
    "Shared Storage acks=1 Durability Matrix": ".github/workflows/shared-storage-acks-one.yml",
    "Shared Storage acks=all Durability Matrix": ".github/workflows/shared-storage-sigkill.yml",
    "Shared Storage WAL Crash Windows": ".github/workflows/shared-storage-wal-crash.yml",
    "Shared Storage WAL Capacity": ".github/workflows/shared-storage-wal-capacity.yml",
    "Shared Storage S3 Outage": ".github/workflows/shared-storage-s3-outage.yml",
    "Shared Storage Upload Crash Points": ".github/workflows/shared-storage-upload-crash.yml",
    "Shared Storage Ring WAL Correctness": ".github/workflows/shared-storage-ring-wal-correctness.yml",
    "Shared Storage Object Format Correctness": ".github/workflows/shared-storage-object-format.yml",
    "Shared Storage Kafka Semantics and HA": ".github/workflows/shared-storage-kafka-semantics-ha.yml",
    "Shared Storage Kafka Client Failover": ".github/workflows/shared-storage-kafka-client-failover.yml",
    "Shared Storage KRaft Controller HA": ".github/workflows/shared-storage-controller-ha.yml",
    "Shared Storage Local State Loss Recovery": ".github/workflows/shared-storage-local-state-loss.yml",
    "Shared Storage Inflight Idempotent Produce": ".github/workflows/shared-storage-inflight-produce.yml",
    "Shared Storage Kafka Multipart E2E": ".github/workflows/shared-storage-multipart-kafka.yml",
    "Shared Storage Topic Lifecycle": ".github/workflows/shared-storage-topic-lifecycle.yml",
    "Shared Storage Performance Baseline": ".github/workflows/shared-storage-performance.yml",
    "Shared Storage Soak and Chaos": ".github/workflows/shared-storage-soak-chaos.yml",
    "Shared Storage Rolling Upgrade": ".github/workflows/shared-storage-rolling-upgrade.yml",
    "Shared Storage Real S3 Compatibility": ".github/workflows/shared-storage-real-s3.yml",
}

COMMON_EVIDENCE_CONTRACT_PATHS = (
    ".github/actions/setup-gradle/action.yml",
)

EVIDENCE_EXTRA_CONTRACT_PATHS = {
    "Shared Storage Real S3 Compatibility": (
        "storage/shared-storage-s3/src/test/java/org/apache/kafka/storage/internals/shared/s3/"
        "S3RealCompatibilityTest.java",
    ),
}

AUTOMATIC_EVIDENCE_EVENT = "push"
REAL_S3_EVIDENCE_EVENT = "workflow_dispatch"

JAVA_PRODUCTION_PREFIXES = (
    "storage/src/main/java/org/apache/kafka/storage/internals/shared/",
)

PRODUCTION_PREFIXES = (
    "storage/shared-storage-s3/src/main/",
)

PRODUCTION_PATHS = {
    "build.gradle",
    "settings.gradle",
    "storage/shared-storage-s3/build.gradle",
    "storage/src/main/java/org/apache/kafka/storage/internals/log/IncompleteLogInitializationException.java",
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


def is_production_path(path):
    if path in PRODUCTION_PATHS:
        return True
    if any(path.startswith(prefix) for prefix in JAVA_PRODUCTION_PREFIXES):
        return path.endswith(".java")
    return any(path.startswith(prefix) for prefix in PRODUCTION_PREFIXES)


def event_block(text, event):
    match = re.search(
        rf"(?ms)^  {re.escape(event)}:\s*\n(.*?)(?=^  [A-Za-z_][A-Za-z0-9_-]*:\s*(?:\n|$)|\Z)",
        text,
    )
    return "" if match is None else match.group(1)


def workflow_path_patterns(text):
    return re.findall(r"(?m)^\s+- '([^']+)'\s*$", text)


def push_path_patterns(text):
    push = event_block(text, "push")
    match = re.search(
        r"(?ms)^    paths:\s*\n(.*?)(?=^    [A-Za-z_][A-Za-z0-9_-]*:\s*(?:\n|$)|\Z)",
        push,
    )
    return [] if match is None else workflow_path_patterns(match.group(1))


def path_is_selected(path, patterns):
    selected = False
    for pattern in patterns:
        negate = pattern.startswith("!")
        candidate = pattern[1:] if negate else pattern
        if fnmatch(path, candidate):
            selected = not negate
    return selected


def evidence_event(name):
    return REAL_S3_EVIDENCE_EVENT if name == REAL_S3_REQUIRED else AUTOMATIC_EVIDENCE_EVENT


class GitHub:
    def __init__(self, repo, token):
        self.repo = repo
        self.token = token
        self.cache = {}
        self.tree_cache = {}
        self.workflow_text_cache = {}

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

    def tree_blobs(self, ref):
        cached = self.tree_cache.get(ref)
        if cached is not None:
            return cached
        commit = self.get("git/commits/" + urllib.parse.quote(ref, safe=""))
        tree_sha = commit["tree"]["sha"]
        tree = self.get("git/trees/" + tree_sha, {"recursive": "1"})
        if tree.get("truncated"):
            raise RuntimeError(
                f"GitHub returned a truncated recursive tree for {ref} ({tree_sha}); "
                "refusing to compute an incomplete fingerprint"
            )
        blobs = {
            entry["path"]: entry["sha"]
            for entry in tree.get("tree", [])
            if entry.get("type") == "blob"
        }
        self.tree_cache[ref] = blobs
        return blobs

    @staticmethod
    def fingerprint_rows(rows):
        digest = hashlib.sha256()
        for row in sorted(rows):
            digest.update(row.encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    def production_fingerprint(self, ref):
        rows = [
            path + "\0" + sha
            for path, sha in self.tree_blobs(ref).items()
            if is_production_path(path)
        ]
        if not rows:
            raise RuntimeError(f"no Shared Storage production files found for {ref}")
        return self.fingerprint_rows(rows), len(rows)

    def workflow_text(self, ref, workflow_path):
        key = (ref, workflow_path)
        cached = self.workflow_text_cache.get(key)
        if cached is not None:
            return cached
        payload = self.get(
            "contents/" + urllib.parse.quote(workflow_path, safe="/"),
            {"ref": ref},
        )
        if payload.get("type") != "file" or payload.get("encoding") != "base64":
            raise RuntimeError(f"cannot decode workflow {workflow_path} at {ref}")
        text = base64.b64decode(payload.get("content", "")).decode("utf-8")
        self.workflow_text_cache[key] = text
        return text

    def workflow_contract_fingerprint(self, ref, workflow_path, patterns, extra_paths=()):
        blobs = self.tree_blobs(ref)
        contract_paths = {workflow_path}
        contract_paths.update(
            path
            for path in blobs
            if path != workflow_path and path_is_selected(path, patterns)
        )
        contract_paths.update(extra_paths)
        missing = sorted(path for path in contract_paths if path not in blobs)
        if missing:
            raise RuntimeError(
                f"gate contract paths are missing at {ref}: {', '.join(missing)}"
            )
        rows = [path + "\0" + blobs[path] for path in contract_paths]
        return self.fingerprint_rows(rows), len(rows)

    def workflow_runs(self, workflow_path, branch, event, max_pages=10):
        workflow_file = os.path.basename(workflow_path)
        runs = []
        for page in range(1, max_pages + 1):
            payload = self.get(
                "actions/workflows/" + urllib.parse.quote(workflow_file, safe="") + "/runs",
                {
                    "branch": branch,
                    "event": event,
                    "per_page": 100,
                    "page": page,
                },
            )
            page_runs = payload.get("workflow_runs", [])
            runs.extend(page_runs)
            if len(page_runs) < 100:
                break
        return runs


def is_branch_evidence_run(run, repo, branch, name, workflow_path, event):
    head_repository = run.get("head_repository") or {}
    return (
        run.get("event") == event
        and run.get("name") == name
        and run.get("path") == workflow_path
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

    fingerprint_cache = {target_sha: target_fingerprint}
    contract_cache = {}
    rows = []
    failures = []

    for name in required:
        workflow_path = EVIDENCE_WORKFLOW_PATHS[name]
        event = evidence_event(name)
        workflow_text = github.workflow_text(target_sha, workflow_path)
        patterns = push_path_patterns(workflow_text) if event == AUTOMATIC_EVIDENCE_EVENT else []
        extra_paths = (
            COMMON_EVIDENCE_CONTRACT_PATHS
            + EVIDENCE_EXTRA_CONTRACT_PATHS.get(name, ())
        )
        if event == AUTOMATIC_EVIDENCE_EVENT and not patterns:
            raise RuntimeError(f"automatic evidence workflow {workflow_path} has no push path contract")
        target_contract, contract_files = github.workflow_contract_fingerprint(
            target_sha,
            workflow_path,
            patterns,
            extra_paths,
        )

        candidates = github.workflow_runs(
            workflow_path,
            args.evidence_branch,
            event,
        )

        equivalent = []
        for run in candidates:
            if not is_branch_evidence_run(
                run,
                args.repo,
                args.evidence_branch,
                name,
                workflow_path,
                event,
            ):
                continue
            sha = run.get("head_sha")
            if not sha:
                continue
            try:
                fingerprint = fingerprint_cache.get(sha)
                if fingerprint is None:
                    fingerprint, _ = github.production_fingerprint(sha)
                    fingerprint_cache[sha] = fingerprint
                contract_key = (sha, workflow_path, tuple(patterns), tuple(extra_paths))
                contract = contract_cache.get(contract_key)
                if contract is None:
                    contract, _ = github.workflow_contract_fingerprint(
                        sha,
                        workflow_path,
                        patterns,
                        extra_paths,
                    )
                    contract_cache[contract_key] = contract
            except RuntimeError:
                continue
            if fingerprint == target_fingerprint and contract == target_contract:
                equivalent.append(run)

        equivalent.sort(key=lambda item: item.get("created_at", ""), reverse=True)
        run = equivalent[0] if equivalent else None
        if run is None:
            state = "MISSING"
            detail = "no run covers this production tree and gate contract"
            failures.append(name)
        elif run.get("status") != "completed":
            state = "PENDING"
            detail = "equivalent production tree and gate contract are still running"
            failures.append(name)
        elif run.get("conclusion") != "success":
            state = "FAIL"
            detail = "latest equivalent production-tree/gate-contract evidence is not green"
            failures.append(name)
        else:
            state = "PASS"
            detail = f"production + gate contract match ({contract_files} contract files)"
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
        "- Automatic evidence event: `push`",
        "- Real S3 evidence event: `workflow_dispatch`",
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
