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

"""Safely promote the normalized Shared Storage candidate to the Real S3 evidence branch."""

import argparse
import re
import subprocess
import sys


CANONICAL_BRANCH = "shared-wal-s3-4.3.1"
EVIDENCE_BRANCH = CANONICAL_BRANCH + "-real-s3"
FULL_SHA = re.compile(r"^[0-9a-fA-F]{40}$")


class PromotionError(RuntimeError):
    pass


def validate_sha(value, label="SHA"):
    if not FULL_SHA.fullmatch(value):
        raise PromotionError(f"{label} must be a full 40-character Git SHA: {value!r}")
    return value.lower()


def run_git(*args):
    result = subprocess.run(
        ["git", *args],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise PromotionError(f"git {' '.join(args)} failed: {detail}")
    return result.stdout.strip()


def parse_remote_head(output, branch):
    expected_ref = f"refs/heads/{branch}"
    matches = []
    for line in output.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1] == expected_ref:
            matches.append(validate_sha(fields[0], f"remote {branch} SHA"))
    if len(matches) != 1:
        raise PromotionError(
            f"expected exactly one remote ref for {expected_ref}, found {len(matches)}"
        )
    return matches[0]


def remote_head(remote, branch, git=run_git):
    output = git("ls-remote", "--exit-code", remote, f"refs/heads/{branch}")
    return parse_remote_head(output, branch)


def require_expected_head(expected_sha, actual_sha):
    if actual_sha != expected_sha:
        raise PromotionError(
            f"{CANONICAL_BRANCH} moved to {actual_sha}; expected {expected_sha}. "
            "Refresh the canonical candidate before promoting Real S3 evidence."
        )


def promotion_push_args(remote, expected_sha, previous_evidence_sha):
    lease = f"--force-with-lease=refs/heads/{EVIDENCE_BRANCH}:{previous_evidence_sha}"
    refspec = f"{expected_sha}:refs/heads/{EVIDENCE_BRANCH}"
    return ("push", lease, remote, refspec)


def promote(expected_sha, remote="origin", git=run_git):
    expected_sha = validate_sha(expected_sha, "expected candidate SHA")

    first_canonical = remote_head(remote, CANONICAL_BRANCH, git)
    require_expected_head(expected_sha, first_canonical)

    previous_evidence = remote_head(remote, EVIDENCE_BRANCH, git)

    git("fetch", "--no-tags", remote, f"refs/heads/{CANONICAL_BRANCH}")
    fetched_sha = validate_sha(git("rev-parse", "FETCH_HEAD"), "fetched canonical SHA")
    require_expected_head(expected_sha, fetched_sha)

    final_canonical = remote_head(remote, CANONICAL_BRANCH, git)
    require_expected_head(expected_sha, final_canonical)

    git(*promotion_push_args(remote, expected_sha, previous_evidence))
    return previous_evidence


def main():
    parser = argparse.ArgumentParser(
        description="Promote an exact normalized Shared Storage candidate to the dedicated Real S3 evidence branch."
    )
    parser.add_argument(
        "expected_candidate_sha",
        help="Full 40-character SHA currently expected at shared-wal-s3-4.3.1",
    )
    parser.add_argument("--remote", default="origin", help="Git remote to use (default: origin)")
    args = parser.parse_args()

    try:
        previous = promote(args.expected_candidate_sha, args.remote)
    except PromotionError as exc:
        print(f"Real S3 evidence promotion refused: {exc}", file=sys.stderr)
        return 1

    print(f"Canonical candidate: {args.expected_candidate_sha.lower()}")
    print(f"Previous evidence:   {previous}")
    print(f"Evidence branch:     {EVIDENCE_BRANCH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
