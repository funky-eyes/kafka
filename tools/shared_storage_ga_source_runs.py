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

"""Cheaply classify source-SHA GA evidence runs before the full manifest scan."""

import argparse
import os
import sys

from shared_storage_ga_manifest import (
    AUTOMATIC_EVIDENCE_EVENT,
    GitHub,
    source_evidence_run_state,
)


def describe(runs):
    return ", ".join(
        f"{run.get('name')}#{run.get('run_number', '?')}:"
        f"{run.get('conclusion') or run.get('status') or 'unknown'}"
        for run in runs
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True)
    parser.add_argument("--branch", required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN"))
    args = parser.parse_args()

    if not args.token:
        parser.error("--token or GITHUB_TOKEN is required")

    github = GitHub(args.repo, args.token)
    payload = github.get(
        "actions/runs",
        {
            "head_sha": args.sha,
            "event": AUTOMATIC_EVIDENCE_EVENT,
            "per_page": 100,
        },
    )
    pending, failed = source_evidence_run_state(
        payload.get("workflow_runs", []),
        args.repo,
        args.branch,
        args.sha,
    )

    if failed:
        print("Source-SHA GA evidence has terminal failure: " + describe(failed))
        return 2
    if pending:
        print("Source-SHA GA evidence still running: " + describe(pending))
        return 1

    print("Source-SHA GA evidence is terminal; running the full manifest once.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
