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

"""Validate Shared Storage GA workflow wiring and evidence ownership."""

import ast
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github" / "workflows"
MANIFEST = ROOT / "tools" / "shared_storage_ga_manifest.py"
MAIN_WORKFLOW_NAME = "Shared Storage"
GA_RELEASE_WORKFLOW_NAME = "Shared Storage GA Release Gate"

FOCUSED_CORE_EXCLUSIONS = (
    "-x :core:checkstyleMain",
    "-x :core:checkstyleTest",
    "-x :core:spotbugsMain",
    "-x :core:spotbugsTest",
)


def manifest_constants():
    tree = ast.parse(MANIFEST.read_text(encoding="utf-8"), filename=str(MANIFEST))
    values = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if target.id in {"CORE_REQUIRED", "GA_HARDENING_REQUIRED", "REAL_S3_REQUIRED"}:
            values[target.id] = ast.literal_eval(node.value)
    missing = {"CORE_REQUIRED", "GA_HARDENING_REQUIRED", "REAL_S3_REQUIRED"} - values.keys()
    if missing:
        raise AssertionError("GA manifest is missing constants: " + ", ".join(sorted(missing)))
    return values


def workflow_name(text, path):
    match = re.search(r"(?m)^name:\s*(.+?)\s*$", text)
    if match is None:
        raise AssertionError(f"{path} has no top-level workflow name")
    return match.group(1).strip().strip("'\"")


def focused_core_test_blocks(text):
    lines = text.splitlines()
    blocks = []
    index = 0
    while index < len(lines):
        if lines[index].strip().startswith("./gradlew :core:test"):
            block = [lines[index]]
            index += 1
            while index < len(lines):
                block.append(lines[index])
                if lines[index].strip().startswith("--no-scan"):
                    break
                index += 1
            blocks.append("\n".join(block))
        index += 1
    return blocks


def main():
    constants = manifest_constants()
    workflow_files = sorted(WORKFLOW_DIR.glob("shared-storage*.yml"))
    workflows = {}
    texts = {}
    errors = []

    for path in workflow_files:
        text = path.read_text(encoding="utf-8")
        name = workflow_name(text, path)
        if name in workflows:
            errors.append(f"duplicate workflow name {name!r}: {workflows[name]} and {path}")
        workflows[name] = path
        texts[name] = text

    required_names = (
        list(constants["CORE_REQUIRED"])
        + list(constants["GA_HARDENING_REQUIRED"])
        + [constants["REAL_S3_REQUIRED"]]
        + [GA_RELEASE_WORKFLOW_NAME]
    )
    for name in required_names:
        if name not in workflows:
            errors.append(f"required workflow is missing: {name}")

    for name, path in workflows.items():
        if name == MAIN_WORKFLOW_NAME:
            continue
        for block in focused_core_test_blocks(texts[name]):
            missing = [token for token in FOCUSED_CORE_EXCLUSIONS if token not in block]
            if missing:
                errors.append(
                    f"{path}: focused :core:test must exclude global Core verification tasks: "
                    + ", ".join(missing)
                )

    for name in list(constants["GA_HARDENING_REQUIRED"]) + [constants["REAL_S3_REQUIRED"]]:
        text = texts.get(name, "")
        if "workflow_dispatch:" not in text:
            errors.append(f"{name}: release evidence workflow must support workflow_dispatch")

    ga_release = texts.get(GA_RELEASE_WORKFLOW_NAME, "")
    if "actions: read" not in ga_release:
        errors.append(f"{GA_RELEASE_WORKFLOW_NAME}: actions: read permission is required")
    if "tools/check_shared_storage_ga_workflows.py" not in ga_release:
        errors.append(f"{GA_RELEASE_WORKFLOW_NAME}: workflow consistency check must run before manifest evaluation")

    if errors:
        print("Shared Storage GA workflow consistency: FAILED", file=sys.stderr)
        for error in errors:
            print("- " + error, file=sys.stderr)
        return 1

    focused_blocks = sum(
        len(focused_core_test_blocks(text))
        for name, text in texts.items()
        if name != MAIN_WORKFLOW_NAME
    )
    print(
        "Shared Storage GA workflow consistency: PASS "
        f"({len(workflows)} workflows, {focused_blocks} focused Core runtime test command(s))"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
