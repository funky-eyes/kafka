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
from fnmatch import fnmatch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github" / "workflows"
MANIFEST = ROOT / "tools" / "shared_storage_ga_manifest.py"
MAIN_WORKFLOW_NAME = "Shared Storage"
GA_RELEASE_WORKFLOW_NAME = "Shared Storage GA Release Gate"
UNBUILT_ROOT_SHARED_SOURCE_DIRS = (
    ROOT / "src" / "main" / "java" / "org" / "apache" / "kafka" / "storage" / "internals" / "shared",
    ROOT / "src" / "test" / "java" / "org" / "apache" / "kafka" / "storage" / "internals" / "shared",
)

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
        if target.id in {
            "CORE_REQUIRED",
            "GA_HARDENING_REQUIRED",
            "REAL_S3_REQUIRED",
            "EVIDENCE_WORKFLOW_PATHS",
            "COMMON_EVIDENCE_CONTRACT_PATHS",
            "EVIDENCE_EXTRA_CONTRACT_PATHS",
            "JAVA_PRODUCTION_PREFIXES",
            "PRODUCTION_PREFIXES",
            "PRODUCTION_PATHS",
        }:
            values[target.id] = ast.literal_eval(node.value)
    missing = {
        "CORE_REQUIRED",
        "GA_HARDENING_REQUIRED",
        "REAL_S3_REQUIRED",
        "EVIDENCE_WORKFLOW_PATHS",
        "COMMON_EVIDENCE_CONTRACT_PATHS",
        "EVIDENCE_EXTRA_CONTRACT_PATHS",
        "JAVA_PRODUCTION_PREFIXES",
        "PRODUCTION_PREFIXES",
        "PRODUCTION_PATHS",
    } - values.keys()
    if missing:
        raise AssertionError("GA manifest is missing constants: " + ", ".join(sorted(missing)))
    return values


def workflow_name(text, path):
    match = re.search(r"(?m)^name:\s*(.+?)\s*$", text)
    if match is None:
        raise AssertionError(f"{path} has no top-level workflow name")
    return match.group(1).strip().strip("'\"")


def fingerprint_covers(path, constants):
    if path in constants["PRODUCTION_PATHS"]:
        return True
    if any(path.startswith(prefix) for prefix in constants["JAVA_PRODUCTION_PREFIXES"]):
        return path.endswith(".java")
    return any(path.startswith(prefix) for prefix in constants["PRODUCTION_PREFIXES"])


def concrete_production_paths(text):
    paths = set(re.findall(r"(?m)^\s+- '([^']+)'\s*$", text))
    return {
        path
        for path in paths
        if "*" not in path
        and "?" not in path
        and (
            path in {"build.gradle", "settings.gradle", "storage/shared-storage-s3/build.gradle"}
            or path.startswith("core/src/main/")
            or path.startswith("storage/src/main/")
        )
    }


def workflow_path_patterns(text):
    return set(re.findall(r"(?m)^\s+- '([^']+)'\s*$", text))


def event_block(text, event):
    match = re.search(
        rf"(?ms)^  {re.escape(event)}:\s*\n(.*?)(?=^  [A-Za-z_][A-Za-z0-9_-]*:\s*(?:\n|$)|\Z)",
        text,
    )
    return "" if match is None else match.group(1)


def event_path_patterns(text, event):
    return workflow_path_patterns(event_block(text, event))


def local_action_references(text):
    return set(re.findall(r"uses:\s+(\./\.github/actions/[^\s]+)", text))


def local_action_files(reference):
    action_dir = ROOT / reference.removeprefix("./")
    return {
        path.relative_to(ROOT).as_posix()
        for name in ("action.yml", "action.yaml")
        if (path := action_dir / name).is_file()
    }


def exact_test_selectors(text):
    return {
        selector
        for selector in re.findall(r"--tests\s+'([^']+)'", text)
        if "*" not in selector and "?" not in selector
    }


def test_sources(selector):
    parts = selector.split(".")
    for index in range(len(parts) - 1, -1, -1):
        simple_name = parts[index]
        candidates = []
        for extension in ("java", "scala"):
            candidates.extend(
                path
                for path in ROOT.glob(f"**/{simple_name}.{extension}")
                if "/src/test/" in path.as_posix()
            )
        if candidates:
            return {
                path.relative_to(ROOT).as_posix()
                for path in candidates
            }
    return set()


def path_is_triggered(path, patterns):
    return any(fnmatch(path, pattern) for pattern in patterns)


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

    for source_dir in UNBUILT_ROOT_SHARED_SOURCE_DIRS:
        if not source_dir.exists():
            continue
        java_sources = sorted(path.relative_to(ROOT).as_posix() for path in source_dir.rglob("*.java"))
        if java_sources:
            errors.append(
                "shared-storage Java sources must live in a compiled Gradle subproject, not the root src tree: "
                + ", ".join(java_sources)
            )

    required_names = (
        list(constants["CORE_REQUIRED"])
        + list(constants["GA_HARDENING_REQUIRED"])
        + [constants["REAL_S3_REQUIRED"]]
        + [GA_RELEASE_WORKFLOW_NAME]
    )
    for name in required_names:
        if name not in workflows:
            errors.append(f"required workflow is missing: {name}")

    expected_evidence_names = (
        list(constants["CORE_REQUIRED"])
        + list(constants["GA_HARDENING_REQUIRED"])
        + [constants["REAL_S3_REQUIRED"]]
    )
    mapped_names = set(constants["EVIDENCE_WORKFLOW_PATHS"])
    if mapped_names != set(expected_evidence_names):
        missing = sorted(set(expected_evidence_names) - mapped_names)
        extra = sorted(mapped_names - set(expected_evidence_names))
        if missing:
            errors.append("GA evidence workflow path map is missing: " + ", ".join(missing))
        if extra:
            errors.append("GA evidence workflow path map has unexpected entries: " + ", ".join(extra))
    for name in expected_evidence_names:
        path = workflows.get(name)
        mapped = constants["EVIDENCE_WORKFLOW_PATHS"].get(name)
        if path is None or mapped is None:
            continue
        actual = path.relative_to(ROOT).as_posix()
        if actual != mapped:
            errors.append(
                f"GA evidence workflow path mismatch for {name!r}: expected {actual}, manifest has {mapped}"
            )

    for name, extra_paths in constants["EVIDENCE_EXTRA_CONTRACT_PATHS"].items():
        if name not in expected_evidence_names:
            errors.append(f"GA evidence extra contract has unexpected workflow: {name}")
        for extra_path in extra_paths:
            if not (ROOT / extra_path).is_file():
                errors.append(f"GA evidence extra contract path is missing: {extra_path}")

    common_contract = set(constants["COMMON_EVIDENCE_CONTRACT_PATHS"])
    for common_path in sorted(common_contract):
        if not (ROOT / common_path).is_file():
            errors.append(f"GA common evidence contract path is missing: {common_path}")

    for name in expected_evidence_names:
        text = texts.get(name, "")
        gate_contract = common_contract | set(
            constants["EVIDENCE_EXTRA_CONTRACT_PATHS"].get(name, ())
        )
        for reference in sorted(local_action_references(text)):
            action_files = local_action_files(reference)
            if not action_files:
                errors.append(f"{name}: cannot resolve local action reference: {reference}")
                continue
            missing_action_files = sorted(action_files - gate_contract)
            if missing_action_files:
                errors.append(
                    f"{name}: local action is not covered by the GA evidence contract: "
                    + ", ".join(missing_action_files)
                )

    real_s3_name = constants["REAL_S3_REQUIRED"]
    real_s3_contract = set(constants["EVIDENCE_EXTRA_CONTRACT_PATHS"].get(real_s3_name, ()))
    for selector in sorted(exact_test_selectors(texts.get(real_s3_name, ""))):
        sources = test_sources(selector)
        if not sources:
            errors.append(f"{real_s3_name}: cannot resolve test selector to a test source: {selector}")
            continue
        missing_sources = sorted(sources - real_s3_contract)
        if missing_sources:
            errors.append(
                f"{real_s3_name}: selected test source is not covered by the explicit GA contract: "
                + ", ".join(missing_sources)
            )

    # The GA manifest compares one global production fingerprint for every required gate.
    # A production-tree change therefore invalidates every core/hardening gate's evidence.
    # Verify the release-branch push wiring against that same contract so a production change
    # cannot silently leave the manifest BLOCKED because a required workflow did not run.
    automatic_evidence_names = (
        list(constants["CORE_REQUIRED"])
        + list(constants["GA_HARDENING_REQUIRED"])
    )
    for name in automatic_evidence_names:
        if name not in workflows:
            continue
        patterns = event_path_patterns(texts[name], "push")
        if not patterns:
            errors.append(f"{workflows[name]}: GA-required evidence workflow must define push.paths")
            continue
        for contract_path in sorted(constants["COMMON_EVIDENCE_CONTRACT_PATHS"]):
            if not path_is_triggered(contract_path, patterns):
                errors.append(
                    f"{workflows[name]}: push.paths does not cover GA common evidence contract path: "
                    f"{contract_path}"
                )
        for production_path in sorted(constants["PRODUCTION_PATHS"]):
            if not path_is_triggered(production_path, patterns):
                errors.append(
                    f"{workflows[name]}: push.paths does not cover GA production path: {production_path}"
                )
        for prefix in sorted(constants["JAVA_PRODUCTION_PREFIXES"]):
            probe = prefix + "__ga_trigger_probe__.java"
            if not path_is_triggered(probe, patterns):
                errors.append(
                    f"{workflows[name]}: push.paths does not cover GA Java production prefix: {prefix}"
                )
        for prefix in sorted(constants["PRODUCTION_PREFIXES"]):
            probe = prefix + "__ga_trigger_probe__"
            if not path_is_triggered(probe, patterns):
                errors.append(
                    f"{workflows[name]}: push.paths does not cover GA production prefix: {prefix}"
                )

    production_paths = set()
    for text in texts.values():
        production_paths.update(concrete_production_paths(text))
    if any("storage/shared-storage-s3/**" in text for text in texts.values()):
        production_paths.add("storage/shared-storage-s3/build.gradle")

    for path in sorted(production_paths):
        if not fingerprint_covers(path, constants):
            errors.append(f"GA production fingerprint does not cover workflow production path: {path}")

    expected_java_prefixes = {
        "storage/src/main/java/org/apache/kafka/storage/internals/shared/",
    }
    missing_java_prefixes = expected_java_prefixes - set(constants["JAVA_PRODUCTION_PREFIXES"])
    for prefix in sorted(missing_java_prefixes):
        errors.append(f"GA production fingerprint is missing required Java production prefix: {prefix}")

    expected_prefixes = {
        "storage/shared-storage-s3/src/main/",
    }
    missing_prefixes = expected_prefixes - set(constants["PRODUCTION_PREFIXES"])
    for prefix in sorted(missing_prefixes):
        errors.append(f"GA production fingerprint is missing required production prefix: {prefix}")

    for name, path in workflows.items():
        text = texts[name]
        if re.search(r"(?m)^  push:\s*$", text) is None:
            continue
        patterns = event_path_patterns(text, "push")
        for selector in sorted(exact_test_selectors(text)):
            sources = test_sources(selector)
            if not sources:
                errors.append(f"{path}: cannot resolve test selector to a test source: {selector}")
                continue
            if not any(path_is_triggered(source, patterns) for source in sources):
                errors.append(
                    f"{path}: test selector {selector} is not covered by push.paths; "
                    f"resolved source(s): {', '.join(sorted(sources))}"
                )

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
    if "--evidence-branch" not in ga_release:
        errors.append(f"{GA_RELEASE_WORKFLOW_NAME}: release evidence must be explicitly branch-scoped")

    if "git rev-parse HEAD" not in ga_release or "--ref \"${{ steps.release-sha.outputs.sha }}\"" not in ga_release:
        errors.append(
            f"{GA_RELEASE_WORKFLOW_NAME}: release evaluation must use the exact checked-out commit SHA"
        )

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
