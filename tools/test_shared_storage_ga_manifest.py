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

import unittest

from shared_storage_ga_manifest import (
    COMMON_EVIDENCE_CONTRACT_PATHS,
    EVIDENCE_EXTRA_CONTRACT_PATHS,
    GitHub,
    REAL_S3_REQUIRED,
    is_branch_evidence_run,
    path_is_selected,
    push_path_patterns,
)


class StubGitHub(GitHub):
    def __init__(self, responses):
        self.responses = responses
        self.tree_cache = {}
        self.workflow_text_cache = {}

    def get(self, path, params=None):
        return self.responses[path]


class EvidenceRunTest(unittest.TestCase):
    def test_accepts_exact_workflow_event_repository_and_branch(self):
        run = {
            "event": "push",
            "name": "Shared Storage",
            "path": ".github/workflows/shared-storage.yml",
            "head_repository": {"full_name": "apache/kafka"},
            "head_branch": "release",
        }
        self.assertTrue(
            is_branch_evidence_run(
                run,
                "apache/kafka",
                "release",
                "Shared Storage",
                ".github/workflows/shared-storage.yml",
                "push",
            )
        )

    def test_rejects_wrong_event_name_path_fork_and_branch(self):
        base = {
            "event": "push",
            "name": "Shared Storage",
            "path": ".github/workflows/shared-storage.yml",
            "head_repository": {"full_name": "apache/kafka"},
            "head_branch": "release",
        }
        cases = [
            {**base, "event": "workflow_dispatch"},
            {**base, "name": "Shared Storage Performance Baseline"},
            {**base, "path": ".github/workflows/other.yml"},
            {**base, "head_repository": {"full_name": "fork/kafka"}},
            {**base, "head_branch": "other"},
        ]
        for run in cases:
            self.assertFalse(
                is_branch_evidence_run(
                    run,
                    "apache/kafka",
                    "release",
                    "Shared Storage",
                    ".github/workflows/shared-storage.yml",
                    "push",
                )
            )


class GateContractTest(unittest.TestCase):
    def test_push_path_patterns_only_reads_paths_block(self):
        workflow = """on:
  push:
    branches:
      - release
    paths:
      - 'storage/src/main/**'
      - '!storage/src/main/**/README.md'
  workflow_dispatch:
"""
        self.assertEqual(
            ["storage/src/main/**", "!storage/src/main/**/README.md"],
            push_path_patterns(workflow),
        )

    def test_path_selection_honors_ordered_negation(self):
        patterns = ["storage/**", "!storage/**/README.md", "storage/special/README.md"]
        self.assertTrue(path_is_selected("storage/a/File.java", patterns))
        self.assertFalse(path_is_selected("storage/a/README.md", patterns))
        self.assertTrue(path_is_selected("storage/special/README.md", patterns))


class ProductionFingerprintTest(unittest.TestCase):
    def test_rejects_truncated_recursive_tree(self):
        github = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-sha"}},
            "git/trees/tree-sha": {"truncated": True, "tree": []},
        })
        with self.assertRaisesRegex(RuntimeError, "truncated recursive tree"):
            github.production_fingerprint("candidate")

    def test_non_java_marker_under_java_source_prefix_does_not_change_fingerprint(self):
        java_path = "storage/src/main/java/org/apache/kafka/storage/internals/shared/object/ObjectStore.java"
        marker_path = "storage/src/main/java/org/apache/kafka/storage/internals/shared/object/CI_EXACT_TREE_TRIGGER.md"
        first = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-sha"}},
            "git/trees/tree-sha": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": java_path, "sha": "java-blob"},
                    {"type": "blob", "path": marker_path, "sha": "marker-a"},
                ],
            },
        })
        second = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-sha"}},
            "git/trees/tree-sha": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": java_path, "sha": "java-blob"},
                    {"type": "blob", "path": marker_path, "sha": "marker-b"},
                ],
            },
        })

        first_fingerprint, first_count = first.production_fingerprint("candidate")
        second_fingerprint, second_count = second.production_fingerprint("candidate")

        self.assertEqual(1, first_count)
        self.assertEqual(1, second_count)
        self.assertEqual(first_fingerprint, second_fingerprint)

    def test_incomplete_log_initialization_exception_is_part_of_production_fingerprint(self):
        path = "storage/src/main/java/org/apache/kafka/storage/internals/log/IncompleteLogInitializationException.java"
        first = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-sha"}},
            "git/trees/tree-sha": {
                "truncated": False,
                "tree": [{"type": "blob", "path": path, "sha": "blob-a"}],
            },
        })
        second = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-sha"}},
            "git/trees/tree-sha": {
                "truncated": False,
                "tree": [{"type": "blob", "path": path, "sha": "blob-b"}],
            },
        })

        first_fingerprint, first_count = first.production_fingerprint("candidate")
        second_fingerprint, second_count = second.production_fingerprint("candidate")

        self.assertEqual(1, first_count)
        self.assertEqual(1, second_count)
        self.assertNotEqual(first_fingerprint, second_fingerprint)

    def test_common_setup_action_change_invalidates_gate_contract(self):
        workflow_path = ".github/workflows/shared-storage.yml"
        action_path = COMMON_EVIDENCE_CONTRACT_PATHS[0]
        first = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-a"}},
            "git/trees/tree-a": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": workflow_path, "sha": "workflow"},
                    {"type": "blob", "path": action_path, "sha": "action-a"},
                ],
            },
        })
        second = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-b"}},
            "git/trees/tree-b": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": workflow_path, "sha": "workflow"},
                    {"type": "blob", "path": action_path, "sha": "action-b"},
                ],
            },
        })

        first_fp, first_count = first.workflow_contract_fingerprint(
            "candidate",
            workflow_path,
            [],
            (action_path,),
        )
        second_fp, second_count = second.workflow_contract_fingerprint(
            "candidate",
            workflow_path,
            [],
            (action_path,),
        )

        self.assertEqual(2, first_count)
        self.assertEqual(2, second_count)
        self.assertNotEqual(first_fp, second_fp)

    def test_real_s3_contract_changes_when_compatibility_test_changes(self):
        workflow_path = ".github/workflows/shared-storage-real-s3.yml"
        test_path = EVIDENCE_EXTRA_CONTRACT_PATHS[REAL_S3_REQUIRED][0]
        first = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-a"}},
            "git/trees/tree-a": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": workflow_path, "sha": "workflow"},
                    {"type": "blob", "path": test_path, "sha": "test-a"},
                ],
            },
        })
        second = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-b"}},
            "git/trees/tree-b": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": workflow_path, "sha": "workflow"},
                    {"type": "blob", "path": test_path, "sha": "test-b"},
                ],
            },
        })

        first_fp, first_count = first.workflow_contract_fingerprint(
            "candidate",
            workflow_path,
            [],
            (test_path,),
        )
        second_fp, second_count = second.workflow_contract_fingerprint(
            "candidate",
            workflow_path,
            [],
            (test_path,),
        )

        self.assertEqual(2, first_count)
        self.assertEqual(2, second_count)
        self.assertNotEqual(first_fp, second_fp)

    def test_workflow_contract_changes_when_selected_test_or_workflow_changes(self):
        workflow_path = ".github/workflows/shared-storage.yml"
        test_path = "storage/src/test/java/example/SharedStorageTest.java"
        patterns = ["storage/src/test/**"]
        first = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-a"}},
            "git/trees/tree-a": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": workflow_path, "sha": "workflow-a"},
                    {"type": "blob", "path": test_path, "sha": "test-a"},
                ],
            },
        })
        second = StubGitHub({
            "git/commits/candidate": {"tree": {"sha": "tree-b"}},
            "git/trees/tree-b": {
                "truncated": False,
                "tree": [
                    {"type": "blob", "path": workflow_path, "sha": "workflow-b"},
                    {"type": "blob", "path": test_path, "sha": "test-b"},
                ],
            },
        })

        first_fp, first_count = first.workflow_contract_fingerprint("candidate", workflow_path, patterns)
        second_fp, second_count = second.workflow_contract_fingerprint("candidate", workflow_path, patterns)

        self.assertEqual(2, first_count)
        self.assertEqual(2, second_count)
        self.assertNotEqual(first_fp, second_fp)


if __name__ == "__main__":
    unittest.main()
