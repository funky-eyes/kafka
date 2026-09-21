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

from shared_storage_ga_manifest import GitHub, is_branch_evidence_run


class StubGitHub(GitHub):
    def __init__(self, responses):
        self.responses = responses

    def get(self, path, params=None):
        return self.responses[path]


class EvidenceRunTest(unittest.TestCase):
    def test_accepts_push_and_workflow_dispatch_from_evidence_branch(self):
        base = {
            "head_repository": {"full_name": "apache/kafka"},
            "head_branch": "release",
        }
        self.assertTrue(is_branch_evidence_run({**base, "event": "push"}, "apache/kafka", "release"))
        self.assertTrue(
            is_branch_evidence_run({**base, "event": "workflow_dispatch"}, "apache/kafka", "release")
        )

    def test_rejects_pull_request_fork_and_wrong_branch(self):
        base = {
            "head_repository": {"full_name": "apache/kafka"},
            "head_branch": "release",
        }
        self.assertFalse(
            is_branch_evidence_run({**base, "event": "pull_request"}, "apache/kafka", "release")
        )
        self.assertFalse(
            is_branch_evidence_run(
                {
                    **base,
                    "event": "push",
                    "head_repository": {"full_name": "fork/kafka"},
                },
                "apache/kafka",
                "release",
            )
        )
        self.assertFalse(
            is_branch_evidence_run(
                {**base, "event": "push", "head_branch": "other"},
                "apache/kafka",
                "release",
            )
        )


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


if __name__ == "__main__":
    unittest.main()
