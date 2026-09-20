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


if __name__ == "__main__":
    unittest.main()
