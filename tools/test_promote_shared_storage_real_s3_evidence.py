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

from promote_shared_storage_real_s3_evidence import (
    CANONICAL_BRANCH,
    EVIDENCE_BRANCH,
    PromotionError,
    parse_remote_head,
    promote,
    promotion_push_args,
    validate_sha,
)


CANDIDATE = "1" * 40
PREVIOUS = "2" * 40
MOVED = "3" * 40


class PromotionHelperTest(unittest.TestCase):
    def test_requires_full_sha(self):
        self.assertEqual(CANDIDATE, validate_sha(CANDIDATE))
        with self.assertRaises(PromotionError):
            validate_sha("1234")

    def test_parses_exact_remote_branch(self):
        output = f"{CANDIDATE}\trefs/heads/{CANONICAL_BRANCH}\n"
        self.assertEqual(CANDIDATE, parse_remote_head(output, CANONICAL_BRANCH))
        with self.assertRaises(PromotionError):
            parse_remote_head("", CANONICAL_BRANCH)

    def test_force_with_lease_binds_previous_evidence_sha(self):
        self.assertEqual(
            (
                "push",
                f"--force-with-lease=refs/heads/{EVIDENCE_BRANCH}:{PREVIOUS}",
                "origin",
                f"{CANDIDATE}:refs/heads/{EVIDENCE_BRANCH}",
            ),
            promotion_push_args("origin", CANDIDATE, PREVIOUS),
        )

    def test_promotes_only_after_second_canonical_head_check(self):
        calls = []
        canonical_checks = 0

        def fake_git(*args):
            nonlocal canonical_checks
            calls.append(args)
            if args[:3] == ("ls-remote", "--exit-code", "origin"):
                branch = args[3].removeprefix("refs/heads/")
                if branch == CANONICAL_BRANCH:
                    canonical_checks += 1
                    return f"{CANDIDATE}\trefs/heads/{CANONICAL_BRANCH}"
                if branch == EVIDENCE_BRANCH:
                    return f"{PREVIOUS}\trefs/heads/{EVIDENCE_BRANCH}"
            if args[:3] == ("fetch", "--no-tags", "origin"):
                return ""
            if args == ("rev-parse", "FETCH_HEAD"):
                return CANDIDATE
            if args[0] == "push":
                return ""
            raise AssertionError(f"unexpected git command: {args}")

        self.assertEqual(PREVIOUS, promote(CANDIDATE, git=fake_git))
        self.assertEqual(2, canonical_checks)
        self.assertEqual(
            promotion_push_args("origin", CANDIDATE, PREVIOUS),
            calls[-1],
        )

    def test_refuses_if_canonical_moves_before_push(self):
        calls = []
        canonical_checks = 0

        def fake_git(*args):
            nonlocal canonical_checks
            calls.append(args)
            if args[:3] == ("ls-remote", "--exit-code", "origin"):
                branch = args[3].removeprefix("refs/heads/")
                if branch == CANONICAL_BRANCH:
                    canonical_checks += 1
                    sha = CANDIDATE if canonical_checks == 1 else MOVED
                    return f"{sha}\trefs/heads/{CANONICAL_BRANCH}"
                if branch == EVIDENCE_BRANCH:
                    return f"{PREVIOUS}\trefs/heads/{EVIDENCE_BRANCH}"
            if args[:3] == ("fetch", "--no-tags", "origin"):
                return ""
            if args == ("rev-parse", "FETCH_HEAD"):
                return CANDIDATE
            raise AssertionError(f"unexpected git command: {args}")

        with self.assertRaisesRegex(PromotionError, "moved"):
            promote(CANDIDATE, git=fake_git)
        self.assertFalse(any(args[0] == "push" for args in calls))


if __name__ == "__main__":
    unittest.main()
