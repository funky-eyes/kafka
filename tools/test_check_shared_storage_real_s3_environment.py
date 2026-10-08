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

import json
import unittest
from types import SimpleNamespace

from check_shared_storage_real_s3_environment import (
    EnvironmentMetadataError,
    gh_json,
    inspect_environment,
    missing_configuration,
)


class EnvironmentMetadataTest(unittest.TestCase):
    def test_accepts_nonempty_bucket_and_role_secret_name(self):
        self.assertEqual(
            [],
            missing_configuration(
                [{"name": "SHARED_STORAGE_AWS_S3_BUCKET", "value": "release-fixture"}],
                [{"name": "SHARED_STORAGE_AWS_ROLE_ARN"}],
            ),
        )

    def test_reports_all_missing_settings(self):
        self.assertEqual(
            [
                "Environment variable SHARED_STORAGE_AWS_S3_BUCKET is missing or blank",
                "Environment secret SHARED_STORAGE_AWS_ROLE_ARN is missing",
            ],
            missing_configuration(
                [{"name": "SHARED_STORAGE_AWS_S3_BUCKET", "value": "  "}],
                [],
            ),
        )

    def test_empty_or_malformed_metadata_cannot_report_ready(self):
        with self.assertRaises(EnvironmentMetadataError):
            missing_configuration({}, [])
        with self.assertRaises(EnvironmentMetadataError):
            missing_configuration([None], [])
        with self.assertRaises(EnvironmentMetadataError):
            missing_configuration([], [None])

    def test_uses_environment_scoped_read_only_gh_calls(self):
        calls = []

        def fake_runner(command, **kwargs):
            calls.append((command, kwargs))
            if command[1] == "variable":
                value = [{"name": "SHARED_STORAGE_AWS_S3_BUCKET", "value": "fixture"}]
            else:
                value = [{"name": "SHARED_STORAGE_AWS_ROLE_ARN"}]
            return SimpleNamespace(returncode=0, stdout=json.dumps(value), stderr="")

        self.assertEqual([], inspect_environment("funky-eyes/kafka", "shared-storage-aws-s3", fake_runner))
        self.assertEqual(["variable", "secret"], [call[0][1] for call in calls])
        for command, options in calls:
            self.assertIn("--env", command)
            self.assertIn("shared-storage-aws-s3", command)
            self.assertIn("--repo", command)
            self.assertIn("funky-eyes/kafka", command)
            self.assertEqual({"capture_output": True, "text": True, "check": False}, options)

    def test_fails_closed_on_gh_error_or_invalid_json(self):
        def failed(*args, **kwargs):
            return SimpleNamespace(returncode=1, stdout="", stderr="do not expose")

        def malformed(*args, **kwargs):
            return SimpleNamespace(returncode=0, stdout="not-json", stderr="")

        with self.assertRaises(EnvironmentMetadataError):
            gh_json(["secret", "list"], failed)
        with self.assertRaises(EnvironmentMetadataError):
            gh_json(["secret", "list"], malformed)


if __name__ == "__main__":
    unittest.main()
