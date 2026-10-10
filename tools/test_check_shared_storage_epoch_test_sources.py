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

"""Unit checks for Java regression source preflight; do not execute Java tests."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from tools.check_shared_storage_epoch_test_sources import java_test_source_path, verify_sources


class EpochTestSourcePreflightTest(unittest.TestCase):
    SUITE = "org.apache.kafka.storage.internals.shared.kafka.SharedCommitProgressTest"
    REQUIRED = {SUITE: ("quarantineClearsPriorCommitWindowsAndAllUploadEligibility",)}

    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.source = java_test_source_path(self.root, self.SUITE)
        self.source.parent.mkdir(parents=True)
        self.write(
            "import static org.junit.jupiter.api.Assertions.assertTrue;\n"
            "class SharedCommitProgressTest {\n"
            "  @Test void quarantineClearsPriorCommitWindowsAndAllUploadEligibility() {\n"
            "    assertTrue(true);\n"
            "  }\n"
            "}\n"
        )

    def write(self, text):
        self.source.write_text(text, encoding="utf-8")

    def test_correct_source_and_import_pass(self):
        self.assertEqual(1, verify_sources(self.root, self.REQUIRED))

    def test_missing_assert_true_import_fails_before_gradle(self):
        self.write(self.source.read_text().replace(
            "import static org.junit.jupiter.api.Assertions.assertTrue;\n", ""
        ))
        with self.assertRaisesRegex(ValueError, "Missing JUnit assertion static import"):
            verify_sources(self.root, self.REQUIRED)

    def test_unrelated_framework_static_assertion_import_is_not_accepted(self):
        self.write(self.source.read_text().replace(
            "import static org.junit.jupiter.api.Assertions.assertTrue;",
            "import static org.testng.Assert.assertTrue;"
        ))
        with self.assertRaisesRegex(ValueError, "Missing JUnit assertion static import"):
            verify_sources(self.root, self.REQUIRED)

    def test_junit_wildcard_static_import_is_accepted(self):
        self.write(self.source.read_text().replace(
            "import static org.junit.jupiter.api.Assertions.assertTrue;",
            "import static org.junit.jupiter.api.Assertions.*;"
        ))
        self.assertEqual(1, verify_sources(self.root, self.REQUIRED))

    def test_qualified_junit_assertion_does_not_need_static_import(self):
        self.write(self.source.read_text()
                   .replace("import static org.junit.jupiter.api.Assertions.assertTrue;\n", "")
                   .replace("    assertTrue(true);",
                            "    org.junit.jupiter.api.Assertions.assertTrue(true);"))
        self.assertEqual(1, verify_sources(self.root, self.REQUIRED))

    def test_scala_core_source_method_is_required(self):
        suite = "kafka.server.BrokerLifecycleManagerTest"
        path = java_test_source_path(self.root, suite)
        path.parent.mkdir(parents=True)
        path.write_text(
            "class BrokerLifecycleManagerTest {\n"
            "  @Test def testIncarnationChangesAcrossBrokerProcessRestart(): Unit = {}\n"
            "}\n", encoding="utf-8"
        )
        selected = {suite: ("testIncarnationChangesAcrossBrokerProcessRestart",)}
        self.assertEqual(1, verify_sources(self.root, selected))
        path.write_text(path.read_text().replace(
            "testIncarnationChangesAcrossBrokerProcessRestart", "renamed"
        ))
        with self.assertRaisesRegex(ValueError, "Missing mandatory test method"):
            verify_sources(self.root, selected)

    def test_missing_mandatory_java_test_method_is_rejected(self):
        self.write(self.source.read_text().replace(
            "quarantineClearsPriorCommitWindowsAndAllUploadEligibility",
            "renamedAndNoLongerCovered"
        ))
        with self.assertRaisesRegex(ValueError, "Missing mandatory test method"):
            verify_sources(self.root, self.REQUIRED)

    def test_missing_mandatory_java_file_fails_closed(self):
        self.source.unlink()
        with self.assertRaisesRegex(ValueError, "Missing mandatory Java test source"):
            verify_sources(self.root, self.REQUIRED)

    def test_direct_script_invocation_from_another_directory_resolves_tools_package(self):
        checker = Path(__file__).with_name("check_shared_storage_epoch_test_sources.py")
        process = subprocess.run(
            [sys.executable, str(checker.resolve()), str(self.root)],
            cwd=self.root,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(0, process.returncode)
        self.assertIn("Missing mandatory Java test source", process.stderr)
        self.assertNotIn("ModuleNotFoundError", process.stderr)


if __name__ == "__main__":
    unittest.main()
