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

"""Regression tests for the fail-closed Java 25 shared-storage epoch JUnit gate."""

import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

from tools.check_shared_storage_epoch_test_results import REQUIRED_TESTS, verify


class EpochEvidenceCheckerTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        for suite, methods in REQUIRED_TESTS.items():
            self.write_suite(suite, methods)

    def write_suite(self, suite, methods, *, skip=False, fail=False):
        xml = ET.Element("testsuite", name=suite, tests=str(len(methods)),
                         skipped=str(int(skip)), failures=str(int(fail)), errors="0")
        for index, method in enumerate(methods):
            case = ET.SubElement(xml, "testcase", classname=suite, name=method + "()")
            if index == 0 and skip:
                ET.SubElement(case, "skipped")
            if index == 0 and fail:
                ET.SubElement(case, "failure")
        ET.ElementTree(xml).write(self.root / ("TEST-" + suite + ".xml"), encoding="utf-8")

    def test_all_106_methods_pass(self):
        self.assertEqual(106, verify(self.root))

    def test_missing_controller_module_report_fails_closed(self):
        controller = "org.apache.kafka.controller.PartitionRetirementControllerPrecheckTest"
        (self.root / ("TEST-" + controller + ".xml")).unlink()
        with self.assertRaisesRegex(ValueError, "Missing mandatory test report"):
            verify(self.root)

    def test_distinct_storage_and_controller_report_directories(self):
        controller = "org.apache.kafka.controller.PartitionRetirementControllerPrecheckTest"
        xml_name = "TEST-" + controller + ".xml"
        with tempfile.TemporaryDirectory() as controller_folder:
            other = Path(controller_folder)
            (other / xml_name).write_bytes((self.root / xml_name).read_bytes())
            (self.root / xml_name).unlink()
            self.assertEqual(106, verify(self.root, other))

    def test_missing_suite_fails(self):
        suite = next(iter(REQUIRED_TESTS))
        (self.root / ("TEST-" + suite + ".xml")).unlink()
        with self.assertRaisesRegex(ValueError, "Missing mandatory test report"):
            verify(self.root)

    def test_missing_case_fails(self):
        suite = next(iter(REQUIRED_TESTS))
        self.write_suite(suite, REQUIRED_TESTS[suite][:-1])
        with self.assertRaisesRegex(ValueError, "Mandatory tests not executed"):
            verify(self.root)

    def test_skipped_case_fails(self):
        suite = next(iter(REQUIRED_TESTS))
        self.write_suite(suite, REQUIRED_TESTS[suite], skip=True)
        with self.assertRaisesRegex(ValueError, "Nonzero skipped"):
            verify(self.root)

    def test_failed_case_fails(self):
        suite = next(iter(REQUIRED_TESTS))
        self.write_suite(suite, REQUIRED_TESTS[suite], fail=True)
        with self.assertRaisesRegex(ValueError, "Nonzero failures"):
            verify(self.root)

    def test_wrong_class_fails(self):
        suite = next(iter(REQUIRED_TESTS))
        xml_path = self.root / ("TEST-" + suite + ".xml")
        tree = ET.parse(xml_path)
        tree.getroot().find("testcase").set("classname", "other.Class")
        tree.write(xml_path, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Wrong classname"):
            verify(self.root)

    def test_duplicate_case_fails(self):
        suite = next(iter(REQUIRED_TESTS))
        xml_path = self.root / ("TEST-" + suite + ".xml")
        tree = ET.parse(xml_path)
        root = tree.getroot()
        first = root.find("testcase")
        ET.SubElement(root, "testcase", classname=suite, name=first.get("name"))
        root.set("tests", str(int(root.get("tests")) + 1))
        tree.write(xml_path, encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Duplicate testcase"):
            verify(self.root)


if __name__ == "__main__":
    unittest.main()
