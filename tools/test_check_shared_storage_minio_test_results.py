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

import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree

from check_shared_storage_minio_test_results import PACKAGE, REQUIRED_CASES, validate_reports


class MinIOJUnitEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def report(self, omit=(), skipped=(), failed=(), duplicate=()):
        suite = ElementTree.Element("testsuite")
        for short_class, methods in REQUIRED_CASES.items():
            for method in sorted(methods):
                name = short_class + "." + method
                if name in omit:
                    continue
                for _ in range(2 if name in duplicate else 1):
                    case = ElementTree.SubElement(
                        suite,
                        "testcase",
                        classname=PACKAGE + "." + short_class,
                        name=method + "()",
                    )
                    if name in skipped:
                        ElementTree.SubElement(case, "skipped")
                    if name in failed:
                        ElementTree.SubElement(case, "failure")
        ElementTree.ElementTree(suite).write(self.root / "TEST-fixture.xml")

    def test_accepts_complete_executed_minio_evidence(self):
        self.report()
        self.assertEqual([], validate_reports(self.root))

    def test_rejects_missing_reports(self):
        self.assertTrue(any("missing" in error for error in validate_reports(self.root)))

    def test_rejects_missing_required_case(self):
        self.report(omit={"S3MultipartObjectStoreTest.shouldAbortKnownSizeMultipartAndCloseSourceWhenSourceEndsEarly"})
        self.assertTrue(any("MISSING" in error for error in validate_reports(self.root)))

    def test_rejects_missing_orphan_cleanup_case(self):
        self.report(omit={"S3OrphanObjectCleanerIntegrationTest.claimedOrphanIsDeletedAgainAfterLatePut"})
        self.assertTrue(any("MISSING" in error for error in validate_reports(self.root)))

    def test_rejects_skipped_orphan_cleanup_case(self):
        self.report(skipped={"S3OrphanObjectCleanerIntegrationTest.activePreparedUploadIsProtectedUntilUploadEnds"})
        self.assertTrue(any("SKIPPED" in error for error in validate_reports(self.root)))

    def test_rejects_skipped_case(self):
        self.report(skipped={"S3ObjectStoreTest.roundTripsPutRangeReadAndDeleteAgainstConfiguredS3"})
        self.assertTrue(any("SKIPPED" in error for error in validate_reports(self.root)))

    def test_rejects_failed_case(self):
        self.report(failed={"S3MultipartObjectStoreTest.shouldPullKnownSizeMultipartSourceInOrderAndPublishOneObject"})
        self.assertTrue(any("FAILED" in error for error in validate_reports(self.root)))

    def test_rejects_duplicate_case(self):
        self.report(duplicate={"S3ObjectStoreTest.roundTripsPutRangeReadAndDeleteAgainstConfiguredS3"})
        self.assertTrue(any("executed 2 times" in error for error in validate_reports(self.root)))

    def test_rejects_invalid_xml(self):
        (self.root / "TEST-invalid.xml").write_text("<testsuite><testcase>")
        self.assertTrue(any("cannot read JUnit XML" in error for error in validate_reports(self.root)))


if __name__ == "__main__":
    unittest.main()
