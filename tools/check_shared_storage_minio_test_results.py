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

"""Fail closed if a required MinIO-backed S3 JUnit test did not actually run."""

import argparse
import sys
from pathlib import Path
from xml.etree import ElementTree

PACKAGE = "org.apache.kafka.storage.internals.shared.s3"
REQUIRED_CASES = {
    "S3ObjectStoreTest": {"roundTripsPutRangeReadAndDeleteAgainstConfiguredS3"},
    "S3MultipartObjectStoreTest": {
        "shouldPullKnownSizeMultipartSourceInOrderAndPublishOneObject",
        "shouldAbortKnownSizeMultipartAndCloseSourceWhenSourceEndsEarly",
    },
}


def validate_reports(report_dir):
    report_dir = Path(report_dir)
    files = sorted(report_dir.glob("TEST-*.xml"))
    if not files:
        return ["missing Gradle JUnit XML reports in " + str(report_dir)]

    observed = {}
    errors = []
    for path in files:
        try:
            root = ElementTree.parse(path).getroot()
        except (ElementTree.ParseError, OSError) as exc:
            errors.append("cannot read JUnit XML " + str(path) + ": " + str(exc))
            continue
        for test in root.iter("testcase"):
            classname = test.get("classname", "")
            short_class = classname.rsplit(".", 1)[-1]
            method = test.get("name", "").removesuffix("()")
            if classname != PACKAGE + "." + short_class:
                continue
            if method not in REQUIRED_CASES.get(short_class, set()):
                continue

            name = short_class + "." + method
            observed[name] = observed.get(name, 0) + 1
            if test.find("skipped") is not None:
                errors.append("required MinIO test was SKIPPED: " + name)
            if test.find("failure") is not None or test.find("error") is not None:
                errors.append("required MinIO test FAILED: " + name)

    for short_class, methods in REQUIRED_CASES.items():
        for method in sorted(methods):
            name = short_class + "." + method
            count = observed.get(name, 0)
            if count == 0:
                errors.append("required MinIO test was MISSING: " + name)
            elif count != 1:
                errors.append("required MinIO test executed " + str(count) + " times: " + name)
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_dir", type=Path)
    args = parser.parse_args()
    errors = validate_reports(args.report_dir)
    if errors:
        print("MinIO JUnit evidence: FAIL", file=sys.stderr)
        for error in errors:
            print("- " + error, file=sys.stderr)
        return 1
    print("MinIO JUnit evidence: PASS (3 required S3 object/multipart cases executed, 0 skipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
