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

"""Fast pre-Gradle source checks for mandatory Shared Storage regression cases.

This does not replace compilation, JUnit or the JUnit XML anti-skip gate.
It catches missing mandatory test methods and unimported JUnit assertion
calls before expensive Java 25 and MinIO setup.
"""

import re
import sys
from pathlib import Path

# Running python3 tools/check_shared_storage_epoch_test_sources.py sets
# sys.path[0] to tools/, not the repository root. Support that invocation
# and python3 -m tools.check_shared_storage_epoch_test_sources equally.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tools.check_shared_storage_epoch_test_results import REQUIRED_TESTS

ASSERTION_NAMES = (
    "assertTrue", "assertFalse", "assertEquals", "assertNotEquals",
    "assertThrows", "assertDoesNotThrow", "assertNull", "assertNotNull",
    "assertArrayEquals", "assertIterableEquals", "assertSame", "assertNotSame",
    "assertAll", "assertInstanceOf", "assertTimeout", "assertTimeoutPreemptively",
    "assertLinesMatch", "fail",
)
STATIC_IMPORT_RE = re.compile(
    r"(?m)^\s*import\s+static\s+org\.junit\.jupiter\.api\.Assertions\.(\w+|\*)\s*;"
)


def java_test_source_path(root: Path, suite: str) -> Path:
    if suite.startswith("org.apache.kafka.storage."):
        module = "storage"
    elif suite.startswith("org.apache.kafka.server.common."):
        module = "server-common"
    elif suite.startswith((
        "org.apache.kafka.controller.",
        "org.apache.kafka.metadata.",
        "org.apache.kafka.image.",
    )):
        module = "metadata"
    else:
        raise ValueError(f"Unknown mandatory test module: {suite}")
    return root / module / "src/test/java" / (suite.replace(".", "/") + ".java")


def verify_sources(root: Path, required_tests=None) -> int:
    if required_tests is None:
        required_tests = REQUIRED_TESTS
    total = 0
    for suite, methods in required_tests.items():
        path = java_test_source_path(root, suite)
        if not path.is_file():
            raise ValueError(f"Missing mandatory Java test source: {path}")
        source = path.read_text(encoding="utf-8")
        imported = set(STATIC_IMPORT_RE.findall(source))
        for method in methods:
            pattern = re.compile(r"\bvoid\s+" + re.escape(method) + r"\s*\(")
            if not pattern.search(source):
                raise ValueError(f"Missing mandatory Java test method: {suite}#{method}")
            total += 1

        if "*" in imported:
            continue
        for assertion in ASSERTION_NAMES:
            # Qualified Assertions.assertX(...) calls do not require a
            # static import, but an unqualified assertX(...) invocation does.
            invoked = re.search(r"(?<![\w.])" + assertion + r"\s*\(", source)
            if invoked and assertion not in imported:
                raise ValueError(
                    f"Missing JUnit assertion static import: {suite} uses "
                    f"{assertion}() without importing org.junit.jupiter.api.Assertions.{assertion}"
                )
    return total


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("Usage: check_shared_storage_epoch_test_sources.py REPO_ROOT")
    try:
        checked = verify_sources(Path(sys.argv[1]))
        print(f"PASS: {checked} mandatory Java test methods and JUnit assertion imports verified")
    except (ValueError, OSError) as error:
        raise SystemExit(str(error)) from error
