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
from pathlib import Path

from check_shared_storage_ga_workflows import (
    foreign_evidence_workflow_triggers,
    event_path_patterns,
    path_is_triggered,
    full_manifest_after_source_wait_loop,
    missing_minio_evidence_requirements,
    redundant_runtime_staging_test_compiles,
    release_real_s3_default,
    workflow_job_blocks,
)


class WorkflowTriggerOwnershipTest(unittest.TestCase):
    def test_rejects_exact_foreign_workflow_trigger(self):
        text = """on:
  push:
    paths:
      - '.github/workflows/shared-storage-local-state-loss.yml'
      - '.github/workflows/shared-storage-upload-crash.yml'
"""
        evidence_paths = {
            ".github/workflows/shared-storage-local-state-loss.yml",
            ".github/workflows/shared-storage-upload-crash.yml",
        }
        self.assertEqual(
            [".github/workflows/shared-storage-upload-crash.yml"],
            foreign_evidence_workflow_triggers(
                text,
                ".github/workflows/shared-storage-local-state-loss.yml",
                evidence_paths,
                "push",
            ),
        )

    def test_accepts_own_workflow_trigger(self):
        text = """on:
  push:
    paths:
      - '.github/workflows/shared-storage-local-state-loss.yml'
      - 'storage/src/main/**'
"""
        evidence_paths = {
            ".github/workflows/shared-storage-local-state-loss.yml",
            ".github/workflows/shared-storage-upload-crash.yml",
        }
        self.assertEqual(
            [],
            foreign_evidence_workflow_triggers(
                text,
                ".github/workflows/shared-storage-local-state-loss.yml",
                evidence_paths,
                "push",
            ),
        )

    def test_rejects_foreign_workflow_wildcard(self):
        text = """on:
  pull_request:
    paths:
      - '.github/workflows/shared-storage*.yml'
"""
        evidence_paths = {
            ".github/workflows/shared-storage-local-state-loss.yml",
            ".github/workflows/shared-storage-upload-crash.yml",
        }
        self.assertEqual(
            [".github/workflows/shared-storage-upload-crash.yml"],
            foreign_evidence_workflow_triggers(
                text,
                ".github/workflows/shared-storage-local-state-loss.yml",
                evidence_paths,
                "pull_request",
            ),
        )


class LocalStateLossCompileSourceOwnershipTest(unittest.TestCase):
    def test_quarantine_test_compilation_is_in_the_ga_recovery_push_contract(self):
        root = Path(__file__).resolve().parents[1]
        path = root / ".github/workflows/shared-storage-local-state-loss.yml"
        workflow = path.read_text(encoding="utf-8")
        sources = (
            "storage/src/test/java/org/apache/kafka/storage/internals/shared/kafka/SharedCommitProgressTest.java",
            "storage/src/test/java/org/apache/kafka/storage/internals/shared/kafka/SharedUploadSchedulerTest.java",
        )
        for event in ("push", "pull_request"):
            patterns = event_path_patterns(workflow, event)
            for source in sources:
                with self.subTest(event=event, source=source):
                    self.assertTrue(
                        path_is_triggered(source, patterns),
                        f"{path}: {event}.paths must include compiled quarantine source {source}",
                    )


class RuntimeStagingOwnershipTest(unittest.TestCase):
    def test_rejects_test_compile_in_runtime_staging_workflow(self):
        text = """./gradlew \\
  :storage:shared-storage-s3:stageProcessRuntime \\
  :core:compileTestJava \\
  --no-scan
"""
        self.assertEqual(
            [":core:compileTestJava"],
            redundant_runtime_staging_test_compiles(text),
        )

    def test_ignores_test_compile_without_runtime_staging(self):
        self.assertEqual(
            [],
            redundant_runtime_staging_test_compiles(
                "./gradlew :core:compileTestJava --no-scan"
            ),
        )


class ManifestWaitLoopTest(unittest.TestCase):
    def test_requires_full_manifest_after_wait_loop(self):
        good = """while true; do
          python3 tools/shared_storage_ga_source_runs.py
          sleep 15
          done

          python3 tools/shared_storage_ga_manifest.py
"""
        bad = """while true; do
          python3 tools/shared_storage_ga_source_runs.py
          python3 tools/shared_storage_ga_manifest.py
          sleep 15
          done
"""
        self.assertTrue(
            full_manifest_after_source_wait_loop(
                good,
                "python3 tools/shared_storage_ga_source_runs.py",
                "python3 tools/shared_storage_ga_manifest.py",
            )
        )
        self.assertFalse(
            full_manifest_after_source_wait_loop(
                bad,
                "python3 tools/shared_storage_ga_source_runs.py",
                "python3 tools/shared_storage_ga_manifest.py",
            )
        )


class ReleaseScopeDefaultTest(unittest.TestCase):
    def test_minio_ga_is_default(self):
        workflow = """on:
  workflow_dispatch:
    inputs:
      release_ref:
        default: main
      require_real_s3:
        description: Optional AWS proof
        required: true
        default: false
        type: boolean
"""
        self.assertEqual("false", release_real_s3_default(workflow))

    def test_explicit_aws_opt_in_and_missing_scope(self):
        workflow = """on:
  workflow_dispatch:
    inputs:
      require_real_s3:
        required: true
        default: true
        type: boolean
"""
        self.assertEqual("true", release_real_s3_default(workflow))
        self.assertIsNone(release_real_s3_default("jobs:\n  validate:\n    runs-on: ubuntu-latest\n"))


class MinIOEvidenceOwnershipTest(unittest.TestCase):
    def test_rejects_missing_multipart_and_guard(self):
        workflow = """jobs:
  minio-environment:
    steps:
      - run: |
          ./gradlew :storage:shared-storage-s3:test
          --tests 'org.apache.kafka.storage.internals.shared.s3.S3ObjectStoreTest'
"""
        missing = missing_minio_evidence_requirements(workflow)
        self.assertTrue(any("S3MultipartObjectStoreTest" in item for item in missing))
        self.assertTrue(any("S3OrphanObjectCleanerIntegrationTest" in item for item in missing))
        self.assertTrue(any("check_shared_storage_minio_test_results.py" in item for item in missing))

    def test_accepts_complete_minio_contract(self):
        workflow = """jobs:
  minio-environment:
    steps:
      - uses: ./.github/actions/setup-minio
      - run: |
          SHARED_STORAGE_S3_ENDPOINT: http://127.0.0.1:9000
          --tests 'org.apache.kafka.storage.internals.shared.s3.S3ObjectStoreTest'
          --tests 'org.apache.kafka.storage.internals.shared.s3.S3MultipartObjectStoreTest'
          --tests 'org.apache.kafka.storage.internals.shared.s3.S3OrphanObjectCleanerIntegrationTest'
          python3 tools/check_shared_storage_minio_test_results.py storage/shared-storage-s3/build/test-results/test
"""
        self.assertEqual([], missing_minio_evidence_requirements(workflow))


class WorkflowJobBlockTest(unittest.TestCase):
    def test_splits_top_level_jobs_without_absorbing_next_job(self):
        text = """jobs:
  storage-tests:
    runs-on: ubuntu-latest
    steps:
      - run: echo storage
  minio-environment:
    runs-on: ubuntu-latest
    steps:
      - run: echo minio
"""
        blocks = workflow_job_blocks(text)
        self.assertEqual({"storage-tests", "minio-environment"}, set(blocks))
        self.assertIn("echo storage", blocks["storage-tests"])
        self.assertNotIn("echo minio", blocks["storage-tests"])
        self.assertIn("echo minio", blocks["minio-environment"])


if __name__ == "__main__":
    unittest.main()
