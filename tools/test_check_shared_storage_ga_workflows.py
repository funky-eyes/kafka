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

from shared_storage_ga_manifest import EVIDENCE_WORKFLOW_PATHS, PRODUCTION_PATHS, REAL_S3_REQUIRED, is_production_path

from check_shared_storage_ga_workflows import (
    foreign_evidence_workflow_triggers,
    event_path_patterns,
    concrete_production_paths,
    path_is_triggered,
    full_manifest_after_source_wait_loop,
    missing_minio_evidence_requirements,
    redundant_runtime_staging_test_compiles,
    release_real_s3_default,
    workflow_job_blocks,
)


class BrokerSourceWalNonEmissionContractTest(unittest.TestCase):
    def test_production_broker_heartbeat_does_not_emit_unstable_gc_reports(self):
        # The broker's normal heartbeat must remain a stable request until
        # the transport is independently bound to an authenticated per-Broker
        # principal. This is a negative-path guard, not authentication.
        root = Path(__file__).resolve().parents[1]
        source = (root / "server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java"
                  ).read_text(encoding="utf-8")
        self.assertNotIn("BrokerHeartbeatSourceWalReportDispatch.prepareOnce(", source)
        self.assertNotIn(".setSourceWalProbeReport(", source)
        self.assertIn(
            "channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data), handler);",
            source,
        )


class WorkflowTriggerOwnershipTest(unittest.TestCase):
    def test_heartbeat_network_context_preserves_transport_and_forwarding(self):
        root = Path(__file__).resolve().parents[1]
        source = (root / "core/src/main/scala/kafka/server/ControllerApis.scala"
                  ).read_text(encoding="utf-8")
        block = source.split("def handleBrokerHeartBeatRequest(", 1)[1].split(
            "def handleUnregisterBroker(", 1
        )[0]
        self.assertIn("new ControllerRequestContext(request.context,", block)
        self.assertIn("request.isForwarded", block)
        self.assertNotIn(
            "new ControllerRequestContext(request.context.header.data", block
        )

    def test_kraft_gc_controller_changes_trigger_every_ga_workflow(self):
        protected = {
            "metadata/src/main/java/org/apache/kafka/controller/QuorumController.java",
            "metadata/src/main/java/org/apache/kafka/controller/ControllerRequestContext.java",
            "core/src/main/scala/kafka/server/ControllerApis.scala",
            "metadata/src/main/java/org/apache/kafka/controller/PartitionRetirementSourceProbeRegistry.java",
            "metadata/src/main/java/org/apache/kafka/controller/PartitionRetirementHeartbeatReportGate.java",
            "metadata/src/main/java/org/apache/kafka/metadata/BrokerHeartbeatReply.java",
            "metadata/src/main/java/org/apache/kafka/metadata/PartitionRetirementAuthorityState.java",
            "metadata/src/main/java/org/apache/kafka/image/PartitionRetirementAuthorityImage.java",
        }
        root = Path(__file__).resolve().parents[1]
        for path in protected:
            self.assertTrue(is_production_path(path), path)
        for name, filename in sorted(EVIDENCE_WORKFLOW_PATHS.items()):
            if name == REAL_S3_REQUIRED:
                continue
            workflow = (root / filename).read_text(encoding="utf-8")
            events = ["push"]
            if "\n  pull_request:\n" in workflow:
                events.append("pull_request")
            for event in events:
                patterns = event_path_patterns(workflow, event)
                for source in sorted(protected):
                    with self.subTest(gate=name, event=event, source=source):
                        self.assertTrue(
                            path_is_triggered(source, patterns),
                            f"{filename}: {event}.paths must cover {source}",
                        )

    def test_gc_protocol_changes_trigger_every_ga_evidence_workflow(self):
        # Exactly these five inputs were added to the release fingerprint
        # for the Source/WAL diagnostics. Every eligible source-SHA gate
        # must rerun when any of their blobs changes.
        protected = {
            "server/src/main/java/org/apache/kafka/server/BrokerHeartbeatSourceWalReportDispatch.java",
            "clients/src/main/java/org/apache/kafka/common/replica/PartitionRetirementSourceProbe.java",
            "clients/src/main/java/org/apache/kafka/common/requests/BrokerHeartbeatRequest.java",
            "clients/src/main/resources/common/message/BrokerHeartbeatRequest.json",
            "clients/src/main/resources/common/message/BrokerHeartbeatResponse.json",
        }
        self.assertTrue(protected.issubset(PRODUCTION_PATHS))
        root = Path(__file__).resolve().parents[1]
        for name, filename in sorted(EVIDENCE_WORKFLOW_PATHS.items()):
            if name == REAL_S3_REQUIRED:
                # The special Real S3 workflow is manually/branch dispatched.
                continue
            workflow = (root / filename).read_text(encoding="utf-8")
            events = ["push"]
            if "\n  pull_request:\n" in workflow:
                events.append("pull_request")
            for event in events:
                patterns = event_path_patterns(workflow, event)
                for source in sorted(protected):
                    with self.subTest(gate=name, event=event, source=source):
                        self.assertTrue(
                            path_is_triggered(source, patterns),
                            f"{filename}: {event}.paths must cover {source}",
                        )

    def test_explicit_client_protocol_paths_require_ga_fingerprint_coverage(self):
        workflow = """on:
  push:
    paths:
      - 'clients/src/main/java/org/apache/kafka/common/replica/PartitionRetirementSourceProbe.java'
      - 'clients/src/main/java/org/apache/kafka/common/requests/BrokerHeartbeatRequest.java'
      - 'clients/src/main/resources/common/message/BrokerHeartbeatRequest.json'
      - 'clients/src/main/resources/common/message/BrokerHeartbeatResponse.json'
      - 'clients/src/test/java/org/apache/kafka/common/requests/HeartbeatRequestTest.java'
"""
        self.assertEqual(
            {
                "clients/src/main/java/org/apache/kafka/common/replica/PartitionRetirementSourceProbe.java",
                "clients/src/main/java/org/apache/kafka/common/requests/BrokerHeartbeatRequest.java",
                "clients/src/main/resources/common/message/BrokerHeartbeatRequest.json",
                "clients/src/main/resources/common/message/BrokerHeartbeatResponse.json",
            },
            concrete_production_paths(workflow),
        )

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
