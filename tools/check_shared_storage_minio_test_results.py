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
HORIZON_CASES = {
    "KafkaObjectMetadataStoreTest": {
        "capturedReadCommittedLsoWaitsForAppliedConsumerPositionAcrossCompactionGaps",
        "emptyReadCommittedMetadataTopicCanCaptureZeroWithoutRecords",
        "replayHorizonPreservesCapturedLsoSeparatelyFromLaterConsumerProgress",
        "malformedOrUncapturedReadCommittedHorizonFailsClosed",
        "failedReadCommittedHorizonNeverReopensAfterLateConsumerProgress",
        "pendingHorizonMailboxRejectsExcessRequestsWithoutBlockingTheConsumer",
        "closingReplayHorizonMailboxFailsAndReleasesEveryOutstandingRequest",
        "oneReplayHorizonProbeFailureDoesNotFailOtherPendingProbes",
        "replayHorizonMailboxNeverAdmitsAnAlreadyFailedOrCanceledFuture",
        "replayHorizonMailboxDoesNotConsumeTwoPermitsForTheSameRequest",
        "waitingReplayHorizonExpiresOnMonotonicDeadlineAndReleasesCapacity",
        "replayHorizonDeadlineIsWrapSafeAndNeverRevokesCompletedEvidence",
        "kafkaMetadataReplayProbeTimeoutOnlyFailsThatFuture",
        "fatalMetadataConsumerErrorsCannotBeIsolatedAsProbeTimeouts",
        "kafkaConsumerWakeupRemainsAnOuterLifecycleSignal",
    },
}

REQUIRED_CASES = {
    "S3ObjectStoreTest": {"roundTripsPutRangeReadAndDeleteAgainstConfiguredS3"},
    "S3MultipartObjectStoreTest": {
        "shouldPullKnownSizeMultipartSourceInOrderAndPublishOneObject",
        "shouldAbortKnownSizeMultipartAndCloseSourceWhenSourceEndsEarly",
    },
    "S3OrphanObjectCleanerIntegrationTest": {
        "claimedOrphanIsDeletedAgainAfterLatePut",
        "committedObjectRemainsPhysicallyReadableAfterCleanup",
        "activePreparedUploadIsProtectedUntilUploadEnds",
        "redundantCommittedPhysicalCopyIsReclaimedWithoutDeletingReadWinner",
        "multiPartitionPhysicalObjectSurvivesWhenAnyRangeIsStillReferenced",
    },
}


def validate_reports(report_dir, required_cases=REQUIRED_CASES, label="MinIO"):
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
            if method not in required_cases.get(short_class, set()):
                continue

            name = short_class + "." + method
            observed[name] = observed.get(name, 0) + 1
            if test.find("skipped") is not None:
                errors.append("required " + label + " test was SKIPPED: " + name)
            if test.find("failure") is not None or test.find("error") is not None:
                errors.append("required " + label + " test FAILED: " + name)

    for short_class, methods in required_cases.items():
        for method in sorted(methods):
            name = short_class + "." + method
            count = observed.get(name, 0)
            if count == 0:
                errors.append("required " + label + " test was MISSING: " + name)
            elif count != 1:
                errors.append("required " + label + " test executed " + str(count) + " times: " + name)
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report_dir", type=Path)
    parser.add_argument(
        "--metadata-horizon", action="store_true",
        help="Require the non-emitting read-committed replay horizon regression methods",
    )
    args = parser.parse_args()
    label = "metadata replay" if args.metadata_horizon else "MinIO"
    cases = HORIZON_CASES if args.metadata_horizon else REQUIRED_CASES
    errors = validate_reports(args.report_dir, cases, label)
    if errors:
        print(label + " JUnit evidence: FAIL", file=sys.stderr)
        for error in errors:
            print("- " + error, file=sys.stderr)
        return 1
    case_count = sum(len(methods) for methods in cases.values())
    print(f"{label} JUnit evidence: PASS ({case_count} required S3 cases executed, 0 skipped)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
