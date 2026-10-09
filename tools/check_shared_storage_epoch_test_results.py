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

"""Verify all required leadership-epoch JUnit tests actually executed without skips."""

import sys
from pathlib import Path
from xml.etree import ElementTree as ET

REQUIRED_TESTS = {
    "org.apache.kafka.storage.internals.log.StoragePartitionRoleListenerEpochCompatibilityTest": (
        "epochAwareMethodPreservesTheLegacyFunctionalCallback",
        "defaultNoOpListenerAlsoAcceptsEpochAwareCallback",
    ),
    "org.apache.kafka.storage.internals.shared.kafka.LocalRetirementLeadershipFenceTest": (
        "unknownOrFollowerNeverIssuesALeaderTicket",
        "demotionImmediatelyInvalidatesCapturedTicket",
        "abaLeaderFollowerLeaderCannotResurrectOldTicket",
        "repeatedLeaderNotificationFencesOldLocalWork",
        "removalAndReassignmentCannotReusePreviousLocalGeneration",
        "recreatedTopicAndOtherPartitionAreIndependent",
        "sourceKafkaEpochIsCapturedByEpochAwareLeaderTicket",
        "unversionedLeaderCallbacksNeverClaimKafkaEpochAuthority",
        "staleLowerEpochCannotReclaimLocalLeadershipAfterDemotion",
        "lowerEpochDoesNotReplaceCurrentKnownLeader",
        "unversionedReelectionCannotErasePreviouslyKnownEpoch",
        "invalidExplicitKafkaEpochIsRejected",
        "ticketFromAnotherFenceInstanceNeverValidates",
        "equalEpochLeaderCallbackAfterFollowerCannotUndoDemotion",
        "sameEpochDuplicateLeaderCallbackOnlyRefreshesLocalGeneration",
    ),
    "org.apache.kafka.storage.internals.shared.kafka.SharedPartitionRoleListenerTest": (
        "routesOnlySelectedUserTopicsAndTracksLeaderDemotion",
        "removedReplicaClearsCommitWindowAndUploadOwnership",
        "removedClassicOrInternalReplicaDoesNotTouchSharedTracking",
        "defaultAllUserTopicRoutingStillNeverTracksInternalTopics",
        "roleCallbackInvalidatesRetirementTicketBeforeReelection",
        "partitionRemovalAndUnselectedTopicsCannotRetainRetirementTickets",
        "epochAwareRoleCallbackCarriesKafkaEpochAndFencesPriorGeneration",
        "legacyCallbackKeepsOldRoleBehaviorWithoutClaimingKafkaEpoch",
        "staleEpochNotificationCannotProduceEpochTicket",
        "epochCallbackIgnoresClassicAndInternalTopics",
        "equalEpochDelayedLeaderCallbackCannotOverrideFollowerRole",
    ),
    "org.apache.kafka.controller.PartitionRetirementControllerPrecheckTest": (
        "currentRecoveredLeaderInIsrMatchesButIsNotWriteAuthorization",
        "missingPartitionOrDeletedTopicIsNotAnAuthoritySource",
        "noActiveLeaderIsRejected",
        "previousLeaderEpochIsRejectedEvenWhenBrokerMatches",
        "futureClaimedEpochIsRejectedEvenWhenBrokerMatches",
        "formerBrokerCannotClaimTheNewLeaderEpoch",
        "uncleanElectionStillRecoveringDoesNotQualify",
        "leaderOutsideIsrDoesNotQualify",
        "leaderOutsideReplicaSetDoesNotQualify",
        "invalidBrokerAndEpochInputsFailFast",
        "controllerLookupRevalidatesCurrentLeaderAfterEpochTransition",
        "deletedTopicIdCannotBorrowRecreatedTopicRegistration",
        "invalidControllerLookupArgumentsFailClosed",
    ),
    "org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthorityModelTest": (
        "initialStateHasNeitherImplicitZeroNorAnAuthoritativeLeader",
        "higherEpochControllerElectionCreatesOnePartitionOwner",
        "duplicateLeaderAtSameEpochCannotMintANewControllerGeneration",
        "equalEpochAfterFollowerCannotReestablishLeadership",
        "newLeaderEpochOnAnotherBrokerPreservesMonotonicWatermark",
        "oldBrokerCannotReinitializeToClaimANewerEpoch",
        "staleExpectedAuthorityVersionCannotCommitOverNewerState",
        "nonIncreasingAuthorityOffsetIsNotAccepted",
        "firstWatermarkMustBeExplicitZeroForTheIncarnation",
        "regressionFailsAndEqualityDoesNotAdvanceState",
        "sourceLogStartUpperBoundIsStillOnlyCallerEvidence",
        "noLeaderCannotAdvanceAnAlreadyRecordedWatermark",
        "differentTopicIncarnationIsNotAuthorizedByAnExistingSnapshot",
        "compactionCannotMakeALowerProposalAdmissibleAgainstRetainedAuthority",
        "controllerOffsetsMaySkipUnrelatedLogEntriesButNeverGoBackward",
        "invalidArgumentsAndFabricatedInitialSnapshotFailFast",
        "deletingActiveTopicIrreversiblyFencesNewElections",
        "topicDeleteBeforeAnyElectionIsTerminal",
        "lateWatermarkAndNoLeaderCallbacksCannotModifyDeletion",
        "staleDeleteRequestCannotOverrideNewAuthorityVersion",
        "duplicateDeleteIsIdempotentButDoesNotMintNewVersion",
        "topicIdRecreationDoesNotInheritTerminalState",
        "terminalSnapshotRetainsWatermarkAcrossCheckpointRoundTrip",
        "terminalStateConstructorRejectsRevivedLeader",
        "initialAuthorityOffsetCannotCarryAnyClaimedGeneration",
        "unknownEpochAtCommittedOffsetRequiresATerminalTombstone",
        "terminalStateRetainsPriorWatermarkButNeverActiveBroker",
        "invalidNegativeDomainAndWatermarkValuesFailClosed",
    ),
    "org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementAuthoritySnapshotCodecTest": (
        "fixedLengthAndBigEndianHeaderAreStable",
        "explicitZeroRoundTripsWithoutBeingConfusedWithMissing",
        "missingWatermarkRoundTripsWithCanonicalZeroPayload",
        "positiveWatermarkSurvivesLeaderHandoverCheckpoint",
        "terminalAfterElectionRetainsHighestWatermarkAcrossReplay",
        "terminalBeforeFirstElectionRestoresWithoutForgingAnEpoch",
        "uncommittedInitialStateCannotBeEncodedAsDurableAuthority",
        "wrongTopicIncarnationFailsEvenWithAValidChecksum",
        "wrongPartitionNumberFailsEvenWithAValidChecksum",
        "callerCannotOmitTheProvenNonnegativeAuthorityHorizon",
        "olderSnapshotThanRequiredCommittedHorizonFailsClosed",
        "truncatedAndExtendedEnvelopesFailClosed",
        "mutatedPayloadWithOriginalChecksumCannotReplay",
        "unsupportedVersionFailsDespiteARecomputedChecksum",
        "mismatchedMagicFailsDespiteARecomputedChecksum",
        "unknownFlagBitsFailDespiteARecomputedChecksum",
        "noncanonicalAbsentWatermarkFailsDespiteARecomputedChecksum",
        "invalidNegativeWatermarkFailsDespiteARecomputedChecksum",
        "terminalEnvelopeCannotResurrectAnActiveBroker",
        "recoveredHigherWatermarkStillRejectsStaleProposal",
    ),
    "org.apache.kafka.storage.internals.shared.metadata.PartitionRetirementCompactionSafetyTest": (
        "liveReplayDetectsBackwardWriteButCompactedReplayCannotRecoverItsHistory",
        "compactionDoesNotSupplyAWriterEpochOrAnAuthenticCatchUpHorizon",
        "topicRecreationKeepsDistinctCompactedKeys",
    ),
    "org.apache.kafka.storage.internals.shared.kafka.PartitionRetirementEpochPrecheckTest": (
        "matchingSourceEpochOnlyProducesAnAdvisoryValueFinding",
        "legacyLeadershipNotificationIsNeverAnEpochProof",
        "observedNewEpochInvalidatesOldEpochCandidateEvenIfLocallyLeader",
        "staleTicketCannotBeRevivedAfterAba",
        "matchedEpochCannotOverrideMissingInitialZero",
        "matchedEpochCannotOverrideReplayHorizon",
        "matchedEpochCannotOverrideSourceLogStartUpperBound",
        "corruptedMetadataImageFailsClosedAfterLocalEpochMatch",
    ),
}


def verify(results_dir: Path, controller_results_dir: Path | None = None) -> int:
    if controller_results_dir is None:
        controller_results_dir = results_dir
    total = 0
    for suite, expected in REQUIRED_TESTS.items():
        suite_dir = controller_results_dir if suite.startswith("org.apache.kafka.controller.") else results_dir
        xml = suite_dir / ("TEST-" + suite + ".xml")
        if not xml.is_file():
            raise ValueError(f"Missing mandatory test report: {xml}")
        root = ET.parse(xml).getroot()
        if root.tag != "testsuite" or root.get("name") != suite:
            raise ValueError(f"Invalid JUnit suite identity: {xml}")
        cases = root.findall("testcase")
        if root.get("tests") != str(len(cases)):
            raise ValueError(f"JUnit test count mismatch: {suite}")
        for field in ("skipped", "failures", "errors"):
            if root.get(field) != "0":
                raise ValueError(f"Nonzero {field} for {suite}: {root.get(field)}")
        executed = set()
        for case in cases:
            method = (case.get("name") or "").removesuffix("()")
            if case.get("classname") != suite:
                raise ValueError(f"Wrong classname in {suite}: {case.get('classname')}")
            if any(case.find(tag) is not None for tag in ("skipped", "failure", "error")):
                raise ValueError(f"Non-success testcase {suite}.{method}")
            if method in executed:
                raise ValueError(f"Duplicate testcase {suite}.{method}")
            executed.add(method)
        missing = set(expected) - executed
        if missing:
            raise ValueError(f"Mandatory tests not executed in {suite}: {sorted(missing)}")
        total += len(expected)
    return total


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(
            "Usage: check_shared_storage_epoch_test_results.py STORAGE_REPORTS [CONTROLLER_REPORTS]",
            file=sys.stderr,
        )
        return 2
    try:
        total = verify(Path(sys.argv[1]), Path(sys.argv[2]) if len(sys.argv) == 3 else None)
    except (OSError, ValueError, ET.ParseError) as exc:
        print(f"SHARED_STORAGE_EPOCH_EVIDENCE FAIL: {exc}", file=sys.stderr)
        return 1
    print(f"SHARED_STORAGE_EPOCH_EVIDENCE PASS: {total} required methods, 0 skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
