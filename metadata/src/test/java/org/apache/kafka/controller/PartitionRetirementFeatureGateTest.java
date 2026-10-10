/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.PartitionRetirementAuthorityVersion;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementFeatureGateTest {
    @Test
    void productionControllerCannotReplayExperimentalFeatureLevel() {
        FeatureControlManager manager = manager(false);
        manager.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_3_IV0.featureLevel()));
        assertFalse(manager.isPartitionRetirementAuthorityEnabled());
        assertThrows(RuntimeException.class, () ->
            manager.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 1))
        );
    }

    @Test
    void negotiatedFeatureStillRequiresMetadataVersionFourFour() {
        FeatureControlManager manager = manager(true);
        manager.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_3_IV0.featureLevel()));
        manager.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 1));
        assertFalse(manager.isPartitionRetirementAuthorityEnabled());
        manager.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV0.featureLevel()));
        assertTrue(manager.isPartitionRetirementAuthorityEnabled());
    }

    @Test
    void finalizedFeatureDisableRevokesSnapshotEligibility() {
        FeatureControlManager manager = manager(true);
        manager.replay(feature(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV0.featureLevel()));
        manager.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 1));
        assertTrue(manager.isPartitionRetirementAuthorityEnabled());
        manager.replay(feature(PartitionRetirementAuthorityVersion.FEATURE_NAME, (short) 0));
        assertFalse(manager.isPartitionRetirementAuthorityEnabled());
    }

    @Test
    void unsupportedFeatureIsNotAdvertisedByProductionQuorum() {
        String name = PartitionRetirementAuthorityVersion.FEATURE_NAME;
        Map<String, VersionRange> production = QuorumFeatures.defaultSupportedFeatureMap(false);
        Map<String, VersionRange> experimental = QuorumFeatures.defaultSupportedFeatureMap(true);
        assertFalse(production.containsKey(name));
        assertTrue(experimental.get(name).contains((short) 1));
    }

    private static FeatureControlManager manager(boolean unstable) {
        return new FeatureControlManager.Builder()
            .setQuorumFeatures(new QuorumFeatures(
                0, QuorumFeatures.defaultSupportedFeatureMap(unstable), List.of(0)
            ))
            .build();
    }

    private static FeatureLevelRecord feature(String name, short level) {
        return new FeatureLevelRecord().setName(name).setFeatureLevel(level);
    }
}
