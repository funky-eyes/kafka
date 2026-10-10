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
package org.apache.kafka.server.common;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionRetirementAuthorityVersionTest {
    @Test
    void productionBuildCannotAdvertiseRecordFeatureLevelOne() {
        Feature feature = Feature.PARTITION_RETIREMENT_AUTHORITY_VERSION;
        assertEquals(0, feature.latestProduction());
        assertEquals(1, feature.latestTesting());
        assertThrows(IllegalArgumentException.class, () -> feature.fromFeatureLevel((short) 1, false));
    }

    @Test
    void featureIsDisabledByDefaultThroughProductionMetadataVersion() {
        Feature feature = Feature.PARTITION_RETIREMENT_AUTHORITY_VERSION;
        assertEquals(0, feature.defaultLevel(MetadataVersion.IBP_4_3_IV0));
        assertEquals(1, feature.defaultLevel(MetadataVersion.IBP_4_4_IV0));
    }

    @Test
    void featureLevelOneRequiresUnstableMetadataVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            Feature.validateVersion(
                PartitionRetirementAuthorityVersion.PRAV_1,
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_3_IV0.featureLevel())
            )
        );
        assertDoesNotThrow(() ->
            Feature.validateVersion(
                PartitionRetirementAuthorityVersion.PRAV_1,
                Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV0.featureLevel())
            )
        );
    }

    @Test
    void featureNameIsRegisteredWithTheStandardKafkaFeatureSystem() {
        assertEquals(
            Feature.PARTITION_RETIREMENT_AUTHORITY_VERSION,
            Feature.featureFromName(PartitionRetirementAuthorityVersion.FEATURE_NAME)
        );
        assertTrue(Feature.PRODUCTION_FEATURES.contains(Feature.PARTITION_RETIREMENT_AUTHORITY_VERSION));
    }

    @Test
    void featureLevelZeroRemainsBackwardCompatible() {
        Feature feature = Feature.PARTITION_RETIREMENT_AUTHORITY_VERSION;
        assertEquals(0, feature.fromFeatureLevel((short) 0, false).featureLevel());
        assertEquals(MetadataVersion.MINIMUM_VERSION,
            PartitionRetirementAuthorityVersion.PRAV_0.bootstrapMetadataVersion());
    }
}
