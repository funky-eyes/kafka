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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedStorageVersionTest {
    @ParameterizedTest
    @EnumSource(MetadataVersion.class)
    void formattingOrUpgradingMetadataNeverEnablesSharedStorage(MetadataVersion version) {
        assertEquals(SharedStorageVersion.SSV_0, Feature.SHARED_STORAGE_VERSION.defaultVersion(version));
        assertEquals(0, Feature.SHARED_STORAGE_VERSION.defaultLevel(version));
    }

    @Test
    void explicitActivationRequiresControllerRegistrationSupport() {
        assertThrows(IllegalArgumentException.class, () -> Feature.validateVersion(
            SharedStorageVersion.SSV_1,
            Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.MINIMUM_VERSION.featureLevel())));
        Feature.validateVersion(SharedStorageVersion.SSV_1,
            Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel()));
    }
}
