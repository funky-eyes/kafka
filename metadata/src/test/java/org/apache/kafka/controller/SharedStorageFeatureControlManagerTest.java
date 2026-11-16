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

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.SharedStorageVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageFeatureControlManagerTest {
    private static final String FEATURE = SharedStorageVersion.FEATURE_NAME;
    private static final Map<String, VersionRange> SUPPORT = Map.of(
        FEATURE, VersionRange.of(0, 1),
        MetadataVersion.FEATURE_NAME, VersionRange.of(MetadataVersion.MINIMUM_VERSION.featureLevel(),
            MetadataVersion.LATEST_PRODUCTION.featureLevel()));

    private static FeatureControlManager manager(
        MetadataVersion metadataVersion,
        Map<Integer, Map<String, VersionRange>> brokers,
        Map<Integer, Map<String, VersionRange>> controllers
    ) {
        FeatureControlManager manager = new FeatureControlManager.Builder()
            .setQuorumFeatures(new QuorumFeatures(0, QuorumFeatures.defaultSupportedFeatureMap(false), List.of(0, 1)))
            .setClusterFeatureSupportDescriber(new ClusterFeatureSupportDescriber() {
                @Override
                public Iterator<Map.Entry<Integer, Map<String, VersionRange>>> brokerSupported() {
                    return brokers.entrySet().iterator();
                }

                @Override
                public Iterator<Map.Entry<Integer, Map<String, VersionRange>>> controllerSupported() {
                    return controllers.entrySet().iterator();
                }
            }).build();
        manager.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME).setFeatureLevel(metadataVersion.featureLevel()));
        return manager;
    }

    private static ControllerResult<ApiError> activate(FeatureControlManager manager, boolean validateOnly) {
        return manager.updateFeatures(Map.of(FEATURE, (short) 1), Map.of(), validateOnly, 0);
    }

    @Test
    void mixedBrokerVersionsCannotActivateSharedStorage() {
        FeatureControlManager manager = manager(MetadataVersion.LATEST_PRODUCTION,
            Map.of(1, SUPPORT, 2, Map.of()), Map.of(1, SUPPORT));
        ControllerResult<ApiError> result = activate(manager, false);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.response().message().contains("Broker 2"));
        assertTrue(result.records().isEmpty());
        assertFalse(manager.isSharedStorageEnabled());
    }

    @Test
    void mixedControllerVersionsCannotActivateSharedStorage() {
        FeatureControlManager manager = manager(MetadataVersion.LATEST_PRODUCTION,
            Map.of(1, SUPPORT), Map.of(1, Map.of()));
        ControllerResult<ApiError> result = activate(manager, false);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.response().message().contains("Controller 1"));
        assertTrue(result.records().isEmpty());
    }

    @Test
    void unregisteredControllerCannotBeAssumedCompatible() {
        ControllerResult<ApiError> result = activate(manager(MetadataVersion.LATEST_PRODUCTION,
            Map.of(1, SUPPORT), Map.of()), false);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.response().message().contains("has not registered"));
        assertTrue(result.records().isEmpty());
    }

    @Test
    void metadataWithoutControllerRegistrationCannotActivate() {
        ControllerResult<ApiError> result = activate(manager(MetadataVersion.MINIMUM_VERSION,
            Map.of(1, SUPPORT), Map.of(1, SUPPORT)), false);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.records().isEmpty());
    }

    @Test
    void combinedMetadataUpgradeCannotBypassControllerRegistration() {
        FeatureControlManager manager = manager(MetadataVersion.MINIMUM_VERSION,
            Map.of(1, SUPPORT), Map.of());
        ControllerResult<ApiError> result = manager.updateFeatures(Map.of(
            MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel(), FEATURE, (short) 1),
            Map.of(), false, 0);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.response().message().contains("register every controller"));
        assertTrue(result.records().isEmpty());
    }

    @Test
    void activationOnlyTakesEffectAfterFeatureRecordReplay() {
        FeatureControlManager manager = manager(MetadataVersion.LATEST_PRODUCTION,
            Map.of(1, SUPPORT), Map.of(1, SUPPORT));
        ControllerResult<ApiError> validateOnly = activate(manager, true);
        assertEquals(ApiError.NONE, validateOnly.response());
        assertTrue(validateOnly.records().isEmpty());
        assertFalse(manager.isSharedStorageEnabled());
        ControllerResult<ApiError> activation = activate(manager, false);
        assertEquals(ApiError.NONE, activation.response());
        assertFalse(manager.isSharedStorageEnabled());
        RecordTestUtils.replayAll(manager, activation.records());
        assertTrue(manager.isSharedStorageEnabled());
        FeatureControlManager recovered = manager(MetadataVersion.LATEST_PRODUCTION,
            Map.of(1, SUPPORT), Map.of(1, SUPPORT));
        RecordTestUtils.replayAll(recovered, activation.records());
        assertTrue(recovered.isSharedStorageEnabled());
    }

    @ParameterizedTest
    @EnumSource(value = FeatureUpdate.UpgradeType.class, names = {"SAFE_DOWNGRADE", "UNSAFE_DOWNGRADE"})
    void cannotRemoveVersionFenceAfterActivation(FeatureUpdate.UpgradeType type) {
        FeatureControlManager manager = manager(MetadataVersion.LATEST_PRODUCTION,
            Map.of(1, SUPPORT), Map.of(1, SUPPORT));
        RecordTestUtils.replayAll(manager, activate(manager, false).records());
        ControllerResult<ApiError> result = manager.updateFeatures(Map.of(FEATURE, (short) 0),
            Map.of(FEATURE, type), false, 0);
        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.records().isEmpty());
        assertTrue(manager.isSharedStorageEnabled());
    }
}
