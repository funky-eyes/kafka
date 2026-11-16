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

import org.apache.kafka.clients.admin.AlterConfigOp.OpType;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.SharedStorageVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedStorageConfigurationControlManagerTest {
    private static final String KEY = TopicConfig.SHARED_STORAGE_ENABLE_CONFIG;
    private static final ConfigResource TOPIC_RESOURCE = new ConfigResource(TOPIC, "events");
    private static final KafkaConfigSchema SCHEMA = new KafkaConfigSchema(Map.of(TOPIC,
        new ConfigDef().define(KEY, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM, "shared storage")
            .define(TopicConfig.RETENTION_MS_CONFIG, ConfigDef.Type.LONG, 1000L,
                ConfigDef.Importance.MEDIUM, "retention")), Map.of());

    private static ConfigurationControlManager manager(boolean featureEnabled) {
        FeatureControlManager features = new FeatureControlManager.Builder()
            .setQuorumFeatures(new QuorumFeatures(0, QuorumFeatures.defaultSupportedFeatureMap(false), List.of(0)))
            .build();
        features.replay(new FeatureLevelRecord().setName(MetadataVersion.FEATURE_NAME)
            .setFeatureLevel(MetadataVersion.LATEST_PRODUCTION.featureLevel()));
        if (featureEnabled) {
            features.replay(new FeatureLevelRecord().setName(SharedStorageVersion.FEATURE_NAME).setFeatureLevel((short) 1));
        }
        return new ConfigurationControlManager.Builder().setKafkaConfigSchema(SCHEMA)
            .setFeatureControl(features).build();
    }

    private static ControllerResult<ApiError> setMode(ConfigurationControlManager manager, String value, boolean creation) {
        return manager.incrementalAlterConfig(TOPIC_RESOURCE,
            Map.of(KEY, new SimpleImmutableEntry<>(value == null ? OpType.DELETE : OpType.SET, value)), creation, false);
    }

    private static void replayMode(ConfigurationControlManager manager, String value) {
        manager.replay(new ConfigRecord().setResourceType(TOPIC.id()).setResourceName(TOPIC_RESOURCE.name())
            .setName(KEY).setValue(value));
    }

    @Test
    void ordinaryTopicsDoNotRequireSharedStorageFeature() {
        ConfigurationControlManager manager = manager(false);
        assertEquals(ApiError.NONE, manager.incrementalAlterConfig(TOPIC_RESOURCE, Map.of(), true, false).response());
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void explicitStorageSelectionRequiresFinalizedFeatureBeforeCreation(String value) {
        ControllerResult<ApiError> rejected = setMode(manager(false), value, true);
        assertEquals(Errors.INVALID_CONFIG, rejected.response().error());
        assertTrue(rejected.records().isEmpty());
        assertTrue(rejected.response().message().contains("shared.storage.version=1"));
        ConfigurationControlManager manager = manager(true);
        ControllerResult<ApiError> accepted = setMode(manager, value, true);
        assertEquals(ApiError.NONE, accepted.response());
        RecordTestUtils.replayAll(manager, accepted.records());
        assertEquals(value, manager.getConfigs(TOPIC_RESOURCE).get(KEY));
    }

    @Test
    void sharedStorageCannotBeEnabledOnInternalTopics() {
        ControllerResult<ApiError> result = manager(true).incrementalAlterConfig(new ConfigResource(TOPIC, "__consumer_offsets"),
            Map.of(KEY, Map.entry(OpType.SET, "true")), true, false);
        assertEquals(Errors.INVALID_CONFIG, result.response().error());
        assertTrue(result.records().isEmpty());
    }

    @Test
    void invalidBooleanIsRejectedBeforeRecordingConfiguration() {
        ControllerResult<ApiError> result = setMode(manager(true), "enabled", true);
        assertEquals(Errors.INVALID_CONFIG, result.response().error());
        assertTrue(result.records().isEmpty());
    }

    @Test
    void untaggedExistingTopicCannotBeEnabledByAlterConfigs() {
        ConfigurationControlManager manager = manager(true);
        ControllerResult<ApiError> incremental = setMode(manager, "true", false);
        assertEquals(Errors.INVALID_CONFIG, incremental.response().error());
        assertTrue(incremental.records().isEmpty());
        ControllerResult<Map<ConfigResource, ApiError>> legacy = manager.legacyAlterConfigs(
            Map.of(TOPIC_RESOURCE, Map.of(KEY, "true")), false, false);
        assertEquals(Errors.INVALID_CONFIG, legacy.response().get(TOPIC_RESOURCE).error());
        assertTrue(legacy.records().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void existingModeCannotBeChangedAndSharedCannotBeDeleted(String value) {
        ConfigurationControlManager manager = manager(true);
        replayMode(manager, value);
        String opposite = value.equals("true") ? "false" : "true";
        assertEquals(Errors.INVALID_CONFIG, setMode(manager, opposite, false).response().error());
        Errors deletionResult = value.equals("true") ? Errors.INVALID_CONFIG : Errors.NONE;
        assertEquals(deletionResult, setMode(manager, null, false).response().error());
        ControllerResult<Map<ConfigResource, ApiError>> legacy = manager.legacyAlterConfigs(
            Map.of(TOPIC_RESOURCE, Map.of(TopicConfig.RETENTION_MS_CONFIG, "2000")), false, false);
        assertEquals(deletionResult, legacy.response().get(TOPIC_RESOURCE).error());
        if (value.equals("true")) assertTrue(legacy.records().isEmpty());
        assertEquals(value, manager.getConfigs(TOPIC_RESOURCE).get(KEY));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ordinaryFalseNormalizationRequiresActivatedTopicRouting(boolean featureEnabled) {
        ConfigurationControlManager manager = manager(featureEnabled);
        ControllerResult<ApiError> normalized = setMode(manager, "false", false);
        assertEquals(featureEnabled ? Errors.NONE : Errors.INVALID_CONFIG, normalized.response().error());
        if (!featureEnabled) {
            assertTrue(normalized.records().isEmpty());
            return;
        }
        RecordTestUtils.replayAll(manager, normalized.records());
        assertEquals("false", manager.getConfigs(TOPIC_RESOURCE).get(KEY));
        ControllerResult<Map<ConfigResource, ApiError>> reset = manager.legacyAlterConfigs(
            Map.of(TOPIC_RESOURCE, Map.of(TopicConfig.RETENTION_MS_CONFIG, "2000")), false, false);
        assertEquals(ApiError.NONE, reset.response().get(TOPIC_RESOURCE));
        RecordTestUtils.replayAll(manager, reset.records());
        assertEquals(Map.of(TopicConfig.RETENTION_MS_CONFIG, "2000"), manager.getConfigs(TOPIC_RESOURCE));
        assertEquals(Errors.INVALID_CONFIG, setMode(manager, "true", false).response().error());
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "TRUE", " true "})
    void unchangedModeAndOtherConfigChangesRemainSupported(String sameMode) {
        ConfigurationControlManager manager = manager(true);
        replayMode(manager, "true");
        assertEquals(ApiError.NONE, setMode(manager, sameMode, false).response());
        ControllerResult<ApiError> changed = manager.incrementalAlterConfig(TOPIC_RESOURCE,
            Map.of(TopicConfig.RETENTION_MS_CONFIG, Map.entry(OpType.SET, "2000")), false, false);
        assertEquals(ApiError.NONE, changed.response());
        RecordTestUtils.replayAll(manager, changed.records());
        assertEquals("true", manager.getConfigs(TOPIC_RESOURCE).get(KEY));
        assertEquals("2000", manager.getConfigs(TOPIC_RESOURCE).get(TopicConfig.RETENTION_MS_CONFIG));
        assertEquals(ApiError.NONE, manager.legacyAlterConfigs(Map.of(TOPIC_RESOURCE,
            Map.of(KEY, "true", TopicConfig.RETENTION_MS_CONFIG, "3000")), false, false).response().get(TOPIC_RESOURCE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"true", "false"})
    void replayingPersistedModeRestoresMutationProtection(String mode) {
        ControllerResult<ApiError> creation = setMode(manager(true), mode, true);
        ConfigurationControlManager recovered = manager(true);
        RecordTestUtils.replayAll(recovered, creation.records());
        assertEquals(mode, recovered.getConfigs(TOPIC_RESOURCE).get(KEY));
        assertEquals(mode.equals("true") ? Errors.INVALID_CONFIG : Errors.NONE,
            setMode(recovered, null, false).response().error());
        assertEquals(Errors.INVALID_CONFIG, setMode(recovered, mode.equals("true") ? "false" : "true", false).response().error());
    }

    @Test
    void ordinaryLegacyTopicStillAcceptsUnrelatedUpdates() {
        ConfigurationControlManager manager = manager(false);
        assertEquals(ApiError.NONE, manager.incrementalAlterConfig(TOPIC_RESOURCE,
            Map.of(TopicConfig.RETENTION_MS_CONFIG, Map.entry(OpType.SET, "2000")), false, false).response());
        assertEquals(ApiError.NONE, manager.legacyAlterConfigs(Map.of(TOPIC_RESOURCE,
            Map.of(TopicConfig.RETENTION_MS_CONFIG, "3000")), false, false).response().get(TOPIC_RESOURCE));
    }

    @Test
    void unsafeModeChangeRejectsOtherChangesAtomically() {
        ConfigurationControlManager manager = manager(true);
        replayMode(manager, "true");
        ControllerResult<ApiError> result = manager.incrementalAlterConfig(TOPIC_RESOURCE,
            Map.of(KEY, Map.entry(OpType.SET, "false"), TopicConfig.RETENTION_MS_CONFIG, Map.entry(OpType.SET, "2000")), false, false);
        assertEquals(Errors.INVALID_CONFIG, result.response().error());
        assertTrue(result.records().isEmpty());
        assertEquals(Map.of(KEY, "true"), manager.getConfigs(TOPIC_RESOURCE));
    }
}
