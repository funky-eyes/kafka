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

import java.util.Map;

/**
 * Feature negotiation for versioned KRaft partition retirement authority records.
 *
 * <p>Version 1 is intentionally NOT production ready. It requires an unstable
 * metadata.version and support from every registered controller/broker. Until
 * rollout and commit-time authority are proven, there are no record emitters.</p>
 */
public enum PartitionRetirementAuthorityVersion implements FeatureVersion {
    PRAV_0(0, MetadataVersion.MINIMUM_VERSION, Map.of()),
    PRAV_1(
        1,
        MetadataVersion.IBP_4_4_IV0,
        Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV0.featureLevel())
    );

    public static final String FEATURE_NAME = "partition.retirement.authority.version";

    public static final PartitionRetirementAuthorityVersion LATEST_PRODUCTION = PRAV_0;

    private final short featureLevel;
    private final MetadataVersion bootstrapMetadataVersion;
    private final Map<String, Short> dependencies;

    PartitionRetirementAuthorityVersion(
        int featureLevel,
        MetadataVersion bootstrapMetadataVersion,
        Map<String, Short> dependencies
    ) {
        this.featureLevel = (short) featureLevel;
        this.bootstrapMetadataVersion = bootstrapMetadataVersion;
        this.dependencies = dependencies;
    }

    @Override
    public short featureLevel() {
        return featureLevel;
    }

    @Override
    public String featureName() {
        return FEATURE_NAME;
    }

    @Override
    public MetadataVersion bootstrapMetadataVersion() {
        return bootstrapMetadataVersion;
    }

    @Override
    public Map<String, Short> dependencies() {
        return dependencies;
    }
}
