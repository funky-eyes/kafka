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
package org.apache.kafka.storage.internals.shared.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Kafka-client configuration for the classic compacted shared-storage metadata topic.
 *
 * <p>All properties under {@code shared.storage.metadata.client.*} are copied without interpretation after stripping
 * the prefix. This supports SASL/SSL and future client settings without expanding the Kafka broker ConfigDef. If
 * bootstrap.servers or security.protocol are omitted, they are derived from one of the broker's resolved listeners.</p>
 */
public final class SharedMetadataClientConfiguration {
    public static final String TOPIC_NAME = "__shared_storage_metadata";
    public static final String CLIENT_PREFIX = "shared.storage.metadata.client.";
    public static final String LISTENER_NAME_CONFIG = "shared.storage.metadata.listener.name";
    public static final String REPLICATION_FACTOR_CONFIG = "shared.storage.metadata.replication.factor";
    public static final String MIN_ISR_CONFIG = "shared.storage.metadata.min.insync.replicas";

    public static final short DEFAULT_REPLICATION_FACTOR = 3;
    public static final int DEFAULT_MIN_ISR = 2;

    private final Map<String, Object> commonClientProperties;
    private final short replicationFactor;
    private final int minInSyncReplicas;
    private final int brokerId;
    private final String clusterId;

    private SharedMetadataClientConfiguration(
        Map<String, Object> commonClientProperties,
        short replicationFactor,
        int minInSyncReplicas,
        int brokerId,
        String clusterId
    ) {
        this.commonClientProperties = Map.copyOf(commonClientProperties);
        this.replicationFactor = replicationFactor;
        this.minInSyncReplicas = minInSyncReplicas;
        this.brokerId = brokerId;
        this.clusterId = clusterId;
    }

    public static SharedMetadataClientConfiguration from(StorageExtensionBrokerContext context) {
        Objects.requireNonNull(context, "context");
        short replicationFactor = positiveShort(
            context.originals().get(REPLICATION_FACTOR_CONFIG),
            DEFAULT_REPLICATION_FACTOR,
            REPLICATION_FACTOR_CONFIG
        );
        int minIsr = positiveInt(
            context.originals().get(MIN_ISR_CONFIG),
            DEFAULT_MIN_ISR,
            MIN_ISR_CONFIG
        );
        if (minIsr > replicationFactor) {
            throw new IllegalArgumentException(MIN_ISR_CONFIG + " must not exceed " + REPLICATION_FACTOR_CONFIG);
        }

        Map<String, Object> clientProperties = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : context.originals().entrySet()) {
            if (entry.getKey().startsWith(CLIENT_PREFIX) && entry.getValue() != null) {
                String clientKey = entry.getKey().substring(CLIENT_PREFIX.length());
                if (!clientKey.isBlank()) {
                    clientProperties.put(clientKey, entry.getValue());
                }
            }
        }

        Endpoint endpoint = selectEndpoint(context);
        clientProperties.putIfAbsent(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            formatAddress(endpoint.host(), endpoint.port())
        );
        clientProperties.putIfAbsent(
            CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            endpoint.securityProtocol().name
        );
        return new SharedMetadataClientConfiguration(
            clientProperties,
            replicationFactor,
            minIsr,
            context.brokerId(),
            context.clusterId()
        );
    }

    public Properties adminProperties() {
        Properties result = commonProperties();
        result.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "shared-storage-metadata-admin-" + brokerId);
        return result;
    }

    public Properties producerProperties() {
        Properties result = commonProperties();
        result.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "shared-storage-metadata-producer-" + brokerId);
        result.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        result.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        result.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        result.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return result;
    }

    public Properties sequenceProducerProperties() {
        Properties result = producerProperties();
        result.put(ProducerConfig.CLIENT_ID_CONFIG, "shared-storage-sequence-producer-" + brokerId);
        result.put(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            "shared-storage-sequence-" + clusterId + "-broker-" + brokerId
        );
        return result;
    }

    public Properties consumerProperties() {
        Properties result = commonProperties();
        result.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "shared-storage-metadata-consumer-" + brokerId);
        result.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        result.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        result.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        result.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        result.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        return result;
    }

    public NewTopic newMetadataTopic() {
        return new NewTopic(TOPIC_NAME, 1, replicationFactor)
            .configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, Integer.toString(minInSyncReplicas)
            ));
    }

    public short replicationFactor() {
        return replicationFactor;
    }

    public int minInSyncReplicas() {
        return minInSyncReplicas;
    }

    public int brokerId() {
        return brokerId;
    }

    public String clusterId() {
        return clusterId;
    }

    private Properties commonProperties() {
        Properties result = new Properties();
        result.putAll(commonClientProperties);
        return result;
    }

    private static Endpoint selectEndpoint(StorageExtensionBrokerContext context) {
        Object configured = context.originals().get(LISTENER_NAME_CONFIG);
        if (configured == null || configured.toString().isBlank()) {
            return validateEndpoint(context.listeners().get(0));
        }
        String listenerName = configured.toString().trim();
        return context.listeners().stream()
            .filter(endpoint -> listenerName.equals(endpoint.listener()))
            .findFirst()
            .map(SharedMetadataClientConfiguration::validateEndpoint)
            .orElseThrow(() -> new IllegalArgumentException(
                LISTENER_NAME_CONFIG + " does not match a broker listener: " + listenerName));
    }

    private static Endpoint validateEndpoint(Endpoint endpoint) {
        if (endpoint.host() == null || endpoint.host().isBlank() || endpoint.port() <= 0) {
            throw new IllegalArgumentException(
                "Shared metadata client requires a resolved broker endpoint, got " + endpoint);
        }
        return endpoint;
    }

    private static String formatAddress(String host, int port) {
        String formattedHost = host.indexOf(':') >= 0 && !(host.startsWith("[") && host.endsWith("]"))
            ? "[" + host + "]"
            : host;
        return formattedHost + ":" + port;
    }

    private static short positiveShort(Object value, short defaultValue, String name) {
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        int parsed = value instanceof Number number
            ? number.intValue()
            : Integer.parseInt(value.toString().trim());
        if (parsed <= 0 || parsed > Short.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be in [1, " + Short.MAX_VALUE + "]");
        }
        return (short) parsed;
    }

    private static int positiveInt(Object value, int defaultValue, String name) {
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        int parsed = value instanceof Number number
            ? number.intValue()
            : Integer.parseInt(value.toString().trim());
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return parsed;
    }
}
