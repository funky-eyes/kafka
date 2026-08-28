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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.StorageExtensionBrokerContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedMetadataClientConfigurationTest {
    @Test
    void derivesBootstrapAndSecurityProtocolFromResolvedListener() {
        Endpoint endpoint = new Endpoint("SASL_SSL", SecurityProtocol.SASL_SSL, "broker.example", 9093);
        SharedMetadataClientConfiguration config = SharedMetadataClientConfiguration.from(
            context(List.of(endpoint), Map.of())
        );

        Properties producer = config.producerProperties();
        assertEquals("broker.example:9093", producer.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("SASL_SSL", producer.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("all", producer.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, producer.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertEquals("read_committed", config.consumerProperties().get(ConsumerConfig.ISOLATION_LEVEL_CONFIG));
        assertTrue(config.sequenceProducerProperties()
            .get(ProducerConfig.TRANSACTIONAL_ID_CONFIG)
            .toString()
            .contains("cluster-a-broker-7"));
    }

    @Test
    void passesThroughPrefixedClientPropertiesAndSelectsNamedListener() {
        Endpoint plaintext = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "plain", 9092);
        Endpoint ssl = new Endpoint("SSL", SecurityProtocol.SSL, "secure", 9093);
        Map<String, Object> originals = new HashMap<>();
        originals.put(SharedMetadataClientConfiguration.LISTENER_NAME_CONFIG, "SSL");
        originals.put(
            SharedMetadataClientConfiguration.CLIENT_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
            "override:19093"
        );
        originals.put(
            SharedMetadataClientConfiguration.CLIENT_PREFIX + CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
            "SASL_SSL"
        );
        originals.put(
            SharedMetadataClientConfiguration.CLIENT_PREFIX + "sasl.mechanism",
            "PLAIN"
        );

        SharedMetadataClientConfiguration config = SharedMetadataClientConfiguration.from(
            context(List.of(plaintext, ssl), originals)
        );
        Properties properties = config.adminProperties();

        assertEquals("override:19093", properties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("SASL_SSL", properties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("PLAIN", properties.get("sasl.mechanism"));
    }

    @Test
    void formatsIpv6BootstrapAddress() {
        Endpoint endpoint = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "2001:db8::1", 9092);
        SharedMetadataClientConfiguration config = SharedMetadataClientConfiguration.from(
            context(List.of(endpoint), Map.of())
        );

        assertEquals(
            "[2001:db8::1]:9092",
            config.adminProperties().get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG)
        );
    }

    @Test
    void createsSinglePartitionCompactedMetadataTopic() {
        Map<String, Object> originals = Map.of(
            SharedMetadataClientConfiguration.REPLICATION_FACTOR_CONFIG, 2,
            SharedMetadataClientConfiguration.MIN_ISR_CONFIG, 1
        );
        SharedMetadataClientConfiguration config = SharedMetadataClientConfiguration.from(
            context(
                List.of(new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092)),
                originals
            )
        );

        assertEquals(2, config.replicationFactor());
        assertEquals(1, config.minInSyncReplicas());
        assertEquals(1, config.newMetadataTopic().numPartitions());
        assertEquals(2, config.newMetadataTopic().replicationFactor());
        assertEquals(
            TopicConfig.CLEANUP_POLICY_COMPACT,
            config.newMetadataTopic().configs().get(TopicConfig.CLEANUP_POLICY_CONFIG)
        );
        assertEquals("1", config.newMetadataTopic().configs().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG));
    }

    @Test
    void rejectsInvalidTopicDurabilityOrListenerSelection() {
        Endpoint endpoint = new Endpoint("PLAINTEXT", SecurityProtocol.PLAINTEXT, "localhost", 9092);
        Map<String, Object> invalidDurability = Map.of(
            SharedMetadataClientConfiguration.REPLICATION_FACTOR_CONFIG, 1,
            SharedMetadataClientConfiguration.MIN_ISR_CONFIG, 2
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataClientConfiguration.from(context(List.of(endpoint), invalidDurability))
        );

        assertThrows(
            IllegalArgumentException.class,
            () -> SharedMetadataClientConfiguration.from(context(
                List.of(endpoint),
                Map.of(SharedMetadataClientConfiguration.LISTENER_NAME_CONFIG, "SSL")
            ))
        );
    }

    private static StorageExtensionBrokerContext context(
        List<Endpoint> listeners,
        Map<String, ?> originals
    ) {
        return new StorageExtensionBrokerContext(
            "cluster-a",
            7,
            listeners,
            originals,
            Time.SYSTEM
        );
    }
}
