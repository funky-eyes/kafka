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
package kafka.server;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Relative performance guardrail for the shared-storage data path.
 *
 * <p>Classic and shared clusters execute the same workload sequentially on the same test host. The gate compares
 * ratios instead of absolute throughput so release evidence is less sensitive to runner hardware variance.</p>
 */
@Tag("integration")
@Timeout(value = 15, unit = TimeUnit.MINUTES)
public class SharedStoragePerformanceBaselineTest {
    private static final int PARTITIONS = 3;
    private static final int DEFAULT_RECORDS = 10_000;
    private static final int PAYLOAD_BYTES = 1024;
    private static final double DEFAULT_MIN_PRODUCE_RATIO = 0.60d;
    private static final double DEFAULT_MIN_CONSUME_RATIO = 0.50d;

    @Test
    public void sharedStorageStaysWithinClassicKafkaThroughputEnvelope() throws Exception {
        String endpoint = System.getenv("SHARED_STORAGE_S3_ENDPOINT");
        assumeTrue(endpoint != null && !endpoint.isBlank(), "S3/MinIO integration endpoint is not configured");

        int records = positiveIntEnvironment("SHARED_STORAGE_PERF_RECORDS", DEFAULT_RECORDS);
        double minProduceRatio = ratioEnvironment(
            "SHARED_STORAGE_MIN_PRODUCE_RATIO",
            DEFAULT_MIN_PRODUCE_RATIO
        );
        double minConsumeRatio = ratioEnvironment(
            "SHARED_STORAGE_MIN_CONSUME_RATIO",
            DEFAULT_MIN_CONSUME_RATIO
        );
        String bucket = environment("SHARED_STORAGE_S3_BUCKET", "kafka-shared-storage-performance");
        String region = environment("SHARED_STORAGE_S3_REGION", "us-east-1");

        BenchmarkResult classic = benchmark(false, endpoint, region, bucket, records);
        BenchmarkResult shared = benchmark(true, endpoint, region, bucket, records);

        double produceRatio = shared.produceRecordsPerSecond() / classic.produceRecordsPerSecond();
        double consumeRatio = shared.consumeRecordsPerSecond() / classic.consumeRecordsPerSecond();

        System.out.printf(
            "SHARED_STORAGE_PERF classicProduce=%.2f sharedProduce=%.2f produceRatio=%.4f " +
                "classicConsume=%.2f sharedConsume=%.2f consumeRatio=%.4f records=%d%n",
            classic.produceRecordsPerSecond(),
            shared.produceRecordsPerSecond(),
            produceRatio,
            classic.consumeRecordsPerSecond(),
            shared.consumeRecordsPerSecond(),
            consumeRatio,
            records
        );

        assertTrue(
            produceRatio >= minProduceRatio,
            () -> "Shared produce throughput ratio " + produceRatio + " is below " + minProduceRatio
        );
        assertTrue(
            consumeRatio >= minConsumeRatio,
            () -> "Shared consume throughput ratio " + consumeRatio + " is below " + minConsumeRatio
        );
    }

    private static BenchmarkResult benchmark(
        boolean sharedStorage,
        String endpoint,
        String region,
        String bucket,
        int records
    ) throws Exception {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setNumDisksPerBroker(1)
            .build();
        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
        String topic = "shared-performance-" + (sharedStorage ? "shared" : "classic");

        if (sharedStorage) {
            builder
                .setConfigProp("storage.extension.class",
                    "org.apache.kafka.storage.internals.shared.s3.S3SharedStorageExtension")
                .setConfigProp("shared.storage.topics", topic)
                .setConfigProp("shared.storage.wal.engine", "ring")
                .setConfigProp("shared.storage.wal.capacity.bytes", 64L * 1024 * 1024)
                .setConfigProp("shared.storage.object.target.bytes", 4L * 1024 * 1024)
                .setConfigProp("shared.storage.upload.interval.ms", 100L)
                .setConfigProp("shared.storage.upload.max.linger.ms", 1_000L)
                .setConfigProp("shared.storage.metadata.replication.factor", 3)
                .setConfigProp("shared.storage.metadata.min.insync.replicas", 2)
                .setConfigProp("shared.storage.s3.endpoint", endpoint)
                .setConfigProp("shared.storage.s3.region", region)
                .setConfigProp("shared.storage.s3.bucket", bucket)
                .setConfigProp("shared.storage.s3.key.prefix", "performance/" + UUID.randomUUID() + "/objects")
                .setConfigProp("shared.storage.s3.path.style", true)
                .setConfigProp("shared.storage.s3.io.threads", 4);
        }

        try (KafkaClusterTestKit cluster = builder.build()) {
            cluster.format();
            cluster.startup();
            cluster.waitForReadyBrokers();
            String bootstrapServers = cluster.bootstrapServers();
            try (Admin admin = cluster.admin()) {
                admin.createTopics(List.of(new NewTopic(topic, PARTITIONS, (short) 3)
                    .configs(Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
                    .all().get(30, TimeUnit.SECONDS);
            }

            double produceRate = produce(bootstrapServers, topic, records);
            double consumeRate = consume(bootstrapServers, topic, records);
            return new BenchmarkResult(produceRate, consumeRate);
        }
    }

    private static double produce(String bootstrapServers, String topic, int records) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        byte[] payload = new byte[PAYLOAD_BYTES];
        Arrays.fill(payload, (byte) 7);
        List<Future<RecordMetadata>> futures = new ArrayList<>(records);

        long started = System.nanoTime();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            for (int index = 0; index < records; index++) {
                byte[] key = new byte[] {
                    (byte) (index >>> 24),
                    (byte) (index >>> 16),
                    (byte) (index >>> 8),
                    (byte) index
                };
                futures.add(producer.send(new ProducerRecord<>(topic, index % PARTITIONS, key, payload)));
            }
            producer.flush();
            for (Future<RecordMetadata> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        }
        return recordsPerSecond(records, System.nanoTime() - started);
    }

    private static double consume(String bootstrapServers, String topic, int records) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        List<TopicPartition> partitions = new ArrayList<>();
        for (int partition = 0; partition < PARTITIONS; partition++) {
            partitions.add(new TopicPartition(topic, partition));
        }

        long consumed = 0L;
        long started = System.nanoTime();
        long deadline = started + TimeUnit.SECONDS.toNanos(120);
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (consumed < records && System.nanoTime() < deadline) {
                consumed = Math.addExact(consumed, consumer.poll(Duration.ofMillis(250)).count());
            }
        }
        if (consumed != records) {
            throw new AssertionError("Expected " + records + " records but consumed " + consumed);
        }
        return recordsPerSecond(records, System.nanoTime() - started);
    }

    private static double recordsPerSecond(int records, long elapsedNanos) {
        return records * 1_000_000_000.0d / Math.max(1L, elapsedNanos);
    }

    private static int positiveIntEnvironment(String name, int defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        int parsed = Integer.parseInt(value.trim());
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return parsed;
    }

    private static double ratioEnvironment(String name, double defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        double parsed = Double.parseDouble(value.trim());
        if (!(parsed > 0.0d && parsed <= 1.0d)) {
            throw new IllegalArgumentException(name + " must be in (0, 1]");
        }
        return parsed;
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private record BenchmarkResult(double produceRecordsPerSecond, double consumeRecordsPerSecond) {
    }
}
