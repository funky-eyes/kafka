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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class MultiWalBrokerProbe {
    static final String TOPIC = "parallel-wal-multi";
    static void check(boolean ok, String msg) { if (!ok) throw new AssertionError(msg); }
    static long latest(Admin admin, int partition) throws Exception {
        TopicPartition tp = new TopicPartition(TOPIC, partition);
        return admin.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp).get(5, TimeUnit.SECONDS).offset();
    }

    static long produceSingle(int port, int id) throws Exception {
        var partition = new ProduceRequestData.PartitionProduceData().setIndex(0)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(("same-" + id).getBytes())));
        var data = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(15000).setTopicData(
            new ProduceRequestData.TopicProduceDataCollection(List.of(new ProduceRequestData.TopicProduceData()
                .setName(TOPIC).setPartitionData(List.of(partition))).iterator()));
        var header = new RequestHeader(ApiKeys.PRODUCE, (short) 9, "same-partition-probe", id);
        ByteBuffer serialized = ProduceRequest.builder(data).build((short) 9).serializeWithHeader(header);
        byte[] body = new byte[serialized.remaining()]; serialized.get(body);
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(15000);
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(body.length); output.write(body); output.flush();
            var input = new DataInputStream(socket.getInputStream());
            int size = input.readInt();
            check(size > 0 && size < 1024 * 1024, "invalid single-partition response length");
            byte[] bytes = new byte[size]; input.readFully(bytes);
            var response = (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(bytes), header);
            var result = response.data().responses().iterator().next().partitionResponses().get(0);
            check(result.errorCode() == 0, "single-partition produce error=" + result.errorCode());
            return result.baseOffset();
        }
    }

    static void verifyConcurrentSamePartition(Admin admin, Path gate, Process broker, int port) throws Exception {
        Files.deleteIfExists(gate.resolve("data.entered"));
        Files.createFile(gate.resolve("data.arm"));
        var executor = java.util.concurrent.Executors.newFixedThreadPool(8);
        var start = new java.util.concurrent.CountDownLatch(1);
        try {
            List<java.util.concurrent.Future<Long>> replies = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                final int id = i;
                replies.add(executor.submit(() -> { start.await(); return produceSingle(port, id); }));
            }
            start.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!Files.exists(gate.resolve("data.entered"))) {
                check(broker.isAlive() && System.nanoTime() < deadline, "same-partition force gate not reached");
                Thread.sleep(10);
            }
            Set<Long> offsets = new HashSet<>();
            for (var reply : replies) offsets.add(reply.get(10, TimeUnit.SECONDS));
            check(offsets.equals(Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L)), "unexpected offsets " + offsets);
            check(latest(admin, 0) == 1, "HW advanced while same-partition writes were held");
            Files.createFile(gate.resolve("data.allow"));
            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (latest(admin, 0) != 9) {
                check(System.nanoTime() < deadline, "same-partition HW did not advance");
                Thread.sleep(20);
            }
            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                "bootstrap.servers", "127.0.0.1:" + port, "group.id", "same-partition-probe",
                "enable.auto.commit", "false", "auto.offset.reset", "earliest"),
                new StringDeserializer(), new StringDeserializer())) {
                var tp = new TopicPartition(TOPIC, 0);
                consumer.assign(List.of(tp));
                consumer.seekToBeginning(List.of(tp));
                Set<String> actual = new HashSet<>();
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (actual.size() < 9 && System.nanoTime() < deadline)
                    consumer.poll(Duration.ofMillis(100)).forEach(record -> actual.add(record.value()));
                Set<String> expected = new HashSet<>();
                expected.add("v0");
                for (int i = 0; i < 8; i++) expected.add("same-" + i);
                check(actual.equals(expected), "same-partition data differs: " + actual);
            }
            System.out.println("PASS eight same-partition ACKs precede force; HW and values follow durability");
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String cp = args[1]; int port = Integer.parseInt(args[2]);
        boolean early = Boolean.parseBoolean(args[3]);
        Path gate = Files.createDirectory(root.resolve("gate"));
        Process broker = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xms128m", "-Xmx512m", "-XX:ActiveProcessorCount=2", "-Dkafka.test.wal.gate=" + gate,
            "-cp", cp, "kafka.Kafka", root.resolve("server.properties").toString())
            .redirectErrorStream(true).redirectOutput(root.resolve("broker.log").toFile()).start();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", "127.0.0.1:" + port, "request.timeout.ms", 5000))) {
            check(!admin.describeCluster().nodes().get(60, TimeUnit.SECONDS).isEmpty(), "no broker");
            admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get(30, TimeUnit.SECONDS);
            long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true) {
                try { if (latest(admin, 0) == 0 && latest(admin, 1) == 0) break; }
                catch (Exception notReady) { }
                check(System.nanoTime() < readyDeadline, "partitions were not ready");
                Thread.sleep(50);
            }
            var partitions = new ArrayList<ProduceRequestData.PartitionProduceData>();
            for (int i = 0; i < 2; i++) partitions.add(new ProduceRequestData.PartitionProduceData().setIndex(i)
                .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(("v" + i).getBytes()))));
            var data = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(10000).setTopicData(
                new ProduceRequestData.TopicProduceDataCollection(List.of(new ProduceRequestData.TopicProduceData()
                    .setName(TOPIC).setPartitionData(partitions)).iterator()));
            short version = 9;
            var header = new RequestHeader(ApiKeys.PRODUCE, version, "multi-probe", 123);
            ByteBuffer serialized = ProduceRequest.builder(data).build(version).serializeWithHeader(header);
            byte[] body = new byte[serialized.remaining()]; serialized.get(body);
            Files.createFile(gate.resolve("data.arm"));
            try (Socket socket = new Socket("127.0.0.1", port)) {
                socket.setSoTimeout(5000);
                var output = new DataOutputStream(socket.getOutputStream());
                output.writeInt(body.length); output.write(body); output.flush();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (!Files.exists(gate.resolve("data.entered"))) {
                    check(broker.isAlive() && System.nanoTime() < deadline, "force gate not reached");
                    Thread.sleep(10);
                }
                var input = new DataInputStream(socket.getInputStream());
                if (!early) {
                    socket.setSoTimeout(1000);
                    try { input.readInt(); throw new AssertionError("durable ACK returned before force"); }
                    catch (SocketTimeoutException expected) { }
                    check(latest(admin, 0) == 0 && latest(admin, 1) == 0, "HW advanced before force");
                    Files.createFile(gate.resolve("data.allow"));
                    socket.setSoTimeout(15000);
                }
                int size;
                try { size = input.readInt(); }
                catch (SocketTimeoutException timeout) {
                    new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jstack").toString(),
                        Long.toString(broker.pid())).redirectErrorStream(true)
                        .redirectOutput(root.resolve("threads.txt").toFile()).start().waitFor(10, TimeUnit.SECONDS);
                    throw timeout;
                }
                check(size > 0 && size < 1024 * 1024, "invalid response length");
                var bytes = new byte[size]; input.readFully(bytes);
                var response = (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(bytes), header);
                check(response.data().responses().size() == 1, "wrong topic response count");
                var result = response.data().responses().iterator().next().partitionResponses();
                check(result.size() == 2, "wrong partition response count");
                for (var item : result) check(item.errorCode() == 0 && item.baseOffset() == 0,
                    "failed partition " + item.index() + " code=" + item.errorCode());
                if (early) {
                    check(latest(admin, 0) == 0 && latest(admin, 1) == 0, "HW advanced before force");
                    Files.createFile(gate.resolve("data.allow"));
                }
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (latest(admin, 0) != 1 || latest(admin, 1) != 1) {
                    check(System.nanoTime() < deadline, "HW did not advance"); Thread.sleep(20);
                }
                System.out.println("PASS two-partition request early=" + early + "; both HWs advance after force");
            }
            if (early) verifyConcurrentSamePartition(admin, gate, broker, port);
        } finally {
            if (Files.exists(gate) && Files.exists(gate.resolve("data.entered")) && !Files.exists(gate.resolve("data.allow")))
                Files.createFile(gate.resolve("data.allow"));
            broker.destroy();
            if (!broker.waitFor(15, TimeUnit.SECONDS)) { broker.destroyForcibly(); broker.waitFor(10, TimeUnit.SECONDS); }
        }
    }
}
