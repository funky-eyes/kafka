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
import org.apache.kafka.common.TopicPartitionReplica;
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** A local multi-process fault test, not a cross-host or power-loss test. */
public final class MultiWalRf2Probe {
    private static final String TOPIC = "parallel-wal-multi";
    private static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    private static Process start(Path root, String cp, int id) throws Exception {
        Path gate = Files.createDirectories(root.resolve("gate-" + id));
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xms96m", "-Xmx384m", "-XX:ActiveProcessorCount=2", "-Dkafka.test.wal.gate=" + gate,
            "-cp", cp, "kafka.Kafka", root.resolve("server-" + id + ".properties").toString())
            .redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.appendTo(root.resolve("broker-" + id + ".log").toFile())).start();
    }
    private static boolean ready(Admin admin, int leader, int isrSize, long offset) {
        try {
            var description = admin.describeTopics(List.of(TOPIC)).allTopicNames().get(5, TimeUnit.SECONDS).get(TOPIC);
            for (var partition : description.partitions()) {
                if (partition.leader() == null || partition.leader().id() != leader || partition.isr().size() != isrSize)
                    return false;
                var tp = new TopicPartition(TOPIC, partition.partition());
                if (admin.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp).get(5, TimeUnit.SECONDS).offset() != offset)
                    return false;
            }
            return description.partitions().size() == 2;
        } catch (Exception notReady) {
            return false;
        }
    }
    private static void awaitReady(Admin admin, int leader, int isrSize, long offset) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (!ready(admin, leader, isrSize, offset)) {
            check(System.nanoTime() < deadline, "partition state did not converge: leader=" + leader + " isr=" + isrSize);
            Thread.sleep(100);
        }
    }
    private static void produce(int port, int expectedOffset) throws Exception {
        var partitions = new ArrayList<ProduceRequestData.PartitionProduceData>();
        for (int i = 0; i < 2; i++) partitions.add(new ProduceRequestData.PartitionProduceData().setIndex(i)
            .setRecords(MemoryRecords.withRecords(Compression.NONE, new SimpleRecord(("v" + i).getBytes()))));
        var data = new ProduceRequestData().setAcks((short) 1).setTimeoutMs(15000).setTopicData(
            new ProduceRequestData.TopicProduceDataCollection(List.of(new ProduceRequestData.TopicProduceData()
                .setName(TOPIC).setPartitionData(partitions)).iterator()));
        var header = new RequestHeader(ApiKeys.PRODUCE, (short) 9, "rf2-probe", 123);
        ByteBuffer serialized = ProduceRequest.builder(data).build((short) 9).serializeWithHeader(header);
        byte[] body = new byte[serialized.remaining()];
        serialized.get(body);
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(15000);
            var output = new DataOutputStream(socket.getOutputStream());
            output.writeInt(body.length); output.write(body); output.flush();
            var input = new DataInputStream(socket.getInputStream());
            int size = input.readInt();
            check(size > 0 && size < 1024 * 1024, "invalid response length");
            byte[] bytes = new byte[size]; input.readFully(bytes);
            var response = (ProduceResponse) AbstractResponse.parseResponse(ByteBuffer.wrap(bytes), header);
            check(response.data().responses().size() == 1, "wrong topic count");
            var result = response.data().responses().iterator().next().partitionResponses();
            check(result.size() == 2, "wrong partition count");
            for (var item : result)
                check(item.errorCode() == 0 && item.baseOffset() == expectedOffset, "failed partition " + item.index());
        }
    }
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args[0]); String cp = args[1];
        int port1 = Integer.parseInt(args[2]); int port2 = Integer.parseInt(args[3]);
        boolean loseBeforeForce = Boolean.parseBoolean(args[4]);
        List<Process> processes = new ArrayList<>();
        try {
            for (int id = 0; id < 3; id++) processes.add(start(root, cp, id));
            try (Admin admin = Admin.create(Map.of("bootstrap.servers", "127.0.0.1:" + port1 + ",127.0.0.1:" + port2,
                "request.timeout.ms", 5000, "default.api.timeout.ms", 30000))) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (true) {
                    try { if (admin.describeCluster().nodes().get(5, TimeUnit.SECONDS).size() == 2) break; }
                    catch (Exception notReady) { }
                    check(System.nanoTime() < deadline, "brokers did not register");
                    Thread.sleep(100);
                }
                admin.createTopics(List.of(new NewTopic(TOPIC, Map.of(0, List.of(1, 2), 1, List.of(1, 2)))))
                    .all().get(30, TimeUnit.SECONDS);
                awaitReady(admin, 1, 2, 0);
                Path gate = root.resolve("gate-1");
                Files.createFile(gate.resolve("data.arm"));
                // Runs on a separate thread so the test can observe the held force.
                var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
                try {
                    var ack = executor.submit(() -> { produce(port1, 0); return null; });
                    deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                    while (!Files.exists(gate.resolve("data.entered"))) {
                        check(processes.get(1).isAlive() && System.nanoTime() < deadline, "leader force was not held");
                        Thread.sleep(10);
                    }
                    ack.get(10, TimeUnit.SECONDS);
                    check(ready(admin, 1, 2, 0), "HW advanced before leader force");
                    if (!loseBeforeForce) {
                        Files.createFile(gate.resolve("data.allow"));
                        awaitReady(admin, 1, 2, 1);
                        var replicas = List.of(new TopicPartitionReplica(TOPIC, 0, 2),
                            new TopicPartitionReplica(TOPIC, 1, 2));
                        var infos = admin.describeReplicaLogDirs(replicas).all().get(15, TimeUnit.SECONDS);
                        for (var info : infos.values())
                            check(info.getCurrentReplicaLogDir() != null && info.getCurrentReplicaOffsetLag() == 0,
                                "follower has not reached the high watermark");
                    }
                    Path followerWal = root.resolve("wal-2");
                    Path leaderWal = root.resolve("wal-1");
                    for (Path wal : List.of(followerWal, leaderWal)) {
                        Path ring = wal.resolve("shared-ring.wal");
                        check(Files.isRegularFile(ring) && Files.size(ring) > 0, "independent ring WAL is absent: " + ring);
                    }
                    processes.get(1).destroyForcibly();
                    check(processes.get(1).waitFor(15, TimeUnit.SECONDS), "leader did not terminate");
                    try (var files = Files.walk(leaderWal)) {
                        for (Path path : files.sorted(Comparator.reverseOrder()).toList()) Files.delete(path);
                    }
                    check(!Files.exists(leaderWal), "leader WAL was not removed");
                    int survivingOffset = loseBeforeForce ? 0 : 1;
                    awaitReady(admin, 2, 1, survivingOffset);
                    try (var consumer = new KafkaConsumer<String, String>(Map.of(
                        "bootstrap.servers", "127.0.0.1:" + port2, "group.id", "rf2-probe",
                        "enable.auto.commit", "false", "auto.offset.reset", "earliest"),
                        new StringDeserializer(), new StringDeserializer())) {
                        var tps = List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1));
                        consumer.assign(tps); consumer.seekToBeginning(tps);
                        var seen = new java.util.HashMap<Integer, String>();
                        deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(loseBeforeForce ? 2 : 30);
                        while ((loseBeforeForce || seen.size() < 2) && System.nanoTime() < deadline) {
                            consumer.poll(Duration.ofMillis(250)).forEach(record -> seen.put(record.partition(), record.value()));
                        }
                        if (loseBeforeForce) check(seen.isEmpty(), "unexpected records after pre-force leader loss: " + seen);
                        else check(seen.equals(Map.of(0, "v0", 1, "v1")), "failover records differ: " + seen);
                    }
                    if (!loseBeforeForce) {
                        processes.get(2).destroyForcibly();
                        check(processes.get(2).waitFor(15, TimeUnit.SECONDS), "follower did not terminate");
                        processes.set(2, start(root, cp, 2));
                        awaitReady(admin, 2, 1, 1);
                        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                            "bootstrap.servers", "127.0.0.1:" + port2, "group.id", "rf2-cold-probe",
                            "enable.auto.commit", "false", "auto.offset.reset", "earliest"),
                            new StringDeserializer(), new StringDeserializer())) {
                            var tps = List.of(new TopicPartition(TOPIC, 0), new TopicPartition(TOPIC, 1));
                            consumer.assign(tps); consumer.seekToBeginning(tps);
                            var seen = new java.util.HashMap<Integer, String>();
                            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                            while (seen.size() < 2 && System.nanoTime() < deadline) {
                                consumer.poll(Duration.ofMillis(250)).forEach(record -> seen.put(record.partition(), record.value()));
                            }
                            check(seen.equals(Map.of(0, "v0", 1, "v1")), "cold WAL replay differs: " + seen);
                        }
                    }
                    produce(port2, survivingOffset);
                    awaitReady(admin, 2, 1, survivingOffset + 1);
                    System.out.println(loseBeforeForce
                        ? "PASS RF2 pre-force leader loss exposes acknowledged-data loss window"
                        : "PASS RF2 early ACK before force; both records survive leader loss and follower restart");
                } finally {
                    Files.deleteIfExists(gate.resolve("data.arm"));
                    if (Files.exists(gate.resolve("data.entered")) && !Files.exists(gate.resolve("data.allow")))
                        Files.createFile(gate.resolve("data.allow"));
                    executor.shutdownNow();
                }
            }
        } finally {
            for (Process process : processes) if (process.isAlive()) process.destroy();
            for (Process process : processes) if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS);
            }
        }
    }
}
