#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

from pathlib import Path

path = Path("core/src/test/java/kafka/server/SharedStorageAcksOneIndependentProcessTest.java")
text = path.read_text(encoding="utf-8")

old_call = """                recoverOriginalLeader(
                    repositoryRoot,
                    processRuntime,
                    bootstrapServers,
                    admin,
                    brokers,
                    replicationFactor,
                    firstLeader,
                    1
                );
                restartBrokers(repositoryRoot, processRuntime, brokers, followers);
                waitForTopicState(admin, replicationFactor, replicationFactor, -1);
"""
new_call = """                recoverOriginalLeader(
                    repositoryRoot,
                    processRuntime,
                    bootstrapServers,
                    admin,
                    brokers,
                    replicationFactor,
                    firstLeader,
                    followers,
                    1
                );
"""
if text.count(old_call) != 1:
    raise SystemExit(f"expected one recovery call block, found {text.count(old_call)}")
text = text.replace(old_call, new_call)

old_method = """    private static void recoverOriginalLeader(
        Path repositoryRoot,
        Path processRuntime,
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        short replicationFactor,
        int leaderId,
        int expectedRecords
    ) throws Exception {
        restartBroker(repositoryRoot, processRuntime, brokers, leaderId);
        waitForTopicState(admin, replicationFactor, (short) 1, leaderId);
        assertExpectedValues(
            consumeAll(bootstrapServers, expectedRecords),
            expectedRecords,
            \"Original leader WAL must recover the leader-only acks=1 record\"
        );
        System.out.println(\"ACKS1_ORIGINAL_DISK_RECOVERED rf=\" + replicationFactor +
            \" leader=\" + leaderId + \" records=\" + expectedRecords);
    }
"""
new_method = """    private static void recoverOriginalLeader(
        Path repositoryRoot,
        Path processRuntime,
        String bootstrapServers,
        Admin admin,
        Map<Integer, BrokerProcess> brokers,
        short replicationFactor,
        int leaderId,
        List<Integer> followers,
        int expectedRecords
    ) throws Exception {
        restartBroker(repositoryRoot, processRuntime, brokers, leaderId);
        restartBrokers(repositoryRoot, processRuntime, brokers, followers);
        waitForTopicState(admin, replicationFactor, replicationFactor, leaderId);
        assertExpectedValues(
            consumeAll(bootstrapServers, expectedRecords),
            expectedRecords,
            \"Original leader WAL must recover and replicate the leader-only acks=1 record\"
        );
        System.out.println(\"ACKS1_ORIGINAL_DISK_RECOVERED rf=\" + replicationFactor +
            \" leader=\" + leaderId + \" records=\" + expectedRecords +
            \" recoveredWithFullMetadataQuorum=true\");
    }
"""
if text.count(old_method) != 1:
    raise SystemExit(f"expected one recovery method block, found {text.count(old_method)}")

path.write_text(text.replace(old_method, new_method), encoding="utf-8")
