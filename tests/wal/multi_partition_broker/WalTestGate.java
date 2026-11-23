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
package org.apache.kafka.storage.internals.shared.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/** Test-only gate; never included in production source. */
final class WalTestGate {
    static void await(String stage) throws IOException {
        String configured = System.getProperty("kafka.test.wal.gate");
        if (configured == null) return;
        Path gate = Path.of(configured);
        if (!Files.deleteIfExists(gate.resolve(stage + ".arm"))) return;
        Files.createFile(gate.resolve(stage + ".entered"));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (Files.deleteIfExists(gate.resolve(stage + ".allow"))) return;
            if (Files.deleteIfExists(gate.resolve(stage + ".fail"))) throw new IOException("Injected " + stage + " failure");
            try { Thread.sleep(5); } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }
        throw new IOException("Timed out at test gate " + stage);
    }
}
