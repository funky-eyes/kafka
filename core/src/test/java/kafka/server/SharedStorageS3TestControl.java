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

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

final class SharedStorageS3TestControl {
    static final String CONTROL_ENV = "SHARED_STORAGE_S3_CONTROL";

    private SharedStorageS3TestControl() {
    }

    static void stop(String controlCommand) throws Exception {
        run(controlCommand, "stop");
    }

    static void start(String controlCommand) throws Exception {
        run(controlCommand, "start");
    }

    static void startIgnoringFailure(String controlCommand) {
        try {
            start(controlCommand);
        } catch (Exception e) {
            System.out.println("Unable to restart S3 test fixture with " + controlCommand + ": " + e);
        }
    }

    private static void run(String controlCommand, String action) throws Exception {
        Process process = new ProcessBuilder(controlCommand, action)
            .redirectErrorStream(true)
            .start();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("S3 test-control command timed out: " + controlCommand + " " + action);
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            throw new AssertionError(
                "S3 test-control command failed: " + controlCommand + " " + action + "\n" + output
            );
        }
    }
}
