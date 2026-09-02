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

import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.CodeSource;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RingWalProcessCrashTest {
    private static final long DATA_CAPACITY = 128L;
    private static final long TOTAL_CAPACITY = RingWalLayout.DATA_START + DATA_CAPACITY;
    private static final long RECLAIMED_OFFSET = 100L;

    @TempDir
    Path tempDir;

    @Test
    void processCrashCannotExposeReusedBytesThroughOlderFallback() throws Exception {
        Path walPath = tempDir.resolve("process-crash-reuse.wal");
        Path crashMarker = tempDir.resolve("process-crash.ready");
        Path childOutput = tempDir.resolve("process-crash-child.log");
        Process process = startCrashProcess(walPath, crashMarker, childOutput);

        try {
            waitForCrashMarker(process, crashMarker, childOutput);
            process.destroyForcibly();
            assertTrue(
                process.waitFor(10L, TimeUnit.SECONDS),
                "child JVM did not terminate after destroyForcibly"
            );
        } finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(10L, TimeUnit.SECONDS);
            }
        }

        assertTrue(Files.exists(walPath), "child JVM must publish the real ring WAL before the crash boundary");
        corruptSuperblock(walPath, 0);

        try (RingWalFile reopened = new RingWalFile(walPath, TOTAL_CAPACITY)) {
            assertEquals(
                new RingWalSuperblock.State(2L, RECLAIMED_OFFSET, RECLAIMED_OFFSET, DATA_CAPACITY),
                reopened.state(),
                "fallback must remain at the reclaim fence after replacement bytes were durably reused"
            );
            assertThrows(
                IllegalArgumentException.class,
                () -> reopened.read(0L, 1),
                "generation-0 logical bytes must never become readable after their physical slot was reused"
            );
        }
    }

    private Process startCrashProcess(Path walPath, Path crashMarker, Path childOutput) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(
            javaExecutable().toString(),
            "-cp",
            childClasspath(),
            RingWalCrashProcess.class.getName(),
            walPath.toString(),
            crashMarker.toString()
        );
        builder.redirectErrorStream(true);
        builder.redirectOutput(childOutput.toFile());
        return builder.start();
    }

    private static Path javaExecutable() {
        boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).startsWith("windows");
        return Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java");
    }

    private static String childClasspath() throws Exception {
        Set<String> entries = new LinkedHashSet<>();
        String configuredClasspath = System.getProperty("java.class.path");
        if (configuredClasspath != null && !configuredClasspath.isBlank()) {
            for (String entry : configuredClasspath.split(java.io.File.pathSeparator)) {
                if (!entry.isBlank()) {
                    entries.add(entry);
                }
            }
        }

        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        while (loader != null) {
            if (loader instanceof URLClassLoader urlLoader) {
                for (URL url : urlLoader.getURLs()) {
                    addFileUrl(entries, url);
                }
            }
            loader = loader.getParent();
        }
        addCodeSource(entries, RingWalProcessCrashTest.class);
        addCodeSource(entries, RingWalCrashProcess.class);
        addCodeSource(entries, RingWalFile.class);
        addCodeSource(entries, Utils.class);

        assertFalse(entries.isEmpty(), "Unable to construct child JVM classpath from the Gradle test worker");
        return String.join(java.io.File.pathSeparator, entries);
    }

    private static void addCodeSource(Set<String> entries, Class<?> type) throws Exception {
        CodeSource codeSource = type.getProtectionDomain().getCodeSource();
        if (codeSource != null) {
            addFileUrl(entries, codeSource.getLocation());
        }
    }

    private static void addFileUrl(Set<String> entries, URL url) throws Exception {
        if (url != null && "file".equalsIgnoreCase(url.getProtocol())) {
            URI uri = url.toURI();
            entries.add(Path.of(uri).toString());
        }
    }

    private static void waitForCrashMarker(Process process, Path crashMarker, Path childOutput) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L);
        while (System.nanoTime() < deadlineNanos) {
            if (Files.exists(crashMarker)) {
                return;
            }
            if (!process.isAlive()) {
                fail(
                    "child JVM exited before reaching crash boundary: exit=" + process.exitValue() +
                        ", output=" + readChildOutput(childOutput)
                );
            }
            Thread.sleep(25L);
        }
        fail("Timed out waiting for child JVM crash boundary; output=" + readChildOutput(childOutput));
    }

    private static String readChildOutput(Path childOutput) throws Exception {
        return Files.exists(childOutput) ? Files.readString(childOutput) : "<missing>";
    }

    private static void corruptSuperblock(Path path, int copyIndex) throws Exception {
        long position = (long) copyIndex * RingWalSuperblock.SUPERBLOCK_BYTES + 8L;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer one = ByteBuffer.allocate(1);
            int read = channel.read(one, position);
            if (read != 1) {
                throw new IllegalStateException("Unable to read ring WAL superblock byte for corruption");
            }
            one.flip();
            byte changed = (byte) (one.get() ^ 0x01);
            int written = channel.write(ByteBuffer.wrap(new byte[] {changed}), position);
            if (written != 1) {
                throw new IllegalStateException("Unable to corrupt ring WAL superblock byte");
            }
            channel.force(false);
        }
    }
}
