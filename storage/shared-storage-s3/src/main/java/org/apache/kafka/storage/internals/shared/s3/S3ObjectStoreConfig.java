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
package org.apache.kafka.storage.internals.shared.s3;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Kafka-independent S3 client settings for the shared object store. */
public record S3ObjectStoreConfig(
    String bucket,
    String keyPrefix,
    String region,
    Optional<URI> endpoint,
    boolean pathStyleAccess,
    int ioThreads,
    long connectionTimeoutMs,
    long socketTimeoutMs,
    long apiCallAttemptTimeoutMs,
    long apiCallTimeoutMs,
    int maxAttempts
) {
    public static final String BUCKET_CONFIG = "shared.storage.s3.bucket";
    public static final String KEY_PREFIX_CONFIG = "shared.storage.s3.key.prefix";
    public static final String REGION_CONFIG = "shared.storage.s3.region";
    public static final String ENDPOINT_CONFIG = "shared.storage.s3.endpoint";
    public static final String PATH_STYLE_ACCESS_CONFIG = "shared.storage.s3.path.style";
    public static final String IO_THREADS_CONFIG = "shared.storage.s3.io.threads";
    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "shared.storage.s3.connection.timeout.ms";
    public static final String SOCKET_TIMEOUT_MS_CONFIG = "shared.storage.s3.socket.timeout.ms";
    public static final String API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG =
        "shared.storage.s3.api.call.attempt.timeout.ms";
    public static final String API_CALL_TIMEOUT_MS_CONFIG = "shared.storage.s3.api.call.timeout.ms";
    public static final String MAX_ATTEMPTS_CONFIG = "shared.storage.s3.max.attempts";

    public static final String DEFAULT_KEY_PREFIX = "objects";
    public static final String DEFAULT_REGION = "us-east-1";
    public static final int DEFAULT_IO_THREADS = 8;
    public static final long DEFAULT_CONNECTION_TIMEOUT_MS = 5_000L;
    public static final long DEFAULT_SOCKET_TIMEOUT_MS = 30_000L;
    public static final long DEFAULT_API_CALL_ATTEMPT_TIMEOUT_MS = 60_000L;
    public static final long DEFAULT_API_CALL_TIMEOUT_MS = 180_000L;
    public static final int DEFAULT_MAX_ATTEMPTS = 3;

    public S3ObjectStoreConfig(
        String bucket,
        String keyPrefix,
        String region,
        Optional<URI> endpoint,
        boolean pathStyleAccess,
        int ioThreads
    ) {
        this(
            bucket,
            keyPrefix,
            region,
            endpoint,
            pathStyleAccess,
            ioThreads,
            DEFAULT_CONNECTION_TIMEOUT_MS,
            DEFAULT_SOCKET_TIMEOUT_MS,
            DEFAULT_API_CALL_ATTEMPT_TIMEOUT_MS,
            DEFAULT_API_CALL_TIMEOUT_MS,
            DEFAULT_MAX_ATTEMPTS
        );
    }

    public S3ObjectStoreConfig {
        bucket = requireNonBlank(bucket, "bucket");
        keyPrefix = normalizePrefix(keyPrefix);
        region = requireNonBlank(region, "region");
        endpoint = Objects.requireNonNull(endpoint, "endpoint");
        endpoint.ifPresent(S3ObjectStoreConfig::validateEndpoint);
        if (ioThreads <= 0) {
            throw new IllegalArgumentException("ioThreads must be positive");
        }
        requirePositive(connectionTimeoutMs, CONNECTION_TIMEOUT_MS_CONFIG);
        requirePositive(socketTimeoutMs, SOCKET_TIMEOUT_MS_CONFIG);
        requirePositive(apiCallAttemptTimeoutMs, API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG);
        requirePositive(apiCallTimeoutMs, API_CALL_TIMEOUT_MS_CONFIG);
        if (apiCallAttemptTimeoutMs > apiCallTimeoutMs) {
            throw new IllegalArgumentException(
                API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG + " must not exceed " + API_CALL_TIMEOUT_MS_CONFIG);
        }
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException(MAX_ATTEMPTS_CONFIG + " must be positive");
        }
    }

    public static S3ObjectStoreConfig from(Map<String, ?> originals) {
        Objects.requireNonNull(originals, "originals");
        String bucket = requiredString(originals, BUCKET_CONFIG);
        String keyPrefix = optionalString(originals, KEY_PREFIX_CONFIG, DEFAULT_KEY_PREFIX);
        String region = optionalString(originals, REGION_CONFIG, DEFAULT_REGION);
        Optional<URI> endpoint = optionalUri(originals.get(ENDPOINT_CONFIG));
        boolean pathStyleAccess = booleanValue(originals.get(PATH_STYLE_ACCESS_CONFIG), false);
        int ioThreads = positiveInt(originals.get(IO_THREADS_CONFIG), DEFAULT_IO_THREADS, IO_THREADS_CONFIG);
        long connectionTimeoutMs = positiveLong(
            originals.get(CONNECTION_TIMEOUT_MS_CONFIG),
            DEFAULT_CONNECTION_TIMEOUT_MS,
            CONNECTION_TIMEOUT_MS_CONFIG
        );
        long socketTimeoutMs = positiveLong(
            originals.get(SOCKET_TIMEOUT_MS_CONFIG),
            DEFAULT_SOCKET_TIMEOUT_MS,
            SOCKET_TIMEOUT_MS_CONFIG
        );
        long apiCallAttemptTimeoutMs = positiveLong(
            originals.get(API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG),
            DEFAULT_API_CALL_ATTEMPT_TIMEOUT_MS,
            API_CALL_ATTEMPT_TIMEOUT_MS_CONFIG
        );
        long apiCallTimeoutMs = positiveLong(
            originals.get(API_CALL_TIMEOUT_MS_CONFIG),
            DEFAULT_API_CALL_TIMEOUT_MS,
            API_CALL_TIMEOUT_MS_CONFIG
        );
        int maxAttempts = positiveInt(
            originals.get(MAX_ATTEMPTS_CONFIG),
            DEFAULT_MAX_ATTEMPTS,
            MAX_ATTEMPTS_CONFIG
        );
        return new S3ObjectStoreConfig(
            bucket,
            keyPrefix,
            region,
            endpoint,
            pathStyleAccess,
            ioThreads,
            connectionTimeoutMs,
            socketTimeoutMs,
            apiCallAttemptTimeoutMs,
            apiCallTimeoutMs,
            maxAttempts
        );
    }

    public String objectKey(long objectId) {
        if (objectId < 0) {
            throw new IllegalArgumentException("objectId must be non-negative");
        }
        return keyPrefix.isEmpty() ? Long.toString(objectId) : keyPrefix + "/" + objectId;
    }

    private static String requiredString(Map<String, ?> originals, String name) {
        Object value = originals.get(name);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException(name + " must be configured");
        }
        return value.toString().trim();
    }

    private static String optionalString(Map<String, ?> originals, String name, String defaultValue) {
        Object value = originals.get(name);
        return value == null || value.toString().isBlank() ? defaultValue : value.toString().trim();
    }

    private static Optional<URI> optionalUri(Object value) {
        if (value == null || value.toString().isBlank()) {
            return Optional.empty();
        }
        URI uri = URI.create(value.toString().trim());
        validateEndpoint(uri);
        return Optional.of(uri);
    }

    private static void validateEndpoint(URI endpoint) {
        String scheme = endpoint.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new IllegalArgumentException(ENDPOINT_CONFIG + " must use http or https");
        }
        if (endpoint.getHost() == null) {
            throw new IllegalArgumentException(ENDPOINT_CONFIG + " must contain a host");
        }
    }

    private static boolean booleanValue(Object value, boolean defaultValue) {
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        String parsed = value.toString().trim();
        if (parsed.equalsIgnoreCase("true")) {
            return true;
        }
        if (parsed.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException(PATH_STYLE_ACCESS_CONFIG + " must be true or false");
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

    private static long positiveLong(Object value, long defaultValue, String name) {
        if (value == null || value.toString().isBlank()) {
            return defaultValue;
        }
        long parsed = value instanceof Number number
            ? number.longValue()
            : Long.parseLong(value.toString().trim());
        requirePositive(parsed, name);
        return parsed;
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private static String normalizePrefix(String prefix) {
        String normalized = prefix == null ? "" : prefix.trim();
        int start = 0;
        int end = normalized.length();
        while (start < end && normalized.charAt(start) == '/') {
            start++;
        }
        while (end > start && normalized.charAt(end - 1) == '/') {
            end--;
        }
        return normalized.substring(start, end);
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
