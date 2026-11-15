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

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketLifecycleConfigurationRequest;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Guards the COMMITTED object write path against *external* S3 lifecycle deletion.
 *
 * <p>Object creation time is not Kafka log retention time, and one immutable
 * object can pack ranges from several topics. Even a bucket TTL longer than
 * one topic retention is not a proven safe bound. Therefore NO enabled
 * expiration rule may overlap this data key prefix. The Kafka Controller,
 * rather than S3 Lifecycle, must eventually own COMMITTED physical GC.</p>
 *
 * <p>The check is cached for 30 seconds to avoid a control-plane S3 request
 * for every WAL upload. It cannot prevent an administrator changing the
 * bucket concurrently; operators must also monitor external configuration.
 * An inability to inspect the bucket, including AccessDenied or unsupported
 * S3 lifecycle APIs, denies new object publication instead of guessing.</p>
 */
final class S3BucketLifecycleGuard {
    private static final long RECHECK_NANOS = TimeUnit.SECONDS.toNanos(30);

    private final Supplier<List<LifecycleRule>> loadRules;
    private final String keyPrefix;
    private final LongSupplier nanoClock;
    private boolean checked;
    private long lastCheck;
    private RuntimeException lastFailure;

    S3BucketLifecycleGuard(S3Client client, S3ObjectStoreConfig config) {
        this(() -> client.getBucketLifecycleConfiguration(
            GetBucketLifecycleConfigurationRequest.builder().bucket(config.bucket()).build()
        ).rules(), config.keyPrefix(), System::nanoTime);
    }

    S3BucketLifecycleGuard(
        Supplier<List<LifecycleRule>> loadRules,
        String keyPrefix,
        LongSupplier nanoClock
    ) {
        this.loadRules = Objects.requireNonNull(loadRules, "loadRules");
        this.keyPrefix = Objects.requireNonNull(keyPrefix, "keyPrefix");
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    synchronized void requireSafeForUpload() {
        long now = nanoClock.getAsLong();
        if (!checked || now - lastCheck < 0L || now - lastCheck >= RECHECK_NANOS) {
            checked = true;
            lastCheck = now;
            lastFailure = checkRules();
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
    }

    private RuntimeException checkRules() {
        final List<LifecycleRule> rules;
        try {
            rules = Objects.requireNonNull(loadRules.get(),
                "Bucket lifecycle API returned null rules");
        } catch (S3Exception e) {
            if (e.statusCode() == 404 && e.awsErrorDetails() != null
                && "NoSuchLifecycleConfiguration".equals(e.awsErrorDetails().errorCode())) {
                return null;
            }
            return new IllegalStateException(
                "Unable to verify S3 Bucket lifecycle. New shared-storage object writes "
                    + "are refused until s3:GetLifecycleConfiguration is available and "
                    + "no expiration rule covers the Kafka data prefix.", e);
        } catch (RuntimeException e) {
            return new IllegalStateException(
                "Unable to verify S3 Bucket lifecycle; new shared-storage writes are refused.", e);
        }
        try {
            String blocking = firstExpiringRule(rules, keyPrefix);
            return blocking == null ? null : unsafeRuleFailure(blocking);
        } catch (RuntimeException e) {
            // Never leave a previously cached safe verdict in place after
            // an unexpected parse/SDK failure: fail closed on every upload.
            return new IllegalStateException("Cannot interpret S3 Bucket lifecycle rules.", e);
        }
    }

    private IllegalStateException unsafeRuleFailure(String rule) {
        return new IllegalStateException(
            "S3 Bucket lifecycle expiration rule '" + rule
                + "' applies to immutable shared-storage data keys under prefix '"
                + keyPrefix + "'. Disable the rule or isolate the Kafka data prefix; "
                + "Kafka retention and WAL replicas do not protect objects after external expiration."
        );
    }

    /**
     * null means no applicable expiration. Unrecognized filter shapes are
     * conservatively treated as potentially matching the entire bucket.
     */
    static String firstExpiringRule(List<LifecycleRule> rules, String keyPrefix) {
        Objects.requireNonNull(rules, "rules");
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        String dataPrefix = keyPrefix.isEmpty() ? "" : keyPrefix + "/";
        for (LifecycleRule rule : rules) {
            if (rule == null) {
                return "<invalid-null-rule>";
            }
            if (rule.status() == ExpirationStatus.DISABLED
                || !expiresCurrentObject(rule.expiration())) {
                continue;
            }
            String rulePrefix = filterPrefix(rule);
            if (dataPrefix.startsWith(rulePrefix) || rulePrefix.startsWith(dataPrefix)) {
                return rule.id() == null ? "<unnamed>" : rule.id();
            }
        }
        return null;
    }

    private static boolean expiresCurrentObject(LifecycleExpiration expiration) {
        if (expiration == null) {
            return false;
        }
        // ExpiredObjectDeleteMarker alone removes an obsolete delete marker,
        // not the current live object. Days/Date make it a real expiration.
        if (expiration.days() != null || expiration.date() != null) {
            return true;
        }
        // true removes only obsolete markers; false explicitly takes no
        // action. Missing mode is malformed/unknown, hence unsafe.
        return expiration.expiredObjectDeleteMarker() == null;
    }

    private static String filterPrefix(LifecycleRule rule) {
        LifecycleRuleFilter filter = rule.filter();
        if (filter == null) {
            return rule.prefix() == null ? "" : rule.prefix();
        }
        if (filter.prefix() != null) {
            return filter.prefix();
        }
        if (filter.and() != null && filter.and().prefix() != null) {
            return filter.and().prefix();
        }
        // Tag and object-size filters may match future object metadata.
        // Treat unknown or unqualified filters as potentially matching.
        return "";
    }
}
