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

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3BucketLifecycleGuardTest {
    @Test
    void oneHourOrOneDayExpirationIsNotKafkaThreeHourRetention() {
        // AWS lifecycle usually uses days. An S3-compatible backend can
        // implement a shorter interval; any external expiration is unsafe.
        assertEquals("expire-objects", S3BucketLifecycleGuard.firstExpiringRule(
            List.of(expiring("expire-objects", "objects/", true)), "objects"));
        assertEquals("whole-bucket", S3BucketLifecycleGuard.firstExpiringRule(
            List.of(expiring("whole-bucket", "", true)), "objects"));
    }

    @Test
    void unrelatedMonitoringPrefixCanExpireWithoutTouchingKafkaObjects() {
        assertNull(S3BucketLifecycleGuard.firstExpiringRule(
            List.of(expiring("monitoring", "metrics/", true)), "objects"));
        assertNull(S3BucketLifecycleGuard.firstExpiringRule(
            List.of(expiring("other-keyspace", "objects-old/", true)), "objects"));
        assertEquals("subset", S3BucketLifecycleGuard.firstExpiringRule(
            List.of(expiring("subset", "objects/123", true)), "objects"));
    }

    @Test
    void disabledAndExpiredDeleteMarkerOnlyRulesAreNotCurrentObjectExpiry() {
        assertNull(S3BucketLifecycleGuard.firstExpiringRule(List.of(
            expiring("disabled", "objects/", false),
            LifecycleRule.builder().id("delete-old-markers")
                .status(ExpirationStatus.ENABLED)
                .filter(LifecycleRuleFilter.builder().prefix("objects/").build())
                .expiration(LifecycleExpiration.builder()
                    .expiredObjectDeleteMarker(true).build())
                .build()
        ), "objects"));
    }

    @Test
    void explicitlyFalseExpiredObjectDeleteMarkerDoesNotDeleteLiveObjects() {
        LifecycleRule noop = LifecycleRule.builder().id("no-op")
            .status(ExpirationStatus.ENABLED)
            .filter(LifecycleRuleFilter.builder().prefix("objects/").build())
            .expiration(LifecycleExpiration.builder()
                .expiredObjectDeleteMarker(false).build()).build();
        assertNull(S3BucketLifecycleGuard.firstExpiringRule(List.of(noop), "objects"));
    }

    @Test
    void unknownOrTagOnlyRuleIsConservativelyGlobal() {
        LifecycleRule rule = LifecycleRule.builder().id("tag-filter")
            .status(ExpirationStatus.ENABLED)
            .filter(LifecycleRuleFilter.builder().build())
            .expiration(LifecycleExpiration.builder().days(1).build())
            .build();
        assertEquals("tag-filter", S3BucketLifecycleGuard.firstExpiringRule(
            List.of(rule), "objects"));
        assertEquals("root", S3BucketLifecycleGuard.firstExpiringRule(
            List.of(expiring("root", "any/", true)), ""));
    }

    @Test
    void noLifecycleConfigurationAllowsWritesButAccessDeniedFailsClosed() {
        AtomicInteger attempts = new AtomicInteger();
        S3Exception.Builder missingBuilder = S3Exception.builder();
        missingBuilder.statusCode(404);
        missingBuilder.awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("NoSuchLifecycleConfiguration").build());
        S3Exception absent = (S3Exception) missingBuilder.build();
        S3BucketLifecycleGuard noRules = new S3BucketLifecycleGuard(() -> {
            attempts.incrementAndGet();
            throw absent;
        }, "objects", () -> 1L);
        noRules.requireSafeForUpload();
        noRules.requireSafeForUpload();
        assertEquals(1, attempts.get());

        S3BucketLifecycleGuard denied = new S3BucketLifecycleGuard(() -> {
            throw S3Exception.builder().statusCode(403)
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build()).build();
        }, "objects", () -> 1L);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            denied::requireSafeForUpload);
        assertTrue(failure.getMessage().contains("GetLifecycleConfiguration"));
    }

    @Test
    void rechecksAfterThirtySecondsAndDoesNotReuseOldSafeVerdict() {
        AtomicLong time = new AtomicLong();
        AtomicInteger attempts = new AtomicInteger();
        S3BucketLifecycleGuard guard = new S3BucketLifecycleGuard(() -> {
            if (attempts.incrementAndGet() == 1) {
                return List.of();
            }
            return List.of(expiring("new-policy", "objects/", true));
        }, "objects", time::get);
        guard.requireSafeForUpload();
        time.set(29_000_000_000L);
        guard.requireSafeForUpload();
        assertEquals(1, attempts.get());
        time.set(30_000_000_000L);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            guard::requireSafeForUpload);
        assertTrue(failure.getMessage().contains("new-policy"));
        assertEquals(2, attempts.get());
    }

    private static LifecycleRule expiring(String id, String prefix, boolean enabled) {
        return LifecycleRule.builder().id(id)
            .status(enabled ? ExpirationStatus.ENABLED : ExpirationStatus.DISABLED)
            .filter(LifecycleRuleFilter.builder().prefix(prefix).build())
            .expiration(LifecycleExpiration.builder().days(1).build())
            .build();
    }
}
