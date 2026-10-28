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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Objects;

public class BrokerHeartbeatRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<BrokerHeartbeatRequest> {
        private final BrokerHeartbeatRequestData data;
        private final boolean explicitSourceWalReport;

        public Builder(BrokerHeartbeatRequestData data) {
            super(ApiKeys.BROKER_HEARTBEAT);
            this.data = Objects.requireNonNull(data, "data").duplicate();
            this.explicitSourceWalReport = false;
            if (this.data.sourceWalProbeReport() != null) {
                throw new IllegalArgumentException(
                    "Source/WAL reports require an explicitly pinned v3 builder");
            }
        }

        /**
         * Use only for explicitly gated v3 diagnostics, not ordinary heartbeats.
         * Pinning both bounds prevents controller version negotiation from
         * silently downgrading a report to an older protocol.
         */
        public Builder(BrokerHeartbeatRequestData data, short reportVersion) {
            super(ApiKeys.BROKER_HEARTBEAT, reportVersion);
            this.data = Objects.requireNonNull(data, "data").duplicate();
            this.explicitSourceWalReport = true;
            if (reportVersion != 3 || this.data.sourceWalProbeReport() == null) {
                throw new IllegalArgumentException("A non-null Source/WAL report requires exact v3");
            }
        }

        @Override
        public BrokerHeartbeatRequest build(short version) {
            if (data.sourceWalProbeReport() != null && (!explicitSourceWalReport || version != 3)) {
                throw new UnsupportedVersionException(
                    "Source/WAL reports require an explicitly pinned v3 heartbeat");
            }
            // Do not share a mutable generated protocol message with the
            // caller or between successive builds of this request.
            return new BrokerHeartbeatRequest(data.duplicate(), version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final BrokerHeartbeatRequestData data;

    public BrokerHeartbeatRequest(BrokerHeartbeatRequestData data, short version) {
        super(ApiKeys.BROKER_HEARTBEAT, version);
        this.data = data;
    }

    @Override
    public BrokerHeartbeatRequestData data() {
        return data;
    }

    @Override
    public BrokerHeartbeatResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new BrokerHeartbeatResponse(new BrokerHeartbeatResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static BrokerHeartbeatRequest parse(Readable readable, short version) {
        return new BrokerHeartbeatRequest(new BrokerHeartbeatRequestData(readable, version),
                version);
    }
}
