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
package org.apache.kafka.storage.internals.log;

import java.io.IOException;

/**
 * Signals that a crash interrupted log initialization before any durable log state was published.
 *
 * <p>A {@link UnifiedLogFactory} may throw this only after proving that the partial partition directory contains no
 * durable log state and safely removing that directory. Startup may then skip the stale directory without taking the
 * entire parent log directory offline; authoritative metadata can recreate the partition with its durable identity.</p>
 */
public final class IncompleteLogInitializationException extends IOException {
    public IncompleteLogInitializationException(String message) {
        super(message);
    }
}
