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
package org.apache.kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.SourceWalProbeReport;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData.SourceWalProbeChallenge;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.replica.PartitionRetirementSourceProbe;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatResponse;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.BrokerRegistrationResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The broker lifecycle manager owns the broker state.
 *
 * <p>Its inputs are messages passed in from other parts of the broker and from the
 * controller: requests to start up, or shut down, for example. Its output are the broker
 * state and various futures that can be used to wait for broker state transitions to
 * occur.
 *
 * <p>The lifecycle manager handles registering the broker with the controller, as described
 * in KIP-631. After registration is complete, it handles sending periodic broker
 * heartbeats and processing the responses. Once the broker has caught up with the cluster metadata, it starts
 * sending the Uuid of its cordoned log directories in its heartbeats.
 *
 * <p>This code uses an event queue paradigm. Modifications get translated into events, which
 * are placed on the queue to be processed sequentially. As described in the JavaDoc for
 * each variable, most mutable state can be accessed only from that event queue thread.
 * In some cases we expose a volatile variable which can be read from any thread, but only
 * written from the event queue thread.
 */
public class BrokerLifecycleManager {

    private final Logger logger;
    private final KafkaEventQueue eventQueue;
    private final AbstractKafkaConfig config;
    private final Time time;
    private final Map<String, Uuid> logDirs;
    private final Runnable shutdownHook;
    private final Supplier<Boolean> cordonedLogDirsSupported;

    /**
     * Installed only by an explicit caller. A null sampler keeps the feature
     * completely dormant in production and never schedules WAL reads.
     * Both fields are accessed only from the lifecycle event queue.
     */
    private SourceWalProbeSampler sourceWalProbeSampler;
    private final SourceWalProbeCaptureState sourceWalProbeCaptureState = new SourceWalProbeCaptureState();

    /**
     * The broker id.
     */
    private final int nodeId;

    /**
     * The broker rack, or null if there is no configured rack.
     */
    private final Optional<String> rack;

    /**
     * How long to wait for registration to succeed before failing the startup process.
     */
    private final long initialTimeoutNs;

    /**
     * The broker incarnation ID.  This ID uniquely identifies each time we start the broker
     */
    private final Uuid incarnationId = Uuid.randomUuid();

    /**
     * A future which is completed just as soon as the broker has caught up with the latest
     * metadata offset for the first time.
     */
    private final CompletableFuture<Void> initialCatchUpFuture = new CompletableFuture<>();

    /**
     * A future which is completed when the broker is unfenced for the first time.
     */
    private final CompletableFuture<Void> initialUnfenceFuture = new CompletableFuture<>();

    /**
     * A future which is completed when controlled shutdown is done.
     */
    private final CompletableFuture<Void> controlledShutdownFuture = new CompletableFuture<>();

    /**
     * The broker epoch, or -1 if the broker has not yet registered.  This variable can only
     * be written from the event queue thread.
     */
    private volatile long brokerEpoch = -1L;

    /**
     * The current broker state.  This variable can only be written from the event queue
     * thread.
     */
    private volatile BrokerState state = BrokerState.NOT_RUNNING;

    /**
     * A thread-safe callback function which gives this manager the current highest metadata
     * offset.  This variable can only be read or written from the event queue thread.
     */
    private Supplier<Long> highestMetadataOffsetProvider;

    /**
     * True only if we are ready to unfence the broker.  This variable can only be read or
     * written from the event queue thread.
     */
    private boolean readyToUnfence = false;

    /**
     * Map of accumulated offline directories. The value is true if the directory has been successfully communicated
     * to the Controller.
     * This variable can only be read or written from the event queue thread.
     */
    private final Map<Uuid, Boolean> offlineDirs = new HashMap<>();

    /**
     * Set of cordoned log directories. The is null at startup until the broker has caught up with the metadata
     * This variable can only be read or written from the event queue thread.
     */
    private Set<Uuid> cordonedLogDirs;

    /**
     * True if we sent an event queue to the active controller requesting controlled
     * shutdown.  This variable can only be read or written from the event queue thread.
     */
    private boolean gotControlledShutdownResponse = false;

    /**
     * Whether this broker is registered with the controller quorum.
     * This variable can only be read or written from the event queue thread.
     */
    private boolean registered = false;

    /**
     * True if a request has been sent and a response or timeout has not yet been processed.
     * This variable can only be read or written from the event queue thread.
     */
    private boolean communicationInFlight = false;

    /**
     * True if we should schedule the next communication immediately. This is used to delay
     * an immediate scheduling of a communication event if one is already in flight.
     * This variable can only be read or written from the event queue thread.
     */
    private boolean nextSchedulingShouldBeImmediate = false;

    /**
     * True if the initial registration succeeded.  This variable can only be read or
     * written from the event queue thread.
     */
    private boolean initialRegistrationSucceeded = false;

    /**
     * The cluster ID, or null if this manager has not been started yet.  This variable can
     * only be read or written from the event queue thread.
     */
    private String clusterId;

    /**
     * The listeners which this broker advertises.  This variable can only be read or
     * written from the event queue thread.
     */
    private ListenerCollection advertisedListeners;

    /**
     * The features supported by this broker.  This variable can only be read or written
     * from the event queue thread.
     */
    private Map<String, VersionRange> supportedFeatures;

    /**
     * The channel manager, or null if this manager has not been started yet.  This variable
     * can only be read or written from the event queue thread.
     */
    private NodeToControllerChannelManager channelManager;

    /**
     * The broker epoch from the previous run, or empty if the epoch is not found.
     */
    private volatile OptionalLong previousBrokerEpoch = OptionalLong.empty();

    public BrokerLifecycleManager(
            AbstractKafkaConfig config,
            Time time,
            String threadNamePrefix,
            Map<String, Uuid> logDirs) {
        this(config, time, threadNamePrefix, logDirs, () -> { }, () -> false);
    }

    public BrokerLifecycleManager(
            AbstractKafkaConfig config,
            Time time,
            String threadNamePrefix,
            Map<String, Uuid> logDirs,
            Runnable shutdownHook,
            Supplier<Boolean> cordonedLogDirsSupported) {
        this.config = config;
        this.time = time;
        this.logDirs = logDirs;
        this.shutdownHook = shutdownHook;
        this.cordonedLogDirsSupported = cordonedLogDirsSupported;
        LogContext logContext = new LogContext("[BrokerLifecycleManager id=" + this.config.nodeId() + "] ");
        this.logger = logContext.logger(BrokerLifecycleManager.class);
        this.nodeId = config.nodeId();
        this.rack = config.rack();
        this.initialTimeoutNs = MILLISECONDS.toNanos(config.initialRegistrationTimeoutMs());
        this.eventQueue = new KafkaEventQueue(
                time,
                logContext,
                threadNamePrefix + "lifecycle-manager-",
                new ShutdownEvent());
    }

    /**
     * Start the BrokerLifecycleManager.
     *
     * @param highestMetadataOffsetProvider Provides the current highest metadata offset.
     * @param channelManager                The NodeToControllerChannelManager to use.
     * @param clusterId                     The cluster ID.
     * @param advertisedListeners           The advertised listeners for this broker.
     * @param supportedFeatures             The features for this broker.
     * @param previousBrokerEpoch           The broker epoch before the reboot.
     */
    public void start(Supplier<Long> highestMetadataOffsetProvider,
               NodeToControllerChannelManager channelManager,
               String clusterId,
               ListenerCollection advertisedListeners,
               Map<String, VersionRange> supportedFeatures,
               OptionalLong previousBrokerEpoch) {
        this.previousBrokerEpoch = previousBrokerEpoch;
        eventQueue.append(new StartupEvent(highestMetadataOffsetProvider,
                channelManager, clusterId, advertisedListeners, supportedFeatures));
    }

    /**
     * Installs a read-only asynchronous capture adapter, or null to disable it.
     * Does not enable any network Source/WAL report or GC authorization.
     * Registration of a real native adapter must remain behind a separate
     * authenticated feature negotiation gate.
     */
    public void setSourceWalProbeSampler(SourceWalProbeSampler sampler) {
        eventQueue.append(() -> {
            sourceWalProbeCaptureState.clear();
            sourceWalProbeSampler = sampler;
        });
    }

    public CompletableFuture<Void> setReadyToUnfence() {
        eventQueue.append(new SetReadyToUnfenceEvent());
        return initialUnfenceFuture;
    }

    /**
     * Propagate directory failures to the controller.
     *
     * @param directory The ID for the directory that failed.
     */
    public void propagateDirectoryFailure(Uuid directory, long timeout) {
        eventQueue.append(new OfflineDirEvent(directory));
        // If we can't communicate the offline directory to the controller, we should shut down.
        eventQueue.scheduleDeferred("offlineDirFailure",
                new EventQueue.DeadlineFunction(time.nanoseconds() + MILLISECONDS.toNanos(timeout)),
                new OfflineDirBrokerFailureEvent(directory));
    }

    /**
     * Propagate directory cordoned to the controller.
     * @param directories The IDs for the directories that is cordoned.
     */
    public void propagateDirectoryCordoned(Set<Uuid> directories) {
        if (cordonedLogDirsSupported.get()) {
            eventQueue.append(new CordonedDirEvent(directories));
        }
    }

    public void resendBrokerRegistration() {
        eventQueue.append(new ResendBrokerRegistrationEvent());
    }

    private class ResendBrokerRegistrationEvent implements EventQueue.Event {
        @Override
        public void run() {
            registered = false;
            scheduleNextCommunicationImmediately();
        }
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    /**
     * Returns the immutable UUID used in this process's broker registration
     * requests. This alone does not prove that the controller still recognizes
     * the registration or that this broker is unfenced.
     */
    public Uuid incarnationId() {
        return incarnationId;
    }

    public BrokerState state() {
        return state;
    }

    public CompletableFuture<Void> initialCatchUpFuture() {
        return initialCatchUpFuture;
    }

    public CompletableFuture<Void> initialUnfenceFuture() {
        return initialUnfenceFuture;
    }

    public CompletableFuture<Void> controlledShutdownFuture() {
        return controlledShutdownFuture;
    }

    public KafkaEventQueue eventQueue() {
        return eventQueue;
    }

    private class BeginControlledShutdownEvent implements EventQueue.Event {
        @Override
        public void run() {
            switch (state) {
                case PENDING_CONTROLLED_SHUTDOWN ->
                    logger.info("Attempted to enter pending controlled shutdown state, but we are already in that state.");
                case RUNNING -> {
                    logger.info("Beginning controlled shutdown.");
                    sourceWalProbeCaptureState.clear();
                    state = BrokerState.PENDING_CONTROLLED_SHUTDOWN;
                    // Send the next heartbeat immediately in order to let the controller
                    // begin processing the controlled shutdown as soon as possible.
                    scheduleNextCommunicationImmediately();
                }
                default -> {
                    logger.info("Skipping controlled shutdown because we are in state {}.", state);
                    beginShutdown();
                }
            }
        }
    }

    /**
     * Enter the controlled shutdown state if we are in RUNNING state.
     * Or, if we're not running, shut down immediately.
     */
    public void beginControlledShutdown() {
        eventQueue.append(new BeginControlledShutdownEvent());
    }

    /**
     * Start shutting down the BrokerLifecycleManager, but do not block.
     */
    public void beginShutdown() {
        eventQueue.beginShutdown("beginShutdown");
    }

    /**
     * Shut down the BrokerLifecycleManager and block until all threads are joined.
     */
    public void close() throws InterruptedException {
        beginShutdown();
        eventQueue.close();
    }

    private class SetReadyToUnfenceEvent implements EventQueue.Event {
        @Override
        public void run() {
            readyToUnfence = true;
            scheduleNextCommunicationImmediately();
        }
    }

    private class OfflineDirEvent implements EventQueue.Event {

        private final Uuid dir;

        OfflineDirEvent(Uuid dir) {
            this.dir = dir;
        }

        @Override
        public void run() {
            offlineDirs.put(dir, false);
            if (registered) {
                scheduleNextCommunicationImmediately();
            }
        }
    }

    private class OfflineDirBrokerFailureEvent implements EventQueue.Event {

        private final Uuid offlineDir;

        OfflineDirBrokerFailureEvent(Uuid offlineDir) {
            this.offlineDir = offlineDir;
        }

        @Override
        public void run() {
            if (!offlineDirs.getOrDefault(offlineDir, false)) {
                logger.error("Shutting down because couldn't communicate offline log dir {} with controllers", offlineDir);
                shutdownHook.run();
            }
        }
    }

    private class CordonedDirEvent implements EventQueue.Event {

        private final Set<Uuid> dirs;

        CordonedDirEvent(Set<Uuid> dirs) {
            this.dirs = dirs;
        }

        @Override
        public void run() {
            cordonedLogDirs = dirs;
            if (registered) {
                scheduleNextCommunicationImmediately();
            }
        }
    }

    private class StartupEvent implements EventQueue.Event {

        private final Supplier<Long> highestMetadataOffsetProvider;
        private final NodeToControllerChannelManager channelManager;
        private final String clusterId;
        private final ListenerCollection advertisedListeners;
        private final Map<String, VersionRange> supportedFeatures;

        StartupEvent(Supplier<Long> highestMetadataOffsetProvider,
                     NodeToControllerChannelManager channelManager,
                     String clusterId,
                     ListenerCollection advertisedListeners,
                     Map<String, VersionRange> supportedFeatures) {
            this.highestMetadataOffsetProvider = highestMetadataOffsetProvider;
            this.channelManager = channelManager;
            this.clusterId = clusterId;
            this.advertisedListeners = advertisedListeners;
            this.supportedFeatures = supportedFeatures;
        }

        @Override
        public void run() {
            BrokerLifecycleManager.this.highestMetadataOffsetProvider = this.highestMetadataOffsetProvider;
            BrokerLifecycleManager.this.channelManager = channelManager;
            BrokerLifecycleManager.this.channelManager.start();
            state = BrokerState.STARTING;
            BrokerLifecycleManager.this.clusterId = clusterId;
            BrokerLifecycleManager.this.advertisedListeners = advertisedListeners.duplicate();
            BrokerLifecycleManager.this.supportedFeatures = Map.copyOf(supportedFeatures);
            eventQueue.scheduleDeferred("initialRegistrationTimeout",
                    new EventQueue.DeadlineFunction(time.nanoseconds() + initialTimeoutNs),
                    new RegistrationTimeoutEvent());
            sendBrokerRegistration();
            logger.info("Incarnation {} of broker {} in cluster {} is now STARTING.", incarnationId, nodeId, clusterId);
        }
    }

    private void sendBrokerRegistration() {
        BrokerRegistrationRequestData.FeatureCollection features = new BrokerRegistrationRequestData.FeatureCollection();
        supportedFeatures.forEach((name, range) ->
                features.add(new BrokerRegistrationRequestData.Feature()
                        .setName(name)
                        .setMinSupportedVersion(range.min())
                        .setMaxSupportedVersion(range.max()))
        );
        List<Uuid> sortedLogDirs = new ArrayList<>(logDirs.values());
        sortedLogDirs.sort(Uuid::compareTo);
        BrokerRegistrationRequestData data = new BrokerRegistrationRequestData()
            .setBrokerId(nodeId)
            .setIsMigratingZkBroker(false)
            .setClusterId(clusterId)
            .setFeatures(features)
            .setIncarnationId(incarnationId)
            .setListeners(advertisedListeners)
            .setRack(rack.orElse(null))
            .setPreviousBrokerEpoch(previousBrokerEpoch.orElse(-1L))
            .setLogDirs(sortedLogDirs);
        if (logger.isDebugEnabled()) {
            logger.debug("Sending broker registration {}", data);
        }
        channelManager.sendRequest(new BrokerRegistrationRequest.Builder(data),
                new BrokerRegistrationResponseHandler());
        communicationInFlight = true;
    }

    // the response handler is not invoked from the event handler thread,
    // so it is not safe to update state here, instead, schedule an event
    // to continue handling the response on the event handler thread
    private class BrokerRegistrationResponseHandler implements ControllerRequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse response) {
            eventQueue.prepend(new BrokerRegistrationResponseEvent(response, false));
        }

        @Override
        public void onTimeout() {
            logger.info("Unable to register the broker because the RPC got timed out before it could be sent.");
            eventQueue.prepend(new BrokerRegistrationResponseEvent(null, true));
        }
    }

    private class BrokerRegistrationResponseEvent implements EventQueue.Event {

        private final ClientResponse response;
        private final boolean timedOut;

        BrokerRegistrationResponseEvent(ClientResponse response, boolean timedOut) {
            this.response = response;
            this.timedOut = timedOut;
        }

        @Override
        public void run() {
            communicationInFlight = false;
            if (timedOut) {
                scheduleNextCommunicationAfterFailure();
                return;
            }
            if (response.authenticationException() != null) {
                logger.error("Unable to register broker $nodeId because of an authentication exception.", response.authenticationException());
                scheduleNextCommunicationAfterFailure();
            } else if (response.versionMismatch() != null) {
                logger.error("Unable to register broker $nodeId because of an API version problem.", response.versionMismatch());
                scheduleNextCommunicationAfterFailure();
            } else if (response.responseBody() == null) {
                logger.warn("Unable to register broker {}.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else if (!(response.responseBody() instanceof BrokerRegistrationResponse message)) {
                logger.error("Unable to register broker {} because the controller returned an invalid response type.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else {
                Errors errorCode = Errors.forCode(message.data().errorCode());
                if (errorCode == Errors.NONE) {
                    sourceWalProbeCaptureState.clear();
                    brokerEpoch = message.data().brokerEpoch();
                    registered = true;
                    initialRegistrationSucceeded = true;
                    logger.info("Successfully registered broker {} with broker epoch {}", nodeId, brokerEpoch);
                    scheduleNextCommunicationImmediately(); // Immediately send a heartbeat
                } else {
                    logger.info("Unable to register broker {} because the controller returned error {}", nodeId, errorCode);
                    scheduleNextCommunicationAfterFailure();
                }
            }
        }
    }

    private void sendBrokerHeartbeat() {
        Long metadataOffset = highestMetadataOffsetProvider.get();
        BrokerHeartbeatRequestData data = new BrokerHeartbeatRequestData()
            .setBrokerEpoch(brokerEpoch)
            .setBrokerId(nodeId)
            .setCurrentMetadataOffset(metadataOffset)
            .setWantFence(!readyToUnfence)
            .setWantShutDown(state == BrokerState.PENDING_CONTROLLED_SHUTDOWN)
            .setOfflineLogDirs(new ArrayList<>(offlineDirs.keySet()));
        if (initialCatchUpFuture.isDone() && !initialCatchUpFuture.isCompletedExceptionally() && cordonedLogDirsSupported.get()) {
            data.setCordonedLogDirs(List.copyOf(cordonedLogDirs));
        }
        if (logger.isTraceEnabled()) {
            logger.trace("Sending broker heartbeat {}", data);
        }
        BrokerHeartbeatResponseHandler handler = new BrokerHeartbeatResponseHandler(offlineDirs.keySet());
        channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data), handler);
        communicationInFlight = true;
    }

    // the response handler is not invoked from the event handler thread,
    // so it is not safe to update state here, instead, schedule an event
    // to continue handling the response on the event handler thread
    private class BrokerHeartbeatResponseHandler implements ControllerRequestCompletionHandler {

        private final Iterable<Uuid> currentOfflineDirs;

        BrokerHeartbeatResponseHandler(Iterable<Uuid> currentOfflineDirs) {
            this.currentOfflineDirs = currentOfflineDirs;
        }

        @Override
        public void onComplete(ClientResponse response) {
            eventQueue.prepend(new BrokerHeartbeatResponseEvent(response, false, currentOfflineDirs));
        }

        @Override
        public void onTimeout() {
            logger.info("Unable to send a heartbeat because the RPC got timed out before it could be sent.");
            eventQueue.prepend(new BrokerHeartbeatResponseEvent(null, true, currentOfflineDirs));
        }
    }

    private class BrokerHeartbeatResponseEvent implements EventQueue.Event {

        private final ClientResponse response;
        private final boolean timedOut;
        private final Iterable<Uuid> currentOfflineDirs;

        BrokerHeartbeatResponseEvent(ClientResponse response, boolean timedOut, Iterable<Uuid> currentOfflineDirs) {
            this.response = response;
            this.timedOut = timedOut;
            this.currentOfflineDirs = currentOfflineDirs;
        }

        @SuppressWarnings({"CyclomaticComplexity"})
        @Override
        public void run() {
            communicationInFlight = false;
            if (timedOut) {
                sourceWalProbeCaptureState.clear();
                scheduleNextCommunicationAfterFailure();
                return;
            }
            if (response.authenticationException() != null) {
                sourceWalProbeCaptureState.clear();
                logger.error("Unable to send broker heartbeat for {} because of an authentication exception.",
                        nodeId, response.authenticationException());
                scheduleNextCommunicationAfterFailure();
            } else if (response.versionMismatch() != null) {
                sourceWalProbeCaptureState.clear();
                logger.error("Unable to send broker heartbeat for {} because of an API version problem.", nodeId, response.versionMismatch());
                scheduleNextCommunicationAfterFailure();
            } else if (response.responseBody() == null) {
                sourceWalProbeCaptureState.clear();
                logger.warn("Unable to send broker heartbeat for {}. Retrying.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else if (!(response.responseBody() instanceof BrokerHeartbeatResponse message)) {
                sourceWalProbeCaptureState.clear();
                logger.error("Unable to send broker heartbeat for {} because the controller returned an invalid response type.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else {
                Errors errorCode = Errors.forCode(message.data().errorCode());
                if (errorCode == Errors.NONE) {
                    BrokerHeartbeatResponseData responseData = message.data();
                    inspectSourceWalChallenge(response, responseData);
                    currentOfflineDirs.forEach(cur -> offlineDirs.put(cur, true));
                    switch (state) {
                        case STARTING -> {
                            if (responseData.isCaughtUp()) {
                                logger.info("The broker has caught up. Transitioning from STARTING to RECOVERY.");
                                state = BrokerState.RECOVERY;
                                initialCatchUpFuture.complete(null);
                                // Now that the broker has caught up with the latest metadata, the configuration should
                                // be up to date, so we can retrieve the cordoned log dirs to include them in the
                                // next heartbeat request
                                cordonedLogDirs = config.cordonedLogDirs().stream()
                                    .flatMap(logDir -> Optional.ofNullable(logDirs.get(logDir)).stream())
                                    .collect(Collectors.toSet());
                            } else {
                                logger.debug("The broker is STARTING. Still waiting to catch up with cluster metadata.");
                            }
                            // Schedule the heartbeat after only 10 ms so that in the case where
                            // there is no recovery work to be done, we start up a bit quicker.
                            scheduleNextCommunication(NANOSECONDS.convert(10, MILLISECONDS));
                        }
                        case RECOVERY -> {
                            if (!responseData.isFenced()) {
                                logger.info("The broker has been unfenced. Transitioning from RECOVERY to RUNNING.");
                                initialUnfenceFuture.complete(null);
                                state = BrokerState.RUNNING;
                            } else {
                                logger.info("The broker is in RECOVERY.");
                            }
                            scheduleNextCommunicationAfterSuccess();
                        }
                        case RUNNING -> {
                            logger.debug("The broker is RUNNING. Processing heartbeat response.");
                            scheduleNextCommunicationAfterSuccess();
                        }
                        case PENDING_CONTROLLED_SHUTDOWN -> {
                            if (!responseData.shouldShutDown()) {
                                logger.info("The broker is in PENDING_CONTROLLED_SHUTDOWN state, still waiting " +
                                        "for the active controller.");
                                if (!gotControlledShutdownResponse) {
                                    // If this is the first pending controlled shutdown response we got,
                                    // schedule our next heartbeat a little bit sooner than we usually would.
                                    // In the case where controlled shutdown completes quickly, this will
                                    // speed things up a little bit.
                                    scheduleNextCommunication(NANOSECONDS.convert(50, MILLISECONDS));
                                } else {
                                    scheduleNextCommunicationAfterSuccess();
                                }
                            } else {
                                logger.info("The controller has asked us to exit controlled shutdown.");
                                beginShutdown();
                            }
                            gotControlledShutdownResponse = true;
                        }
                        case SHUTTING_DOWN -> logger.info("The broker is SHUTTING_DOWN. Ignoring heartbeat response.");
                        default -> {
                            logger.error("Unexpected broker state {}", state);
                            scheduleNextCommunicationAfterSuccess();
                        }
                    }
                } else {
                    sourceWalProbeCaptureState.clear();
                    logger.warn("Broker {} sent a heartbeat request but received error {}.", nodeId, errorCode);
                    scheduleNextCommunicationAfterFailure();
                }
            }
        }
    }

    /**
     * Observe a challenge on the real Controller heartbeat response path.
     * A matched v3 challenge is intentionally discarded: neither the
     * provider of actual native Source/WAL evidence nor per-broker network
     * authentication is wired into this lifecycle manager yet.
     *
     * <p>Never allow a speculative v3 field to affect broker readiness,
     * registration, ordinary heartbeat scheduling or GC authority.</p>
     */
    private void inspectSourceWalChallenge(ClientResponse response, BrokerHeartbeatResponseData data) {
        if (data.sourceWalProbeChallenge() == null) {
            return;
        }
        if (data.isFenced() || data.shouldShutDown() || !data.isCaughtUp()) {
            sourceWalProbeCaptureState.clear();
            return;
        }
        short requestVersion = response.requestHeader() == null
            ? (short) -1 : response.requestHeader().apiVersion();
        Optional<PartitionRetirementSourceProbe.Request> request =
            eligibleSourceWalChallenge(data, requestVersion);
        request.ifPresent(this::scheduleSourceWalCapture);
    }

    private Optional<PartitionRetirementSourceProbe.Request> eligibleSourceWalChallenge(
        BrokerHeartbeatResponseData data,
        short requestVersion
    ) {
        BrokerHeartbeatSourceProbeChallengeGate.Finding finding =
            BrokerHeartbeatSourceProbeChallengeGate.inspect(
                data, requestVersion, clusterId, nodeId, brokerEpoch, incarnationId, state, registered
            );
        if (finding != BrokerHeartbeatSourceProbeChallengeGate.Finding.MATCHED_UNTRUSTED
            || sourceWalProbeSampler == null) {
            logger.debug("Ignoring non-authorizing Controller Source/WAL challenge ({}).", finding);
            return Optional.empty();
        }
        return BrokerHeartbeatSourceProbeChallengeGate.diagnosticRequest(
            data, requestVersion, clusterId, nodeId, brokerEpoch, incarnationId, state, registered
        );
    }

    private void scheduleSourceWalCapture(PartitionRetirementSourceProbe.Request request) {
        SourceWalProbeCaptureState.Capture capture =
            sourceWalProbeCaptureState.begin(request, time.nanoseconds());
        if (capture == null) {
            return;
        }
        try {
            CompletionStage<Optional<PartitionRetirementSourceProbe.Report>> future =
                sourceWalProbeSampler.capture(request);
            if (future == null) {
                sourceWalProbeCaptureState.abandon(capture);
                return;
            }
            future.whenComplete((report, error) -> enqueueSourceWalCapture(capture, report, error));
        } catch (RuntimeException failure) {
            logger.debug("Local Source/WAL diagnostic capture could not be scheduled.", failure);
            sourceWalProbeCaptureState.abandon(capture);
        }
    }

    private void enqueueSourceWalCapture(
        SourceWalProbeCaptureState.Capture capture,
        Optional<PartitionRetirementSourceProbe.Report> report,
        Throwable error
    ) {
        try {
            eventQueue.append(() -> {
                boolean retained = sourceWalProbeCaptureState.complete(
                    capture, report, error, time.nanoseconds(),
                    nodeId, brokerEpoch, incarnationId, state, registered
                );
                logger.debug("Completed local, non-authorizing Source/WAL diagnostic: {}", retained);
            });
        } catch (RejectedExecutionException closed) {
            // No callback may resurrect a closed lifecycle manager.
        }
    }

    private void scheduleNextCommunicationImmediately() {
        scheduleNextCommunication(0);
    }

    private void scheduleNextCommunicationAfterFailure() {
        nextSchedulingShouldBeImmediate = false; // never immediately reschedule after a failure
        scheduleNextCommunication(NANOSECONDS.convert(config.brokerHeartbeatIntervalMs(), MILLISECONDS));
    }

    private void scheduleNextCommunicationAfterSuccess() {
        scheduleNextCommunication(NANOSECONDS.convert(config.brokerHeartbeatIntervalMs(), MILLISECONDS));
    }

    private void scheduleNextCommunication(long intervalNs) {
        long adjustedIntervalNs = nextSchedulingShouldBeImmediate ? 0 : intervalNs;
        nextSchedulingShouldBeImmediate = false;
        logger.trace("Scheduling next communication at {}ms from now.", MILLISECONDS.convert(adjustedIntervalNs, NANOSECONDS));
        long deadlineNs = time.nanoseconds() + adjustedIntervalNs;
        eventQueue.scheduleDeferred("communication", new EventQueue.DeadlineFunction(deadlineNs), new CommunicationEvent());
    }

    private class RegistrationTimeoutEvent implements EventQueue.Event {
        @Override
        public void run() {
            if (!initialRegistrationSucceeded) {
                logger.error("Shutting down because we were unable to register with the controller quorum.");
                eventQueue.beginShutdown("registrationTimeout");
            }
        }
    }

    private class CommunicationEvent implements EventQueue.Event {
        @Override
        public void run() {
            if (communicationInFlight) {
                logger.trace("Delaying communication because there is already one in flight.");
                nextSchedulingShouldBeImmediate = true;
            } else if (registered) {
                sendBrokerHeartbeat();
            } else {
                sendBrokerRegistration();
            }
        }
    }

    private class ShutdownEvent implements EventQueue.Event {
        @Override
        public void run() throws InterruptedException {
            sourceWalProbeCaptureState.clear();
            sourceWalProbeSampler = null;
            logger.info("Transitioning from {} to {}.", state, BrokerState.SHUTTING_DOWN);
            state = BrokerState.SHUTTING_DOWN;
            controlledShutdownFuture.complete(null);
            initialCatchUpFuture.cancel(false);
            initialUnfenceFuture.cancel(false);
            if (channelManager != null) {
                channelManager.shutdown();
                channelManager = null;
            }
        }
    }

    /**
     * Invoked on the lifecycle event queue. Implementations MUST return
     * promptly with a CompletionStage; all native Source/WAL I/O must run
     * off-thread. This callback never grants Controller authority or
     * permission to delete an object.
     */
    @FunctionalInterface
    public interface SourceWalProbeSampler {
        CompletionStage<Optional<PartitionRetirementSourceProbe.Report>> capture(
            PartitionRetirementSourceProbe.Request challenge
        );
    }

    /**
     * One-slot diagnostic mailbox. All methods run on the broker lifecycle
     * event thread; an asynchronous producer can complete only by enqueuing
     * its result back onto that thread. A capture is never automatically
     * attached to BrokerHeartbeat, so no unauthenticated traffic is emitted.
     */
    static final class SourceWalProbeCaptureState {
        static final long MAX_AGE_NS = TimeUnit.SECONDS.toNanos(5);
        static final int MAX_RECENT_NONCES = 64;

        static final class Capture {
            private final PartitionRetirementSourceProbe.Request request;
            private final long startedNs;

            private Capture(PartitionRetirementSourceProbe.Request request, long startedNs) {
                this.request = request;
                this.startedNs = startedNs;
            }
        }

        // Keep this fence across provider replacement: an abandoned Future
        // might still be executing native I/O, even after its result is void.
        private Capture active;
        private Capture pending;
        private Capture ready;
        private PartitionRetirementSourceProbe.Report report;
        // Track all recent nonces, not just the immediately preceding one.
        // Fail closed on saturation instead of evicting an unexpired replay
        // fence. This remains bounded even under repeated challenges.
        private final Map<Uuid, Long> recentNonces = new LinkedHashMap<>();

        Capture begin(PartitionRetirementSourceProbe.Request request, long nowNs) {
            Objects.requireNonNull(request, "request");
            // Bound native Source/WAL capture concurrency to one. An unfinished
            // provider must not cause unbounded tasks as heartbeats arrive.
            if (active != null) {
                return null;
            }
            // Never accept a replay among multiple in-flight/previously
            // processed challenges within the five-second diagnostic TTL.
            expireRecentNonces(nowNs);
            if (recentNonces.containsKey(request.nonce())
                || recentNonces.size() >= MAX_RECENT_NONCES) {
                return null;
            }
            recentNonces.put(request.nonce(), nowNs);
            pending = new Capture(request, nowNs);
            active = pending;
            ready = null;
            report = null;
            return pending;
        }

        boolean complete(
            Capture capture,
            Optional<PartitionRetirementSourceProbe.Report> observed,
            Throwable error,
            long nowNs,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId,
            BrokerState state,
            boolean registered
        ) {
            // Even a revoked capture releases its underlying native I/O
            // fence only after its completion returns to this event queue.
            if (active == capture) {
                active = null;
            }
            // Compare by object identity: late callbacks from an earlier
            // provider/registration cannot resurrect revoked evidence.
            if (pending == null || pending != capture) {
                return false;
            }
            pending = null;
            if (error != null || observed == null || observed.isEmpty()
                || !eligible(capture, nowNs, brokerId, brokerEpoch, incarnationId, state, registered)) {
                return false;
            }
            PartitionRetirementSourceProbe.Report candidate = observed.get();
            if (!capture.request.equals(candidate.request())) {
                return false;
            }
            ready = capture;
            report = candidate;
            return true;
        }

        Optional<PartitionRetirementSourceProbe.Report> takeOnce(
            long nowNs,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId,
            BrokerState state,
            boolean registered
        ) {
            Capture staged = ready;
            PartitionRetirementSourceProbe.Report stagedReport = report;
            ready = null;
            report = null;
            if (staged == null || !eligible(
                staged, nowNs, brokerId, brokerEpoch, incarnationId, state, registered
            )) {
                return Optional.empty();
            }
            return Optional.ofNullable(stagedReport);
        }

        /**
         * Convert one already verified *local diagnostic* to the optional
         * unstable v3 heartbeat wire value. The caller must still have its
         * own authenticated return-channel policy and version negotiation.
         *
         * <p>This method is never invoked by sendBrokerHeartbeat: reporting
         * self-asserted WAL bytes is disabled in production.</p>
         */
        Optional<SourceWalProbeReport> takeWireOnce(
            long nowNs,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId,
            BrokerState state,
            boolean registered
        ) {
            return takeOnce(
                nowNs, brokerId, brokerEpoch, incarnationId, state, registered
            ).map(SourceWalProbeCaptureState::toWire);
        }

        private static SourceWalProbeReport toWire(PartitionRetirementSourceProbe.Report report) {
            PartitionRetirementSourceProbe.Request request = report.request();
            return new SourceWalProbeReport()
                .setNonce(request.nonce())
                .setClusterId(request.clusterId())
                .setTopicId(request.topicId())
                .setPartitionId(request.partitionId())
                .setBrokerIncarnationId(request.brokerIncarnationId())
                .setSourceLeaderEpoch(request.sourceLeaderEpoch())
                .setExpectedPreviousAuthorityOffset(request.expectedPreviousAuthorityOffset())
                .setNextAuthorityOffset(request.nextAuthorityOffset())
                .setLocalLeadershipGeneration(report.localLeadershipGeneration())
                .setNativeLogStartOffset(report.nativeLogStartOffset())
                .setNativeHighWatermark(report.nativeHighWatermark())
                .setNativeLogEndOffset(report.nativeLogEndOffset())
                .setWalMutationRevision(report.observedWalMutationRevision())
                .setWalAdmissionRevision(report.observedWalAdmissionRevision());
        }

        void abandon(Capture capture) {
            if (active == capture) {
                active = null;
            }
            if (pending == capture) {
                pending = null;
            }
        }

        void clear() {
            pending = null;
            ready = null;
            report = null;
            // Keep active until the old asynchronous callback has actually
            // finished, preventing concurrent I/O across provider changes.
            // Keep recentNonces, including those from revoked captures, so
            // neither reconfiguration nor heartbeat errors reopen replay.
        }

        private void expireRecentNonces(long nowNs) {
            Iterator<Long> times = recentNonces.values().iterator();
            while (times.hasNext()) {
                long age = nowNs - times.next();
                // Signed subtraction also handles nanoTime crossing the
                // Long.MAX_VALUE boundary for five-second intervals.
                if (age >= MAX_AGE_NS) {
                    times.remove();
                }
            }
        }

        private static boolean eligible(
            Capture capture,
            long nowNs,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId,
            BrokerState state,
            boolean registered
        ) {
            if (!registered || state != BrokerState.RUNNING) {
                return false;
            }
            long elapsed = nowNs - capture.startedNs;
            if (elapsed < 0L || elapsed >= MAX_AGE_NS) {
                return false;
            }
            PartitionRetirementSourceProbe.Request request = capture.request;
            return request.brokerId() == brokerId && request.brokerEpoch() == brokerEpoch
                && request.brokerIncarnationId().equals(incarnationId);
        }
    }

    /**
     * Local, non-authorizing parser for optional unstable v3 challenges.
     * This is nested to keep the Broker lifecycle's GA production fingerprint
     * anchored to its existing, release-gated source path.
     */
    static final class BrokerHeartbeatSourceProbeChallengeGate {
        enum Finding {
            ABSENT,
            VERSION_NOT_NEGOTIATED,
            BROKER_NOT_RUNNING,
            IDENTITY_MISMATCH,
            MALFORMED,
            MATCHED_UNTRUSTED
        }
    
        private BrokerHeartbeatSourceProbeChallengeGate() {
        }
    
        static Finding inspect(
            BrokerHeartbeatResponseData response,
            short requestVersion,
            String clusterId,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId,
            BrokerState state,
            boolean registered
        ) {
            Objects.requireNonNull(response, "response");
            SourceWalProbeChallenge challenge = response.sourceWalProbeChallenge();
            if (challenge == null) {
                return Finding.ABSENT;
            }
            // Reject future versions until their challenge semantics have been reviewed.
            if (requestVersion != 3) {
                return Finding.VERSION_NOT_NEGOTIATED;
            }
            if (!registered || state != BrokerState.RUNNING || brokerEpoch < 0L) {
                return Finding.BROKER_NOT_RUNNING;
            }
            if (!sameRegistration(challenge, clusterId, brokerId, brokerEpoch, incarnationId)) {
                return Finding.IDENTITY_MISMATCH;
            }
            if (!shapeValid(challenge)) {
                return Finding.MALFORMED;
            }
            // A syntactically matched, self-contained challenge is still not
            // permission to send or accept an unauthenticated source report.
            return Finding.MATCHED_UNTRUSTED;
        }
    
        static Optional<PartitionRetirementSourceProbe.Request> diagnosticRequest(
            BrokerHeartbeatResponseData response,
            short requestVersion,
            String clusterId,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId,
            BrokerState state,
            boolean registered
        ) {
            Finding finding = inspect(
                response, requestVersion, clusterId, brokerId, brokerEpoch, incarnationId, state, registered
            );
            if (finding != Finding.MATCHED_UNTRUSTED) {
                return Optional.empty();
            }
            SourceWalProbeChallenge wire = response.sourceWalProbeChallenge();
            return Optional.of(new PartitionRetirementSourceProbe.Request(
                wire.nonce(), wire.clusterId(), wire.topicId(), wire.partitionId(),
                wire.brokerId(), wire.brokerEpoch(), wire.brokerIncarnationId(),
                wire.sourceLeaderEpoch(), wire.expectedPreviousAuthorityOffset(),
                wire.nextAuthorityOffset(), wire.claimedLogStartOffset()
            ));
        }
    
        private static boolean sameRegistration(
            SourceWalProbeChallenge challenge,
            String clusterId,
            int brokerId,
            long brokerEpoch,
            Uuid incarnationId
        ) {
            return clusterId != null && clusterId.equals(challenge.clusterId())
                && brokerId == challenge.brokerId() && brokerEpoch == challenge.brokerEpoch()
                && incarnationId != null && incarnationId.equals(challenge.brokerIncarnationId());
        }
    
        private static boolean shapeValid(SourceWalProbeChallenge challenge) {
            if (challenge.nonce() == null || challenge.clusterId() == null
                || challenge.topicId() == null || challenge.brokerIncarnationId() == null) {
                return false;
            }
            try {
                new PartitionRetirementSourceProbe.Request(
                    challenge.nonce(), challenge.clusterId(), challenge.topicId(),
                    challenge.partitionId(), challenge.brokerId(), challenge.brokerEpoch(),
                    challenge.brokerIncarnationId(), challenge.sourceLeaderEpoch(),
                    challenge.expectedPreviousAuthorityOffset(), challenge.nextAuthorityOffset(),
                    challenge.claimedLogStartOffset()
                );
                return true;
            } catch (IllegalArgumentException invalid) {
                return false;
            }
        }
    }
}
