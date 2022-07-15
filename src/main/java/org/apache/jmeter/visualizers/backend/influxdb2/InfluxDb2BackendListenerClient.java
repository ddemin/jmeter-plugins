/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jmeter.visualizers.backend.influxdb2;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import static org.apache.jmeter.visualizers.backend.influxdb2.Utils.*;

/**
 * Implementation of {@link AbstractBackendListenerClient} to write in an InfluxDB 2.0
 *
 * @since 5.3
 */
public class InfluxDb2BackendListenerClient extends AbstractBackendListenerClient implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDb2BackendListenerClient.class);
    private static final int MAX_POOL_SIZE = 1;
    private static final int DEFAULT_SEND_INTERVAL = 10;
    private static final Map<String, String> DEFAULT_ARGS = new LinkedHashMap<>();

    private static final String ARG_INFLUXDB_2_URL = "influxdb2_url";
    private static final String ARG_INFLUXDB_2_TOKEN = "influxdb2_token";
    private static final String ARG_INFLUXDB_2_ORG = "influxdb2_org";
    private static final String ARG_INFLUXDB_2_BUCKET_TRX = "influxdb2_bucket_for_trx";
    private static final String ARG_INFLUXDB_2_BUCKET_META = "influxdb2_bucket_for_meta";
    private static final String ARG_INTERVAL_SEC = "send_interval_sec";
    private static final String ARG_ROOT_ID = "root_uuid";
    private static final String ARG_LAUNCH_ID = "launch_uuid";
    private static final String ARG_ENV = "environment_name";
    private static final String ARG_SCENARIO = "load_scenario_name";
    private static final String ARG_VERSION = "env_version";
    private static final String ARG_DETAILS = "env_details";
    private static final String ARG_PROFILE = "load_profile_name";
    private static final String ARG_TAGS = "additional_meta_tags";
    private static final String ARG_ALLOWED_SAMPLERS_REGEX = "reported_samplers_regex";
    private static final String ARG_ALLOWED_VARIABLES_REGEX = "reported_variables_regex";
    private static final String ARG_STATISTIC_MODE = "statistic_mode";

    private static final String TAG_ROOT = "root";
    private static final String TAG_STARTED = "isStarted";

    static {
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_URL, "https://influxdb2_url:9999");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_TOKEN, "access_token");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_ORG, "org_name");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_BUCKET_TRX, "bucket_name_for_main_data");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_BUCKET_META, "bucket_name_for_metadata");
        DEFAULT_ARGS.put(ARG_INTERVAL_SEC, String.valueOf(DEFAULT_SEND_INTERVAL));

        DEFAULT_ARGS.put(ARG_ROOT_ID, "${__UUID()}");
        DEFAULT_ARGS.put(ARG_LAUNCH_ID, "${__UUID()}");
        DEFAULT_ARGS.put(ARG_ENV, "Environment name");
        DEFAULT_ARGS.put(ARG_PROFILE, "Load profile name");
        DEFAULT_ARGS.put(ARG_SCENARIO, "JMeter test/scenario name");
        DEFAULT_ARGS.put(ARG_VERSION, "N/A");
        DEFAULT_ARGS.put(ARG_DETAILS, "N/A");
        DEFAULT_ARGS.put(ARG_TAGS, "");

        DEFAULT_ARGS.put(ARG_ALLOWED_SAMPLERS_REGEX, ".*");
        DEFAULT_ARGS.put(ARG_ALLOWED_VARIABLES_REGEX, "NOTHING");
        DEFAULT_ARGS.put(ARG_STATISTIC_MODE, "true");
    }

    private final ConcurrentHashMap<String, Boolean> labelsWhiteListCache = new ConcurrentHashMap<>();
    private final Queue<Map.Entry<String, String>> retryQueue = new ConcurrentLinkedQueue<>();

    private Pattern samplersFilter;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerHandle;

    private InfluxHttpClient influxClient;
    private boolean isReady;
    private String bucketMeta;
    private String bucketMetrics;
    private LineProtocolBuffer buffer;
    private BackendListenerContext context;

    public InfluxDb2BackendListenerClient() {
        super();
    }

    @Override
    public void run() {
        sendMeasurements();

        List<Map.Entry<String, String>> failedRepeatedReqs = new LinkedList<>();
        while (retryQueue.peek() != null) {
            Map.Entry<String, String> dataForBucket = retryQueue.poll();
            try {
                // TODO Hack, prevent very frequent requests
                Thread.sleep(1000);
                tryToSend(dataForBucket.getKey(), dataForBucket.getValue());
            } catch (IOException | InterruptedException e) {
                failedRepeatedReqs.add(dataForBucket);
            }
        }
        retryQueue.addAll(failedRepeatedReqs);
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        if (!isReady) {
            return;
        }

        for (SampleResult sampleResult : sampleResults) {
            boolean isAllowedLabel = labelsWhiteListCache.computeIfAbsent(
                    sampleResult.getSampleLabel(),
                    (k) -> samplersFilter.matcher(sampleResult.getSampleLabel()).find()
            );
            if (isAllowedLabel) {
                buffer.putSampleResult(sampleResult);
            }
        }
    }

    @Override
    public void setupTest(BackendListenerContext context) {
        LOG.info("Initialize InfluxDB2 listener...");
        Runtime.getRuntime().addShutdownHook(new Thread(this::cleanUpAndReset));

        this.context = context;

        this.bucketMeta = context.getParameter(ARG_INFLUXDB_2_BUCKET_TRX);
        this.bucketMetrics = context.getParameter(ARG_INFLUXDB_2_BUCKET_TRX);
        this.samplersFilter = Pattern.compile(context.getParameter(ARG_ALLOWED_SAMPLERS_REGEX, ""));
        this.buffer = new LineProtocolBuffer(
                context.getBooleanParameter(ARG_STATISTIC_MODE, true),
                context.getParameter(ARG_LAUNCH_ID),
                context.getIntParameter(ARG_INTERVAL_SEC)
        );


        this.influxClient = new InfluxHttpClient(
                context.getParameter(ARG_INFLUXDB_2_URL),
                context.getParameter(ARG_INFLUXDB_2_ORG),
                context.getParameter(ARG_INFLUXDB_2_TOKEN)
        );
        this.isReady = influxClient.isConnected();

        if (this.isReady) {
            start();
            LOG.info("Initialization completed");
        } else {
            LOG.error("Initialization failed. Please check the logs");
        }
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        super.teardownTest(context);
        LOG.info("Destroy InfluxDB2 listener...");
        cleanUpAndReset();
        LOG.info("Done!");
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        DEFAULT_ARGS.forEach(arguments::addArgument);
        return arguments;
    }

    private void start() {
        sendLaunchEvent(true);
        this.scheduler = Executors.newScheduledThreadPool(MAX_POOL_SIZE);
        this.timerHandle = scheduler.scheduleAtFixedRate(
                this,
                0,
                context.getIntParameter(ARG_INTERVAL_SEC),
                TimeUnit.SECONDS
        );
    }

    private void cleanUpAndReset() {
        if (!isReady) {
            return;
        }

        try {
            timerHandle.cancel(false);
            if (scheduler != null) {
                scheduler.shutdown();
                try {
                    scheduler.awaitTermination(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.error("Error waiting for end of scheduler");
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage());
        }

        try {
            // TODO
            Thread.sleep(10_000);
            sendLaunchEvent(false);
            sendMeasurements();
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage());
        } finally {
            labelsWhiteListCache.clear();
            isReady = false;
        }
    }

    private void sendMeasurements() {
        synchronized (this) {
            try {
                String lineProtocolData = buffer.packMeasurements();
                if (Strings.isNotBlank(lineProtocolData)) {
                    tryToSend(bucketMetrics, lineProtocolData);
                }
            } catch (Throwable tr) {
                LOG.error("Something goes wrong during InfluxDB integration: " + tr.getMessage());
            }
        }
    }

    private void sendLaunchEvent(boolean isTestStarted) {
        try {
            SortedMap<String, String> launchDefaultTags = new TreeMap<>();

            launchDefaultTags.put(TAG_ROOT, context.getParameter(ARG_ROOT_ID));
            launchDefaultTags.put(TAG_LAUNCH, context.getParameter(ARG_LAUNCH_ID));
            launchDefaultTags.put("environment", context.getParameter(ARG_ENV, NOT_AVAILABLE));
            launchDefaultTags.put("interval", context.getParameter(ARG_INTERVAL_SEC));
            launchDefaultTags.put("host", InetAddress.getLocalHost().getHostName());
            launchDefaultTags.put("profile", context.getParameter(ARG_PROFILE, NOT_AVAILABLE));
            launchDefaultTags.put(TAG_STARTED, String.valueOf(isTestStarted));

            List<String> additionalTagsEntries
                    = Arrays.asList(context.getParameter(ARG_TAGS, "").split(","));
            additionalTagsEntries.forEach(
                    tagEntry -> {
                        if (tagEntry.contains("=")) {
                            String[] keyValue = tagEntry.split("=");
                            launchDefaultTags.put(keyValue[0].trim(), keyValue[1].trim());
                        }
                    }
            );

            String launchEven = buffer.packLaunchEvent(
                    isTestStarted,
                    launchDefaultTags,
                    context.getParameter(ARG_SCENARIO, NOT_AVAILABLE),
                    context.getParameter(ARG_VERSION, NOT_AVAILABLE),
                    context.getParameter(ARG_DETAILS, NOT_AVAILABLE),
                    Pattern.compile(context.getParameter(ARG_ALLOWED_VARIABLES_REGEX, ""))
            );

            tryToSend(bucketMeta, launchEven);
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB events write operation: " + tr.getMessage());
        }

    }

    private void tryToSend(String bucket, String content) throws IOException, InterruptedException {
        if (!influxClient.tryToSend(bucket, content)) {
            retryQueue.add(new SimpleEntry<>(bucket, content));
        }
    }

}
