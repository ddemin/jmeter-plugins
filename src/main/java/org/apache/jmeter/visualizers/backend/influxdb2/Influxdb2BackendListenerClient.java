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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Ready;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.SamplerMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Implementation of {@link AbstractBackendListenerClient} to write in an InfluxDB 2.0
 *
 * @since 5.3
 */
public class Influxdb2BackendListenerClient extends AbstractBackendListenerClient implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Influxdb2BackendListenerClient.class);

    private ConcurrentHashMap<String, SamplerMetric> metricsPerSampler = new ConcurrentHashMap<>();

    private static final String ANNOTATION_MEASUREMENT = "jmeter_events";
    private static final String DEFAULT_MEASUREMENT = "jmeter_transactions";

    private static final int MAX_POOL_SIZE = 1;
    private static final int DEFAULT_SEND_INTERVAL = 10;
    private static final Object LOCK = new Object();
    private static final Map<String, String> DEFAULT_ARGS = new LinkedHashMap<>();

    private static final String INFLUXDB_2_URL = "influxdb2_url";
    private static final String INFLUXDB_2_TOKEN = "influxdb2_token";
    private static final String INFLUXDB_2_ORG = "influxdb2_org";
    private static final String INFLUXDB_2_BUCKET = "influxdb2_bucket";
    private static final String SEND_INTERVAL_SEC = "send_interval_sec";
    private static final String LAUNCH_ID = "launch_id";
    private static final String EVENTS_SCENARIO = "events_scenario";
    private static final String EVENTS_PROFILE = "events_profile";
    private static final String EVENTS_TAGS = "events_tags";
    private static final String SAMPLERS_REGEX = "samplers_regex";

    private static final String PATH_ANONYMIZE_REGEXP = "([0-9]+|[0-9a-z-]+-[0-9a-z-]+)";

    static {
        DEFAULT_ARGS.put(INFLUXDB_2_URL, "https://localhost:9999");
        DEFAULT_ARGS.put(INFLUXDB_2_TOKEN, "define_me");
        DEFAULT_ARGS.put(INFLUXDB_2_ORG, "some_org_name");
        DEFAULT_ARGS.put(INFLUXDB_2_BUCKET, "some_bucket_name");
        DEFAULT_ARGS.put(SEND_INTERVAL_SEC, String.valueOf(DEFAULT_SEND_INTERVAL));

        DEFAULT_ARGS.put(LAUNCH_ID, "${__UUID()}");
        DEFAULT_ARGS.put(EVENTS_SCENARIO, "JMeter test/scenario name");
        DEFAULT_ARGS.put(EVENTS_PROFILE, "Name of load profile");
        DEFAULT_ARGS.put(EVENTS_TAGS, "");

        DEFAULT_ARGS.put(SAMPLERS_REGEX, ".*");
    }

    private final List<Point> measurementsBuffer = new LinkedList<>();
    private final SortedMap<String, String> eventsTagsMap = new TreeMap<>();

    private long sendIntervalSec;
    private String launchId;
    private String samplersRegex;
    private Pattern samplersToFilter;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerHandle;
    private InfluxDBClient influxDBClient;

    public Influxdb2BackendListenerClient() {
        super();
    }

    @Override
    public void run() {
        sendMeasurements();
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        synchronized (LOCK) {
            for (SampleResult sampleResult : sampleResults) {
                Matcher matcher = samplersToFilter.matcher(sampleResult.getSampleLabel());
                if (matcher.find()) {
                    saveMeasurement(sampleResult);
                }
            }
        }
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        influxDBClient = InfluxDBClientFactory
                .create(
                        context.getParameter(INFLUXDB_2_URL),
                        context.getParameter(INFLUXDB_2_TOKEN).toCharArray(),
                        context.getParameter(INFLUXDB_2_ORG),
                        context.getParameter(INFLUXDB_2_BUCKET)
                )
                .enableGzip();
        Ready readiness = influxDBClient.ready();
        if (readiness == null || readiness.getStatus() != Ready.StatusEnum.READY) {
            throw new RuntimeException(
                    "InfluxDB2 server isn't ready: " +
                            (readiness == null ? "Can't check server readiness" : readiness.getUp())
                    );
        }

        sendIntervalSec = context.getIntParameter(SEND_INTERVAL_SEC);
        launchId = context.getParameter(LAUNCH_ID);

        eventsTagsMap.putAll(
                Splitter
                        .on(';')
                        .trimResults()
                        .omitEmptyStrings()
                        .withKeyValueSeparator('=')
                        .split(context.getParameter(EVENTS_TAGS))
        );
        eventsTagsMap.put("interval", String.valueOf(sendIntervalSec));
        eventsTagsMap.put("host", InetAddress.getLocalHost().getHostName());
        eventsTagsMap.put("profile", context.getParameter(EVENTS_PROFILE, "N/A"));
        eventsTagsMap.put("scenario", context.getParameter(EVENTS_SCENARIO, "N/A"));

        samplersRegex = context.getParameter(SAMPLERS_REGEX, "");
        samplersToFilter = Pattern.compile(samplersRegex);

        sendEvent(true);

        scheduler = Executors.newScheduledThreadPool(MAX_POOL_SIZE);
        this.timerHandle = scheduler.scheduleAtFixedRate(
                this,
                0,
                sendIntervalSec,
                TimeUnit.SECONDS
        );
    }


    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        try {
            sendEvent(false);

            boolean cancelState = timerHandle.cancel(false);
            log.debug("Canceled state: {}", cancelState);
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Error waiting for end of scheduler");
                Thread.currentThread().interrupt();
            }

            // Send last set of data before ending
            log.info("Sending last metrics");
            sendMeasurements();
        } finally {
            influxDBClient.close();
        }
        super.teardownTest(context);
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        DEFAULT_ARGS.forEach(arguments::addArgument);
        return arguments;
    }

    private void saveMeasurement(SampleResult sampleResult) {
        long timestampNano = sampleResult.getTimeStamp() * 1_000_000 + RandomUtils.nextLong(100000, 999999);

        String trx = sampleResult.getSampleLabel().trim();
        String code = sampleResult.getResponseCode();
        String ok = sampleResult.isSuccessful() ? "true" : "false";
        String endpoint;
        String server;
        if (sampleResult.getURL() != null) {
            endpoint = anonymizeUrl(sampleResult.getURL());
            server = sampleResult.getURL().getHost();
        } else {
            endpoint = "N/A";
            server = "N/A";
        }

        long allThreads = sampleResult.getAllThreads();
        long duration = sampleResult.getTime();
        String message = sampleResult.getResponseMessage();
        String assertion = sampleResult.getFirstAssertionFailureMessage();
        long inBytes = sampleResult.getBytesAsLong();
        long outBytes = sampleResult.getSentBytes();

        Point point = Point.measurement(DEFAULT_MEASUREMENT)
                .time(timestampNano, WritePrecision.NS)
                .addField("bin", inBytes)
                .addField("bout", outBytes)
                .addField("duration", duration)
                .addField("msg", Strings.isNullOrEmpty(assertion) ? message : assertion)
                .addField("threads", allThreads)
                .addTag("code", code)
                .addTag("endpoint", endpoint)
                .addTag("launch_uuid", launchId)
                .addTag("name", trx)
                .addTag("ok", ok)
                .addTag("server", server)
                .addTag("code", code);
        measurementsBuffer.add(point);
    }

    private void sendMeasurements() {
        synchronized (LOCK) {
            try {
                influxDBClient.getWriteApi().writePoints(measurementsBuffer);
            } catch (Throwable tr) {
                log.error("Something goes wrong during InfluxDB integration: " + tr.getMessage());
            }
            measurementsBuffer.clear();
        }
    }

    private void sendEvent(boolean isStartOfTest) throws UnknownHostException {
        eventsTagsMap.put("started", String.valueOf(isStartOfTest));

        log.debug("Send event with tags: {}", eventsTagsMap);
        influxDBClient.getWriteApi().writePoint(
                Point
                        .measurement(ANNOTATION_MEASUREMENT)
                        .addField("launch_uuid", launchId)
                        .addTags(eventsTagsMap)
                        .time(Instant.now(), WritePrecision.NS)
        );
    }

    private String anonymizeUrl(URL url) {
        return StringUtils
                .stripEnd(url.getPath(), "/ ")
                .replaceAll(PATH_ANONYMIZE_REGEXP, "{ID}");
    }

}
