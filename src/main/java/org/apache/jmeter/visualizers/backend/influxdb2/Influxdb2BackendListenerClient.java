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
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
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

    private static final Logger LOG = LoggerFactory.getLogger(Influxdb2BackendListenerClient.class);

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
    private static final String URL_ANONYMIZE_REGEX = "url_anonymize_regex";
    private static final String SAMPLERS_REGEX = "samplers_filter_regex";
    private static final String VARIABLES_REGEX = "variables_filter_regex";
    private static final String DEFAULT_URL_ANONYMIZE_REGEXP
            = "\\/([0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+|[0-9a-z-]+-[0-9a-z-]+|[0-9]+)";
    private static final String NOT_AVAILABLE = "N/A";
    private static final int MAX_CHARS_IN_MSG = 32;

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

        DEFAULT_ARGS.put(URL_ANONYMIZE_REGEX, DEFAULT_URL_ANONYMIZE_REGEXP);
        DEFAULT_ARGS.put(SAMPLERS_REGEX, ".*");
        DEFAULT_ARGS.put(VARIABLES_REGEX, "NOTHING");
    }

    private final List<Point> measurementsBuffer = new LinkedList<>();
    private final SortedMap<String, String> eventsTagsMap = new TreeMap<>();

    private long sendIntervalSec;
    private String launchId;
    private String samplersRegex;
    private Pattern samplersToFilter;
    private String variablesRegex;
    private Pattern variablesToFilter;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerHandle;
    private InfluxDBClient influxDBClient;
    private String anonymizeRegex;

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
        eventsTagsMap.put("host", InetAddress.getLocalHost().getHostName());
        eventsTagsMap.put("interval", String.valueOf(sendIntervalSec));
        eventsTagsMap.put("profile", context.getParameter(EVENTS_PROFILE, "N/A"));
        eventsTagsMap.put("scenario", context.getParameter(EVENTS_SCENARIO, "N/A"));

        anonymizeRegex = context.getParameter(URL_ANONYMIZE_REGEX, DEFAULT_URL_ANONYMIZE_REGEXP);
        samplersRegex = context.getParameter(SAMPLERS_REGEX, "");
        samplersToFilter = Pattern.compile(samplersRegex);
        variablesRegex = context.getParameter(VARIABLES_REGEX, "");
        variablesToFilter = Pattern.compile(variablesRegex);
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
            LOG.debug("Canceled state: {}", cancelState);
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Error waiting for end of scheduler");
                Thread.currentThread().interrupt();
            }

            // Send last set of data before ending
            LOG.info("Sending last metrics");
            sendMeasurements();
        } finally {
            try {
                influxDBClient.close();
            } finally {
                super.teardownTest(context);
            }
        }
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        DEFAULT_ARGS.forEach(arguments::addArgument);
        return arguments;
    }

    private void saveMeasurement(SampleResult sampleResult) {
        String endpoint;
        String server;
        if (sampleResult.getURL() != null) {
            endpoint = anonymizeUrl(sampleResult.getURL());
            sampleResult.getRequestHeaders()
            server = StringUtils.defaultIfEmpty(sampleResult.getURL().getHost(), NOT_AVAILABLE);
        } else {
            endpoint = NOT_AVAILABLE;
            server = NOT_AVAILABLE;
        }

        String message = StringUtils.defaultIfEmpty(
                sampleResult.getFirstAssertionFailureMessage(),
                sampleResult.getResponseMessage()
        );
        String code = StringUtils.substring(
                StringUtils.defaultIfEmpty(sampleResult.getResponseCode(), NOT_AVAILABLE),
                0,
                MAX_CHARS_IN_MSG
        );

        Point point = Point
                .measurement(DEFAULT_MEASUREMENT)
                .time(
                        sampleResult.getTimeStamp() * 1_000_000 + RandomUtils.nextLong(100000, 999999),
                        WritePrecision.NS
                )
                .addField("bytes_recv", (float) sampleResult.getBytesAsLong())
                .addField("bytes_sent", (float) sampleResult.getSentBytes())
                .addField("tg_threads", (float) sampleResult.getGroupThreads())
                .addField("time_ms", (float) sampleResult.getTime())
                .addTag("code", code)
                .addTag("endpoint", endpoint)
                .addTag("launch_uuid", launchId)
                .addTag("passed", sampleResult.isSuccessful() ? "true" : "false")
                .addTag("server", server)
                .addTag("trx", sampleResult.getSampleLabel().trim());

        if (!sampleResult.isSuccessful()) {
            point
                    .addField(
                            "msg",
                            Strings.isNullOrEmpty(message) ? NOT_AVAILABLE : StringUtils.substring(message, 0, MAX_CHARS_IN_MSG)
                    );
        }

        measurementsBuffer.add(point);
    }

    private void sendMeasurements() {
        synchronized (LOCK) {
            try {
                influxDBClient.getWriteApi().writePoints(measurementsBuffer);
            } catch (Throwable tr) {
                LOG.error("Something goes wrong during InfluxDB integration: " + tr.getMessage());
            }
            measurementsBuffer.clear();
        }
    }

    private void sendEvent(boolean isStartOfTest) throws UnknownHostException {
        eventsTagsMap.put("started", String.valueOf(isStartOfTest));
        eventsTagsMap.put("launch_uuid", launchId);

        LOG.debug("Send event with tags: {}", eventsTagsMap);
        Point eventPoint = Point
                .measurement(ANNOTATION_MEASUREMENT)
                .time(Instant.now(), WritePrecision.NS)
                .addField("total_threads", (float) JMeterContextService.getTotalThreads())
                .addTags(eventsTagsMap);

        // Add User Variables
        if (!isStartOfTest) {
            JMeterContextService.getContext().getVariables().entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != null)
                    .filter(entry ->
                            variablesToFilter
                                    .matcher(String.valueOf(entry.getKey()))
                                    .find()
                    )
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> eventPoint.addField(entry.getKey(), String.valueOf(entry.getValue())));
        }

        influxDBClient.getWriteApi().writePoint(eventPoint);
    }

    private String anonymizeUrl(URL url) {
        if (url.getPath() == null) {
            return NOT_AVAILABLE;
        }
        return StringUtils
                .stripEnd(url.getPath(), "/ ")
                .replaceAll(anonymizeRegex, "/{X}")
                .replace("//", "/");
    }

}
