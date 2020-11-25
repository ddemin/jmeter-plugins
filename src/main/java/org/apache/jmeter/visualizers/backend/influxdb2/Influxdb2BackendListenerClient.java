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

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Implementation of {@link AbstractBackendListenerClient} to write in an InfluxDB 2.0
 *
 * @since 5.3
 */
public class Influxdb2BackendListenerClient extends AbstractBackendListenerClient implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(Influxdb2BackendListenerClient.class);

    private static final int MAX_POOL_SIZE = 1;
    private static final int DEFAULT_SEND_INTERVAL = 10;
    private static final Object LOCK = new Object();

    private static final Map<String, String> DEFAULT_ARGS = new LinkedHashMap<>();
    private static final String ARG_INFLUXDB_2_URL = "influxdb2_url";
    private static final String ARG_INFLUXDB_2_TOKEN = "influxdb2_token";
    private static final String ARG_INFLUXDB_2_ORG = "influxdb2_org";
    private static final String ARG_INFLUXDB_2_BUCKET_TRX = "influxdb2_bucket_for_trx";
    private static final String ARG_INFLUXDB_2_BUCKET_META = "influxdb2_bucket_for_meta";
    private static final String ARG_INTERVAL_SEC = "send_interval_sec";
    private static final String ARG_LAUNCH_ID = "launch_uuid";
    private static final String ARG_ENV = "environment_name";
    private static final String ARG_SCENARIO = "load_scenario_name";
    private static final String ARG_PROFILE = "load_profile_name";
    private static final String ARG_TAGS = "additional_meta_tags";
    private static final String ARG_URL_ANONYMIZE_REGEX = "url_anonymize_regex";
    private static final String ARG_ALLOWED_SAMPLERS_REGEX = "reported_samplers_regex";
    private static final String ARG_ALLOWED_VARIABLES_REGEX = "reported_variables_regex";

    private static final String LAUNCH_MEASUREMENT = "launches";
    private static final String VARIABLE_MEASUREMENT = "variables";
    private static final String MAIN_MEASUREMENT = "main";
    private static final String AUX_MEASUREMENT = "aux";

    private static final String LAUNCH_TAG = "launch";
    private static final String IS_STARTED_TAG = "isStarted";
    private static final String THREADS_FIELD = "threads";

    private static final String DEFAULT_URL_ANONYMIZE_REGEXP
            = "([0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+|[0-9a-z-]+-[0-9a-z-]+|[0-9]+)";
    private static final String NOT_AVAILABLE = "N/A";
    private static final int MAX_CHARS_IN_MSG = 256;

    static {
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_URL, "https://influxdb2_url:9999");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_TOKEN, "access_token");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_ORG, "org_name");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_BUCKET_TRX, "bucket_name_for_main_data");
        DEFAULT_ARGS.put(ARG_INFLUXDB_2_BUCKET_META, "bucket_name_for_metadata");
        DEFAULT_ARGS.put(ARG_INTERVAL_SEC, String.valueOf(DEFAULT_SEND_INTERVAL));

        DEFAULT_ARGS.put(ARG_LAUNCH_ID, "${__UUID()}");
        DEFAULT_ARGS.put(ARG_ENV, "Environment name");
        DEFAULT_ARGS.put(ARG_SCENARIO, "JMeter test/scenario name");
        DEFAULT_ARGS.put(ARG_PROFILE, "Name of load profile");
        DEFAULT_ARGS.put(ARG_TAGS, "");

        DEFAULT_ARGS.put(ARG_URL_ANONYMIZE_REGEX, DEFAULT_URL_ANONYMIZE_REGEXP);
        DEFAULT_ARGS.put(ARG_ALLOWED_SAMPLERS_REGEX, ".*");
        DEFAULT_ARGS.put(ARG_ALLOWED_VARIABLES_REGEX, "NOTHING");
    }

    private final SortedMap<String, String> eventsTagsMap = new TreeMap<>();
    private final ConcurrentHashMap<String, Boolean> labelsWhiteList = new ConcurrentHashMap<>();

    private LineProtocolMessageBuilder lineProtocolMessageBuilder;

    private String launchId;
    private String samplersRegex;
    private String anonymizeRegex;
    private String variablesRegex;
    private Pattern samplersToFilter;
    private Pattern variablesToFilter;
    private boolean isDestroyed;

    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> timerHandle;

    private HttpClient client;
    private URI influxDbMainWriteUrl;
    private URI influxDBMetaWriteUrl;
    private String influxDbAuthHeader;
    private boolean isReady;

    public Influxdb2BackendListenerClient() {
        super();
    }

    @Override
    public void run() {
        sendMeasurements();
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        if (!isReady) {
            return;
        }
        for (SampleResult sampleResult : sampleResults) {
            boolean isAllowedLabel = labelsWhiteList.computeIfAbsent(
                    sampleResult.getSampleLabel(),
                    (k) -> samplersToFilter.matcher(sampleResult.getSampleLabel()).find()
            );
            if (isAllowedLabel) {
                saveMeasurement(sampleResult);
            }
        }
    }

    // TODO Make more cleaner and easy. Split to methods/classes
    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(this::destroyInfluxDbClient));

        labelsWhiteList.clear();
        cleanDataPointBuilder();

        client = HttpClient.newHttpClient();
        isDestroyed = false;
        influxDbAuthHeader = "Token " + context.getParameter(ARG_INFLUXDB_2_TOKEN);

        String influxDbUrl = context.getParameter(ARG_INFLUXDB_2_URL);
        HttpRequest authRequest = HttpRequest.newBuilder()
                .uri(URI.create((influxDbUrl + "/ready").replace("//ready", "/ready")))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "plain/text")
                .header("Authorization", influxDbAuthHeader)
                .GET()
                .build();
        isReady = false;
        HttpResponse<String> response = client.send(authRequest, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            LOG.error(
                    "Can't authorize to InfluxDB2, HTTP response: "
                            + response.body()
                            + ", HTTP code: "
                            + response.statusCode()
            );
            return;
        } else {
            isReady = true;
        }

        String influxDbOrg = context.getParameter(ARG_INFLUXDB_2_ORG);
        influxDbMainWriteUrl = URI.create(
                (influxDbUrl
                        + "/api/v2/write?"
                        + "org=" + influxDbOrg
                        + "&bucket=" + context.getParameter(ARG_INFLUXDB_2_BUCKET_TRX)
                        + "&precision=ns")
                        .replace("//api", "/api")
        );
        influxDBMetaWriteUrl = URI.create(
                (influxDbUrl
                        + "/api/v2/write?"
                        + "org=" + influxDbOrg
                        + "&bucket=" + context.getParameter(ARG_INFLUXDB_2_BUCKET_META)
                        + "&precision=ns")
                        .replace("//api", "/api")
        );

        int sendIntervalSec = context.getIntParameter(ARG_INTERVAL_SEC);

        launchId = context.getParameter(ARG_LAUNCH_ID);
        eventsTagsMap.put(LAUNCH_TAG, launchId);
        eventsTagsMap.put("environment", context.getParameter(ARG_ENV, NOT_AVAILABLE));
        eventsTagsMap.put("host", InetAddress.getLocalHost().getHostName());
        eventsTagsMap.put("profile", context.getParameter(ARG_PROFILE, NOT_AVAILABLE));
        eventsTagsMap.put("scenario", context.getParameter(ARG_SCENARIO, NOT_AVAILABLE));
        List<String> additionalTagsEntries = Arrays.asList(context.getParameter(ARG_TAGS, "").split(","));
        additionalTagsEntries.forEach(
                tagEntry -> {
                    if (tagEntry.contains("=")) {
                        String[] keyValue = tagEntry.split("=");
                        eventsTagsMap.put(keyValue[0].trim(), keyValue[1].trim());
                    }
                }
        );

        anonymizeRegex = context.getParameter(ARG_URL_ANONYMIZE_REGEX, DEFAULT_URL_ANONYMIZE_REGEXP);
        samplersRegex = context.getParameter(ARG_ALLOWED_SAMPLERS_REGEX, "");
        samplersToFilter = Pattern.compile(samplersRegex);
        variablesRegex = context.getParameter(ARG_ALLOWED_VARIABLES_REGEX, "");
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
        super.teardownTest(context);
        labelsWhiteList.clear();
        destroyInfluxDbClient();
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        DEFAULT_ARGS.forEach(arguments::addArgument);
        return arguments;
    }

    private HttpRequest buildWriteRequest(URI targetUri, LineProtocolMessageBuilder lineProtocolMessageBuilder) {
        return HttpRequest.newBuilder()
                .uri(targetUri)
                .timeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_2)
                .header("Accept", "application/json")
                .header("Content-Type", "text/plain; charset=utf-8")
                .header("Authorization", influxDbAuthHeader)
                .POST(HttpRequest.BodyPublishers.ofString(lineProtocolMessageBuilder.build(), UTF_8))
                .build();
    }

    private void destroyInfluxDbClient() {
        if (isDestroyed) {
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
            if (client != null) {
                // TODO Think about
                Thread.sleep(10_000);
                sendEvent(false);
                sendMeasurements();
            }
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage());
        } finally {
            eventsTagsMap.clear();
            cleanDataPointBuilder();
            isDestroyed = true;
        }
    }

    private void saveMeasurement(SampleResult sampleResult) {
        String code = StringUtils.defaultIfEmpty(sampleResult.getResponseCode(), NOT_AVAILABLE);
        boolean nonDigitCode = NumberUtils.isDigits(code);

        String label = sampleResult.getSampleLabel().trim();
        String endpoint = sampleResult.getURL() == null || StringUtils.isEmpty(sampleResult.getURL().getPath())
                ? NOT_AVAILABLE
                : anonymizeUrl(sampleResult.getURL().getPath().replace("//", "/").trim());

        synchronized (LOCK) {
            lineProtocolMessageBuilder
                    .appendLineProtocolMeasurement(MAIN_MEASUREMENT)
                    .appendLineProtocolTag(
                            "code",
                            nonDigitCode ? code : "See msg field"
                    )
                    .appendLineProtocolTag("endpoint", endpoint)
                    .appendLineProtocolTag("isPassed", sampleResult.isSuccessful() ? "true" : "false")
                    .appendLineProtocolTag(LAUNCH_TAG, launchId)
                    .appendLineProtocolTag("name", Strings.isEmpty(label) ? NOT_AVAILABLE : label)
                    .appendLineProtocolTag(
                            "server",
                            sampleResult.getURL() != null
                                    ? StringUtils.defaultIfEmpty(sampleResult.getURL().getHost(), NOT_AVAILABLE)
                                    : NOT_AVAILABLE
                    )
                    .appendLineProtocolField("bytes_recv", sampleResult.getBytesAsLong() + 0.0f)
                    .appendLineProtocolField("bytes_sent", sampleResult.getSentBytes() + 0.0f)
                    .appendLineProtocolField("time_ms", sampleResult.getTime() + 0.0f)
                    .appendLineProtocolTimestampNs(enrichMsTimestamp(sampleResult.getTimeStamp()));

            if (!sampleResult.isSuccessful() || nonDigitCode) {
                lineProtocolMessageBuilder
                        .appendLineProtocolMeasurement(AUX_MEASUREMENT)
                        .appendLineProtocolTag(
                                "code",
                                nonDigitCode ? code : "See msg field"
                        )
                        .appendLineProtocolTag("endpoint", endpoint)
                        .appendLineProtocolTag("isPassed", sampleResult.isSuccessful() ? "true" : "false")
                        .appendLineProtocolTag(LAUNCH_TAG, launchId)
                        .appendLineProtocolTag("name", Strings.isEmpty(label) ? NOT_AVAILABLE : label)
                        .appendLineProtocolTag(
                                "server",
                                sampleResult.getURL() != null
                                        ? StringUtils.defaultIfEmpty(sampleResult.getURL().getHost(), NOT_AVAILABLE)
                                        : NOT_AVAILABLE
                        )
                        .appendLineProtocolField(
                                "msg",
                                StringUtils.substring(
                                        StringUtils.firstNonEmpty(
                                                nonDigitCode
                                                        ? code
                                                        : sampleResult.getFirstAssertionFailureMessage(),
                                                sampleResult.getResponseMessage(),
                                                NOT_AVAILABLE
                                        ),
                                        0,
                                        MAX_CHARS_IN_MSG
                                )
                        )
                        .appendLineProtocolTimestampNs(enrichMsTimestamp(sampleResult.getTimeStamp()));
            }
        }
    }

    private void sendMeasurements() {
        synchronized (LOCK) {
            try {
                if (lineProtocolMessageBuilder.getAddedLines() == 0) {
                    return;
                }
                HttpResponse<String> response = client.send(
                        buildWriteRequest(influxDbMainWriteUrl, lineProtocolMessageBuilder),
                        HttpResponse.BodyHandlers.ofString()
                );
                if (response.statusCode() >= 400) {
                    throw new IllegalStateException(
                            "HTTP body: " + response.body() + ", HTTP code: " + response.statusCode()
                    );
                }
            } catch (Throwable tr) {
                LOG.error("Something goes wrong during InfluxDB integration: " + tr.getMessage());
            } finally {
                cleanDataPointBuilder();
            }
        }
    }

    private void sendEvent(boolean isTestStarted) {
        eventsTagsMap.put(IS_STARTED_TAG, String.valueOf(isTestStarted));
        LineProtocolMessageBuilder eventPointBuilder = new LineProtocolMessageBuilder()
                .appendLineProtocolMeasurement(LAUNCH_MEASUREMENT)
                .appendTags(eventsTagsMap)
                .appendLineProtocolField(THREADS_FIELD, JMeterContextService.getTotalThreads() + 0.0f)
                .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));

        if (isTestStarted) {
            List<Map.Entry<String, Object>> filteredAndSortedVariables = JMeterContextService
                    .getContext()
                    .getVariables()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != null)
                    .filter(entry ->
                            variablesToFilter
                                    .matcher(String.valueOf(entry.getKey()))
                                    .find()
                    )
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            if (filteredAndSortedVariables.size() > 0) {
                eventPointBuilder
                        .appendLineProtocolMeasurement(VARIABLE_MEASUREMENT)
                        .appendLineProtocolTag(LAUNCH_TAG, launchId);
                filteredAndSortedVariables.forEach(
                        entry ->
                                eventPointBuilder.appendLineProtocolField(
                                        entry.getKey(),
                                        String.valueOf(entry.getValue())
                                )
                );
                eventPointBuilder
                        .appendLineProtocolField(THREADS_FIELD, JMeterContextService.getTotalThreads() + 0.0f)
                        .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));
            }
        }

        try {
            HttpResponse<String> response = client.send(
                    buildWriteRequest(influxDBMetaWriteUrl, eventPointBuilder),
                    HttpResponse.BodyHandlers.ofString()
            );
            if (response.statusCode() >= 400) {
                throw new IllegalStateException(
                        "HTTP code:  " + response.statusCode() + ", HTTP body:  " + response.body()
                );
            }
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB events write operation: " + tr.getMessage());
        }
    }

    private void cleanDataPointBuilder() {
        synchronized (LOCK) {
            lineProtocolMessageBuilder = new LineProtocolMessageBuilder();
        }
    }

    private long enrichMsTimestamp(long ms) {
        return ms * 1_000_000 + RandomUtils.nextInt(0, 999_999);
    }

    private String anonymizeUrl(String path) {
        if (path.equals("/")) {
            return path;
        } else {
            return StringUtils
                    .stripEnd(path, "/ ")
                    .replaceAll(anonymizeRegex, "X")
                    .replace("//", "/");
        }
    }

}
