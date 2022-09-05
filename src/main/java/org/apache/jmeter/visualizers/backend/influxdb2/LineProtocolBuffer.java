package org.apache.jmeter.visualizers.backend.influxdb2;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.jmeter.visualizers.backend.influxdb2.Utils.*;

public class LineProtocolBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(LineProtocolBuffer.class);
    private static final String MSG_ANONYMIZATION_REGEXP = "([0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+|[0-9]+)";
    private static final String MSG_ANONYMIZATION_PLACEMENT = "X";
    private static final int MAX_CHARS_IN_MSG = 256;

    private static final String MEASUREMENT_NETWORK = "network";
    private static final String MEASUREMENT_RESPONSE_TIME = "latency_ms";
    private static final String MEASUREMENT_RATE = "rate";
    private static final String MEASUREMENT_ERRORS = "errors";
    private static final String RAW_MEASUREMENT_FIELD = "raw";
    private static final String MEASUREMENT_LAUNCHES = "launches";
    private static final String MEASUREMENT_VERSIONS = "versions";
    private static final String MEASUREMENT_VARIABLES = "variables";
    public static final String TAG_COMPONENT = "component";
    public static final String FIELD_TRX = "trx";
    public static final String TAG_INTERVAL = "interval";

    private final boolean isStatisticMode;
    private final String launchId;
    private final int sendIntervalSec;
    private final ConcurrentHashMap<String, HashMap<String, ValuesPackage>> metricsBuffer = new ConcurrentHashMap<>();

    private LineProtocolBuilder lineProtocolMessageBuilder;

    public LineProtocolBuffer(boolean isStatisticMode, String launchId, int sendIntervalSec) {
        this.isStatisticMode = isStatisticMode;
        this.launchId = launchId;
        this.sendIntervalSec = sendIntervalSec;
        this.lineProtocolMessageBuilder = new LineProtocolBuilder();
    }

    public String packLaunchVersions(String componentsVersions) {
        List<Map.Entry<String, Object>> versionsByComponent = Arrays
                .stream(componentsVersions.trim().split(DELIMITER_COMPONENT_VERSION_ITEM))
                .filter(StringUtils::isNoneEmpty)
                .filter(
                        cv -> {
                            if (cv.contains(DELIMITER_COMPONENT_VERSION)) {
                                return true;
                            } else {
                                LOG.error("Incorrect component-version row: " + cv);
                                return false;
                            }
                        }
                )
                .map(
                        cv -> {
                            String[] arr = cv.trim().split(DELIMITER_COMPONENT_VERSION);
                            if (arr.length != 2) {
                                LOG.error("Incorrect component-version row: " + cv);
                                return null;
                            } else if (StringUtils.isEmpty(arr[0].trim()) || StringUtils.isEmpty(arr[1].trim())) {
                                LOG.error("Incorrect component-version row: " + cv);
                                return null;
                            } else {
                                return (Map.Entry<String, Object>) new AbstractMap.SimpleEntry<String, Object>(
                                        arr[0].trim().toLowerCase(),
                                        arr[1].trim().toLowerCase()
                                );
                            }
                        }
                )
                .filter(Objects::nonNull)
                .sorted(Map.Entry.comparingByKey())
                .toList();

        LineProtocolBuilder builder = LineProtocolBuilder.withFirstRow(
                MEASUREMENT_VERSIONS,
                Map.of(TAG_LAUNCH, launchId),
                versionsByComponent
        );

        return builder.build();
    }

    public String packLaunchMetadata(
            boolean isTestStarted,
            Map<String, String> tags,
            String scenario,
            String envVersion,
            String details,
            Pattern variablesFilter
    ) {
        tags.put(TAG_INTERVAL, String.valueOf(sendIntervalSec));

        LineProtocolBuilder builder = LineProtocolBuilder.withFirstRow(
                MEASUREMENT_LAUNCHES,
                tags,
                List.of(new AbstractMap.SimpleEntry<>(TAG_LAUNCH, launchId))
        );

        if (isTestStarted) {
            Set<Map.Entry<String, Object>> variableSet = JMeterContextService
                    .getContext()
                    .getVariables()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() != null)
                    .filter(entry ->
                            variablesFilter
                                    .matcher(String.valueOf(entry.getKey()))
                                    .find()
                    ).collect(Collectors.toSet());

            // Mandatory fields
            variableSet.add(new AbstractMap.SimpleEntry<>("scenario", scenario));
            variableSet.add(new AbstractMap.SimpleEntry<>("version", envVersion));
            variableSet.add(new AbstractMap.SimpleEntry<>("details", details));

            List<Map.Entry<String, Object>> filteredAndSortedVariables = variableSet
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            builder.appendRow(
                    MEASUREMENT_VARIABLES,
                    Map.of(TAG_LAUNCH, launchId),
                    filteredAndSortedVariables
            );
        }

        return builder.build();
    }

    public void putSampleResult(SampleResult result) {
        String label = result.getSampleLabel().trim().toLowerCase();
        String trx = StringUtils.substringAfter(label, ":").trim();
        String component = StringUtils.substringBefore(label, ":").trim();

        if (StringUtils.isEmpty(component)) {
            throw new IllegalArgumentException(
                    "Component tag can't be empty! "
                            + "Please follow next template for JMeter sampler name/label: "
                            + "{COMPONENT}: {Any text as transaction name}"
            );
        }

        if (StringUtils.isEmpty(trx)) {
            throw new IllegalArgumentException(
                    "Trx tag can't be empty! "
                            + "Please follow next template for JMeter sampler name/label: "
                            + "{Component name}: {Any text as transaction name}"
            );
        }

        String measurementTags = buildMeasurementTags(component, launchId, trx);

        synchronized (this) {
            if (isStatisticMode) {
                HashMap<String, ValuesPackage> fieldsValuesMap = metricsBuffer.computeIfAbsent(
                        measurementTags,
                        k -> new HashMap<>()
                );
                fieldsValuesMap
                        .computeIfAbsent(MEASUREMENT_NETWORK, k -> new ValuesPackage())
                        .add(result.getBytesAsLong() + result.getSentBytes());
                fieldsValuesMap
                        .computeIfAbsent(MEASUREMENT_RESPONSE_TIME, k -> new ValuesPackage())
                        .add(
                                result.getSampleCount() == 0
                                        ? result.getTime()
                                        : (result.getTime()/ result.getSampleCount())
                        );
                for(int sampleNum = 0; sampleNum < result.getSampleCount(); sampleNum++) {
                    fieldsValuesMap
                            .computeIfAbsent(MEASUREMENT_RATE, k -> new ValuesPackage())
                            .add(sampleNum < result.getErrorCount() ? 1L : 0L);
                }
            } else {
                long nowTs = System.nanoTime();
                lineProtocolMessageBuilder
                        .appendLineProtocolMeasurement(MEASUREMENT_NETWORK)
                        .appendLineProtocolRawData(measurementTags)
                        .appendLineProtocolField(RAW_MEASUREMENT_FIELD, result.getBytesAsLong() + result.getSentBytes())
                        .appendLineProtocolTimestampNs(nowTs)
                        .appendLineProtocolMeasurement(MEASUREMENT_RESPONSE_TIME)
                        .appendLineProtocolRawData(measurementTags)
                        .appendLineProtocolField(RAW_MEASUREMENT_FIELD, result.getTime())
                        .appendLineProtocolTimestampNs(nowTs);
            }

            if (!result.isSuccessful()) {
                String code = StringUtils.defaultIfEmpty(result.getResponseCode(), NOT_AVAILABLE);
                boolean isDigitCode = NumberUtils.isDigits(code);

                LineProtocolBuilder auxBuilder = new LineProtocolBuilder();
                String auxMeasurementTags = auxBuilder
                        .appendLineProtocolTag(TAG_COMPONENT, component)
                        .appendLineProtocolTag(
                                "error",
                                (StringUtils.isNoneEmpty(code) && isDigitCode
                                        ? "HTTP " + code + ": "
                                        : "")
                                        + StringUtils.substring(
                                                StringUtils.firstNonEmpty(
                                                        result.getFirstAssertionFailureMessage(),
                                                        result.getResponseMessage(),
                                                        code,
                                                        NOT_AVAILABLE
                                                ),
                                                0,
                                                MAX_CHARS_IN_MSG
                                        )
                                        .replaceAll(MSG_ANONYMIZATION_REGEXP, MSG_ANONYMIZATION_PLACEMENT)
                        )
                        .appendLineProtocolTag(TAG_LAUNCH, launchId)
                        .appendLineProtocolTag(FIELD_TRX, trx)
                        .build();

                if (isStatisticMode) {
                    metricsBuffer
                            .computeIfAbsent(auxMeasurementTags, k -> new HashMap<>())
                            .computeIfAbsent(MEASUREMENT_ERRORS, k -> new ValuesPackage())
                            .add(result.getErrorCount());
                } else {
                    lineProtocolMessageBuilder
                            .appendLineProtocolMeasurement(MEASUREMENT_ERRORS)
                            .appendLineProtocolRawData(auxMeasurementTags)
                            .appendLineProtocolField(RAW_MEASUREMENT_FIELD, result.getErrorCount());
                }
            }
        }
    }

    public String pollPackedMeasurements() {
        try {
            if (isStatisticMode) {
                processMeasurementsBatch();
            }
            return lineProtocolMessageBuilder.build();
        } finally {
            reset();
        }
    }

    private void processMeasurementsBatch() {
        long timestamp = Instant.now().toEpochMilli();
        metricsBuffer.forEach(
                (tags, measurements) -> {
                    measurements.forEach(
                            (measurement, stats) -> {
                                long n = stats.getSize();
                                if (n <= 0) {
                                    return;
                                }

                                lineProtocolMessageBuilder
                                        // TODO temporary hack. I want to see network bytes-per-second metric in RATE section
                                        // TODO I think it is a time to redesign JMeter metrics -> InfluxDB metrics mapping
                                        .appendLineProtocolMeasurement(
                                                measurement.equalsIgnoreCase(MEASUREMENT_NETWORK)
                                                        ? MEASUREMENT_RATE
                                                        : measurement
                                        )
                                        .appendLineProtocolRawData(tags);

                                float avg = stats.getAverage();
                                switch (measurement) {
                                    case MEASUREMENT_RESPONSE_TIME:
                                        lineProtocolMessageBuilder
                                                .appendLineProtocolField("avg", avg)
                                                .appendLineProtocolField("max", stats.getMax())
                                                .appendLineProtocolField("min", stats.getMin())
                                                .appendLineProtocolField("p50", stats.getPercentile(50))
                                                .appendLineProtocolField("p95", stats.getPercentile(95))
                                                .appendLineProtocolField("p99", stats.getPercentile(99));
                                        break;
                                    // TODO not a truly standalone measurement in InfluxDB. See TODO above.
                                    case MEASUREMENT_NETWORK:
                                        lineProtocolMessageBuilder
                                                .appendLineProtocolField(
                                                        "network_bps",
                                                        stats.getSum() / (float) sendIntervalSec
                                                );
                                        break;
                                    case MEASUREMENT_RATE:
                                        lineProtocolMessageBuilder
                                                .appendLineProtocolField("load_tps", n / (float) sendIntervalSec)
                                                .appendLineProtocolField("error_share", stats.getSum() / (float) n);
                                        break;
                                    case MEASUREMENT_ERRORS:
                                        lineProtocolMessageBuilder
                                                .appendLineProtocolField("count", stats.getSum());
                                        break;
                                    default:
                                        LOG.error("Unknown field: " + measurement);
                                        return;
                                }
                                lineProtocolMessageBuilder
                                        .appendLineProtocolTimestampNs(enrichMsTimestamp(timestamp));
                            }
                    );
                }
        );
    }

    private void reset() {
        synchronized (this) {
            metricsBuffer.forEach((k, v) -> v.forEach((field, stat) -> stat.clear()));
            lineProtocolMessageBuilder = new LineProtocolBuilder();
        }
    }

    private String buildMeasurementTags(String component, String launchId, String trx) {
        return new LineProtocolBuilder()
                .appendLineProtocolTag(TAG_COMPONENT, component)
                .appendLineProtocolTag(TAG_LAUNCH, launchId)
                .appendLineProtocolTag(FIELD_TRX, trx)
                .build();
    }

}
