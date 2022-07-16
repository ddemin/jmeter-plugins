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

    private static final String MEASUREMENT_BYTES = "network_bytes";
    private static final String MEASUREMENT_RESPONSE_TIME = "latency_ms";
    private static final String MEASUREMENT_RATE = "rate";
    private static final String MEASUREMENT_ERRORS = "errors";
    private static final String RAW_MEASUREMENT_FIELD = "raw";
    private static final String MEASUREMENT_LAUNCHES = "launches";
    private static final String MEASUREMENT_VARIABLES = "variables";
    public static final String TAG_COMPONENT = "component";
    public static final String FIELD_THREADS = "threads";
    public static final String FIELD_TRX = "trx";

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

    public String packLaunchEvent(
            boolean isTestStarted,
            Map<String, String> tags,
            String scenario,
            String version,
            String details,
            Pattern variablesFilter
    ) {
        LineProtocolBuilder builder;

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
            variableSet.add(new AbstractMap.SimpleEntry<>(FIELD_THREADS, JMeterContextService.getTotalThreads() + 0.0f));
            variableSet.add(new AbstractMap.SimpleEntry<>("scenario", scenario));
            variableSet.add(new AbstractMap.SimpleEntry<>("version", version));
            variableSet.add(new AbstractMap.SimpleEntry<>("details", details));

            List<Map.Entry<String, Object>> filteredAndSortedVariables = variableSet
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            builder = LineProtocolBuilder.withFirstRow(
                    MEASUREMENT_VARIABLES,
                    Map.of(TAG_LAUNCH, launchId),
                    filteredAndSortedVariables
            );
        } else {
            builder = LineProtocolBuilder
                    .withFirstRow(
                            MEASUREMENT_LAUNCHES,
                            tags,
                            List.of(new AbstractMap.SimpleEntry<>(FIELD_THREADS, JMeterContextService.getTotalThreads() + 0.0f))
                    );
        }

        return builder.build();
    }

    public void putSampleResult(SampleResult sampleResult) {
        String label = sampleResult.getSampleLabel().trim().toLowerCase();
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
                        .computeIfAbsent(MEASUREMENT_BYTES, k -> new ValuesPackage())
                        .add(sampleResult.getBytesAsLong() + sampleResult.getSentBytes());
                fieldsValuesMap
                        .computeIfAbsent(MEASUREMENT_RESPONSE_TIME, k -> new ValuesPackage())
                        .add(sampleResult.getTime());
                fieldsValuesMap
                        .computeIfAbsent(MEASUREMENT_RATE, k -> new ValuesPackage())
                        .add(sampleResult.isSuccessful() ? 0L : 1L);
            } else {
                long nowTs = System.nanoTime();
                lineProtocolMessageBuilder
                        .appendLineProtocolMeasurement(MEASUREMENT_BYTES)
                        .appendLineProtocolRawData(measurementTags)
                        .appendLineProtocolField(RAW_MEASUREMENT_FIELD, sampleResult.getBytesAsLong() + sampleResult.getSentBytes())
                        .appendLineProtocolTimestampNs(nowTs)
                        .appendLineProtocolMeasurement(MEASUREMENT_RESPONSE_TIME)
                        .appendLineProtocolRawData(measurementTags)
                        .appendLineProtocolField(RAW_MEASUREMENT_FIELD, sampleResult.getTime())
                        .appendLineProtocolTimestampNs(nowTs);
            }

            if (!sampleResult.isSuccessful()) {
                String code = StringUtils.defaultIfEmpty(sampleResult.getResponseCode(), NOT_AVAILABLE);
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
                                                        sampleResult.getFirstAssertionFailureMessage(),
                                                        sampleResult.getResponseMessage(),
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
                            .add(1L);
                } else {
                    lineProtocolMessageBuilder
                            .appendLineProtocolMeasurement(MEASUREMENT_ERRORS)
                            .appendLineProtocolRawData(auxMeasurementTags)
                            .appendLineProtocolField(RAW_MEASUREMENT_FIELD, 1L);
                }
            }
        }
    }

    public String packMeasurements() {
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
                                        .appendLineProtocolMeasurement(measurement)
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
                                    case MEASUREMENT_BYTES:
                                        lineProtocolMessageBuilder
                                                .appendLineProtocolField("avg", avg);
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
        synchronized(this) {
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
