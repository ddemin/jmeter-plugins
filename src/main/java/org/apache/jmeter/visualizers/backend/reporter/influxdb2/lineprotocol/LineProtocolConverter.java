package org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.visualizers.backend.reporter.container.MetaTypeEnum;
import org.apache.jmeter.visualizers.backend.reporter.container.StatisticTypeEnum;
import org.apache.jmeter.visualizers.backend.reporter.container.StatisticCounter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.*;

public class LineProtocolConverter {

    private static final Logger LOG = LoggerFactory.getLogger(LineProtocolConverter.class);
    private static final String STAT_TYPE_AVERAGE = "avg";
    private static final String STAT_TYPE_MAX = "max";
    private static final String STAT_TYPE_MIN = "min";
    private static final String STAT_TYPE_MEDIAN = "p50";
    private static final String STAT_TYPE_PERC_95 = "p95";
    private static final String STAT_TYPE_PERC_99 = "p99";
    private static final String STAT_TYPE_COUNT = "count";
    private static final String STAT_TYPE_RATE = "rate";
    private static final String STAT_TYPE_SHARE = "share";

    private static final String MEASUREMENT_EXECUTION = "execution";
    private static final String MEASUREMENT_ENVIRONMENT = "environment";
    private static final String MEASUREMENT_LABEL = "label";
    private static final String MEASUREMENT_VERSION = "version";
    private static final String DELIMITER_TAGS_LIST_ITEMS = "|";

    private final String testUuid;
    private final String environment;
    private final String executionUuid;
    private final String profile;
    private final String details;
    private final String testname;
    private final Map<String, Object> userLabels;
    private final Integer warmupInterval;
    private final Integer batchingPeriod;

    public LineProtocolConverter(String testUuid, String executionUuid,
                                  String environment,
                                 String profile, String details, String testname,
                                 Map<String, Object> userLabels,
                                 int warmupInterval, int batchingPeriod) {
        this.testUuid = testUuid;
        this.executionUuid = executionUuid;
        this.environment = environment;
        this.profile = profile;
        this.details = details;
        this.testname = testname;
        this.userLabels = userLabels;
        this.warmupInterval = warmupInterval;
        this.batchingPeriod = batchingPeriod;
    }

    public LineProtocolBuilder createBuilderForTestEvent(boolean isItStarted, long timestampNs) {
        LineProtocolBuilder protocolBuilder = LineProtocolBuilder.withFirstRow(
                MEASUREMENT_EXECUTION,
                buildTestMetaTags(),
                List.of(new AbstractMap.SimpleEntry<>("is_it_started", isItStarted)),
                timestampNs
        );

        return protocolBuilder;
    }

    public LineProtocolBuilder enrichWithTestMetadata(
            LineProtocolBuilder protocolBuilder,
            Map<String, Object> additionalJmeterVariables,
            long timestampNs
    ) {
        final Map<String, String> tags = buildTestMetaTags();

        enrichWithLabels(additionalJmeterVariables, timestampNs, protocolBuilder, tags);
        enrichWithEnvironments(timestampNs, protocolBuilder, tags);

        return protocolBuilder;
    }

    public LineProtocolBuilder createBuilderForTags(String tags, long timestampNs) {
        String formattedFieldValue = toListWithLowerCase(tags, "\\" + DELIMITER_TAGS_LIST_ITEMS)
                .stream()
                .sorted()
                .collect(Collectors.joining(DELIMITER_LIST_ITEM));

        return LineProtocolBuilder.withFirstRow(
                MEASUREMENT_LABEL,
                buildTestMetaTags(),
                List.of(new AbstractMap.SimpleEntry<>("tags", formattedFieldValue)),
                timestampNs
        );
    }

    public LineProtocolBuilder createBuilderForVersions(String versions, long timestampNs) {
        return createBuilderForKeyValueString(MEASUREMENT_VERSION, versions, timestampNs);
    }

    public LineProtocolBuilder createBuilderForOperationsStatistic(
            Map<String, Map<StatisticTypeEnum, StatisticCounter>> statisticsByMetricName,
            long timestampNs
    ) {
        LineProtocolBuilder lpBuilder = new LineProtocolBuilder();

        statisticsByMetricName
                .forEach((key, value) -> {
                    Map<String, String> tags = buildOperationsTags(key);

                    value.forEach(
                            (metric, stats) -> {
                                lpBuilder
                                        .appendLineProtocolMeasurement(metric.getTagName())
                                        .appendTags(tags);

                                // Prevent any stats changing (#add method) during processing
                                synchronized (stats) {
                                    switch (metric) {
                                        case LATENCY, NETWORK -> lpBuilder
                                                .appendLineProtocolField(STAT_TYPE_AVERAGE, stats.getAverage())
                                                .appendLineProtocolField(STAT_TYPE_MAX, stats.getMax())
                                                .appendLineProtocolField(STAT_TYPE_MIN, stats.getMin())
                                                .appendLineProtocolField(STAT_TYPE_MEDIAN, stats.getPercentile(50))
                                                .appendLineProtocolField(STAT_TYPE_PERC_95, stats.getPercentile(95))
                                                .appendLineProtocolField(STAT_TYPE_PERC_99, stats.getPercentile(99));
                                        case LOAD, ERROR -> lpBuilder
                                                .appendLineProtocolField(STAT_TYPE_COUNT, stats.getSum())
                                                .appendLineProtocolField(
                                                        STAT_TYPE_RATE,
                                                        stats.getSum() / (float) batchingPeriod
                                                );
                                    }
                                    // Additional statistic
                                    switch (metric) {
                                        case ERROR -> lpBuilder.appendLineProtocolField(
                                                STAT_TYPE_SHARE,
                                                stats.getSum() / (float) stats.getSamples()
                                        );
                                        case NETWORK -> lpBuilder.appendLineProtocolField(
                                                STAT_TYPE_RATE,
                                                stats.getSum() / (float) batchingPeriod
                                        );
                                    }
                                }
                                lpBuilder.appendLineProtocolTimestampNs(timestampNs);
                            }
                    );
                });

        return lpBuilder;
    }

    public LineProtocolBuilder createBuilderForOperationsMetadata(
            Map<String, Map<MetaTypeEnum, List<Map.Entry<String, Object>>>> metaByMetricName,
            long timestampNs
    ) {
        LineProtocolBuilder lpBuilder = new LineProtocolBuilder();
        metaByMetricName.forEach((samplerName, metricMeta) -> {
            Map<String, String> tags = buildOperationsTags(samplerName);
            metricMeta.forEach((metric, meta) -> {
                meta.forEach(
                        (entry) ->
                                lpBuilder
                                        .appendLineProtocolMeasurement(metric.getTagName())
                                        .appendTags(tags)
                                        .appendLineProtocolField(
                                                entry.getKey(),
                                                StringUtils.isEmpty(String.valueOf(entry.getValue())) ? UNDEFINED : entry.getValue()
                                        )
                                        .appendLineProtocolTimestampNs(timestampNs)
                );
            });
        });

        return lpBuilder;
    }

    // TODO unit test
    public LineProtocolBuilder enrichWithOperationsErrorsMetadata(
            @NotNull LineProtocolBuilder lpBuilder,
            Map<String, Map<String, Integer>> errorStatsBySamplerName,
            long timestampNs
    ) {
        errorStatsBySamplerName.forEach((samplerName, errorsMeta) -> {
            Map<String, String> tags = buildOperationsTags(samplerName);

            errorsMeta.forEach((errorDetail, statistic) -> {
                tags.put("reason", errorDetail);
                lpBuilder
                        .appendLineProtocolMeasurement("error")
                        .appendTags(tags)
                        .appendLineProtocolField("count", statistic)
                        .appendLineProtocolTimestampNs(timestampNs);
            });
        });

        return lpBuilder;
    }

    Map<String, String> buildTestMetaTags() {
        return new TreeMap<>(
                Map.of(
                        "uuid", executionUuid,
                        "test_uuid", testUuid
                )
        );
    }

    Map<String, String> buildOperationsTags(String sampleLabel) {
        var tagsMap = new TreeMap<>(Map.of(MEASUREMENT_EXECUTION, executionUuid));
        tagsMap.putAll(parseSamplerNameToTags(sampleLabel));
        return tagsMap;
    }

    Map<String, String> parseSamplerNameToTags(String sampleLabel) {
        String formattedLabel = sampleLabel.trim().toLowerCase();
        String operation = StringUtils.substringAfter(formattedLabel, DELIMITER_KEY_VALUE).trim();
        String targetService = StringUtils.substringBefore(formattedLabel, DELIMITER_KEY_VALUE).trim();

        if (StringUtils.isEmpty(targetService)) {
            LOG.error(
                    "'Target' tag can't be empty! "
                            + "Please follow next template for JMeter sampler name/label: "
                            + "{Target service/component name}: {Any text as operation name}"
            );
        }

        if (StringUtils.isEmpty(operation)) {
            LOG.error(
                    "Operation tag can't be empty! "
                            + "Please follow next template for JMeter sampler name/label: "
                            + "{Target service/component name}: {Any text as transaction name}"
            );
        }

        return Map.of("target", targetService, "operation", operation);
    }

    LineProtocolBuilder createBuilderForKeyValueString(String measurement, String mapAsString, long timestampNs) {
        List<Map.Entry<String, Object>> valuesByKeys = toMapWithLowerCaseKey(mapAsString)
                .entrySet()
                .stream()
                .map(
                        entry -> {
                            String valueAsStr = String.valueOf(entry.getValue());
                            if (StringUtils.isEmpty(entry.getKey().trim()) || StringUtils.isEmpty(valueAsStr.trim())) {
                                LOG.error("Incorrect key-value row: " + entry);
                                return null;
                            } else {
                                return (Map.Entry<String, Object>) new AbstractMap.SimpleEntry<String, Object>(
                                        entry.getKey().toLowerCase(),
                                        valueAsStr.trim()
                                );
                            }
                        }
                )
                .filter(Objects::nonNull)
                .sorted(Map.Entry.comparingByKey())
                .toList();

        return LineProtocolBuilder.withFirstRow(
                measurement,
                buildTestMetaTags(),
                valuesByKeys,
                timestampNs
        );
    }

    void enrichWithEnvironments(long timestampNs, LineProtocolBuilder protocolBuilder, Map<String, String> tags) {
        final Map<String, String> environmentFields = new TreeMap<>();
        environmentFields.put("name", environment);
        environmentFields.put("profile", profile);

        protocolBuilder.appendRowWithTextFields(
                MEASUREMENT_ENVIRONMENT,
                tags,
                environmentFields.entrySet().stream().toList(),
                timestampNs
        );
    }

    void enrichWithLabels(Map<String, Object> additionalJmeterVariables, long timestampNs, LineProtocolBuilder protocolBuilder, Map<String, String> tags) {
        final Map<String, Object> labelFields = new TreeMap<>();
        labelFields.put("name", testname);
        labelFields.put("details", details);
        labelFields.put("warmup_sec", warmupInterval);
        labelFields.put("period_sec", batchingPeriod);

        if (additionalJmeterVariables != null) {
            additionalJmeterVariables.entrySet()
                    .stream()
                    .map(
                            e ->
                                    e.getValue() == null
                                            ? new AbstractMap.SimpleEntry<>(e.getKey(), (Object) UNDEFINED)
                                            : new AbstractMap.SimpleEntry<>(e.getKey().toLowerCase(), e.getValue())
                    )
                    .map(
                            e ->
                                    (e.getValue() instanceof String && StringUtils.isEmpty((String) e.getValue()))
                                            ? new AbstractMap.SimpleEntry<>(e.getKey(), (Object) UNDEFINED)
                                            : new AbstractMap.SimpleEntry<>(e.getKey().toLowerCase(), e.getValue())
                    )
                    .forEach(entry -> labelFields.put(entry.getKey(), entry.getValue()));
        }

        userLabels.forEach(
                (key, value) -> {
                    if (StringUtils.isEmpty(value.toString())) {
                        labelFields.put(key, UNDEFINED);
                    } else {
                        labelFields.put(key, value);
                    }
                }
        );

        protocolBuilder.appendRow(
                MEASUREMENT_LABEL,
                tags,
                labelFields.entrySet().stream().toList(),
                timestampNs
        );
    }

}
