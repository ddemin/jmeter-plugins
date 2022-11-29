package org.apache.jmeter.visualizers.backend.influxdb2.lineprotocol;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.visualizers.backend.influxdb2.container.MetaTypeEnum;
import org.apache.jmeter.visualizers.backend.influxdb2.container.StatisticTypeEnum;
import org.apache.jmeter.visualizers.backend.influxdb2.container.StatisticCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.*;

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

    private final String testUuid;
    private final String hostname;
    private final String environment;
    private final String executionUuid;
    private final String profile;
    private final String details;
    private final String testname;
    private final Map<String, String> userLabels;
    private final Integer warmupInterval;
    private final Integer batchingPeriod;

    public LineProtocolConverter(String testUuid, String executionUuid,
                                 String hostname, String environment,
                                 String profile, String details, String testname,
                                 Map<String, String> userLabels,
                                 int warmupInterval, int batchingPeriod) {
        this.testUuid = testUuid;
        this.executionUuid = executionUuid;
        this.environment = environment;
        this.hostname = hostname;
        this.profile = profile;
        this.details = details;
        this.testname = testname;
        this.userLabels = userLabels;
        this.warmupInterval = warmupInterval;
        this.batchingPeriod = batchingPeriod;
    }

    public LineProtocolBuilder createBuilderForTestMetadata(
            boolean isItStarted,
            Map<String, Object> additionalVariables
    ) {
        LineProtocolBuilder protocolBuilder = LineProtocolBuilder.withFirstRow(
                "execution",
                buildTestEventTags(true),
                List.of(new AbstractMap.SimpleEntry<>("uuid", executionUuid))
        );

        if (isItStarted) {
            final Map<String, String> tags = buildTestMetaTags();

            final ArrayList<Map.Entry<String, String>> labelFields = new ArrayList<>();
            labelFields.add(new AbstractMap.SimpleEntry<>("profile", profile));
            labelFields.add(new AbstractMap.SimpleEntry<>("details", details));
            labelFields.add(new AbstractMap.SimpleEntry<>("name", testname));

            userLabels.entrySet().forEach(labelFields::add);

            protocolBuilder.appendRowWithTextFields(
                    "label",
                    tags,
                    labelFields
            );

            final ArrayList<Map.Entry<String, Object>> variableFields = new ArrayList<>();
            variableFields.add(new AbstractMap.SimpleEntry<>("warmup_sec", warmupInterval));
            variableFields.add(new AbstractMap.SimpleEntry<>("period_sec", batchingPeriod));

            if (additionalVariables != null) {
                additionalVariables.entrySet().forEach(variableFields::add);
            }

            protocolBuilder.appendRow(
                    "variable",
                    tags,
                    variableFields
            );
        }

        return protocolBuilder;
    }

    public LineProtocolBuilder createBuilderForVersions(String versions) {
        List<Map.Entry<String, Object>> versionsByComponent = parseStringToMap(versions)
                .entrySet()
                .stream()
                .map(
                        entry -> {
                            if (StringUtils.isEmpty(entry.getKey().trim()) || StringUtils.isEmpty(entry.getValue().trim())) {
                                LOG.error("Incorrect component-version row: " + entry);
                                return null;
                            } else {
                                return (Map.Entry<String, Object>) new AbstractMap.SimpleEntry<String, Object>(
                                        entry.getKey().toLowerCase(),
                                        entry.getValue().trim().toLowerCase()
                                );
                            }
                        }
                )
                .filter(Objects::nonNull)
                .sorted(Map.Entry.comparingByKey())
                .toList();

        LineProtocolBuilder builder = LineProtocolBuilder.withFirstRow(
                "version",
                buildTestMetaTags(),
                versionsByComponent
        );

        return builder;
    }

    public LineProtocolBuilder createBuilderForOperationsStatistic(
            Map<String, Map<StatisticTypeEnum, StatisticCounter>> statisticsByMetricName
    ) {
        LineProtocolBuilder lpBuilder = new LineProtocolBuilder();

        statisticsByMetricName
                .forEach((key, value) -> {
                    Map<String, String> tags = parseSamplerNameToTags(key);

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
                                                .appendLineProtocolField(STAT_TYPE_COUNT, stats.getSize())
                                                .appendLineProtocolField(
                                                        STAT_TYPE_RATE,
                                                        stats.getSize() / (float) batchingPeriod
                                                );
                                    }
                                    // Additional statistic
                                    switch (metric) {
                                        case ERROR -> lpBuilder.appendLineProtocolField(
                                                STAT_TYPE_SHARE,
                                                stats.getSum() / (float) stats.getSize()
                                        );
                                        case NETWORK -> lpBuilder.appendLineProtocolField(
                                                STAT_TYPE_RATE,
                                                stats.getSum() / (float) batchingPeriod
                                        );
                                    }
                                }
                            }
                    );
                });

        return lpBuilder;
    }

    public LineProtocolBuilder createBuilderForOperationsMetadata(
            Map<String, Map<MetaTypeEnum, Map<String, String>>> metaByMetricName
    ) {
        LineProtocolBuilder lpBuilder = new LineProtocolBuilder();

        metaByMetricName.forEach((samplerName, metricMeta) -> {
            Map<String, String> tags = parseSamplerNameToTags(samplerName);
            metricMeta.forEach((metric, meta) -> {
                lpBuilder
                        .appendLineProtocolMeasurement(metric.getTagName())
                        .appendTags(tags);
                meta.forEach(
                        (labelKey, labelValue) ->
                                lpBuilder.appendLineProtocolField(
                                        labelKey,
                                        StringUtils.isEmpty(labelValue) ? UNDEFINED : labelValue
                                )
                );
            });
        });

        return lpBuilder;
    }

    Map<String, String> buildTestEventTags(boolean isItStart) {
        return Map.of(
                "test", testUuid,
                "is_it_start", String.valueOf(isItStart),
                "hostname", hostname,
                "environment", environment
        );
    }

    Map<String, String> buildTestMetaTags() {
        return Map.of("test", testUuid);
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

        return Map.of(
                "execution", executionUuid,
                "target", targetService,
                "operation", operation
        );
    }


}
