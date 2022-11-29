package org.apache.jmeter.visualizers.backend.influxdb2;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.influxdb2.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.UNDEFINED;

public class InfluxDbListenerArguments extends Arguments {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbListenerArguments.class);

    private static final String ARG_INFLUXDB2_URL = "InfluxdDB2 gateway URL";
    private static final String ARG_INFLUXDB2_TOKEN = "InfluxdDB2 access token";
    private static final String ARG_INFLUXDB2_ORG = "InfluxdDB2 organization name";
    private static final String ARG_INFLUXDB2_BUCKET_TEST_META = "InfluxdDB2 bucket name for test metadata";
    private static final String ARG_INFLUXDB2_BUCKET_OPERATION_STATS
            = "InfluxdDB2 bucket name for test operations statistic";
    private static final String ARG_INFLUXDB2_BUCKET_OPERATION_META
            = "InfluxdDB2 bucket name for test operations metadata";

    private static final String ARG_TEST_ID
            = "Unique id of entire performance test. Must be the same across all JMeter instances";
    private static final String ARG_EXECUTION_ID = "Unique id of execution by this JMeter instance";

    private static final String ARG_ENVIRONMENT = "Test environment name (e.g. dev/test/prodlike/production)";
    private static final String ARG_HOSTNAME = "hostname";

    private static final String ARG_NAME = "Test name (high-level description of test scenario)";
    private static final String ARG_PROFILE = "load_profile_name";
    private static final String ARG_DETAILS = "Test environment global version/state/constraints description";
    private static final String ARG_LABELS_ADDITIONAL
            = "User-defined test labels in format 'label1:value;label2:value' without quotes";

    private static final String ARG_ALLOWED_SAMPLERS_REGEX
            = "Regular expression for filtering of allowed (reported) samplers names";
    private static final String ARG_ALLOWED_VARIABLES_REGEX
            = "Regular expression for filtering of allowed (reported) jmeter variables names";

    private static final String ARG_BATCHING_SEC = "Reporting/batching interval, seconds";
    private static final String ARG_WARMUP_SEC
            = "Warm-up interval, seconds (if defined first X seconds will be excluded from analysis and report)";

    private static final Arguments ARGUMENTS_DEFAULT = new Arguments();

    static {
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_URL, "https://host:port");
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_TOKEN, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_ORG, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_BUCKET_TEST_META, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_BUCKET_OPERATION_STATS, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_BUCKET_OPERATION_META, UNDEFINED);

        ARGUMENTS_DEFAULT.addArgument(ARG_TEST_ID, "${__UUID()}");
        ARGUMENTS_DEFAULT.addArgument(ARG_EXECUTION_ID, "${__UUID()}");

        ARGUMENTS_DEFAULT.addArgument(ARG_ENVIRONMENT, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_HOSTNAME, UNDEFINED);

        ARGUMENTS_DEFAULT.addArgument(ARG_NAME, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_PROFILE, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_DETAILS, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_LABELS_ADDITIONAL, UNDEFINED);

        ARGUMENTS_DEFAULT.addArgument(ARG_ALLOWED_SAMPLERS_REGEX, ".*");
        ARGUMENTS_DEFAULT.addArgument(ARG_ALLOWED_VARIABLES_REGEX, "EXCLUDE_EVERYTHING");
        ARGUMENTS_DEFAULT.addArgument(ARG_BATCHING_SEC, "30");
        ARGUMENTS_DEFAULT.addArgument(ARG_WARMUP_SEC, UNDEFINED);
    }

    private final URL influxDbUrl;
    private final String influxDbToken;
    private final String influxDbOrg;
    private final String influxDbBucketTestMeta;
    private final String influxDbBucketOperationStats;
    private final String influxDbBucketOperationMeta;
    private final String testId;
    private final String executionId;
    private final String environment;
    private final String hostname;
    private final String testname;
    private final String loadprofile;
    private final String details;
    private final Map<String, String> additionalLabels;
    private final Pattern samplersRegex;
    private final Pattern variablesRegex;
    private final int batchingPeriod;
    private final int warmupInterval;

    static Arguments getDefaultArguments() {
        return ARGUMENTS_DEFAULT;
    }

    InfluxDbListenerArguments(BackendListenerContext context) throws Exception {
        this.influxDbUrl = new URL(context.getParameter(ARG_INFLUXDB2_URL));
        this.influxDbToken = context.getParameter(ARG_INFLUXDB2_TOKEN);
        this.influxDbOrg = context.getParameter(ARG_INFLUXDB2_ORG);
        this.influxDbBucketTestMeta = context.getParameter(ARG_INFLUXDB2_BUCKET_TEST_META);
        this.influxDbBucketOperationStats = context.getParameter(ARG_INFLUXDB2_BUCKET_OPERATION_STATS);
        this.influxDbBucketOperationMeta = context.getParameter(ARG_INFLUXDB2_BUCKET_OPERATION_META);

        this.testId = context.getParameter(ARG_TEST_ID);
        this.executionId = context.getParameter(ARG_EXECUTION_ID);

        this.environment = context.getParameter(ARG_ENVIRONMENT);
        this.hostname = context.getParameter(ARG_HOSTNAME);

        this.testname = context.getParameter(ARG_NAME);
        this.loadprofile = context.getParameter(ARG_PROFILE);
        this.details = context.getParameter(ARG_DETAILS);
        this.additionalLabels = Utils.parseStringToMap(
                context.getParameter(InfluxDbListenerArguments.ARG_LABELS_ADDITIONAL)
        );

        this.samplersRegex = Pattern.compile(context.getParameter(ARG_ALLOWED_SAMPLERS_REGEX));
        this.variablesRegex = Pattern.compile(context.getParameter(ARG_ALLOWED_VARIABLES_REGEX));

        this.batchingPeriod = context.getIntParameter(ARG_BATCHING_SEC);
        this.warmupInterval = context.getIntParameter(ARG_WARMUP_SEC);
    }

    URL getInfluxDbUrl() {
        return this.influxDbUrl;
    }

    String getInfluxDbToken() {
        return this.influxDbToken;
    }

    String getInfluxDbOrg() {
        return this.influxDbOrg;
    }

    String getInfluxDbBucketTestMeta() {
        return this.influxDbBucketTestMeta;
    }

    String getInfluxDbBucketOperationStats() {
        return this.influxDbBucketOperationStats;
    }

    String getInfluxDbBucketOperationMeta() {
        return this.influxDbBucketOperationMeta;
    }

    String getTestId() {
        return this.testId;
    }

    String getExecutionId() {
        return this.executionId;
    }

    String getEnvironment() {
        return this.environment;
    }

    String getHostname() {
        return this.hostname;
    }

    String getTestName() {
        return this.testname;
    }

    String getTestLoadProfile() {
        return this.loadprofile;
    }

    String getTestDetails() {
        return this.details;
    }

    Map<String, String> getTestAdditionalLabels() {
        return this.additionalLabels;
    }

    Pattern getAllowedSamplersRegex() {
        return this.samplersRegex;
    }

    Pattern getAllowedVariablesRegex() {
        return this.variablesRegex;
    }

    int getBatchingPeriod() {
        return this.batchingPeriod;
    }

    int getWarmupInterval() {
        return this.warmupInterval;
    }

}
