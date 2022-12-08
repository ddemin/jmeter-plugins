package org.apache.jmeter.visualizers.backend.reporter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.reporter.util.Utils;

import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.UNDEFINED;

public class AbstractListenerArguments extends Arguments {
    protected static final Arguments ARGUMENTS_DEFAULT = new Arguments();

    private static final String ARG_TEST_ID
            = "Unique id of entire performance test. Must be the same across all JMeter instances";
    private static final String ARG_EXECUTION_ID = "Unique id of execution by this JMeter instance";

    private static final String ARG_ENVIRONMENT = "Test environment name (e.g. dev/test/prodlike/production)";
    private static final String ARG_HOSTNAME = "Host name";

    private static final String ARG_NAME = "Test name (high-level description of test scenario)";
    private static final String ARG_PROFILE = "Load profile name";
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

    static {
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

    private final String testId;
    private final String executionId;
    private final String environment;
    private final String hostname;
    private final String testname;
    private final String loadprofile;
    private final String details;
    private final Map<String, Object> additionalLabels;
    private final Pattern samplersRegex;
    private final Pattern variablesRegex;
    private final int batchingPeriod;
    private final int warmupInterval;

    public AbstractListenerArguments(BackendListenerContext context) throws Exception {
        this.testId = context.getParameter(ARG_TEST_ID);
        this.executionId = context.getParameter(ARG_EXECUTION_ID);

        this.environment = context.getParameter(ARG_ENVIRONMENT);
        this.hostname = context.getParameter(ARG_HOSTNAME);

        this.testname = context.getParameter(ARG_NAME);
        this.loadprofile = context.getParameter(ARG_PROFILE);
        this.details = context.getParameter(ARG_DETAILS);
        this.additionalLabels = Utils.toMapWithLowerCaseKey(context.getParameter(ARG_LABELS_ADDITIONAL));

        this.samplersRegex = Pattern.compile(context.getParameter(ARG_ALLOWED_SAMPLERS_REGEX));
        this.variablesRegex = Pattern.compile(context.getParameter(ARG_ALLOWED_VARIABLES_REGEX));

        this.batchingPeriod = Integer.parseInt(context.getParameter(ARG_BATCHING_SEC).trim());
        this.warmupInterval = Integer.parseInt(context.getParameter(ARG_WARMUP_SEC).trim());
    }

    public String getTestId() {
        return this.testId;
    }

    public String getExecutionId() {
        return this.executionId;
    }

    public String getEnvironment() {
        return this.environment;
    }

    public String getHostname() {
        return this.hostname;
    }

    public String getTestName() {
        return this.testname;
    }

    public String getTestLoadProfile() {
        return this.loadprofile;
    }

    public String getTestDetails() {
        return this.details;
    }

    public Map<String, Object> getTestAdditionalLabels() {
        return this.additionalLabels;
    }

    Pattern getAllowedSamplersRegex() {
        return this.samplersRegex;
    }

    Pattern getAllowedVariablesRegex() {
        return this.variablesRegex;
    }

    public int getBatchingPeriod() {
        return this.batchingPeriod;
    }

    public int getWarmupInterval() {
        return this.warmupInterval;
    }
}
