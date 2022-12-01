package org.apache.jmeter.visualizers.backend.influxdb2;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.BackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.influxdb2.container.OperationErrorsBuffer;
import org.apache.jmeter.visualizers.backend.influxdb2.container.OperationMetaBuffer;
import org.apache.jmeter.visualizers.backend.influxdb2.container.OperationStatisticBuffer;
import org.apache.jmeter.visualizers.backend.influxdb2.influx.InfluxDbHttpClient;
import org.apache.jmeter.visualizers.backend.influxdb2.influx.InfluxDbService;
import org.apache.jmeter.visualizers.backend.influxdb2.influx.InfluxDbServiceScheduledTrigger;
import org.apache.jmeter.visualizers.backend.influxdb2.lineprotocol.LineProtocolConverter;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class InfluxDbListener implements BackendListenerClient {

    private final ConcurrentHashMap<String, Boolean> samplersWhitelistCache = new ConcurrentHashMap<>();
    private InfluxDbService influxService;
    private InfluxDbServiceScheduledTrigger influxScheduledTrigger;
    private OperationStatisticBuffer operationsStatisticBuffer;
    private OperationErrorsBuffer operationsErrorsBuffer;
    private OperationMetaBuffer operationsMetaBuffer;
    private Pattern samplersFilteringPattern;
    private String componentsVersion = "";
    private boolean areVersionsSent = false;

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        InfluxDbListenerArguments arguments = new InfluxDbListenerArguments(context);
        setupTestImpl(arguments);
    }

    @Override
    public void handleSampleResults(List<SampleResult> sampleResults, BackendListenerContext context) {
        sampleResults
                .stream()
                .filter(this::isSampleResultAllowed)
                .forEach(
                        result -> {
                            operationsStatisticBuffer.putMetric(result);
                            operationsErrorsBuffer.putMetric(result);
                        }
                );
    }

    @Override
    public void teardownTest(BackendListenerContext context) {
        this.influxScheduledTrigger.destroy();
        this.influxService.destroy();
        this.samplersWhitelistCache.clear();
    }

    @Override
    public Arguments getDefaultParameters() {
        return InfluxDbListenerArguments.getDefaultArguments();
    }

    void setupTestImpl(InfluxDbListenerArguments arguments) throws URISyntaxException {
        this.areVersionsSent = false;
        this.componentsVersion = "";

        this.samplersFilteringPattern = arguments.getAllowedSamplersRegex();

        LineProtocolConverter converter = new LineProtocolConverter(
                arguments.getTestId(),
                arguments.getExecutionId(),
                arguments.getHostname(),
                arguments.getEnvironment(),
                arguments.getTestLoadProfile(),
                arguments.getTestDetails(),
                arguments.getTestName(),
                arguments.getTestAdditionalLabels(),
                arguments.getWarmupInterval(),
                arguments.getBatchingPeriod()
        );

        InfluxDbHttpClient influxHttpClient = new InfluxDbHttpClient(
                arguments.getInfluxDbUrl(),
                arguments.getInfluxDbOrg(),
                arguments.getInfluxDbToken()
        );

        this.operationsStatisticBuffer = new OperationStatisticBuffer();
        this.operationsErrorsBuffer = new OperationErrorsBuffer();
        this.operationsMetaBuffer = new OperationMetaBuffer();
        Map<String, Object> allowedVariablesForTestMetadata = filterJmeterVariables(arguments);

        this.influxService = new InfluxDbService(
                influxHttpClient,
                converter,
                this.operationsStatisticBuffer,
                this.operationsErrorsBuffer,
                this.operationsMetaBuffer,
                arguments.getInfluxDbBucketTestMeta(),
                arguments.getInfluxDbBucketOperationStats(),
                arguments.getInfluxDbBucketOperationMeta(),
                allowedVariablesForTestMetadata
        );

        this.influxScheduledTrigger = new InfluxDbServiceScheduledTrigger(influxService, arguments.getBatchingPeriod());

        this.samplersWhitelistCache.clear();

        this.influxService.init();
        this.influxScheduledTrigger.init();
    }

    Map<String, Object> filterJmeterVariables(InfluxDbListenerArguments arguments) {
        return JMeterContextService
                .getContext()
                .getVariables()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .filter(
                        entry ->
                                arguments
                                        .getAllowedVariablesRegex()
                                        .matcher(String.valueOf(entry.getKey()))
                                        .find()
                )
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Boolean isSampleResultAllowed(SampleResult result) {
        return samplersWhitelistCache.computeIfAbsent(
                result.getSampleLabel(),
                (k) -> samplersFilteringPattern.matcher(result.getSampleLabel()).find()
        );
    }

}
