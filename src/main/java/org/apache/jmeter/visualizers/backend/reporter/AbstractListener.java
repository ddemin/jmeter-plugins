package org.apache.jmeter.visualizers.backend.reporter;

import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.BackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationErrorsBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationMetaBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationStatisticBuffer;

import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class AbstractListener<T extends AbstractListenerArguments> implements BackendListenerClient {

    private final ConcurrentHashMap<String, Boolean> samplersWhitelistCache = new ConcurrentHashMap<>();

    private MetricsReportServiceScheduledTrigger influxScheduledTrigger;
    private OperationStatisticBuffer operationsStatisticBuffer;
    private OperationErrorsBuffer operationsErrorsBuffer;
    private OperationMetaBuffer operationsMetaBuffer;
    private Pattern samplersFilteringPattern;
    private Map<String, Object> allowedVariablesForTestMetadata;

    public abstract T getArgumentsFromContext(BackendListenerContext context) throws Exception;

    public abstract AbstractMetricsReportService setupMetricsReporterService(T arguments) throws URISyntaxException;

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        T arguments = getArgumentsFromContext(context);

        this.samplersFilteringPattern = arguments.getAllowedSamplersRegex();

        this.operationsStatisticBuffer = new OperationStatisticBuffer();
        this.operationsErrorsBuffer = new OperationErrorsBuffer();
        this.operationsMetaBuffer = new OperationMetaBuffer();
        this.allowedVariablesForTestMetadata = filterJmeterVariables(arguments);
        this.samplersWhitelistCache.clear();

        this.influxScheduledTrigger = new MetricsReportServiceScheduledTrigger(
                arguments.isItPrimary(),
                setupMetricsReporterService(arguments),
                arguments.getBatchingPeriod()
        );
        this.influxScheduledTrigger.init();
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
        this.samplersWhitelistCache.clear();
    }

    protected OperationStatisticBuffer getOperationsStatisticBuffer() {
        return operationsStatisticBuffer;
    }

    protected OperationErrorsBuffer getOperationsErrorsBuffer() {
        return operationsErrorsBuffer;
    }

    protected OperationMetaBuffer getOperationsMetaBuffer() {
        return operationsMetaBuffer;
    }

    protected Map<String, Object> getAllowedVariablesForTestMetadata() {
        return allowedVariablesForTestMetadata;
    }

    private Map<String, Object> filterJmeterVariables(AbstractListenerArguments arguments) {
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

    private Boolean isSampleResultAllowed(SampleResult result) {
        return samplersWhitelistCache.computeIfAbsent(
                result.getSampleLabel(),
                (k) -> samplersFilteringPattern.matcher(result.getSampleLabel()).find()
        );
    }

}
