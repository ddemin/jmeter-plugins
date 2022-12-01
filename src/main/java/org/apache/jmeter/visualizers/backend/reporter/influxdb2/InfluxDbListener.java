package org.apache.jmeter.visualizers.backend.reporter.influxdb2;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.reporter.AbstractListener;
import org.apache.jmeter.visualizers.backend.reporter.AbstractMetricsReportService;
import org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolConverter;

import java.net.URISyntaxException;

public class InfluxDbListener extends AbstractListener<InfluxDbListenerArguments> {

    @Override
    public Arguments getDefaultParameters() {
        return InfluxDbListenerArguments.getDefaultArguments();
    }

    @Override
    public InfluxDbListenerArguments getArgumentsFromContext(BackendListenerContext context) throws Exception {
        return new InfluxDbListenerArguments(context);
    }

    public AbstractMetricsReportService setupMetricsReporterService(InfluxDbListenerArguments arguments) throws URISyntaxException {
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

        return new InfluxDbService(
                influxHttpClient,
                converter,
                this.getOperationsStatisticBuffer(),
                this.getOperationsErrorsBuffer(),
                this.getOperationsMetaBuffer(),
                arguments.getInfluxDbBucketTestMeta(),
                arguments.getInfluxDbBucketOperationStats(),
                arguments.getInfluxDbBucketOperationMeta(),
                getAllowedVariablesForTestMetadata()
        );
    }

}
