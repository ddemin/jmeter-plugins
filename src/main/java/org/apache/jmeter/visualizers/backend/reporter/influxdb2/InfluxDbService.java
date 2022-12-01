package org.apache.jmeter.visualizers.backend.reporter.influxdb2;

import org.apache.jmeter.visualizers.backend.reporter.AbstractMetricsReportService;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationErrorsBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationMetaBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationStatisticBuffer;
import org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolBuilder;
import org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolConverter;
import org.apache.jmeter.visualizers.backend.reporter.util.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class InfluxDbService extends AbstractMetricsReportService {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbService.class);
   private final InfluxDbHttpClient influxClient;
    private final LineProtocolConverter converter;
    private final String bucketTestMeta;
    private final String bucketOperationStats;
    private final String bucketOperationMeta;

    public InfluxDbService(
            InfluxDbHttpClient httpClient,
            LineProtocolConverter converter,
            OperationStatisticBuffer statisticBuffer,
            OperationErrorsBuffer errorsBuffer,
            OperationMetaBuffer metaBuffer,
            String bucketTestMeta,
            String bucketOperationStats,
            String bucketOperationMeta,
            Map<String, Object> additionalTestMetadataVariables
    ) {
        super(
                statisticBuffer,
                errorsBuffer,
                metaBuffer,
                additionalTestMetadataVariables
        );
        this.converter = converter;
        this.influxClient = httpClient;
        this.bucketOperationMeta = bucketOperationMeta;
        this.bucketOperationStats = bucketOperationStats;
        this.bucketTestMeta = bucketTestMeta;
    }

    @Override
    public void init() {
        if (influxClient.isConnected()) {
            LOG.info("Initialization completed");
        } else {
            LOG.error("Initialization failed. Please check the logs");
        }
        super.init();
    }

    public void processRetryQueue() {
        influxClient.processRetryQueue();
    }

    public void sendVersions(String versions) {
        LOG.info("Send versions metadata");
        LineProtocolBuilder builder = converter.createBuilderForVersions(versions);
        send(bucketTestMeta, builder.build());
    }

    public void sendFinishEvent() {
        LOG.info("Send 'test finished' event");
        LineProtocolBuilder builder = converter.createBuilderForTestMetadata(false, null);
        send(bucketTestMeta, builder.build());
    }

    public void sendStartEventAndMetadata(Map<String, Object> additionalVariables) {
        LOG.info("Send 'test started' event and meta data");
        LineProtocolBuilder builder = converter.createBuilderForTestMetadata(true, additionalVariables);
        send(bucketTestMeta, builder.build());
    }

    public void packAndSendOperationsStatistic(long timestampNs) {
        LineProtocolBuilder lineProtocolBuilderStats;
        synchronized (getStatisticBuffer().getBuffer()) {
            lineProtocolBuilderStats = converter.createBuilderForOperationsStatistic(
                    getStatisticBuffer().getBuffer(),
                    timestampNs
            );
            this.getStatisticBuffer().clear();
        }

        if (lineProtocolBuilderStats.getRows() > 0) {
            send(bucketOperationStats, lineProtocolBuilderStats.build());
        }
    }

    public void packAndSendOperationsMetadata(long timestampNs) {
        LineProtocolBuilder lineProtocolBuilderMeta;
        synchronized (getMetaBuffer().getBuffer()) {
            lineProtocolBuilderMeta = converter.createBuilderForOperationsMetadata(
                    getMetaBuffer().getBuffer(),
                    timestampNs
            );
            this.getMetaBuffer().clear();
        }

        synchronized (getErrorsBuffer().getBuffer()) {
            converter.enrichWithOperationsErrorsMetadata(
                    lineProtocolBuilderMeta,
                    getErrorsBuffer().getBuffer(),
                    timestampNs
            );
            this.getErrorsBuffer().clear();
        }

        if (lineProtocolBuilderMeta.getRows() > 0) {
            send(bucketOperationMeta, lineProtocolBuilderMeta.build());
        }
    }

    void send(String bucket, String content) {
        try {
            influxClient.send(bucket, content);
        } catch (RetriableException e) {
            // Do nothing
        }
    }

}
