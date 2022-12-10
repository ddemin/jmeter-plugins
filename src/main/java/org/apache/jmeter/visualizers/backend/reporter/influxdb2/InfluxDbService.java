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
            boolean isItPrimaryJMeter,
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
                isItPrimaryJMeter,
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

    public void retryFailedRequests() {
        influxClient.processRetryQueue();
    }

    public void sendTags(String tags, long timestampNs) {
        LOG.info("Send test tags");
        LineProtocolBuilder builder = converter.createBuilderForTags(tags, timestampNs);
        send(bucketTestMeta, builder.build());
    }

    public void sendVersions(String versions, long timestampNs) {
        LOG.info("Send versions metadata");
        LineProtocolBuilder builder = converter.createBuilderForVersions(versions, timestampNs);
        send(bucketTestMeta, builder.build());
    }

    public void sendFinishEvent(long timestampNs) {
        LOG.info("Send 'test finished' event");

        LineProtocolBuilder builder = converter.createBuilderForTestEvent(false, timestampNs);

        send(bucketTestMeta, builder.build());
    }

    public void sendStartEventAndMetadata(boolean isItPrimaryJMeter, Map<String, Object> additionalVariables, long timestampNs) {
        LOG.info("Send 'test started' event and meta data");

        LineProtocolBuilder builder = converter.createBuilderForTestEvent(true, timestampNs);
        if (isItPrimaryJMeter) {
            converter.enrichWithTestMetadata(builder, additionalVariables, timestampNs);
        }

        send(bucketTestMeta, builder.build());
    }

    @Override
    protected void packAndSendOperationsMetadata(
            long timestampNs, OperationMetaBuffer buffer, OperationErrorsBuffer errorsBuffer
    ) {
        LineProtocolBuilder lineProtocolBuilderMeta;
        // TODO Move critical section to abstract class somehow
        synchronized (buffer.getBuffer()) {
            lineProtocolBuilderMeta = converter.createBuilderForOperationsMetadata(
                    buffer.getBuffer(),
                    timestampNs
            );
            buffer.clear();
        }

        // TODO Move critical section to abstract class somehow
        synchronized (errorsBuffer.getBuffer()) {
            converter.enrichWithOperationsErrorsMetadata(
                    lineProtocolBuilderMeta,
                    errorsBuffer.getBuffer(),
                    timestampNs
            );
            errorsBuffer.clear();
        }

        if (lineProtocolBuilderMeta.getRows() > 0) {
            send(bucketOperationMeta, lineProtocolBuilderMeta.build());
        }
    }

    @Override
    protected void packAndSendOperationsStatistic(long timestampNs, OperationStatisticBuffer buffer) {
        LineProtocolBuilder lineProtocolBuilderStats;
        // TODO Move critical section to abstract class somehow
        synchronized (buffer.getBuffer()) {
            lineProtocolBuilderStats = converter.createBuilderForOperationsStatistic(
                    buffer.getBuffer(),
                    timestampNs
            );
            buffer.clear();
        }

        if (lineProtocolBuilderStats.getRows() > 0) {
            send(bucketOperationStats, lineProtocolBuilderStats.build());
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
