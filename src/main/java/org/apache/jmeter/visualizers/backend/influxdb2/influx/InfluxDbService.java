package org.apache.jmeter.visualizers.backend.influxdb2.influx;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.influxdb2.container.OperationMetaBuffer;
import org.apache.jmeter.visualizers.backend.influxdb2.lineprotocol.LineProtocolBuilder;
import org.apache.jmeter.visualizers.backend.influxdb2.lineprotocol.LineProtocolConverter;
import org.apache.jmeter.visualizers.backend.influxdb2.container.OperationStatisticBuffer;
import org.apache.jmeter.visualizers.backend.influxdb2.util.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.*;

public class InfluxDbService {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbService.class);
    private static final int PAUSE_BEFORE_LAST_BATCH_MS = 10_000;
    private static final String VERSIONS_PROPERTY_NAME = "jmeter.components.versions";
    private static final String OPERATIONS_LABELS_PROPERTY = "jmeter.operations.labels";
    private static final String DELIMITER_SAMPLERS_LABELS_KV = "=";
    private static final String DELIMITER_SAMPLERS_LABELS_ITEMS = ";";

    private final Set<String> labelsThatReported = Collections.synchronizedSet(new HashSet<>());
    private final OperationStatisticBuffer statisticBuffer;
    private final OperationMetaBuffer metaBuffer;
    private final InfluxDbHttpClient influxClient;
    private final LineProtocolConverter converter;
    private final Map<String, Object> additionalTestMetadataVariables;
    private final String bucketTestMeta;
    private final String bucketOperationStats;
    private final String bucketOperationMeta;

    private int samplersLabelsHash;
    private String componentsVersion;
    private boolean areVersionsSent;

    public InfluxDbService(
            InfluxDbHttpClient httpClient,
            LineProtocolConverter converter,
            OperationStatisticBuffer statisticBuffer,
            OperationMetaBuffer metaBuffer,
            String bucketTestMeta,
            String bucketOperationStats,
            String bucketOperationMeta,
            Map<String, Object> additionalTestMetadataVariables
    ) {
        this.statisticBuffer = statisticBuffer;
        this.metaBuffer = metaBuffer;
        this.converter = converter;
        this.influxClient = httpClient;
        this.bucketOperationMeta = bucketOperationMeta;
        this.bucketOperationStats = bucketOperationStats;
        this.bucketTestMeta = bucketTestMeta;
        this.additionalTestMetadataVariables = additionalTestMetadataVariables;
    }

    public void init() {
        if (influxClient.isConnected()) {
            LOG.info("Initialization completed");
        } else {
            LOG.error("Initialization failed. Please check the logs");
        }
        sendStartEventAndMetadata(additionalTestMetadataVariables);
    }

    public void destroy() {
        LOG.info("Terminate InfluxDB service and job ...");

        try {
            Thread.sleep(PAUSE_BEFORE_LAST_BATCH_MS);
            sendOperationsMetrics();
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage(), tr);
        }

        try {
            sendFinishEvent();
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage(), tr);
        }

        this.statisticBuffer.clear();
        this.metaBuffer.clear();
        this.labelsThatReported.clear();
        this.componentsVersion = null;
        this.areVersionsSent = false;
        this.samplersLabelsHash = 0;

        LOG.info("InfluxDB service and job have been stopped, buffer has been cleaned");
    }

    public void sendVersions(String versions) {
        LOG.info("Send versions metadata");

        LineProtocolBuilder builder = converter.createBuilderForVersions(versions);
        send(bucketTestMeta, builder.build());
    }

    void processRetryQueue() {
        influxClient.processRetryQueue();
    }

    // TODO Write the unit-test
    void collectOperationsLabels() {
        String samplersLabels = JMeterContextService.getContext().getProperties().getProperty(
               OPERATIONS_LABELS_PROPERTY,
                ""
        );

        if (samplersLabelsHash != samplersLabels.hashCode()) {
            samplersLabelsHash = samplersLabels.hashCode();

            Map<String, String> labelsMap = parseStringToMap(
                    samplersLabels, DELIMITER_SAMPLERS_LABELS_ITEMS, DELIMITER_SAMPLERS_LABELS_KV
            );
            labelsMap.forEach((key, value) -> {
                if (!labelsThatReported.contains(key)) {
                    labelsThatReported.add(key);
                    metaBuffer.putLabelsMeta(key, value);
                }
            });
        }
    }
    void sendOperationsMetrics() {
        try {
            LOG.debug("Send operations metrics");

            LineProtocolBuilder lineProtocolBuilderStats;
            // Pause any new metrics collection during batch preparation
            // Map instance == Map mutex instance (see SynchronizedMap code)
            synchronized (statisticBuffer.getBuffer()) {
                lineProtocolBuilderStats = converter.createBuilderForOperationsStatistic(statisticBuffer.getBuffer());
                statisticBuffer.clear();
            }

            LineProtocolBuilder lineProtocolBuilderMeta;
            // Pause any new metrics collection during batch preparation
            // Map instance == Map mutex instance (see SynchronizedMap code)
            synchronized (metaBuffer.getBuffer()) {
                lineProtocolBuilderMeta = converter.createBuilderForOperationsMetadata(metaBuffer.getBuffer());
                metaBuffer.clear();
            }

            if (lineProtocolBuilderStats.getRows() > 0) {
                send(bucketOperationStats, lineProtocolBuilderStats.build());
            }

            if (lineProtocolBuilderMeta.getRows() > 0) {
                send(bucketOperationMeta, lineProtocolBuilderMeta.build());
            }
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration: " + tr.getMessage(), tr);
        }
    }

    void sendFinishEvent() {
        LOG.info("Send 'test finished' event");

        LineProtocolBuilder builder = converter.createBuilderForTestMetadata(false, null);
        send(bucketTestMeta, builder.build());
    }

    void sendStartEventAndMetadata(Map<String, Object> additionalVariables) {
        LOG.info("Send 'test started' event and meta data");

        LineProtocolBuilder builder = converter.createBuilderForTestMetadata(true, additionalVariables);
        send(bucketTestMeta, builder.build());
    }

    void send(String bucket, String content) {
        try {
            influxClient.send(bucket, content);
        } catch (RetriableException e) {
            // Do nothing
        }
    }

    boolean isComponentsVersionsDefined() {
        componentsVersion = JMeterContextService.getContext().getProperties().getProperty(VERSIONS_PROPERTY_NAME);
        return StringUtils.isNotEmpty(componentsVersion) && componentsVersion.contains(DELIMITER_KEY_VALUE);
    }

    void collectAndSendVersions() {
        if (!areVersionsSent && isComponentsVersionsDefined()) {
            LOG.info(
                    "Property '" + VERSIONS_PROPERTY_NAME + "' with components versions was detected. Send versions: "
                            + componentsVersion
            );
            sendVersions(componentsVersion);
            areVersionsSent = true;
        }
    }

}
