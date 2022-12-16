package org.apache.jmeter.visualizers.backend.reporter;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationErrorsBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationMetaBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationStatisticBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.*;

public abstract class AbstractMetricsReportService {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractMetricsReportService.class);

    private static final int PAUSE_BEFORE_LAST_BATCH_MS = 10_000;
    private static final String TAGS_PROPERTY_NAME = "jmeter.test.tags";
    private static final String VERSIONS_PROPERTY_NAME = "jmeter.components.versions";
    private static final String OPERATIONS_LABELS_PROPERTY = "jmeter.operations.labels";

    private final Set<String> labelsThatReported = Collections.synchronizedSet(new HashSet<>());
    private final OperationStatisticBuffer statisticBuffer;
    private final OperationErrorsBuffer errorsBuffer;
    private final OperationMetaBuffer metaBuffer;
    private final Map<String, Object> additionalTestMetadataVariables;
    private final boolean isItPrimaryJMeter;

    private int samplersLabelsHash;
    private String componentsVersion;
    private String testTags;
    private boolean areVersionsSent;
    private boolean areTagsSent;

    protected abstract void retryFailedRequests();
    protected abstract void sendStartEventAndMetadata(
           boolean isItPrimaryJMeter, Map<String, Object> additionalTestMetadataVariables, long timestampNs
    );
    protected abstract void sendFinishEvent(long timestampNs);
    protected abstract void sendOperationsMetadata(
            long timestampNs, OperationMetaBuffer buffer, OperationErrorsBuffer errorsBuffer
    );
    protected abstract void sendOperationsStatistic(long timestampNs, OperationStatisticBuffer buffer);
    protected abstract void sendTags(String testTags, long timestampNs);
    protected abstract void sendVersions(String componentsVersion, long timestampNs);

    public AbstractMetricsReportService(
            boolean isItPrimaryJMeter,
            OperationStatisticBuffer statisticBuffer,
            OperationErrorsBuffer errorsBuffer,
            OperationMetaBuffer metaBuffer,
            Map<String, Object> additionalTestMetadataVariables
    ) {
        this.isItPrimaryJMeter = isItPrimaryJMeter;
        this.statisticBuffer = statisticBuffer;
        this.errorsBuffer = errorsBuffer;
        this.metaBuffer = metaBuffer;
        this.additionalTestMetadataVariables = additionalTestMetadataVariables;
    }

    protected void init() {
        resetContext();

        sendStartEventAndMetadata(
                isItPrimaryJMeter, additionalTestMetadataVariables, toNsPrecision(System.currentTimeMillis())
        );
    }

    void destroy() {
        LOG.info("Terminate InfluxDB service and job ...");

        try {
            Thread.sleep(PAUSE_BEFORE_LAST_BATCH_MS);
            sendOperationsMetrics(toNsPrecision(System.currentTimeMillis()));
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage(), tr);
        }

        try {
            sendFinishEvent(toNsPrecision(System.currentTimeMillis()));
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage(), tr);
        }

        resetContext();

        LOG.info("InfluxDB service and job have been stopped, buffer has been cleaned");
    }

    // TODO unit test
    void collectOperationsLabels() {
        String samplersLabels = null;
        try {
            samplersLabels = JMeterContextService.getContext().getProperties().getProperty(
                    OPERATIONS_LABELS_PROPERTY,
                    ""
            );
        } catch (NullPointerException ex) {
            LOG.error(ex.getMessage(), ex);
        }

        if (samplersLabels != null && samplersLabelsHash != samplersLabels.hashCode()) {
            samplersLabelsHash = samplersLabels.hashCode();

            Map<String, Object> labelsMap = toMapWithLowerCaseKey(
                    samplersLabels, DELIMITER_SAMPLERS_LABELS_ITEMS, DELIMITER_SAMPLERS_LABELS_KV
            );
            labelsMap.forEach((key, value) -> {
                if (!labelsThatReported.contains(key)) {
                    labelsThatReported.add(key);
                    metaBuffer.putLabelsMeta(key, String.valueOf(value));
                }
            });
        }
    }

    void sendOperationsMetrics(long timestampNs) {
        try {
            sendOperationsMetadata(timestampNs, metaBuffer, errorsBuffer);

            sendOperationsStatistic(timestampNs, statisticBuffer);

        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration: " + tr.getMessage(), tr);
        }
    }

    void collectAndSendTags(long timestampNs) {
        if (!areTagsSent && isTestTagsDefined()) {
            LOG.info(
                    "Property '" + TAGS_PROPERTY_NAME + "' with test tags was detected. Send tags: "
                            + testTags
            );
            sendTags(testTags, timestampNs);
            areTagsSent = true;
        }
    }

    void resetContext() {
        this.statisticBuffer.clear();
        this.errorsBuffer.clear();
        this.metaBuffer.clear();

        this.componentsVersion = null;
        this.areVersionsSent = false;
        this.areTagsSent = false;

        this.labelsThatReported.clear();
        this.samplersLabelsHash = 0;
    }

    void collectAndSendVersions(long timestampNs) {
        if (!areVersionsSent && isComponentsVersionsDefined()) {
            LOG.info(
                    "Property '" + VERSIONS_PROPERTY_NAME + "' with components versions was detected. Send versions: "
                            + componentsVersion
            );
            sendVersions(componentsVersion, timestampNs);
            areVersionsSent = true;
        }
    }

    private boolean isTestTagsDefined() {
        try {
            testTags = JMeterContextService.getContext().getProperties().getProperty(TAGS_PROPERTY_NAME, "");
        } catch (NullPointerException ex) {
            LOG.error(ex.getMessage(), ex);
            return false;
        }
        return StringUtils.isNotEmpty(testTags);
    }

    private boolean isComponentsVersionsDefined() {
        try {
            componentsVersion = JMeterContextService.getContext().getProperties().getProperty(VERSIONS_PROPERTY_NAME, "");
        } catch (NullPointerException ex) {
            LOG.error(ex.getMessage(), ex);
            return false;
        }
        return StringUtils.isNotEmpty(componentsVersion) && componentsVersion.contains(DELIMITER_KEY_VALUE);
    }

}
