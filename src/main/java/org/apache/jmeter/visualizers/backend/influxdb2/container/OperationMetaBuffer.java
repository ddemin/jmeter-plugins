package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.UNDEFINED;
import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.parseStringToMap;

public class OperationMetaBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationMetaBuffer.class);
    private static final String ERROR_OBFUSCATION_REGEXP
            = "("
            + "[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+"
            + "|"
            + "[0-9]+"
            + ")";
    private static final String ERROR_OBFUSCATION_PLACEHOLDER = "x";
    private static final int ERROR_MAX_CHARS = 256;

    private final Map<String, Map<MetaTypeEnum, Map<String, String>>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public Map<String, Map<MetaTypeEnum, Map<String, String>>> getBuffer() {
        return buffer;
    }

    public void clear() {
        LOG.debug("Cleanup");
        buffer.clear();
    }

    public void putErrorMeta(SampleResult sampleResult) {
        if (sampleResult.getFirstAssertionFailureMessage() == null) {
            LOG.error("Sample is marked as failed, but assertion message is null: " + sampleResult.getSampleLabel());
        }

        getResultBucket(sampleResult)
                .putIfAbsent(MetaTypeEnum.ERROR, new ConcurrentHashMap<>())
                .put("description", getObfuscatedErrorMessage(sampleResult));
    }

    public void putLabelsMeta(SampleResult sampleResult, String labels) {
        getResultBucket(sampleResult)
                .putIfAbsent(MetaTypeEnum.LABEL, new ConcurrentHashMap<>())
                .putAll(parseStringToMap(labels));
    }

    String getObfuscatedErrorMessage(SampleResult sampleResult) {
        String errCode = sampleResult.getResponseCode();
        boolean hasDigitCode = NumberUtils.isDigits(errCode);

        return (hasDigitCode ? "Error code " + errCode + ": " : "")
                        + StringUtils.substring(
                                StringUtils.firstNonEmpty(
                                        sampleResult.getFirstAssertionFailureMessage(),
                                        sampleResult.getResponseMessage(),
                                        hasDigitCode ? "" : sampleResult.getResponseCode(),
                                        UNDEFINED
                                ),
                                0,
                                ERROR_MAX_CHARS
                        )
                        .replaceAll(ERROR_OBFUSCATION_REGEXP, ERROR_OBFUSCATION_PLACEHOLDER);
    }

    Map<MetaTypeEnum, Map<String, String>> getResultBucket(SampleResult sampleResult) {
        return buffer
                .putIfAbsent(
                        sampleResult.getSampleLabel(),
                        new ConcurrentHashMap<>()
                );
    }

}
