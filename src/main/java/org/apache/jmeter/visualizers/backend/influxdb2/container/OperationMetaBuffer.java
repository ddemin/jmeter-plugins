package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

    private final Map<String, Map<MetaTypeEnum, List<Map.Entry<String, String>>>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public Map<String, Map<MetaTypeEnum, List<Map.Entry<String, String>>>> getBuffer() {
        return buffer;
    }

    public void clear() {
        LOG.debug("Cleanup");
        buffer.clear();
    }

    public void putErrorMeta(SampleResult sampleResult) {
        if (sampleResult.isSuccessful()) {
            LOG.error(
                    "Sample is reported as failed, but isSuccessful flag is TRUE. Sample will be ignored: "
                    + sampleResult.getSampleLabel()
            );
            return;
        }

        getResultBucket(sampleResult)
                .computeIfAbsent(MetaTypeEnum.ERROR,  k -> Collections.synchronizedList(new ArrayList<>()))
                .add(new AbstractMap.SimpleEntry<>("description", getObfuscatedErrorMessage(sampleResult)));
    }

    public void putLabelsMeta(String sampleName, String labels) {
        getResultBucket(sampleName)
                .computeIfAbsent(MetaTypeEnum.LABEL, k -> Collections.synchronizedList(new ArrayList<>()))
                .addAll(
                        parseStringToMap(labels).entrySet().stream().toList()
                );
    }

    public void putLabelsMeta(SampleResult sampleResult, String labels) {
        putLabelsMeta(sampleResult.getSampleLabel(), labels);
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

    Map<MetaTypeEnum, List<Map.Entry<String, String>>> getResultBucket(SampleResult sampleResult) {
        return getResultBucket(sampleResult.getSampleLabel());
    }

    Map<MetaTypeEnum, List<Map.Entry<String, String>>> getResultBucket(String sampleName) {
        return buffer
                .computeIfAbsent(
                        sampleName,
                        k -> new ConcurrentHashMap<>()
                );
    }

}
